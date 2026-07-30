use std::{collections::HashMap, path::Path, time::Duration};

use lycoris_client::{ClusterClient, ExtensionClient};
use lycoris_config::{ClusterConfig, DaemonConfig, ExtensionsConfig, NodeConfig, TlsConfig};
use lycoris_core::ClusterKey;
use lycoris_daemon::{overlay_transport::OverlayPool, runtime::NodeHandles};
use lycoris_overlay::{
  AdmissionCandidate, AdmissionRequest, AdmissionResponse, AuthorizationRecord,
  AuthorizationRegistry, ClusterId, Envelope, EnvelopeHeader, JoinProof, LinkConfig, LinkHandle,
  LinkRuntime, MessageKind, Multiaddr, NodeId, NodeIdentity, PROTOCOL_VERSION, ProtocolId,
  RequestId,
};
use lycoris_proto::node::{
  RegisterExtensionRequest, ResourceKind, ResourceScope as ProtoResourceScope,
};
use lycoris_storage::{DEFAULT_EMBEDDING_DIM, MemoryEntry, ResourceScope, Storage};
use tempfile::TempDir;
use tokio::sync::{oneshot, watch};

const WAIT: Duration = Duration::from_secs(10);
const FAR_FUTURE_MS: i64 = 4_111_111_111_111;

fn daemon_config(
  data_dir: &Path, control_port: u16, overlay_listen: Vec<String>, join: Option<String>,
) -> DaemonConfig {
  let certs = data_dir.join("certs");
  DaemonConfig {
    node: NodeConfig {
      id: "daemon-0".to_string(),
      address: format!("https://127.0.0.1:{control_port}"),
      labels: HashMap::new(),
    },
    cluster: ClusterConfig {
      listen_address: format!("127.0.0.1:{control_port}"),
      bootstrap_peers: Vec::new(),
      overlay_listen,
      join,
    },
    tls: TlsConfig {
      ca_cert: certs.join("ca.crt").to_string_lossy().to_string(),
      ca_key: certs.join("ca.key").to_string_lossy().to_string(),
      cert: certs.join("node.crt").to_string_lossy().to_string(),
      key: certs.join("node.key").to_string_lossy().to_string(),
    },
    data_dir: data_dir.to_string_lossy().to_string(),
    extensions: ExtensionsConfig::default(),
  }
}

struct TestDaemon {
  dir: TempDir,
  handles: NodeHandles,
  shutdown: watch::Sender<bool>,
  task: tokio::task::JoinHandle<()>,
  control_url: String,
}

impl TestDaemon {
  fn identity(&self) -> NodeIdentity {
    NodeIdentity::load_or_generate(self.dir.path().join("node.identity")).unwrap()
  }

  fn node_id(&self) -> NodeId {
    self.identity().node_id()
  }

  async fn overlay_tcp_address(&self) -> Multiaddr {
    wait_for_tcp_listen(&self.handles.overlay).await
  }

  fn tls_bundle(&self) -> lycoris_tls::TlsBundle {
    let certs = self.dir.path().join("certs");
    match lycoris_tls::load_tls_bundle(
      &certs.join("node.crt"),
      &certs.join("node.key"),
      &certs.join("ca.crt"),
    ) {
      Ok(tls) => tls,
      Err(error) => panic!("failed to load test TLS material: {error}"),
    }
  }

  async fn client(&self, cluster_key: &ClusterKey) -> ClusterClient {
    let tls = self.tls_bundle();
    let started = std::time::Instant::now();
    loop {
      match ClusterClient::connect(&self.control_url, &tls).await {
        Ok(client) => return client.with_cluster_key(cluster_key.to_hex()),
        Err(error) if started.elapsed() < WAIT => {
          let _ = error;
          tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(error) => panic!("failed to connect to {}: {error}", self.control_url),
      }
    }
  }

  async fn extension_client(&self, cluster_key: &ClusterKey) -> ExtensionClient {
    let tls = self.tls_bundle();
    let started = std::time::Instant::now();
    loop {
      match ExtensionClient::connect(&self.control_url, &tls).await {
        Ok(client) => return client.with_cluster_key(cluster_key.to_hex()),
        Err(error) if started.elapsed() < WAIT => {
          let _ = error;
          tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(error) => panic!("failed to connect to {}: {error}", self.control_url),
      }
    }
  }

  async fn stop(self) {
    let _ = self.shutdown.send(true);
    let stopped = tokio::time::timeout(WAIT, self.task).await;
    assert!(stopped.is_ok(), "daemon did not shut down cleanly");
  }
}

async fn spawn_daemon(
  dir: TempDir, overlay_listen: Vec<String>, join: Option<String>, cluster_key: &ClusterKey,
) -> TestDaemon {
  let control_port = std::net::TcpListener::bind("127.0.0.1:0")
    .and_then(|listener| listener.local_addr())
    .map(|address| address.port())
    .unwrap();
  let control_url = format!("https://127.0.0.1:{control_port}");
  let mut config = daemon_config(dir.path(), control_port, overlay_listen, join);
  if config.cluster.join.is_some() {
    config
      .node
      .labels
      .insert("role".to_string(), "runner".to_string());
  }
  let (handles_tx, handles_rx) = oneshot::channel();
  let (shutdown_tx, shutdown_rx) = watch::channel(false);
  let key = cluster_key.clone();
  let runtime_shutdown = shutdown_tx.clone();
  let task = tokio::spawn(async move {
    if let Err(error) = lycoris_daemon::runtime::run_with_shutdown_and_handles(
      config,
      runtime_shutdown,
      shutdown_rx,
      Some(key),
      handles_tx,
    )
    .await
    {
      eprintln!("daemon runtime error: {error:?}");
    }
  });
  let handles = handles_rx.await.unwrap();
  TestDaemon {
    dir,
    handles,
    shutdown: shutdown_tx,
    task,
    control_url,
  }
}

async fn wait_for_member(client: &mut ClusterClient, node_id: NodeId) {
  let observed = tokio::time::timeout(WAIT, async {
    loop {
      let resources = client
        .list_resources(
          ResourceKind::Node,
          HashMap::new(),
          ProtoResourceScope::Unspecified,
        )
        .await
        .unwrap();
      let present = resources.into_iter().any(|resource| {
        matches!(
          resource.body,
          Some(lycoris_proto::node::resource::Body::Node(
            lycoris_proto::node::NodeBody {
              node: Some(ref node),
            },
          )) if node.id == node_id.to_string()
        )
      });
      if present {
        return;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    observed.is_ok(),
    "membership did not converge within 10 seconds"
  );
}

async fn wait_for_extension_route(client: &mut ExtensionClient, expected_node: NodeId) {
  let observed = tokio::time::timeout(WAIT, async {
    loop {
      match client
        .invoke("overlay-echo", "invoke", br#"{"ok":true}"#.to_vec(), None)
        .await
      {
        Ok(response) if response.executed_by == expected_node.to_string() => return,
        Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
      }
    }
  })
  .await;
  assert!(
    observed.is_ok(),
    "extension route did not converge within 10 seconds"
  );
}

async fn wait_for_resource(client: &mut ClusterClient, id: &str) {
  let observed = tokio::time::timeout(WAIT, async {
    loop {
      match client.get_resource(ResourceKind::Memory, id).await {
        Ok(Some(_)) => return,
        Ok(None) | Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
      }
    }
  })
  .await;
  assert!(
    observed.is_ok(),
    "resource did not converge within 10 seconds"
  );
}

async fn seed_shared_memory(data_dir: &Path) -> Result<NodeId, Box<dyn std::error::Error>> {
  let identity = NodeIdentity::load_or_generate(data_dir.join("node.identity"))?;
  let content = b"overlay resource".to_vec();
  let storage = Storage::open(data_dir.join("lycoris.redb"))?;
  storage
    .agent()
    .memory()
    .store(&MemoryEntry {
      id: "overlay-memory".to_string(),
      content_hash: MemoryEntry::compute_content_hash(&content),
      content,
      embedding: vec![0.0; DEFAULT_EMBEDDING_DIM],
      metadata: HashMap::new(),
      scope: ResourceScope::ClusterShared,
      source_node_id: Some(identity.node_id().to_string()),
      created_at_ms: 1,
      updated_at_ms: 1,
      version: 1,
    })
    .await?;
  Ok(identity.node_id())
}

async fn wait_for_tcp_listen(handle: &LinkHandle) -> Multiaddr {
  let mut snapshots = handle.subscribe();
  let observed = tokio::time::timeout(WAIT, async {
    loop {
      if let Some(address) = snapshots
        .borrow()
        .listen_addresses
        .iter()
        .find(|address| {
          let text = address.to_string();
          text.contains("/tcp/") && !text.ends_with("/tcp/0")
        })
        .cloned()
      {
        return address;
      }
      if snapshots.changed().await.is_err() {
        panic!("overlay snapshots closed before a listen address appeared");
      }
    }
  })
  .await;
  match observed {
    Ok(address) => address,
    Err(_) => panic!("overlay listen address did not become ready"),
  }
}

async fn admission_call(
  handle: &LinkHandle, source: NodeId, cluster_id: ClusterId, nonce: u8, request: &AdmissionRequest,
) -> AdmissionResponse {
  let payload = postcard::to_stdvec(request).unwrap();
  let header = EnvelopeHeader {
    version: PROTOCOL_VERSION,
    cluster_id,
    request_id: RequestId::from_bytes([nonce; RequestId::BYTE_LENGTH]),
    source,
    destination: NodeId::from_bytes([0; NodeId::BYTE_LENGTH]),
    protocol: ProtocolId::Admission,
    kind: MessageKind::Request,
    deadline_unix_ms: FAR_FUTURE_MS,
    remaining_hops: 0,
  };
  let envelope = Envelope::new(header, payload).unwrap();
  let response = handle.request(envelope).await.unwrap();
  postcard::from_bytes(response.payload()).unwrap()
}

fn start_client(identity: &NodeIdentity) -> LinkRuntime {
  let (cluster_id, genesis) = AuthorizationRecord::genesis(identity).unwrap();
  let registry = AuthorizationRegistry::from_records(cluster_id, [genesis]).unwrap();
  LinkRuntime::start(
    identity,
    LinkConfig::new(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()]),
    registry,
  )
  .unwrap()
}

/// Drive the full join-key enrollment of a test-side client against a sponsor
/// address, adopting the sponsor's registry checkpoint. Returns the cluster id.
async fn enroll_client(
  handle: &LinkHandle, identity: &NodeIdentity, sponsor_address: Multiaddr,
  cluster_key: &ClusterKey,
) -> ClusterId {
  handle.dial_admission(sponsor_address).await.unwrap();
  let quarantined = tokio::time::timeout(WAIT, async {
    loop {
      if handle.snapshot().quarantined_count > 0 {
        return;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    quarantined.is_ok(),
    "admission connection was not quarantined"
  );

  let candidate = AdmissionCandidate::new(identity).unwrap();
  let begin = AdmissionRequest::Begin(candidate.clone());
  let AdmissionResponse::Challenge(challenge) = admission_call(
    handle,
    identity.node_id(),
    ClusterId::from_bytes([0; 32]),
    31,
    &begin,
  )
  .await
  else {
    panic!("expected an admission challenge");
  };
  let proof = JoinProof::create(cluster_key, candidate, challenge.clone()).unwrap();
  let prove = AdmissionRequest::Prove(proof);
  let AdmissionResponse::Admitted(outcome) = admission_call(
    handle,
    identity.node_id(),
    challenge.cluster_id(),
    33,
    &prove,
  )
  .await
  else {
    panic!("expected an admission outcome");
  };
  assert_eq!(outcome.record().node_id(), identity.node_id());
  let adopted =
    AuthorizationRegistry::from_records(challenge.cluster_id(), outcome.records().to_vec())
      .unwrap();
  handle.adopt_authorization(adopted).await.unwrap();
  challenge.cluster_id()
}

#[tokio::test]
async fn joiner_enrolls_against_a_live_daemon() {
  let _ = lycoris_tls::install_crypto_provider();
  let cluster_key = ClusterKey::generate().unwrap();
  let daemon = spawn_daemon(
    TempDir::new().unwrap(),
    vec!["/ip4/127.0.0.1/tcp/0".to_string()],
    None,
    &cluster_key,
  )
  .await;
  let daemon_node = daemon.node_id();
  let cluster_id = ClusterId::from_genesis(daemon_node);
  let daemon_address = daemon.overlay_tcp_address().await;

  let joiner_identity = NodeIdentity::generate();
  let joiner = start_client(&joiner_identity);
  let joiner_handle = joiner.handle();
  let admission_address: Multiaddr =
    format!("{daemon_address}/p2p/{}", daemon.identity().peer_id())
      .parse()
      .unwrap();
  let adopted_cluster = enroll_client(
    &joiner_handle,
    &joiner_identity,
    admission_address,
    &cluster_key,
  )
  .await;
  assert_eq!(adopted_cluster, cluster_id);

  joiner_handle
    .wait_connected(daemon_node, WAIT)
    .await
    .unwrap();
  let daemon_seen = tokio::time::timeout(WAIT, async {
    loop {
      if daemon
        .handles
        .overlay
        .snapshot()
        .connected_nodes
        .contains(&joiner_identity.node_id())
      {
        return;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(daemon_seen.is_ok(), "daemon never saw the admitted joiner");

  joiner.shutdown().await.unwrap();
  daemon.stop().await;
}

#[tokio::test]
async fn membership_requests_ride_the_overlay() {
  let _ = lycoris_tls::install_crypto_provider();
  let cluster_key = ClusterKey::generate().unwrap();
  let daemon = spawn_daemon(
    TempDir::new().unwrap(),
    vec!["/ip4/127.0.0.1/tcp/0".to_string()],
    None,
    &cluster_key,
  )
  .await;
  let daemon_node = daemon.node_id();
  let daemon_address = daemon.overlay_tcp_address().await;

  let client_identity = NodeIdentity::generate();
  let client = start_client(&client_identity);
  let client_handle = client.handle();
  let admission_address: Multiaddr =
    format!("{daemon_address}/p2p/{}", daemon.identity().peer_id())
      .parse()
      .unwrap();
  let cluster_id = enroll_client(
    &client_handle,
    &client_identity,
    admission_address,
    &cluster_key,
  )
  .await;

  let pool = OverlayPool::new(client_handle.clone(), client_identity.node_id(), cluster_id);
  let mut peer = pool.connect(daemon_node);

  let probe = peer.probe(7, "").await.unwrap();
  assert!(probe.ack);

  let root = peer.merkle_root().await.unwrap();
  assert_eq!(root.len(), 32);

  let snapshot = peer.sync_nodes(Vec::new()).await.unwrap();
  assert_eq!(snapshot.nodes.len(), 1);

  let external = lycoris_proto::node::NodeInfo {
    id: "external-node".to_string(),
    address: "https://127.0.0.1:9".to_string(),
    ..Default::default()
  };
  peer
    .push_node(external, "external-node".to_string(), 1)
    .await
    .unwrap();
  let fetched = peer
    .fetch_registers(vec!["external-node".to_string()])
    .await
    .unwrap();
  assert!(fetched.iter().any(|info| info.id == "external-node"));

  let merkle = peer
    .merkle_nodes(lycoris_proto::node::MerkleNodesRequest { nodes: vec![] })
    .await
    .unwrap();
  assert!(merkle.results.is_empty());

  let pushed = lycoris_proto::node::NodeInfo {
    id: "second-external".to_string(),
    address: "https://127.0.0.1:10".to_string(),
    ..Default::default()
  };
  peer.push_registers(vec![pushed]).await.unwrap();
  let fetched = peer
    .fetch_registers(vec!["second-external".to_string()])
    .await
    .unwrap();
  assert!(fetched.iter().any(|info| info.id == "second-external"));

  let state = peer
    .state(lycoris_proto::node::StateMessage { payload: None })
    .await
    .unwrap();
  assert!(state.accepted);

  client.shutdown().await.unwrap();
  daemon.stop().await;
}

#[tokio::test]
async fn daemon_joins_an_existing_cluster_on_startup() {
  let _ = lycoris_tls::install_crypto_provider();
  let cluster_key = ClusterKey::generate().unwrap();
  let daemon_a_dir = TempDir::new().unwrap();
  let seeded = seed_shared_memory(daemon_a_dir.path()).await;
  assert!(seeded.is_ok(), "failed to seed the shared resource");
  let daemon_a = spawn_daemon(
    daemon_a_dir,
    vec!["/ip4/127.0.0.1/tcp/0".to_string()],
    None,
    &cluster_key,
  )
  .await;
  let a_node = daemon_a.node_id();
  let cluster_id = ClusterId::from_genesis(a_node);
  let a_address = daemon_a.overlay_tcp_address().await;
  let a_peer = daemon_a.identity().peer_id();

  let daemon_b = spawn_daemon(
    TempDir::new().unwrap(),
    vec!["/ip4/127.0.0.1/tcp/0".to_string()],
    Some(format!("{a_address}/p2p/{a_peer}")),
    &cluster_key,
  )
  .await;
  let b_node = daemon_b.node_id();

  daemon_b
    .handles
    .overlay
    .wait_connected(a_node, WAIT)
    .await
    .unwrap();
  let a_seen = tokio::time::timeout(WAIT, async {
    loop {
      if daemon_a
        .handles
        .overlay
        .snapshot()
        .connected_nodes
        .contains(&b_node)
      {
        return;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(a_seen.is_ok(), "sponsor never saw the joining daemon");

  // The daemon-owned periodic sync tasks, not this test, must exchange the
  // two local membership registers through the overlay within the hard
  // convergence budget.
  let mut a_client = daemon_a.client(&cluster_key).await;
  let mut b_client = daemon_b.client(&cluster_key).await;
  wait_for_member(&mut a_client, b_node).await;
  wait_for_member(&mut b_client, a_node).await;
  wait_for_resource(&mut b_client, "overlay-memory").await;

  let mut extension_client = daemon_a.extension_client(&cluster_key).await;
  let registered = extension_client
    .register(RegisterExtensionRequest {
      id: "overlay-echo".to_string(),
      name: "overlay echo".to_string(),
      version: 1,
      engine: "lua".to_string(),
      entry: "invoke".to_string(),
      artifact: b"function invoke(method, payload) return payload end".to_vec(),
      manifest: HashMap::from([
        ("semver".to_string(), "1.0.0".to_string()),
        ("selector".to_string(), r#"{"role":"runner"}"#.to_string()),
      ]),
      labels: HashMap::new(),
    })
    .await;
  assert!(registered.is_ok(), "failed to register overlay extension");
  wait_for_extension_route(&mut extension_client, b_node).await;

  // A second pool sharing the daemon handle proves request-id allocation is
  // runtime-wide rather than local to one protocol adapter.
  let pool = OverlayPool::new(daemon_b.handles.overlay.clone(), b_node, cluster_id);
  let mut peer = pool.connect(a_node);
  assert!(peer.probe(3, "").await.unwrap().ack);

  daemon_b.stop().await;
  daemon_a.stop().await;
}
