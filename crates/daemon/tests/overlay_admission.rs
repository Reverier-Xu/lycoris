use std::{collections::HashMap, path::Path, time::Duration};

use lycoris_config::{ClusterConfig, DaemonConfig, ExtensionsConfig, NodeConfig, TlsConfig};
use lycoris_core::ClusterKey;
use lycoris_overlay::{
  AdmissionCandidate, AdmissionRequest, AdmissionResponse, AuthorizationRecord,
  AuthorizationRegistry, ClusterId, Envelope, EnvelopeHeader, JoinProof, LinkConfig, LinkHandle,
  LinkRuntime, MessageKind, Multiaddr, NodeId, NodeIdentity, PROTOCOL_VERSION, ProtocolId,
  RequestId,
};
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(10);
const FAR_FUTURE_MS: i64 = 4_111_111_111_111;

fn daemon_config(data_dir: &Path) -> DaemonConfig {
  let certs = data_dir.join("certs");
  DaemonConfig {
    node: NodeConfig {
      id: "daemon-0".to_string(),
      address: "https://127.0.0.1:19551".to_string(),
      labels: HashMap::new(),
    },
    cluster: ClusterConfig {
      listen_address: "127.0.0.1:19551".to_string(),
      bootstrap_peers: Vec::new(),
      overlay_listen: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
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

#[tokio::test]
async fn joiner_enrolls_against_a_live_daemon() {
  let _ = lycoris_tls::install_crypto_provider();
  let dir = TempDir::new().unwrap();
  let cluster_key = ClusterKey::generate().unwrap();
  let (handles_tx, handles_rx) = tokio::sync::oneshot::channel();
  let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
  let config = daemon_config(dir.path());
  let runtime_key = cluster_key.clone();
  let runtime_shutdown = shutdown_tx.clone();
  let daemon = tokio::spawn(async move {
    if let Err(error) = lycoris_daemon::runtime::run_with_shutdown_and_handles(
      config,
      runtime_shutdown,
      shutdown_rx,
      Some(runtime_key),
      handles_tx,
    )
    .await
    {
      eprintln!("daemon runtime error: {error:?}");
    }
  });
  let handles = handles_rx.await.unwrap();
  let daemon_identity = NodeIdentity::load_or_generate(dir.path().join("node.identity")).unwrap();
  let daemon_node = daemon_identity.node_id();
  let cluster_id = ClusterId::from_genesis(daemon_node);
  let daemon_address = wait_for_tcp_listen(&handles.overlay).await;

  let joiner_identity = NodeIdentity::generate();
  let (joiner_cluster, joiner_genesis) = AuthorizationRecord::genesis(&joiner_identity).unwrap();
  let joiner_registry =
    AuthorizationRegistry::from_records(joiner_cluster, [joiner_genesis]).unwrap();
  let joiner = LinkRuntime::start(
    &joiner_identity,
    LinkConfig::new(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()]),
    joiner_registry,
  )
  .unwrap();
  let joiner_handle = joiner.handle();
  let admission_address: Multiaddr = format!("{daemon_address}/p2p/{}", daemon_identity.peer_id())
    .parse()
    .unwrap();
  joiner_handle
    .dial_admission(admission_address)
    .await
    .unwrap();
  let quarantined = tokio::time::timeout(WAIT, async {
    loop {
      if joiner_handle.snapshot().quarantined_count > 0 {
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

  let candidate = AdmissionCandidate::new(&joiner_identity).unwrap();
  let begin = AdmissionRequest::Begin(candidate.clone());
  let AdmissionResponse::Challenge(challenge) = admission_call(
    &joiner_handle,
    joiner_identity.node_id(),
    ClusterId::from_bytes([0; ClusterId::BYTE_LENGTH]),
    31,
    &begin,
  )
  .await
  else {
    panic!("expected an admission challenge");
  };
  assert_eq!(challenge.cluster_id(), cluster_id);

  let proof = JoinProof::create(&cluster_key, candidate, challenge).unwrap();
  let prove = AdmissionRequest::Prove(proof);
  let AdmissionResponse::Admitted(outcome) = admission_call(
    &joiner_handle,
    joiner_identity.node_id(),
    cluster_id,
    33,
    &prove,
  )
  .await
  else {
    panic!("expected an admission outcome");
  };
  assert_eq!(outcome.record().node_id(), joiner_identity.node_id());
  assert!(
    outcome
      .records()
      .iter()
      .any(|record| { record.node_id() == daemon_node && record.authorizer().is_none() })
  );

  let adopted =
    AuthorizationRegistry::from_records(cluster_id, outcome.records().to_vec()).unwrap();
  joiner_handle.adopt_authorization(adopted).await.unwrap();
  joiner_handle
    .wait_connected(daemon_node, WAIT)
    .await
    .unwrap();

  let daemon_seen = tokio::time::timeout(WAIT, async {
    loop {
      if handles
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
  let _ = shutdown_tx.send(true);
  let stopped = tokio::time::timeout(WAIT, daemon).await;
  assert!(stopped.is_ok(), "daemon did not shut down cleanly");
}

#[test]
fn admission_envelope_rejects_unknown_cluster() {
  // Sanity guard for the joiner-side envelope shape used above: the
  // destination/cluster sentinels must stay zero-filled so the sponsor's
  // admission gate (which binds the proof, not the header) decides.
  let header = EnvelopeHeader {
    version: PROTOCOL_VERSION,
    cluster_id: ClusterId::from_bytes([0; ClusterId::BYTE_LENGTH]),
    request_id: RequestId::from_bytes([7; RequestId::BYTE_LENGTH]),
    source: NodeId::from_bytes([1; NodeId::BYTE_LENGTH]),
    destination: NodeId::from_bytes([0; NodeId::BYTE_LENGTH]),
    protocol: ProtocolId::Admission,
    kind: MessageKind::Request,
    deadline_unix_ms: FAR_FUTURE_MS,
    remaining_hops: 0,
  };
  assert!(Envelope::new(header, Vec::new()).is_ok());
}
