use std::{
  collections::HashMap,
  path::PathBuf,
  sync::atomic::{AtomicU16, Ordering},
  time::Duration,
};

use lycoris_client::{ClusterClient, PeerClient};
use lycoris_config::{ClusterConfig, DaemonConfig, NodeConfig, TlsConfig};
use lycoris_core::{ClusterKey, now_ms};
use lycoris_proto::node::{NodeInfo, ResourceKind, ResourceScope as ProtoResourceScope};
use lycoris_storage::{
  DEFAULT_EMBEDDING_DIM, MemoryEntry, ResourceScope, SkillRecord, Storage, VersionedContentStore,
  WorkspaceRecord,
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use tempfile::TempDir;
use tokio::time;

static NEXT_BASE_PORT: AtomicU16 = AtomicU16::new(56000);

fn alloc_base_port() -> u16 {
  // each test reserves a 100-port block so internal and external addresses cannot
  // collide
  NEXT_BASE_PORT.fetch_add(100, Ordering::SeqCst)
}

async fn wait_for_node(client: &mut ClusterClient, node_id: &str, timeout: Duration) {
  let start = std::time::Instant::now();
  loop {
    let ids = list_node_ids(client).await;
    if ids.iter().any(|id| id == node_id) {
      return;
    }
    if start.elapsed() >= timeout {
      panic!("timed out waiting for {node_id} to appear in membership");
    }
    time::sleep(Duration::from_millis(100)).await;
  }
}

async fn list_node_ids(client: &mut ClusterClient) -> Vec<String> {
  client
    .list_resources(
      ResourceKind::Node,
      HashMap::new(),
      ProtoResourceScope::Unspecified,
    )
    .await
    .expect("list resources failed")
    .into_iter()
    .filter_map(|resource| match resource.body {
      Some(lycoris_proto::node::resource::Body::Node(lycoris_proto::node::NodeBody {
        node: Some(node),
      })) => Some(node.id),
      _ => None,
    })
    .collect()
}

fn generate_test_certs(
  node_count: usize,
) -> (TempDir, PathBuf, PathBuf, Vec<PathBuf>, Vec<PathBuf>) {
  let dir = TempDir::new().unwrap();

  let ca_key = KeyPair::generate().unwrap();
  let mut ca_params = CertificateParams::new(vec!["lycoris-test-ca".to_string()]).unwrap();
  ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
  let ca_cert = ca_params.self_signed(&ca_key).unwrap();

  let ca_cert_path = dir.path().join("ca.crt");
  let ca_key_path = dir.path().join("ca.key");
  std::fs::write(&ca_cert_path, ca_cert.pem()).unwrap();
  std::fs::write(&ca_key_path, ca_key.serialize_pem()).unwrap();

  let mut cert_paths = Vec::with_capacity(node_count);
  let mut key_paths = Vec::with_capacity(node_count);

  for i in 0..node_count {
    let key = KeyPair::generate().unwrap();
    let params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    let cert = params.signed_by(&key, &ca_cert, &ca_key).unwrap();

    let cert_path = dir.path().join(format!("node{i}.crt"));
    let key_path = dir.path().join(format!("node{i}.key"));
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();

    cert_paths.push(cert_path);
    key_paths.push(key_path);
  }

  (dir, ca_cert_path, ca_key_path, cert_paths, key_paths)
}

fn client_tls_bundle(
  cert_path: &std::path::Path, key_path: &std::path::Path, ca_path: &std::path::Path,
) -> lycoris_tls::TlsBundle {
  lycoris_tls::load_tls_bundle(cert_path, key_path, ca_path).unwrap()
}

/// Connect to a freshly spawned node, retrying until its listener is up.
///
/// A fixed startup sleep before connecting is exactly the kind of timing
/// assumption that flakes under parallel test load, so every client goes
/// through this retry loop instead.
async fn connect_client(url: &str, tls: &lycoris_tls::TlsBundle, key_hex: &str) -> ClusterClient {
  let start = std::time::Instant::now();
  loop {
    match ClusterClient::connect(url, tls).await {
      Ok(client) => return client.with_cluster_key(key_hex.to_string()),
      Err(error) => {
        if start.elapsed() >= Duration::from_secs(10) {
          panic!("failed to connect to {url}: {error}");
        }
        time::sleep(Duration::from_millis(100)).await;
      }
    }
  }
}

fn spawn_runtime(config: DaemonConfig, key: ClusterKey) {
  tokio::spawn(async move {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    if let Err(e) =
      lycoris_daemon::runtime::run_with_shutdown(config, shutdown_tx, shutdown_rx, Some(key)).await
    {
      eprintln!("runtime error: {e:?}");
    }
  });
}

#[allow(clippy::too_many_arguments)]
fn build_config(
  id: &str, listen_port: u16, bootstrap_peers: Vec<String>, data_dir: PathBuf,
  ca_cert_path: &std::path::Path, ca_key_path: &std::path::Path, cert_path: &std::path::Path,
  key_path: &std::path::Path,
) -> DaemonConfig {
  DaemonConfig {
    node: NodeConfig {
      id: id.to_string(),
      address: format!("https://127.0.0.1:{listen_port}"),
    },
    cluster: ClusterConfig {
      listen_address: format!("127.0.0.1:{listen_port}"),
      bootstrap_peers,
    },
    tls: TlsConfig {
      ca_cert: ca_cert_path.to_string_lossy().to_string(),
      ca_key: ca_key_path.to_string_lossy().to_string(),
      cert: cert_path.to_string_lossy().to_string(),
      key: key_path.to_string_lossy().to_string(),
    },
    data_dir: data_dir.to_string_lossy().to_string(),
  }
}

#[tokio::test]
async fn registry_converges_across_three_node_chain() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (3, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);

  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peers = match i {
        0 => vec![format!("https://127.0.0.1:{}", base_port + 1)],
        1 => vec![
          format!("https://127.0.0.1:{}", base_port),
          format!("https://127.0.0.1:{}", base_port + 2),
        ],
        2 => vec![format!("https://127.0.0.1:{}", base_port + 1)],
        _ => unreachable!(),
      };
      build_config(
        &format!("node-{i}"),
        port,
        peers,
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  // Register an external node through node-0.
  let client_tls = client_tls_bundle(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let node0_url = format!("https://127.0.0.1:{base_port}");
  let mut client = connect_client(&node0_url, &client_tls, &key_hex).await;

  let external_dir = TempDir::new().unwrap();
  let _external_storage =
    Storage::open(external_dir.path().join("external.redb")).expect("open external storage");
  let external = NodeInfo::new(
    "external-node".to_string(),
    format!("127.0.0.1:{}", base_port + 99),
    HashMap::new(),
    HashMap::new(),
  );
  client.register(external).await.expect("register failed");

  // Poll node-2 (the far end of the chain) until push + anti-entropy deliver
  // every node, instead of assuming a fixed propagation window. The external
  // node's gossip may overtake node-0's own register on the chain, so each id
  // gets its own wait.
  let client_tls = client_tls_bundle(&cert_paths[2], &key_paths[2], &ca_cert_path);
  let node2_url = format!("https://127.0.0.1:{}", base_port + 2);
  let mut client = connect_client(&node2_url, &client_tls, &key_hex).await;
  for id in ["external-node", "node-0", "node-1", "node-2"] {
    wait_for_node(&mut client, id, Duration::from_secs(30)).await;
  }

  let ids = list_node_ids(&mut client).await;

  assert!(ids.contains(&"external-node".to_string()));
  assert!(ids.contains(&"node-0".to_string()));
  assert!(ids.contains(&"node-1".to_string()));
  assert!(ids.contains(&"node-2".to_string()));
}

#[tokio::test]
async fn primary_failure_falls_back_and_promotes() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peer = format!("https://127.0.0.1:{}", base_port + ((i + 1) % 2) as u16);
      build_config(
        &format!("node-{i}"),
        port,
        vec![peer],
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  let (node0_shutdown_tx, node0_shutdown_rx) = tokio::sync::watch::channel(false);

  let mut handles = Vec::new();
  for (i, config) in configs.clone().into_iter().enumerate() {
    if i == 0 {
      let shutdown_rx = node0_shutdown_rx.clone();
      let shutdown_tx = node0_shutdown_tx.clone();
      let key = cluster_key.clone();
      handles.push(tokio::spawn(async move {
        if let Err(e) =
          lycoris_daemon::runtime::run_with_shutdown(config, shutdown_tx, shutdown_rx, Some(key))
            .await
        {
          eprintln!("runtime error: {e:?}");
        }
      }));
    } else {
      let key = cluster_key.clone();
      handles.push(tokio::spawn(async move {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        if let Err(e) =
          lycoris_daemon::runtime::run_with_shutdown(config, shutdown_tx, shutdown_rx, Some(key))
            .await
        {
          eprintln!("runtime error: {e:?}");
        }
      }));
    }
  }

  // Point node-0's primary at an unreachable address.
  let node0_url = format!("https://127.0.0.1:{base_port}");
  let client_tls = client_tls_bundle(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let mut client = connect_client(&node0_url, &client_tls, &key_hex).await;
  client
    .set_primary_endpoint("https://127.0.0.1:1")
    .await
    .expect("set_primary_endpoint failed");

  // Register an external node through node-0. The push/sync must use the
  // fallback (node-1) because the primary is unreachable.
  let external_dir = TempDir::new().unwrap();
  let _external_storage =
    Storage::open(external_dir.path().join("external.redb")).expect("open external storage");
  let external = NodeInfo::new(
    "fallback-external-node".to_string(),
    format!("127.0.0.1:{}", base_port + 99),
    HashMap::new(),
    HashMap::new(),
  );
  client.register(external).await.expect("register failed");

  // Poll node-1 until the fallback sync delivers the external node.
  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let mut client = connect_client(&node1_url, &client_tls, &key_hex).await;
  wait_for_node(
    &mut client,
    "fallback-external-node",
    Duration::from_secs(30),
  )
  .await;

  // Stop node-0 so that its redb database is closed and can be reopened.
  node0_shutdown_tx
    .send(true)
    .expect("signal node-0 shutdown");
  let _ = handles.remove(0).await;
  time::sleep(Duration::from_millis(300)).await;

  // Verify node-0 promoted the reachable fallback to primary in storage.
  let node0_storage =
    Storage::open(data_dirs[0].path().join("lycoris.redb")).expect("reopen node-0 storage");
  let primary = node0_storage
    .node()
    .peers()
    .get_primary()
    .expect("read primary")
    .expect("primary should be set after successful fallback sync");
  assert_eq!(primary, node1_url);
}

#[tokio::test]
async fn partition_merge_reconciles_bidirectional_membership() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peer = format!("https://127.0.0.1:{}", base_port + ((i + 1) % 2) as u16);
      build_config(
        &format!("node-{i}"),
        port,
        vec![peer],
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  let node0_url = format!("https://127.0.0.1:{base_port}");
  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_bundle(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let mut client = connect_client(&node0_url, &client_tls, &key_hex).await;

  let alpha_dir = TempDir::new().unwrap();
  let _alpha_storage =
    Storage::open(alpha_dir.path().join("alpha.redb")).expect("open alpha storage");
  let alpha = NodeInfo::new(
    "alpha".to_string(),
    format!("127.0.0.1:{}", base_port + 98),
    HashMap::new(),
    HashMap::new(),
  );
  client.register(alpha).await.expect("register alpha failed");

  let mut client = connect_client(&node1_url, &client_tls, &key_hex).await;

  let beta_dir = TempDir::new().unwrap();
  let _beta_storage = Storage::open(beta_dir.path().join("beta.redb")).expect("open beta storage");
  let beta = NodeInfo::new(
    "beta".to_string(),
    format!("127.0.0.1:{}", base_port + 97),
    HashMap::new(),
    HashMap::new(),
  );
  client.register(beta).await.expect("register beta failed");

  for url in [&node0_url, &node1_url] {
    let mut client = connect_client(url, &client_tls, &key_hex).await;
    // Gossip for alpha/beta may overtake the peer's own register, so each id
    // gets its own wait.
    for id in ["alpha", "beta", "node-0", "node-1"] {
      wait_for_node(&mut client, id, Duration::from_secs(30)).await;
    }
    let ids = list_node_ids(&mut client).await;
    assert!(ids.contains(&"alpha".to_string()));
    assert!(ids.contains(&"beta".to_string()));
    assert!(ids.contains(&"node-0".to_string()));
    assert!(ids.contains(&"node-1".to_string()));
  }
}

#[tokio::test]
async fn failure_detector_marks_unresponsive_peer() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peer = format!("https://127.0.0.1:{}", base_port + ((i + 1) % 2) as u16);
      build_config(
        &format!("node-{i}"),
        port,
        vec![peer],
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  let mut handles = Vec::new();
  for config in configs.clone() {
    let key = cluster_key.clone();
    handles.push(tokio::spawn(async move {
      let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
      if let Err(e) =
        lycoris_daemon::runtime::run_with_shutdown(config, shutdown_tx, shutdown_rx, Some(key))
          .await
      {
        eprintln!("runtime error: {e:?}");
      }
    }));
  }

  let node0_url = format!("https://127.0.0.1:{base_port}");
  let _node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_bundle(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let mut client = connect_client(&node0_url, &client_tls, &key_hex).await;

  wait_for_node(&mut client, "node-1", Duration::from_secs(30)).await;

  // Stop node-1 so that node-0's SWIM probes begin to fail.
  handles[1].abort();

  // Poll the suspicion state instead of sleeping a fixed window: three
  // consecutive SWIM probe timeouts (1s interval + 3s timeout each, ~11s)
  // drift under parallel load, so check every 500ms with a 30s budget.
  let mut peer = PeerClient::connect(&node0_url, &client_tls)
    .await
    .expect("failed to connect membership client");
  let start = std::time::Instant::now();
  loop {
    let registers = peer
      .membership
      .fetch_registers(vec!["node-1".to_string()])
      .await
      .expect("fetch_registers failed");
    if registers
      .iter()
      .any(|register| register.state == lycoris_proto::node::NodeState::Suspected as i32)
    {
      break;
    }
    if start.elapsed() >= Duration::from_secs(30) {
      panic!("timed out waiting for node-1 to be suspected: {registers:?}");
    }
    time::sleep(Duration::from_millis(500)).await;
  }
}

#[tokio::test]
async fn shared_skills_replicate_via_anti_entropy() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (3, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  {
    let storage =
      Storage::open(data_dirs[0].path().join("lycoris.redb")).expect("open node-0 storage");
    let content = "skill content";
    let content_hash = blake3::hash(content.as_bytes()).to_hex().to_string();
    let skill = SkillRecord {
      id: "shared-skill".to_string(),
      name: "shared skill".to_string(),
      version: 1,
      content_hash: content_hash.clone(),
      scope: ResourceScope::ClusterShared,
      source_node_id: Some("node-0".to_string()),
      created_at_ms: 1_000,
      updated_at_ms: 2_000,
      metadata: Default::default(),
    };
    storage
      .workspace()
      .skills()
      .upsert(&skill)
      .expect("upsert shared skill");
    storage
      .workspace()
      .skill_content()
      .write("shared-skill", content, &content_hash)
      .expect("write shared skill content");
  }

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peers = match i {
        0 => vec![format!("https://127.0.0.1:{}", base_port + 1)],
        1 => vec![
          format!("https://127.0.0.1:{}", base_port),
          format!("https://127.0.0.1:{}", base_port + 2),
        ],
        2 => vec![format!("https://127.0.0.1:{}", base_port + 1)],
        _ => unreachable!(),
      };
      build_config(
        &format!("node-{i}"),
        port,
        peers,
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  let node2_url = format!("https://127.0.0.1:{}", base_port + 2);
  let client_tls = client_tls_bundle(&cert_paths[2], &key_paths[2], &ca_cert_path);
  let mut client = connect_client(&node2_url, &client_tls, &key_hex).await;

  let start = std::time::Instant::now();
  let replicated = loop {
    let resources = client
      .list_resources(
        ResourceKind::Skill,
        HashMap::new(),
        ProtoResourceScope::Unspecified,
      )
      .await
      .expect("list skills failed");
    let found = resources.into_iter().find(|resource| {
      resource
        .metadata
        .as_ref()
        .map(|metadata| metadata.id == "shared-skill")
        .unwrap_or(false)
    });
    if let Some(resource) = found {
      break resource;
    }
    if start.elapsed() >= Duration::from_secs(30) {
      panic!("timed out waiting for shared skill to replicate to node-2");
    }
    time::sleep(Duration::from_millis(200)).await;
  };

  // The stored timestamps must reach the wire verbatim: created_at_ms is the
  // origin's creation time, not a copy of updated_at_ms.
  let metadata = replicated.metadata.expect("replicated skill metadata");
  assert_eq!(metadata.created_at_ms, 1_000);
  assert_eq!(metadata.updated_at_ms, 2_000);
}

async fn wait_for_resource(
  client: &mut ClusterClient, kind: ResourceKind, id: &str, timeout: Duration,
) {
  let start = std::time::Instant::now();
  loop {
    let resources = client
      .list_resources(kind, HashMap::new(), ProtoResourceScope::Unspecified)
      .await
      .expect("list resources failed");
    if resources.iter().any(|resource| {
      resource
        .metadata
        .as_ref()
        .map(|metadata| metadata.id == id)
        .unwrap_or(false)
    }) {
      return;
    }
    if start.elapsed() >= timeout {
      panic!("timed out waiting for {id} to appear in {kind:?}");
    }
    time::sleep(Duration::from_millis(200)).await;
  }
}

fn memory_entry(
  id: &str, embedding: Vec<f32>, scope: ResourceScope, updated_at_ms: i64,
) -> MemoryEntry {
  let content = id.as_bytes().to_vec();
  let content_hash = MemoryEntry::compute_content_hash(&content);
  MemoryEntry {
    id: id.to_string(),
    content,
    embedding,
    metadata: [("source".to_string(), "integration-test".to_string())]
      .into_iter()
      .collect(),
    scope,
    source_node_id: Some("node-0".to_string()),
    created_at_ms: updated_at_ms,
    updated_at_ms,
    content_hash,
    version: updated_at_ms as u64,
  }
}

fn workspace_record(id: &str, scope: ResourceScope, updated_at_ms: i64) -> WorkspaceRecord {
  WorkspaceRecord {
    id: id.to_string(),
    root: PathBuf::from(format!("/tmp/{id}")),
    session_ids: vec![],
    metadata: [("project".to_string(), "lycoris".to_string())]
      .into_iter()
      .collect(),
    scope,
    source_node_id: Some("node-0".to_string()),
    version: 1,
    content_hash: String::new(),
    created_at_ms: updated_at_ms,
    updated_at_ms,
  }
}

#[tokio::test]
async fn shared_memories_replicate_via_anti_entropy() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (3, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  {
    let storage =
      Storage::open(data_dirs[0].path().join("lycoris.redb")).expect("open node-0 storage");
    let dim = DEFAULT_EMBEDDING_DIM;
    let embedding = (0..dim).map(|i| i as f32 * 0.01).collect();
    let memory = memory_entry(
      "shared-memory",
      embedding,
      ResourceScope::ClusterShared,
      now_ms(),
    );
    storage
      .agent()
      .memory()
      .store(&memory)
      .await
      .expect("store shared memory");
  }

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peers = match i {
        0 => vec![format!("https://127.0.0.1:{}", base_port + 1)],
        1 => vec![
          format!("https://127.0.0.1:{}", base_port),
          format!("https://127.0.0.1:{}", base_port + 2),
        ],
        2 => vec![format!("https://127.0.0.1:{}", base_port + 1)],
        _ => unreachable!(),
      };
      build_config(
        &format!("node-{i}"),
        port,
        peers,
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  let node2_url = format!("https://127.0.0.1:{}", base_port + 2);
  let client_tls = client_tls_bundle(&cert_paths[2], &key_paths[2], &ca_cert_path);
  let mut client = connect_client(&node2_url, &client_tls, &key_hex).await;

  wait_for_resource(
    &mut client,
    ResourceKind::Memory,
    "shared-memory",
    Duration::from_secs(30),
  )
  .await;
}

#[tokio::test]
async fn shared_workspaces_replicate_via_anti_entropy() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (3, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  {
    let storage =
      Storage::open(data_dirs[0].path().join("lycoris.redb")).expect("open node-0 storage");
    let workspace = workspace_record("shared-workspace", ResourceScope::ClusterShared, now_ms());
    storage
      .workspace()
      .workspaces()
      .upsert(&workspace)
      .expect("upsert shared workspace");
  }

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peers = match i {
        0 => vec![format!("https://127.0.0.1:{}", base_port + 1)],
        1 => vec![
          format!("https://127.0.0.1:{}", base_port),
          format!("https://127.0.0.1:{}", base_port + 2),
        ],
        2 => vec![format!("https://127.0.0.1:{}", base_port + 1)],
        _ => unreachable!(),
      };
      build_config(
        &format!("node-{i}"),
        port,
        peers,
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  let node2_url = format!("https://127.0.0.1:{}", base_port + 2);
  let client_tls = client_tls_bundle(&cert_paths[2], &key_paths[2], &ca_cert_path);
  let mut client = connect_client(&node2_url, &client_tls, &key_hex).await;

  wait_for_resource(
    &mut client,
    ResourceKind::Workspace,
    "shared-workspace",
    Duration::from_secs(30),
  )
  .await;
}

#[tokio::test]
async fn local_resources_do_not_replicate() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  {
    let storage =
      Storage::open(data_dirs[0].path().join("lycoris.redb")).expect("open node-0 storage");

    let dim = DEFAULT_EMBEDDING_DIM;
    let memory = memory_entry(
      "local-memory",
      (0..dim).map(|i| i as f32 * 0.01).collect(),
      ResourceScope::NodeLocal,
      now_ms(),
    );
    storage
      .agent()
      .memory()
      .store(&memory)
      .await
      .expect("store local memory");

    let workspace = workspace_record("local-workspace", ResourceScope::NodeLocal, now_ms());
    storage
      .workspace()
      .workspaces()
      .upsert(&workspace)
      .expect("upsert local workspace");
  }

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peer = format!("https://127.0.0.1:{}", base_port + ((i + 1) % 2) as u16);
      build_config(
        &format!("node-{i}"),
        port,
        vec![peer],
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_bundle(&cert_paths[1], &key_paths[1], &ca_cert_path);
  let mut client = connect_client(&node1_url, &client_tls, &key_hex).await;

  // Negative assertion: polling cannot prove absence, so give anti-entropy a
  // window in which incorrect replication would surface before checking.
  time::sleep(Duration::from_secs(2)).await;

  let memories = client
    .list_resources(
      ResourceKind::Memory,
      HashMap::new(),
      ProtoResourceScope::Unspecified,
    )
    .await
    .expect("list memories failed");
  assert!(
    !memories.iter().any(|resource| {
      resource
        .metadata
        .as_ref()
        .map(|metadata| metadata.id == "local-memory")
        .unwrap_or(false)
    }),
    "local memory should not replicate"
  );

  let workspaces = client
    .list_resources(
      ResourceKind::Workspace,
      HashMap::new(),
      ProtoResourceScope::Unspecified,
    )
    .await
    .expect("list workspaces failed");
  assert!(
    !workspaces.iter().any(|resource| {
      resource
        .metadata
        .as_ref()
        .map(|metadata| metadata.id == "local-workspace")
        .unwrap_or(false)
    }),
    "local workspace should not replicate"
  );
}

#[tokio::test]
async fn partition_merge_reconciles_shared_resources() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  {
    let storage_a =
      Storage::open(data_dirs[0].path().join("lycoris.redb")).expect("open node-0 storage");
    let dim = DEFAULT_EMBEDDING_DIM;
    let memory_a = memory_entry(
      "partition-memory-a",
      (0..dim).map(|i| i as f32 * 0.01).collect(),
      ResourceScope::ClusterShared,
      now_ms(),
    );
    storage_a
      .agent()
      .memory()
      .store(&memory_a)
      .await
      .expect("store partition memory a");

    let workspace_a = workspace_record(
      "partition-workspace-a",
      ResourceScope::ClusterShared,
      now_ms(),
    );
    storage_a
      .workspace()
      .workspaces()
      .upsert(&workspace_a)
      .expect("upsert partition workspace a");
  }

  {
    let storage_b =
      Storage::open(data_dirs[1].path().join("lycoris.redb")).expect("open node-1 storage");
    let dim = DEFAULT_EMBEDDING_DIM;
    let memory_b = memory_entry(
      "partition-memory-b",
      (0..dim).map(|i| -(i as f32) * 0.01).collect(),
      ResourceScope::ClusterShared,
      now_ms(),
    );
    storage_b
      .agent()
      .memory()
      .store(&memory_b)
      .await
      .expect("store partition memory b");

    let workspace_b = workspace_record(
      "partition-workspace-b",
      ResourceScope::ClusterShared,
      now_ms(),
    );
    storage_b
      .workspace()
      .workspaces()
      .upsert(&workspace_b)
      .expect("upsert partition workspace b");
  }

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peer = format!("https://127.0.0.1:{}", base_port + ((i + 1) % 2) as u16);
      build_config(
        &format!("node-{i}"),
        port,
        vec![peer],
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_bundle(&cert_paths[1], &key_paths[1], &ca_cert_path);
  let mut client = connect_client(&node1_url, &client_tls, &key_hex).await;

  wait_for_resource(
    &mut client,
    ResourceKind::Memory,
    "partition-memory-a",
    Duration::from_secs(30),
  )
  .await;
  wait_for_resource(
    &mut client,
    ResourceKind::Memory,
    "partition-memory-b",
    Duration::from_secs(30),
  )
  .await;
  wait_for_resource(
    &mut client,
    ResourceKind::Workspace,
    "partition-workspace-a",
    Duration::from_secs(30),
  )
  .await;
  wait_for_resource(
    &mut client,
    ResourceKind::Workspace,
    "partition-workspace-b",
    Duration::from_secs(30),
  )
  .await;
}

#[tokio::test]
async fn workspace_content_hash_verifies_on_apply() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  {
    let storage =
      Storage::open(data_dirs[0].path().join("lycoris.redb")).expect("open node-0 storage");
    let mut workspace = workspace_record(
      "hash-checked-workspace",
      ResourceScope::ClusterShared,
      now_ms(),
    );
    workspace.content_hash = workspace
      .compute_content_hash()
      .expect("compute content hash");
    storage
      .workspace()
      .workspaces()
      .upsert(&workspace)
      .expect("upsert workspace");
  }

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peer = format!("https://127.0.0.1:{}", base_port + ((i + 1) % 2) as u16);
      build_config(
        &format!("node-{i}"),
        port,
        vec![peer],
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_bundle(&cert_paths[1], &key_paths[1], &ca_cert_path);
  let mut client = connect_client(&node1_url, &client_tls, &key_hex).await;

  wait_for_resource(
    &mut client,
    ResourceKind::Workspace,
    "hash-checked-workspace",
    Duration::from_secs(30),
  )
  .await;
}

#[tokio::test]
async fn memory_content_hash_verifies_on_apply() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  {
    let storage =
      Storage::open(data_dirs[0].path().join("lycoris.redb")).expect("open node-0 storage");
    let dim = DEFAULT_EMBEDDING_DIM;
    let memory = memory_entry(
      "hash-checked-memory",
      (0..dim).map(|i| i as f32 * 0.01).collect(),
      ResourceScope::ClusterShared,
      now_ms(),
    );
    storage
      .agent()
      .memory()
      .store(&memory)
      .await
      .expect("store memory");
  }

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peer = format!("https://127.0.0.1:{}", base_port + ((i + 1) % 2) as u16);
      build_config(
        &format!("node-{i}"),
        port,
        vec![peer],
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_bundle(&cert_paths[1], &key_paths[1], &ca_cert_path);
  let mut client = connect_client(&node1_url, &client_tls, &key_hex).await;

  wait_for_resource(
    &mut client,
    ResourceKind::Memory,
    "hash-checked-memory",
    Duration::from_secs(30),
  )
  .await;
}

#[tokio::test]
async fn memory_recall_works_after_replication() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");
  let key_hex = cluster_key.to_hex();

  let stored_at = now_ms();
  {
    let storage =
      Storage::open(data_dirs[0].path().join("lycoris.redb")).expect("open node-0 storage");
    let dim = DEFAULT_EMBEDDING_DIM;
    let mut near = vec![0.0_f32; dim];
    near[0] = 1.0;
    let memory = memory_entry(
      "recall-target",
      near,
      ResourceScope::ClusterShared,
      stored_at,
    );
    storage
      .agent()
      .memory()
      .store(&memory)
      .await
      .expect("store memory");
  }

  let configs: Vec<DaemonConfig> = (0..node_count)
    .map(|i| {
      let port = base_port + i as u16;
      let peer = format!("https://127.0.0.1:{}", base_port + ((i + 1) % 2) as u16);
      build_config(
        &format!("node-{i}"),
        port,
        vec![peer],
        data_dirs[i].path().to_path_buf(),
        &ca_cert_path,
        &ca_key_path,
        &cert_paths[i],
        &key_paths[i],
      )
    })
    .collect();

  for config in configs.clone() {
    spawn_runtime(config, cluster_key.clone());
  }

  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_bundle(&cert_paths[1], &key_paths[1], &ca_cert_path);
  let mut client = connect_client(&node1_url, &client_tls, &key_hex).await;

  wait_for_resource(
    &mut client,
    ResourceKind::Memory,
    "recall-target",
    Duration::from_secs(30),
  )
  .await;

  let resource = client
    .get_resource(ResourceKind::Memory, "recall-target")
    .await
    .expect("get memory failed")
    .expect("memory not found");
  // The stored creation time must reach the wire verbatim instead of the
  // previous hardcoded 0.
  let metadata = resource.metadata.expect("memory metadata");
  assert_eq!(metadata.created_at_ms, stored_at);
  assert_eq!(metadata.updated_at_ms, stored_at);
  let body = match resource.body {
    Some(lycoris_proto::node::resource::Body::Memory(body)) => body,
    _ => panic!("unexpected resource body"),
  };
  assert_eq!(body.content, b"recall-target");
  assert!(!body.embedding.is_empty());
}

#[tokio::test]
async fn wrong_cluster_key_is_rejected() {
  let _ = lycoris_tls::install_crypto_provider();

  let (node_count, base_port) = (1, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");

  let config = build_config(
    "node-0",
    base_port,
    vec![],
    data_dirs[0].path().to_path_buf(),
    &ca_cert_path,
    &ca_key_path,
    &cert_paths[0],
    &key_paths[0],
  );
  spawn_runtime(config, cluster_key.clone());

  // The mTLS handshake succeeds (the client cert is valid), but a mismatched
  // cluster key must be rejected at the Cluster admission boundary.
  let node0_url = format!("https://127.0.0.1:{base_port}");
  let client_tls = client_tls_bundle(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let wrong_key = ClusterKey::generate().expect("generate wrong cluster key");
  let mut client = connect_client(&node0_url, &client_tls, &cluster_key.to_hex())
    .await
    .with_cluster_key(wrong_key.to_hex());

  let error = client
    .list_resources(
      ResourceKind::Node,
      HashMap::new(),
      ProtoResourceScope::Unspecified,
    )
    .await
    .expect_err("a wrong cluster key must be rejected");
  match error {
    lycoris_client::ClientError::Status(status) => {
      assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }
    other => panic!("expected an rpc status error, got {other:?}"),
  }
}
