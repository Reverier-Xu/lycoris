use std::{
  collections::HashMap,
  path::PathBuf,
  sync::atomic::{AtomicU16, Ordering},
  time::Duration,
};

use lycoris_client::{ClusterClient, PeerClient};
use lycoris_config::{ClusterConfig, DaemonConfig, NodeConfig, TlsConfig};
use lycoris_core::{ClusterKey, SimpleNode, time::now_ms};
use lycoris_proto::node::ResourceKind;
use lycoris_storage::{ResourceScope, SkillRecord, Storage, workspace::VersionedContentStore};
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
    .list_resources(ResourceKind::Node, HashMap::new(), String::new())
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

fn client_tls_config(
  cert_path: &std::path::Path, key_path: &std::path::Path, ca_path: &std::path::Path,
) -> tonic::transport::ClientTlsConfig {
  lycoris_tls::load_client_tls(cert_path, key_path, ca_path).unwrap()
}

#[allow(clippy::too_many_arguments)]
fn spawn_runtime(config: DaemonConfig, key: ClusterKey) {
  tokio::spawn(async move {
    if let Err(e) = lycoris_daemon::runtime::run(config, Some(key)).await {
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
  let _ = lycoris_client::install_crypto_provider();

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

  // Wait for all nodes to start listening.
  time::sleep(Duration::from_millis(300)).await;

  // Register an external node through node-0.
  let client_tls = client_tls_config(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let node0_url = format!("https://127.0.0.1:{base_port}");
  let mut client = ClusterClient::connect_with_tls(&node0_url, client_tls)
    .await
    .expect("failed to connect to node-0");

  let external_dir = TempDir::new().unwrap();
  let _external_storage =
    Storage::open(external_dir.path().join("external.redb")).expect("open external storage");
  let external = SimpleNode::new(
    "external-node".to_string(),
    format!("127.0.0.1:{}", base_port + 99),
    HashMap::new(),
    HashMap::new(),
  );
  client
    .register(&external, &key_hex)
    .await
    .expect("register failed");

  // Wait for push + anti-entropy to propagate through the chain.
  time::sleep(Duration::from_millis(10000)).await;

  // Query node-2 (the far end of the chain) and verify it knows external-node.
  let client_tls = client_tls_config(&cert_paths[2], &key_paths[2], &ca_cert_path);
  let node2_url = format!("https://127.0.0.1:{}", base_port + 2);
  let mut client = ClusterClient::connect_with_tls(&node2_url, client_tls)
    .await
    .expect("failed to connect to node-2");

  let ids = list_node_ids(&mut client).await;

  assert!(ids.contains(&"external-node".to_string()));
  assert!(ids.contains(&"node-0".to_string()));
  assert!(ids.contains(&"node-1".to_string()));
  assert!(ids.contains(&"node-2".to_string()));
}

#[tokio::test]
async fn primary_failure_falls_back_and_promotes() {
  let _ = lycoris_client::install_crypto_provider();

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
        if let Err(e) = lycoris_daemon::runtime::run(config, Some(key)).await {
          eprintln!("runtime error: {e:?}");
        }
      }));
    }
  }

  time::sleep(Duration::from_millis(300)).await;

  // Point node-0's primary at an unreachable address.
  let node0_url = format!("https://127.0.0.1:{base_port}");
  let client_tls = client_tls_config(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let mut client = ClusterClient::connect_with_tls(&node0_url, client_tls.clone())
    .await
    .expect("failed to connect to node-0");
  client
    .set_primary_endpoint("https://127.0.0.1:1")
    .await
    .expect("set_primary_endpoint failed");

  // Register an external node through node-0. The push/sync must use the
  // fallback (node-1) because the primary is unreachable.
  let external_dir = TempDir::new().unwrap();
  let _external_storage =
    Storage::open(external_dir.path().join("external.redb")).expect("open external storage");
  let external = SimpleNode::new(
    "fallback-external-node".to_string(),
    format!("127.0.0.1:{}", base_port + 99),
    HashMap::new(),
    HashMap::new(),
  );
  client
    .register(&external, &key_hex)
    .await
    .expect("register failed");

  time::sleep(Duration::from_millis(5500)).await;

  // Verify node-1 received the external node via fallback sync.
  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let mut client = ClusterClient::connect_with_tls(&node1_url, client_tls)
    .await
    .expect("failed to connect to node-1");
  let ids = list_node_ids(&mut client).await;
  assert!(ids.contains(&"fallback-external-node".to_string()));

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
    .peers
    .get_primary()
    .expect("read primary")
    .expect("primary should be set after successful fallback sync");
  assert_eq!(primary, node1_url);
}

#[tokio::test]
async fn partition_merge_reconciles_bidirectional_membership() {
  let _ = lycoris_client::install_crypto_provider();

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

  time::sleep(Duration::from_millis(300)).await;

  let node0_url = format!("https://127.0.0.1:{base_port}");
  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_config(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let mut client = ClusterClient::connect_with_tls(&node0_url, client_tls.clone())
    .await
    .expect("failed to connect to node-0");

  let alpha_dir = TempDir::new().unwrap();
  let _alpha_storage =
    Storage::open(alpha_dir.path().join("alpha.redb")).expect("open alpha storage");
  let alpha = SimpleNode::new(
    "alpha".to_string(),
    format!("127.0.0.1:{}", base_port + 98),
    HashMap::new(),
    HashMap::new(),
  );
  client
    .register(&alpha, &key_hex)
    .await
    .expect("register alpha failed");

  let mut client = ClusterClient::connect_with_tls(&node1_url, client_tls.clone())
    .await
    .expect("failed to connect to node-1");

  let beta_dir = TempDir::new().unwrap();
  let _beta_storage = Storage::open(beta_dir.path().join("beta.redb")).expect("open beta storage");
  let beta = SimpleNode::new(
    "beta".to_string(),
    format!("127.0.0.1:{}", base_port + 97),
    HashMap::new(),
    HashMap::new(),
  );
  client
    .register(&beta, &key_hex)
    .await
    .expect("register beta failed");

  time::sleep(Duration::from_millis(1500)).await;

  for url in [&node0_url, &node1_url] {
    let mut client = ClusterClient::connect_with_tls(url, client_tls.clone())
      .await
      .expect("failed to connect");
    let ids = list_node_ids(&mut client).await;
    assert!(ids.contains(&"alpha".to_string()));
    assert!(ids.contains(&"beta".to_string()));
    assert!(ids.contains(&"node-0".to_string()));
    assert!(ids.contains(&"node-1".to_string()));
  }
}

#[tokio::test]
async fn failure_detector_marks_unresponsive_peer() {
  let _ = lycoris_client::install_crypto_provider();

  let (node_count, base_port) = (2, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");

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
      if let Err(e) = lycoris_daemon::runtime::run(config, Some(key)).await {
        eprintln!("runtime error: {e:?}");
      }
    }));
  }

  time::sleep(Duration::from_millis(300)).await;

  let node0_url = format!("https://127.0.0.1:{base_port}");
  let _node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client_tls = client_tls_config(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let mut client = ClusterClient::connect_with_tls(&node0_url, client_tls.clone())
    .await
    .expect("failed to connect to node-0");

  wait_for_node(&mut client, "node-1", Duration::from_millis(2000)).await;

  // Stop node-1 so that node-0's SWIM probes begin to fail.
  handles[1].abort();

  // Wait long enough for three consecutive SWIM probe timeouts
  // (1s interval + 3s timeout per failure = ~11s).
  time::sleep(Duration::from_secs(12)).await;

  let mut peer = PeerClient::connect(&node0_url, client_tls.clone())
    .await
    .expect("failed to connect membership client");
  let registers = peer
    .membership
    .fetch_registers(vec!["node-1".to_string()])
    .await
    .expect("fetch_registers failed");
  assert_eq!(registers.len(), 1);
  assert_eq!(
    registers[0].state, "suspected",
    "node-1 should be suspected after probes time out"
  );
}

#[tokio::test]
async fn shared_skills_replicate_via_anti_entropy() {
  let _ = lycoris_client::install_crypto_provider();

  let (node_count, base_port) = (3, alloc_base_port());
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

  let cluster_key = ClusterKey::generate().expect("generate cluster key");

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
      updated_at_ms: now_ms(),
      metadata: HashMap::new(),
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

  time::sleep(Duration::from_millis(300)).await;

  let node2_url = format!("https://127.0.0.1:{}", base_port + 2);
  let client_tls = client_tls_config(&cert_paths[2], &key_paths[2], &ca_cert_path);
  let mut client = ClusterClient::connect_with_tls(&node2_url, client_tls)
    .await
    .expect("failed to connect to node-2");

  let start = std::time::Instant::now();
  loop {
    let resources = client
      .list_resources(ResourceKind::Skill, HashMap::new(), String::new())
      .await
      .expect("list skills failed");
    let found = resources.iter().any(|resource| {
      resource
        .metadata
        .as_ref()
        .map(|metadata| metadata.id == "shared-skill")
        .unwrap_or(false)
    });
    if found {
      break;
    }
    if start.elapsed() >= Duration::from_secs(10) {
      panic!("timed out waiting for shared skill to replicate to node-2");
    }
    time::sleep(Duration::from_millis(200)).await;
  }
}
