use std::{collections::HashMap, path::PathBuf, time::Duration};

use lycoris_api::ClusterRpcClient;
use lycoris_config::{ClusterConfig, DaemonConfig, NodeConfig, TlsConfig};
use lycoris_daemon::{node::info::LocalNode, storage::Storage};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use tempfile::TempDir;
use tokio::time;

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
  lycoris_api::tls::load_client_tls(cert_path, key_path, ca_path).unwrap()
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
      address: format!("127.0.0.1:{listen_port}"),
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
  let _ = lycoris_daemon::install_crypto_provider();

  let (node_count, base_port) = (3, 56001);
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);

  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

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
    tokio::spawn(async move {
      let _ = lycoris_daemon::runtime::run(config).await;
    });
  }

  // Wait for all nodes to start listening.
  time::sleep(Duration::from_millis(300)).await;

  // Register an external node through node-0.
  let client_tls = client_tls_config(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let node0_url = format!("https://127.0.0.1:{base_port}");
  let client = ClusterRpcClient::connect(&node0_url, client_tls)
    .await
    .expect("failed to connect to node-0");

  let external_dir = TempDir::new().unwrap();
  let external_storage =
    Storage::open(external_dir.path().join("external.db")).expect("open external storage");
  let external = LocalNode::from_config(
    &NodeConfig {
      id: "external-node".to_string(),
      address: "127.0.0.1:56099".to_string(),
    },
    external_storage,
  );
  client.register(&external).await.expect("register failed");

  // Wait for push + anti-entropy to propagate through the chain.
  time::sleep(Duration::from_millis(1500)).await;

  // Query node-2 (the far end of the chain) and verify it knows external-node.
  let client_tls = client_tls_config(&cert_paths[2], &key_paths[2], &ca_cert_path);
  let node2_url = format!("https://127.0.0.1:{}", base_port + 2);
  let client = ClusterRpcClient::connect(&node2_url, client_tls)
    .await
    .expect("failed to connect to node-2");

  let nodes = client
    .list_nodes(HashMap::new())
    .await
    .expect("list_nodes failed")
    .nodes;

  let ids: Vec<String> = nodes.into_iter().map(|n| n.id).collect();
  assert!(ids.contains(&"external-node".to_string()));
  assert!(ids.contains(&"node-0".to_string()));
  assert!(ids.contains(&"node-1".to_string()));
  assert!(ids.contains(&"node-2".to_string()));
}

#[tokio::test]
async fn primary_failure_falls_back_and_promotes() {
  let _ = lycoris_daemon::install_crypto_provider();

  let (node_count, base_port) = (2, 56101);
  let (_dir, ca_cert_path, ca_key_path, cert_paths, key_paths) = generate_test_certs(node_count);
  let data_dirs: Vec<TempDir> = (0..node_count).map(|_| TempDir::new().unwrap()).collect();

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
    tokio::spawn(async move {
      let _ = lycoris_daemon::runtime::run(config).await;
    });
  }

  time::sleep(Duration::from_millis(300)).await;

  // Point node-0's primary at an unreachable address.
  let node0_url = format!("https://127.0.0.1:{base_port}");
  let client_tls = client_tls_config(&cert_paths[0], &key_paths[0], &ca_cert_path);
  let client = ClusterRpcClient::connect(&node0_url, client_tls.clone())
    .await
    .expect("failed to connect to node-0");
  client
    .set_primary_endpoint("https://127.0.0.1:1")
    .await
    .expect("set_primary_endpoint failed");

  // Register an external node through node-0. The push/sync must use the
  // fallback (node-1) because the primary is unreachable.
  let external_dir = TempDir::new().unwrap();
  let external_storage =
    Storage::open(external_dir.path().join("external.db")).expect("open external storage");
  let external = LocalNode::from_config(
    &NodeConfig {
      id: "fallback-external-node".to_string(),
      address: "127.0.0.1:56199".to_string(),
    },
    external_storage,
  );
  client.register(&external).await.expect("register failed");

  time::sleep(Duration::from_millis(1500)).await;

  // Verify node-1 received the external node via fallback sync.
  let node1_url = format!("https://127.0.0.1:{}", base_port + 1);
  let client = ClusterRpcClient::connect(&node1_url, client_tls)
    .await
    .expect("failed to connect to node-1");
  let ids: Vec<String> = client
    .list_nodes(HashMap::new())
    .await
    .expect("list_nodes failed")
    .nodes
    .into_iter()
    .map(|n| n.id)
    .collect();
  assert!(ids.contains(&"fallback-external-node".to_string()));

  // Verify node-0 promoted the reachable fallback to primary in storage.
  let node0_storage =
    Storage::open(data_dirs[0].path().join("lycoris.db")).expect("reopen node-0 storage");
  let primary = node0_storage
    .get_primary()
    .expect("read primary")
    .expect("primary should be set after successful fallback sync");
  assert_eq!(primary, node1_url);
}
