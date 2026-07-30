use std::{collections::HashMap, error::Error, path::Path, time::Duration};

use lycoris_client::{ClientError, ClusterClient};
use lycoris_config::{ClusterConfig, DaemonConfig, ExtensionsConfig, NodeConfig, TlsConfig};
use lycoris_core::ClusterKey;
use lycoris_proto::node::{ResourceKind, ResourceScope as ProtoResourceScope};
use tempfile::TempDir;
use tokio::sync::watch;

const WAIT: Duration = Duration::from_secs(10);

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn daemon_config(
  data_dir: &Path, control_port: u16, certs: &lycoris_testkit::certs::TestCerts,
) -> DaemonConfig {
  DaemonConfig {
    node: NodeConfig {
      id: "control-node".to_string(),
      address: format!("https://127.0.0.1:{control_port}"),
      labels: HashMap::new(),
    },
    cluster: ClusterConfig {
      listen_address: format!("127.0.0.1:{control_port}"),
      bootstrap_peers: Vec::new(),
      overlay_listen: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
      join: None,
    },
    tls: TlsConfig {
      ca_cert: certs.ca_cert.to_string_lossy().to_string(),
      ca_key: certs.ca_key.to_string_lossy().to_string(),
      cert: certs.nodes[0].cert.to_string_lossy().to_string(),
      key: certs.nodes[0].key.to_string_lossy().to_string(),
    },
    data_dir: data_dir.to_string_lossy().to_string(),
    extensions: ExtensionsConfig::default(),
  }
}

async fn connect_client(
  url: &str, tls: &lycoris_tls::TlsBundle, cluster_key: &ClusterKey,
) -> TestResult<ClusterClient> {
  let started = std::time::Instant::now();
  loop {
    match ClusterClient::connect(url, tls).await {
      Ok(client) => return Ok(client.with_cluster_key(cluster_key.to_hex())),
      Err(error) if started.elapsed() < WAIT => {
        let _ = error;
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
      Err(error) => return Err(Box::new(error)),
    }
  }
}

#[tokio::test]
async fn wrong_cluster_key_is_rejected() -> TestResult {
  let _ = lycoris_tls::install_crypto_provider();
  let data_dir = TempDir::new()?;
  let (_cert_dir, certs) = lycoris_testkit::certs::temp_test_certs(1);
  let control_port = std::net::TcpListener::bind("127.0.0.1:0")?
    .local_addr()?
    .port();
  let cluster_key = ClusterKey::generate()?;
  let config = daemon_config(data_dir.path(), control_port, &certs);
  let (shutdown_tx, shutdown_rx) = watch::channel(false);
  let runtime_shutdown = shutdown_tx.clone();
  let runtime_key = cluster_key.clone();
  let runtime = tokio::spawn(async move {
    lycoris_daemon::runtime::run_with_shutdown(
      config,
      runtime_shutdown,
      shutdown_rx,
      Some(runtime_key),
    )
    .await
  });

  let url = format!("https://127.0.0.1:{control_port}");
  let tls =
    lycoris_tls::load_tls_bundle(&certs.nodes[0].cert, &certs.nodes[0].key, &certs.ca_cert)?;
  let wrong_key = ClusterKey::generate()?;
  let mut client = connect_client(&url, &tls, &cluster_key)
    .await?
    .with_cluster_key(wrong_key.to_hex());
  let result = client
    .list_resources(
      ResourceKind::Node,
      HashMap::new(),
      ProtoResourceScope::Unspecified,
    )
    .await;
  let error = match result {
    Err(error) => error,
    Ok(_) => return Err("a wrong cluster key was accepted".into()),
  };
  match error {
    ClientError::Status(status) => assert_eq!(status.code(), tonic::Code::PermissionDenied),
    other => return Err(format!("expected an rpc status error, got {other:?}").into()),
  }

  let _ = shutdown_tx.send(true);
  tokio::time::timeout(WAIT, runtime).await???;
  Ok(())
}
