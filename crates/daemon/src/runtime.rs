use std::{net::SocketAddr, path::PathBuf, time::Duration};

use lycoris_config::{ClientConfig, DaemonConfig, NodeInfo, paths};
use thiserror::Error;
use tonic::transport::ServerTlsConfig;

use crate::{
  gossip::Gossip,
  node::{info::LocalNode, registry::NodeRegistry},
  rpc::server::ClusterService,
  storage::{Storage, StorageError},
  tls::{TlsError, ensure_tls_bundle},
};

const NODE_TTL: Duration = Duration::from_secs(60);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum RuntimeError {
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("storage error: {0}")]
  Storage(#[from] StorageError),
  #[error("tls error: {0}")]
  Tls(#[from] TlsError),
  #[error("invalid bind address: {0}")]
  InvalidAddress(#[from] std::net::AddrParseError),
  #[error("transport error: {0}")]
  Transport(#[from] tonic::transport::Error),
}

pub async fn run(config: DaemonConfig) -> Result<(), RuntimeError> {
  write_client_config(&config);

  let data_dir = PathBuf::from(&config.data_dir);
  std::fs::create_dir_all(&data_dir)?;
  let storage = Storage::open(data_dir.join("lycoris.db"))?;

  let local_node = LocalNode::from_config(&config.node, storage.clone());
  let tls_bundle = ensure_tls_bundle(
    &config.tls.ca_cert,
    &config.tls.ca_key,
    &config.tls.cert,
    &config.tls.key,
    &config.node.id,
  )?;
  let server_tls = ServerTlsConfig::new()
    .identity(tls_bundle.identity.clone())
    .client_ca_root(tls_bundle.ca.clone());

  for peer in &config.cluster.bootstrap_peers {
    storage.seed_peer(peer)?;
  }

  let registry = NodeRegistry::new(storage.clone(), NODE_TTL);
  // Seed the registry with information about the local node.
  registry.register_or_update(&local_node);

  let gossip = Gossip::new(
    local_node.id().to_string(),
    registry.clone(),
    storage.clone(),
    &tls_bundle,
  );

  let cluster_service =
    ClusterService::new(registry.clone(), storage.clone()).with_gossip(gossip.clone());
  let sync_service = gossip.clone().into_sync_server();

  let cleanup_registry = registry.clone();
  tokio::spawn(async move {
    let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
    loop {
      interval.tick().await;
      cleanup_registry.cleanup_offline();
    }
  });

  tokio::spawn(async move {
    gossip.run(DEFAULT_SYNC_INTERVAL).await;
  });

  let addr: SocketAddr = config.cluster.listen_address.parse()?;
  tracing::info!(%addr, "node api server listening");

  tonic::transport::Server::builder()
    .tls_config(server_tls)?
    .timeout(Duration::from_secs(30))
    .add_service(cluster_service.into_server())
    .add_service(sync_service)
    .serve(addr)
    .await?;

  Ok(())
}

fn write_client_config(config: &DaemonConfig) {
  let client_config = ClientConfig::from_daemon_config(config);
  if let Some(path) = paths::default_client_config_path() {
    if let Err(error) = client_config.write_to_file(&path) {
      tracing::warn!(
        %error,
        path = %path.display(),
        "failed to write client configuration; lycoris CLI may not be able to connect"
      );
    } else {
      tracing::info!(path = %path.display(), "wrote client configuration");
    }
  }
}
