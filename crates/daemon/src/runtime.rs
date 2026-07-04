use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use lycoris_config::{ClientConfig, DaemonConfig, paths, time::now_ms};
use lycoris_storage::{ClusterStorage, StorageError};
use thiserror::Error;
use tonic::transport::ServerTlsConfig;

use crate::{
  gossip::Gossip,
  membership::{MemberRegister, MembershipService, SwimConfig},
  rpc::server::ClusterService,
  tls::{TlsError, ensure_tls_bundle},
};

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_SWIM_INTERVAL: Duration = Duration::from_secs(1);

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
  let storage = ClusterStorage::open(data_dir.join("lycoris.db"))?;

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

  let mut local_register = MemberRegister::new(&config.node.id, &config.node.address, 1, 0);
  local_register.labels = storage.local_labels().unwrap_or_default();
  local_register.annotations = storage.local_annotations().unwrap_or_default();
  local_register.updated_at_ms = now_ms();

  let membership_service = Arc::new(MembershipService::new(
    &config.node.id,
    SwimConfig::default(),
    local_register,
  ));

  let gossip = Gossip::new(
    config.node.id.clone(),
    membership_service.clone(),
    storage.clone(),
    &tls_bundle,
  );

  let cluster_service =
    ClusterService::new(membership_service.clone(), storage.clone()).with_gossip(gossip.clone());

  let ae_gossip = gossip.clone();
  tokio::spawn(async move {
    ae_gossip.run(DEFAULT_SYNC_INTERVAL).await;
  });

  let swim_gossip = gossip.clone();
  tokio::spawn(async move {
    swim_gossip.run_swim(DEFAULT_SWIM_INTERVAL).await;
  });

  let (sync_service, membership_service_rpc) = gossip.into_servers();

  let addr: SocketAddr = config.cluster.listen_address.parse()?;
  tracing::info!(%addr, "node api server listening");

  tonic::transport::Server::builder()
    .tls_config(server_tls)?
    .timeout(Duration::from_secs(30))
    .add_service(cluster_service.into_server())
    .add_service(sync_service)
    .add_service(membership_service_rpc)
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
