use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use lycoris_config::{ClientConfig, DaemonConfig, paths, time::now_ms};
use lycoris_storage::{Storage, StorageError};
use thiserror::Error;
use tokio::sync::watch;
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

/// Run the daemon until the process is interrupted.
pub async fn run(config: DaemonConfig) -> Result<(), RuntimeError> {
  // Hold the sender so the receiver never sees the channel close. The
  // daemon then runs until the surrounding task/runtime is shut down.
  let (_shutdown_tx, shutdown_rx) = watch::channel(false);
  run_with_shutdown(config, shutdown_rx).await
}

/// Run the daemon until the supplied shutdown signal becomes `true`.
pub async fn run_with_shutdown(
  config: DaemonConfig, shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
  write_client_config(&config);

  let data_dir = PathBuf::from(&config.data_dir);
  std::fs::create_dir_all(&data_dir)?;
  let storage = Storage::open(data_dir.join("lycoris.redb"))?;
  let node = storage.node();

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
    node.peers.seed(peer)?;
  }

  if node.peers.get_primary()?.is_none()
    && let Some(first_peer) = config.cluster.bootstrap_peers.first()
  {
    node.peers.set_primary(first_peer)?;
  }

  let mut local_register = MemberRegister::new(&config.node.id, &config.node.address, 1, 0);
  local_register.labels = node.local.labels().unwrap_or_default();
  local_register.annotations = node.local.annotations().unwrap_or_default();
  local_register.updated_at_ms = now_ms();

  let membership_service = Arc::new(MembershipService::new(
    &config.node.id,
    SwimConfig::default(),
    local_register,
  ));

  let gossip = Gossip::new(
    config.node.id.clone(),
    membership_service.clone(),
    node.clone(),
    &tls_bundle,
  );

  let cluster_service =
    ClusterService::new(membership_service.clone(), node.clone()).with_gossip(gossip.clone());

  let mut background = tokio::task::JoinSet::new();

  let ae_gossip = gossip.clone();
  let ae_shutdown = shutdown.clone();
  background.spawn(async move {
    tokio::select! {
      _ = ae_gossip.run(DEFAULT_SYNC_INTERVAL) => {}
      _ = wait_shutdown(ae_shutdown) => {}
    }
  });

  let swim_gossip = gossip.clone();
  let swim_shutdown = shutdown.clone();
  background.spawn(async move {
    tokio::select! {
      _ = swim_gossip.run_swim(DEFAULT_SWIM_INTERVAL) => {}
      _ = wait_shutdown(swim_shutdown) => {}
    }
  });

  let (sync_service, membership_service_rpc) = gossip.into_servers();

  let addr: SocketAddr = config.cluster.listen_address.parse()?;
  tracing::info!(%addr, "node api server listening");

  let server_shutdown = shutdown.clone();
  let result = tonic::transport::Server::builder()
    .tls_config(server_tls)?
    .timeout(Duration::from_secs(30))
    .add_service(cluster_service.into_server())
    .add_service(sync_service)
    .add_service(membership_service_rpc)
    .serve_with_shutdown(addr, wait_shutdown(server_shutdown))
    .await;

  background.abort_all();
  while background.join_next().await.is_some() {}

  result?;
  Ok(())
}

async fn wait_shutdown(mut shutdown: watch::Receiver<bool>) {
  while !*shutdown.borrow() {
    if shutdown.changed().await.is_err() {
      break;
    }
  }
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
