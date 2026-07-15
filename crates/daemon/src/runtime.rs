use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use lycoris_config::{
  ClientConfig, ClusterKey, DaemonConfig, default_cluster_key_path, paths, time::now_ms,
};
use lycoris_storage::{Storage, StorageError};
use thiserror::Error;
use tokio::{
  signal::unix::{SignalKind, signal},
  sync::watch,
};
use tonic::transport::{ClientTlsConfig, ServerTlsConfig};

use crate::{
  cluster_sync::ClusterSync,
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
  let (shutdown_tx, shutdown_rx) = watch::channel(false);

  let shutdown_tx_signal = shutdown_tx.clone();
  tokio::spawn(async move {
    let mut terminate = match signal(SignalKind::terminate()) {
      Ok(signal) => signal,
      Err(_) => return,
    };
    let mut interrupt = match signal(SignalKind::interrupt()) {
      Ok(signal) => signal,
      Err(_) => return,
    };

    tokio::select! {
      _ = terminate.recv() => {}
      _ = interrupt.recv() => {}
    }

    let _ = shutdown_tx_signal.send(true);
  });

  run_with_shutdown(config, shutdown_tx, shutdown_rx).await
}

/// Run the daemon until the supplied shutdown signal becomes `true`.
pub async fn run_with_shutdown(
  config: DaemonConfig, shutdown_tx: watch::Sender<bool>, shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
  write_client_config(&config);

  let data_dir = PathBuf::from(&config.data_dir);
  std::fs::create_dir_all(&data_dir)?;
  let storage = Storage::open(data_dir.join("lycoris.redb"))?;
  let node = storage.node();

  let cluster_key = load_cluster_key();

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
  let client_tls = ClientTlsConfig::new()
    .identity(tls_bundle.identity.clone())
    .ca_certificate(tls_bundle.ca.clone());

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

  let cluster_sync = ClusterSync::new(
    config.node.id.clone(),
    membership_service.clone(),
    node.clone(),
    &tls_bundle,
  );

  let cluster_service =
    ClusterService::new(membership_service.clone(), storage.clone(), client_tls)
      .with_cluster_sync(cluster_sync.clone())
      .with_cluster_key(cluster_key)
      .with_shutdown(shutdown_tx);

  let mut background = tokio::task::JoinSet::new();

  let ae_sync = cluster_sync.clone();
  let ae_shutdown = shutdown.clone();
  background.spawn(async move {
    tokio::select! {
      _ = ae_sync.run(DEFAULT_SYNC_INTERVAL) => {}
      _ = wait_shutdown(ae_shutdown) => {}
    }
  });

  let swim_sync = cluster_sync.clone();
  let swim_shutdown = shutdown.clone();
  background.spawn(async move {
    tokio::select! {
      _ = swim_sync.run_swim(DEFAULT_SWIM_INTERVAL) => {}
      _ = wait_shutdown(swim_shutdown) => {}
    }
  });

  let (sync_service, membership_service_rpc) = cluster_sync.into_servers();

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

fn load_cluster_key() -> Option<ClusterKey> {
  let path = default_cluster_key_path();
  if !path.is_file() {
    return None;
  }

  match ClusterKey::load(&path) {
    Ok(key) => {
      tracing::info!(path = %path.display(), "loaded cluster key");
      Some(key)
    }
    Err(error) => {
      tracing::warn!(
        %error,
        path = %path.display(),
        "failed to load cluster key; join requests will be rejected"
      );
      None
    }
  }
}
