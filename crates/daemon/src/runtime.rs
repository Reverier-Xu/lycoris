use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use lycoris_config::{ClientConfig, DaemonConfig, default_client_config_path};
use lycoris_core::{ClusterKey, now_ms};
use lycoris_storage::{Storage, StorageError};
use lycoris_tls::{TlsError, ensure_tls_bundle, install_crypto_provider};
use thiserror::Error;
use tokio::sync::watch;

use crate::{
  extension::ExtensionManager,
  membership::{LOCAL_INCARNATION_KEY, MemberRegister, MembershipService, SwimConfig},
  resource::ResourceMapper,
  rpc::{extension::ExtensionService, server::ClusterService},
  sync::{ClusterSync, ResourceSync},
  transport::PeerPool,
};

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_SWIM_INTERVAL: Duration = Duration::from_secs(1);
/// Resource anti-entropy runs on the same 5s cadence as membership
/// anti-entropy: shared resources change rarely, and this interval balances
/// propagation delay against RPC churn. It is a separate constant (D5) so the
/// two planes can be tuned independently.
const DEFAULT_RESOURCE_SYNC_INTERVAL: Duration = Duration::from_secs(5);

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
  #[error("extension engine error: {0}")]
  Extension(#[from] lycoris_extension::ExtensionError),
  #[error("failed to install crypto provider")]
  CryptoProvider,
}

/// Process shutdown signal streams, registered synchronously at startup.
///
/// Registering the handlers is separated from waiting on them so that a
/// registration failure aborts startup (`RuntimeError`) instead of leaving
/// the daemon running with no graceful shutdown path: the watcher task flips
/// the shutdown channel unconditionally once its wait returns, so a silent
/// early return would read as an immediate shutdown request.
#[cfg(unix)]
struct ShutdownSignals {
  terminate: tokio::signal::unix::Signal,
  interrupt: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
  fn register() -> Result<Self, RuntimeError> {
    use tokio::signal::unix::{SignalKind, signal};
    Ok(Self {
      terminate: signal(SignalKind::terminate())?,
      interrupt: signal(SignalKind::interrupt())?,
    })
  }

  /// Wait for SIGTERM or SIGINT.
  async fn wait(mut self) {
    tokio::select! {
      _ = self.terminate.recv() => {}
      _ = self.interrupt.recv() => {}
    }
  }
}

/// Spawn the shutdown-signal watcher: it flips the shutdown channel once the
/// process is interrupted. Handlers are registered synchronously before this
/// returns, so a registration failure fails startup fast.
fn spawn_shutdown_watcher(shutdown_tx: watch::Sender<bool>) -> Result<(), RuntimeError> {
  #[cfg(unix)]
  {
    let signals = ShutdownSignals::register()?;
    tokio::spawn(async move {
      signals.wait().await;
      let _ = shutdown_tx.send(true);
    });
  }
  #[cfg(not(unix))]
  {
    tokio::spawn(async move {
      let _ = tokio::signal::ctrl_c().await;
      let _ = shutdown_tx.send(true);
    });
  }
  Ok(())
}

/// Run the daemon until the process is interrupted.
pub async fn run(config: DaemonConfig) -> Result<(), RuntimeError> {
  // Fail startup fast when the provider cannot be installed: every TLS
  // connection depends on it, so continuing would surface as obscure
  // handshake errors much later (same policy as the shell).
  install_crypto_provider().map_err(|_| RuntimeError::CryptoProvider)?;
  let cluster_key = load_cluster_key(&config.data_dir);
  write_client_config(&config, cluster_key.as_ref());

  let (shutdown_tx, shutdown_rx) = watch::channel(false);

  let shutdown_tx_signal = shutdown_tx.clone();
  spawn_shutdown_watcher(shutdown_tx_signal)?;

  run_with_shutdown(config, shutdown_tx, shutdown_rx, cluster_key).await
}

/// Run the daemon until the supplied shutdown signal becomes `true`.
pub async fn run_with_shutdown(
  config: DaemonConfig, shutdown_tx: watch::Sender<bool>, shutdown: watch::Receiver<bool>,
  cluster_key: Option<ClusterKey>,
) -> Result<(), RuntimeError> {
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
    &config.node.address,
  )?;
  let server_tls = tls_bundle.server_config();

  for peer in &config.cluster.bootstrap_peers {
    node.peers().seed(peer)?;
  }

  if node.peers().get_primary()?.is_none()
    && let Some(first_peer) = config
      .cluster
      .bootstrap_peers
      .iter()
      .find(|peer| peer.as_str() != config.node.address)
  {
    node.peers().set_primary(first_peer, &config.node.address)?;
  }

  // Config labels are merged into the node-local store before the register
  // reads it back (set semantics: config keys overwrite stored values, other
  // stored keys are kept), so the local register, gossip and the extension
  // manager's selector evaluation all keep reading labels from the single
  // storage source.
  apply_config_labels(node.local(), &config.node.labels)?;

  // The local incarnation is persisted across restarts (P5b) and bumped on
  // every boot: rejoining with the next incarnation makes the fresh Active
  // register dominate — and thereby refute — any suspect/offline rumor the
  // cluster still holds about this node from before the restart. A fresh node
  // resumes at 0, so its first rejoin lands on incarnation 1.
  let resumed_incarnation =
    crate::persisted_counter(node.meta(), LOCAL_INCARNATION_KEY).unwrap_or(0);
  let mut local_register = MemberRegister::new(
    &config.node.id,
    &config.node.address,
    resumed_incarnation,
    0,
  );
  local_register.rejoin(&config.node.address, now_ms());
  local_register.set_labels(node.local().labels().unwrap_or_default());
  local_register.set_annotations(node.local().annotations().unwrap_or_default());
  // Persist the rejoined incarnation immediately: the in-service persist hook
  // only writes on later changes, and crash-looping must keep advancing the
  // incarnation instead of reusing the same one on every boot.
  if let Err(error) = node.meta().set(
    LOCAL_INCARNATION_KEY,
    &local_register.incarnation().to_string(),
  ) {
    tracing::warn!(%error, "failed to persist rejoined local incarnation");
  }

  let membership_service = Arc::new(
    MembershipService::new(&config.node.id, SwimConfig::default(), local_register)
      .with_meta(node.meta().clone()),
  );

  // The extension manager is constructed before the mapper so the mapper can
  // fire its reconcile trigger on EXTENSION applies; the gossip handle only
  // exists after `ClusterSync` is built, so it is injected afterwards.
  let extension_manager = ExtensionManager::new(
    &config.extensions,
    storage.clone(),
    membership_service.clone(),
  )?;

  let mapper = ResourceMapper::new(storage.clone(), membership_service.clone())
    .with_extension_notify(extension_manager.notify());

  let pool = PeerPool::new(&tls_bundle, cluster_key.as_ref().map(ClusterKey::to_hex));
  let resources = ResourceSync::new(mapper.clone(), node.clone(), pool.clone());
  let cluster_sync = ClusterSync::new(
    config.node.id.clone(),
    membership_service.clone(),
    node.clone(),
    pool.clone(),
    resources.clone(),
  );
  let extension_manager = Arc::new(
    extension_manager
      .with_cluster_sync(cluster_sync.clone())
      .with_peer_pool(pool),
  );

  let cluster_service = ClusterService::new(membership_service.clone(), storage.clone(), mapper)
    .with_cluster_sync(cluster_sync.clone())
    .with_shutdown(shutdown_tx);
  let extension_service = ExtensionService::new(extension_manager.clone());

  let mut background = tokio::task::JoinSet::new();

  // Reconcile once at startup so selector-matching extensions serve before
  // the first periodic pass, then drive the reconcile loop (design section 6).
  extension_manager.reconcile().await;

  spawn_until_shutdown(&mut background, shutdown.clone(), {
    let extension_manager = extension_manager.clone();
    async move { extension_manager.run().await }
  });

  spawn_until_shutdown(&mut background, shutdown.clone(), {
    let cluster_sync = cluster_sync.clone();
    async move { cluster_sync.run(DEFAULT_SYNC_INTERVAL).await }
  });

  spawn_until_shutdown(&mut background, shutdown.clone(), {
    let cluster_sync = cluster_sync.clone();
    async move { cluster_sync.run_swim(DEFAULT_SWIM_INTERVAL).await }
  });

  spawn_until_shutdown(&mut background, shutdown.clone(), {
    let local_address = config.node.address.clone();
    async move {
      resources
        .run(DEFAULT_RESOURCE_SYNC_INTERVAL, local_address)
        .await
    }
  });

  let (sync_service, membership_service_rpc) = cluster_sync.servers();

  let addr: SocketAddr = config.cluster.listen_address.parse()?;
  tracing::info!(%addr, "node api server listening");

  let server_shutdown = shutdown.clone();
  // Authentication boundary (deliberate layering): the cluster-key
  // interceptor guards the admission plane — the Cluster service reached by
  // operators and joining members, and the Extension service whose invocations
  // (direct or forwarded) are admission-level operations (extension system
  // design, section 7). The Sync and Membership services are node-to-node;
  // node identity there comes from the mTLS handshake against the cluster CA,
  // so they carry no cluster-key check.
  // The message limits live on the inner servers, so they are applied before
  // wrapping them into the intercepted services (`with_interceptor` only
  // takes the raw inner service).
  let admission = crate::rpc::interceptor::cluster_key_interceptor(cluster_key.map(Arc::new));
  let cluster_server = lycoris_proto::node::cluster_server::ClusterServer::new(cluster_service)
    .max_decoding_message_size(lycoris_client::MAX_RPC_MESSAGE_BYTES)
    .max_encoding_message_size(lycoris_client::MAX_RPC_MESSAGE_BYTES);
  let cluster_server =
    tonic::service::interceptor::InterceptedService::new(cluster_server, admission.clone());
  let extension_server =
    lycoris_proto::node::extension_server::ExtensionServer::new(extension_service)
      .max_decoding_message_size(lycoris_client::MAX_RPC_MESSAGE_BYTES)
      .max_encoding_message_size(lycoris_client::MAX_RPC_MESSAGE_BYTES);
  let extension_server =
    tonic::service::interceptor::InterceptedService::new(extension_server, admission);
  let result = tonic::transport::Server::builder()
    .tls_config(server_tls)?
    .timeout(Duration::from_secs(30))
    .add_service(cluster_server)
    .add_service(extension_server)
    .add_service(sync_service)
    .add_service(membership_service_rpc)
    .serve_with_shutdown(addr, wait_shutdown(server_shutdown))
    .await;

  background.abort_all();
  while background.join_next().await.is_some() {}
  // Stop tracked fire-and-forget work (gossip forwarding, action dispatch)
  // alongside the periodic loops.
  cluster_sync.abort_tasks().await;

  result?;
  Ok(())
}

/// Spawn `task` onto `background`, cancelling it when the shutdown signal
/// fires. Every periodic daemon loop goes through this single wrapper.
fn spawn_until_shutdown(
  background: &mut tokio::task::JoinSet<()>, shutdown: watch::Receiver<bool>,
  task: impl std::future::Future<Output = ()> + Send + 'static,
) {
  background.spawn(async move {
    tokio::select! {
      _ = task => {}
      _ = wait_shutdown(shutdown) => {}
    }
  });
}

/// Merge the `[node] labels` config map into the node-local store with set
/// semantics: config keys overwrite stored values, keys the config does not
/// mention keep their stored values. Storage stays the single label source the
/// local register and selector evaluation read from.
fn apply_config_labels(
  local: &lycoris_storage::LocalStorage, labels: &std::collections::HashMap<String, String>,
) -> Result<(), RuntimeError> {
  for (key, value) in labels {
    local.set_label(key, value)?;
  }
  Ok(())
}

async fn wait_shutdown(mut shutdown: watch::Receiver<bool>) {
  while !*shutdown.borrow() {
    if shutdown.changed().await.is_err() {
      break;
    }
  }
}

fn write_client_config(config: &DaemonConfig, cluster_key: Option<&ClusterKey>) {
  let mut client_config = ClientConfig::from_daemon_config(config);
  // A daemon without a key records `None` — it has no key to point at. The
  // CLI may still fall back to the default key location; see
  // `ClientConfig::resolve_cluster_key_path` for why that is safe.
  if cluster_key.is_none() {
    client_config.cluster_key_path = None;
  }
  if let Some(path) = default_client_config_path() {
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

fn load_cluster_key(data_dir: &str) -> Option<ClusterKey> {
  let path = lycoris_core::cluster_key_path_in(std::path::Path::new(data_dir));
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

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use lycoris_storage::Storage;
  use tempfile::TempDir;

  use super::apply_config_labels;

  #[test]
  fn apply_config_labels_writes_new_keys() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    let labels = HashMap::from([("role".to_string(), "runner".to_string())]);

    apply_config_labels(storage.node().local(), &labels).unwrap();

    assert_eq!(storage.node().local().labels().unwrap(), labels);
  }

  #[test]
  fn apply_config_labels_overwrites_config_keys_and_keeps_the_rest() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    // A value stored out-of-band (previous config, older deployment) is
    // overwritten when the config names the key and kept when it does not.
    storage.node().local().set_label("role", "worker").unwrap();
    storage.node().local().set_label("zone", "eu").unwrap();
    let config_labels = HashMap::from([("role".to_string(), "runner".to_string())]);

    apply_config_labels(storage.node().local(), &config_labels).unwrap();

    assert_eq!(
      storage.node().local().labels().unwrap(),
      HashMap::from([
        ("role".to_string(), "runner".to_string()),
        ("zone".to_string(), "eu".to_string()),
      ])
    );
  }

  #[test]
  fn apply_config_labels_with_an_empty_map_changes_nothing() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    storage.node().local().set_label("zone", "eu").unwrap();

    apply_config_labels(storage.node().local(), &HashMap::new()).unwrap();

    assert_eq!(
      storage.node().local().labels().unwrap(),
      HashMap::from([("zone".to_string(), "eu".to_string())])
    );
  }
}
