#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ShellError {
  #[error("failed to install rustls crypto provider: {0:?}")]
  CryptoProvider(std::sync::Arc<rustls::crypto::CryptoProvider>),
  #[error("failed to create tokio runtime: {0}")]
  RuntimeCreation(std::io::Error),
  #[error("failed to load configuration: {0}")]
  ConfigLoad(#[from] lycoris_config::ConfigError),
  #[error("failed to load client TLS material: {0}")]
  TlsLoad(#[from] lycoris_tls::TlsError),
  #[error("failed to connect to {address}: {source}")]
  Connect {
    address: String,
    source: lycoris_client::ClientError,
  },
  #[error("failed to list {kind}: {source}")]
  ListResources {
    kind: String,
    source: lycoris_client::ClientError,
  },
  #[error("failed to get {kind} '{id}': {source}")]
  GetResource {
    kind: String,
    id: String,
    source: lycoris_client::ClientError,
  },
  #[error("{kind} '{id}' not found")]
  ResourceNotFound { kind: String, id: String },
  #[error("unknown resource kind '{0}'")]
  UnknownResourceKind(String),
  #[error("unknown scope '{0}', expected 'shared' or 'local'")]
  UnknownScope(String),
  #[error("failed to register node: {0}")]
  Register(lycoris_client::ClientError),
  #[error("failed to join cluster: {0}")]
  Join(lycoris_client::ClientError),
  #[error("failed to leave cluster: {0}")]
  Leave(lycoris_client::ClientError),
  #[error("failed to set primary endpoint: {0}")]
  SetPrimary(lycoris_client::ClientError),
  #[error("cluster key error: {0}")]
  ClusterKey(#[from] lycoris_core::ClusterKeyError),
  #[error("no cluster key found; run 'lycoris cluster init' first")]
  ClusterKeyNotFound,
  #[error("invalid selector '{0}', expected key=value")]
  InvalidSelector(String),
  #[error("failed to spawn daemon process: {0}")]
  DaemonSpawn(String),
  #[error("daemon runtime failed: {0}")]
  DaemonRuntime(#[from] lycoris_daemon::runtime::RuntimeError),
  #[error("setup error: {0}")]
  Setup(String),
}

impl ShellError {
  pub(crate) fn setup(message: impl Into<String>) -> Self {
    Self::Setup(message.into())
  }
}
