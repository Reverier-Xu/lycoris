#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use lycoris_config::ConfigError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShellError {
  #[error("failed to install rustls crypto provider: {0:?}")]
  CryptoProvider(std::sync::Arc<rustls::crypto::CryptoProvider>),
  #[error("failed to create tokio runtime: {0}")]
  RuntimeCreation(std::io::Error),
  #[error("failed to load client configuration: {0}")]
  ConfigLoad(String),
  #[error("failed to load daemon configuration: {0}")]
  DaemonConfigLoad(ConfigError),
  #[error("no daemon configuration found")]
  ConfigNotFound,
  #[error("failed to load client TLS material: {0}")]
  TlsLoad(std::io::Error),
  #[error("failed to connect to {address}: {source}")]
  Connect {
    address: String,
    source: lycoris_api::ClusterClientError,
  },
  #[error("failed to list {kind}: {source}")]
  ListResources {
    kind: String,
    source: lycoris_api::ClusterClientError,
  },
  #[error("failed to get {kind} '{id}': {source}")]
  GetResource {
    kind: String,
    id: String,
    source: lycoris_api::ClusterClientError,
  },
  #[error("failed to describe {kind} '{id}': {source}")]
  DescribeResource {
    kind: String,
    id: String,
    source: lycoris_api::ClusterClientError,
  },
  #[error("{kind} '{id}' not found")]
  ResourceNotFound { kind: String, id: String },
  #[error("unknown resource kind '{0}'")]
  UnknownResourceKind(String),
  #[error("failed to register node: {0}")]
  Register(lycoris_api::ClusterClientError),
  #[error("failed to join cluster: {0}")]
  Join(lycoris_api::ClusterClientError),
  #[error("failed to leave cluster: {0}")]
  Leave(lycoris_api::ClusterClientError),
  #[error("failed to set primary endpoint: {0}")]
  SetPrimary(lycoris_api::ClusterClientError),
  #[error("cluster key error: {0}")]
  ClusterKey(lycoris_core::ClusterKeyError),
  #[error("no cluster key found; run 'lycoris cluster init' first")]
  ClusterKeyNotFound,
  #[error("invalid selector '{0}', expected key=value")]
  InvalidSelector(String),
  #[error("failed to start daemon: {0}")]
  DaemonStart(String),
  #[error("setup error: {0}")]
  Setup(String),
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
}

impl ShellError {
  pub fn setup(message: impl Into<String>) -> Self {
    Self::Setup(message.into())
  }
}

impl From<lycoris_core::ClusterKeyError> for ShellError {
  fn from(error: lycoris_core::ClusterKeyError) -> Self {
    Self::ClusterKey(error)
  }
}

impl From<ConfigError> for ShellError {
  fn from(error: ConfigError) -> Self {
    Self::DaemonConfigLoad(error)
  }
}
