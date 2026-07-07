#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShellError {
  #[error("failed to install rustls crypto provider: {0:?}")]
  CryptoProvider(std::sync::Arc<rustls::crypto::CryptoProvider>),
  #[error("failed to create tokio runtime: {0}")]
  RuntimeCreation(std::io::Error),
  #[error("failed to load client configuration: {0}")]
  ConfigLoad(String),
  #[error("failed to load client TLS material: {0}")]
  TlsLoad(std::io::Error),
  #[error("failed to connect to {address}: {source}")]
  Connect {
    address: String,
    source: lycoris_api::ClusterClientError,
  },
  #[error("failed to list cluster nodes: {0}")]
  ListNodes(lycoris_api::ClusterClientError),
  #[error("failed to register node: {0}")]
  Register(lycoris_api::ClusterClientError),
  #[error("invalid selector '{0}', expected key=value")]
  InvalidSelector(String),
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
