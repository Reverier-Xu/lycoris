#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod cluster_sync;
pub mod membership;
pub mod rpc;
pub mod runtime;
pub mod tls;

pub use lycoris_config::{DaemonConfig, paths};
pub use lycoris_storage;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("config error: {0}")]
  Config(#[from] lycoris_config::ConfigError),
  #[error("tls error: {0}")]
  Tls(#[from] tls::TlsError),
  #[error("runtime error: {0}")]
  Runtime(#[from] runtime::RuntimeError),
}
