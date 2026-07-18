#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod client;
mod daemon;
mod error;
mod paths;
mod toml_file;
mod validation;

pub use client::ClientConfig;
pub use daemon::{ClusterConfig, DaemonConfig, NodeConfig, TlsConfig};
pub use error::{ConfigError, InvalidAddressError};
pub use paths::{
  DAEMON_CONFIG_FILE_NAME, default_client_config_path, default_daemon_config_path, user_config_dir,
  user_data_dir,
};
