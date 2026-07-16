#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod client;
mod daemon;
mod paths;

pub use client::{ClientConfig, ClientConfigError};
pub use daemon::{
  ClusterConfig, ConfigError, DaemonConfig, InvalidAddressError, NodeConfig, TlsConfig,
};
pub use paths::{
  CLIENT_CONFIG_FILE_NAME, DAEMON_CONFIG_FILE_NAME, config_dirs, default_client_config_path,
  default_daemon_config_path, default_data_dir, select_config_path, system_config_dir,
  user_config_dir, user_data_dir,
};
