use lycoris_config::{
  ClientConfig, DaemonConfig, default_client_config_path, default_daemon_config_path,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ShellConfigError {
  #[error("client config not found and no daemon config available to derive one")]
  NotFound,
  #[error("failed to read client config: {0}")]
  Client(#[from] lycoris_config::ClientConfigError),
  #[error("failed to read daemon config: {0}")]
  Daemon(#[from] lycoris_config::ConfigError),
}

/// Load the CLI client configuration.
///
/// 1. If a client config file exists in the user-specific configuration
///    directory, use it.
/// 2. Otherwise, fall back to parsing the daemon configuration and deriving a
///    client configuration from it (same node address + TLS material).
pub(crate) fn load_client_config() -> Result<ClientConfig, ShellConfigError> {
  if let Some(path) = default_client_config_path()
    && path.is_file()
  {
    return ClientConfig::from_file(&path).map_err(Into::into);
  }

  let daemon_path = default_daemon_config_path().ok_or(ShellConfigError::NotFound)?;
  if !daemon_path.is_file() {
    return Err(ShellConfigError::NotFound);
  }

  let daemon = DaemonConfig::from_file(&daemon_path)?;
  Ok(ClientConfig::from_daemon_config(&daemon))
}
