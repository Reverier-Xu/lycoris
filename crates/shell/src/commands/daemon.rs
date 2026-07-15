use std::{path::PathBuf, process::Command};

use lycoris_config::{ClientConfig, DaemonConfig};
use lycoris_core::{ClusterKey, default_cluster_key_path, paths};

use crate::error::ShellError;

/// Run the daemon in the current process.
///
/// This is the entry point used by `lycoris daemon` and by background-service
/// units. It prepares the client configuration and cluster key, then hands off
/// to `lycoris-daemon` runtime.
pub async fn run(config: Option<PathBuf>) -> Result<(), ShellError> {
  let config_path = config
    .or_else(paths::default_daemon_config_path)
    .ok_or(ShellError::ConfigNotFound)?;
  let daemon_config =
    DaemonConfig::from_file(&config_path).map_err(ShellError::DaemonConfigLoad)?;

  write_client_config(&daemon_config);
  let cluster_key = load_cluster_key();

  lycoris_daemon::runtime::run(daemon_config, cluster_key)
    .await
    .map_err(|error| ShellError::DaemonStart(error.to_string()))
}

/// Spawn the daemon as a child process.
///
/// The child runs the same executable with the `daemon` subcommand, so it will
/// perform its own client-config and cluster-key setup.
pub fn spawn(config: Option<PathBuf>) -> Result<std::process::Child, ShellError> {
  let config_path = config
    .or_else(paths::default_daemon_config_path)
    .ok_or(ShellError::ConfigNotFound)?;
  let exe = std::env::current_exe().map_err(|error| {
    ShellError::DaemonStart(format!("failed to locate current executable: {error}"))
  })?;

  Command::new(exe)
    .arg("daemon")
    .arg("--config")
    .arg(config_path)
    .spawn()
    .map_err(|error| ShellError::DaemonStart(format!("failed to spawn daemon: {error}")))
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
