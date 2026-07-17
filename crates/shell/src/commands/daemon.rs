use std::{path::PathBuf, process::Command};

use lycoris_config::{ConfigError, DaemonConfig, default_daemon_config_path};

use crate::error::ShellError;

/// Run the daemon in the current process.
///
/// This is the entry point used by `lycoris daemon` and by background-service
/// units. It loads the daemon configuration and hands off to the
/// `lycoris-daemon` runtime, which performs its own client-config and
/// cluster-key setup.
pub(crate) async fn run(config: Option<PathBuf>) -> Result<(), ShellError> {
  let daemon_config = DaemonConfig::load(config.as_deref())?;
  lycoris_daemon::runtime::run(daemon_config).await?;
  Ok(())
}

/// Spawn the daemon as a child process.
///
/// The child runs the same executable with the `daemon` subcommand, so it will
/// perform its own client-config and cluster-key setup.
pub(crate) fn spawn(config: Option<PathBuf>) -> Result<std::process::Child, ShellError> {
  let config_path = config
    .or_else(default_daemon_config_path)
    .ok_or(ConfigError::NotFound)?;
  let exe = std::env::current_exe().map_err(|error| {
    ShellError::DaemonSpawn(format!("failed to locate current executable: {error}"))
  })?;

  Command::new(exe)
    .arg("daemon")
    .arg("--config")
    .arg(config_path)
    .spawn()
    .map_err(|error| ShellError::DaemonSpawn(format!("failed to spawn daemon: {error}")))
}
