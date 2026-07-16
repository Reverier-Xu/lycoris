use std::{path::PathBuf, process::Command};

use lycoris_config::{DaemonConfig, default_daemon_config_path};

use crate::error::ShellError;

/// Run the daemon in the current process.
///
/// This is the entry point used by `lycoris daemon` and by background-service
/// units. It loads the daemon configuration and hands off to the
/// `lycoris-daemon` runtime, which performs its own client-config and
/// cluster-key setup.
pub(crate) async fn run(config: Option<PathBuf>) -> Result<(), ShellError> {
  let config_path = config
    .or_else(default_daemon_config_path)
    .ok_or(ShellError::ConfigNotFound)?;
  let daemon_config =
    DaemonConfig::from_file(&config_path).map_err(ShellError::DaemonConfigLoad)?;

  lycoris_daemon::runtime::run(daemon_config)
    .await
    .map_err(|error| ShellError::DaemonStart(error.to_string()))
}

/// Spawn the daemon as a child process.
///
/// The child runs the same executable with the `daemon` subcommand, so it will
/// perform its own client-config and cluster-key setup.
pub(crate) fn spawn(config: Option<PathBuf>) -> Result<std::process::Child, ShellError> {
  let config_path = config
    .or_else(default_daemon_config_path)
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
