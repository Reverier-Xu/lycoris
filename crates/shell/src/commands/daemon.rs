use std::path::PathBuf;

use lycoris_config::DaemonConfig;

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
