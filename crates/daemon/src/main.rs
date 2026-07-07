#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use std::path::PathBuf;

use clap::Parser;
use lycoris_config::{DaemonConfig, paths::default_daemon_config_path};
use thiserror::Error;

#[derive(Parser, Debug)]
#[command(name = "lycoris-daemon", version, about = "lycoris daemon")]
struct Args {
  /// Path to the daemon configuration file. Defaults to the user-specific
  /// configuration directory, falling back to the system-wide directory.
  #[arg(short, long)]
  config: Option<PathBuf>,
}

#[derive(Debug, Error)]
enum MainError {
  #[error("failed to install rustls crypto provider: {0:?}")]
  CryptoProvider(std::sync::Arc<rustls::crypto::CryptoProvider>),
  #[error("could not determine configuration file path")]
  MissingConfigPath,
  #[error("failed to load config: {0}")]
  Config(#[from] lycoris_config::ConfigError),
  #[error("runtime error: {0}")]
  Runtime(#[from] lycoris_daemon::runtime::RuntimeError),
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
  lycoris_api::install_crypto_provider().map_err(MainError::CryptoProvider)?;
  tracing_subscriber::fmt::init();

  let args = Args::parse();
  let config_path = args
    .config
    .or_else(default_daemon_config_path)
    .ok_or(MainError::MissingConfigPath)?;

  let config = DaemonConfig::from_file(&config_path)?;

  tracing::info!(
    node_id = %config.node.id,
    config_path = %config_path.display(),
    "starting lycoris daemon node"
  );
  lycoris_daemon::runtime::run(config).await?;

  Ok(())
}
