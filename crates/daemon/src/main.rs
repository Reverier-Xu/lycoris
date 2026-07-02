use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use lycoris_config::{DaemonConfig, paths::default_daemon_config_path};

#[derive(Parser, Debug)]
#[command(name = "lycoris-daemon", version, about = "lycoris daemon")]
struct Args {
  /// Path to the daemon configuration file. Defaults to the user-specific
  /// configuration directory, falling back to the system-wide directory.
  #[arg(short, long)]
  config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  lycoris_daemon::install_crypto_provider()
    .map_err(|error| anyhow::anyhow!("failed to install rustls crypto provider: {error:?}"))?;
  tracing_subscriber::fmt::init();

  let args = Args::parse();
  let config_path = args
    .config
    .or_else(default_daemon_config_path)
    .context("could not determine configuration file path")?;

  let config = DaemonConfig::from_file(&config_path)
    .with_context(|| format!("failed to load config from {:?}", config_path))?;

  tracing::info!(
    node_id = %config.node.id,
    config_path = %config_path.display(),
    "starting lycoris daemon node"
  );
  lycoris_daemon::runtime::run(config).await?;

  Ok(())
}
