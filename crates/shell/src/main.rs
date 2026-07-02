use anyhow::Context;
use clap::Parser;

mod cli;
mod commands;
mod config;

use cli::{Cli, ClusterCommand, Command};
use config::load_client_config;

fn main() -> anyhow::Result<()> {
  lycoris_api::install_crypto_provider()
    .map_err(|error| anyhow::anyhow!("failed to install rustls crypto provider: {error:?}"))?;
  tracing_subscriber::fmt::init();

  let cli = Cli::parse();
  let runtime = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;

  match cli.command {
    Command::Cluster(cluster) => {
      let client_config = load_client_config().with_context(
        || "failed to load client configuration; run a local daemon or create a client config",
      )?;
      runtime.block_on(async move {
        match cluster {
          ClusterCommand::Nodes { selectors } => {
            commands::cluster::list_nodes(&client_config, &selectors).await
          }
        }
      })?;
    }
  }

  Ok(())
}
