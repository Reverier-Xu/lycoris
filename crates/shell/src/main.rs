#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

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

  match cli.command {
    Command::Cluster(cluster) => {
      let runtime = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
      let client_config = load_client_config().with_context(
        || "failed to load client configuration; run a local daemon or create a client config",
      )?;
      runtime.block_on(async move {
        match cluster {
          ClusterCommand::Nodes { selectors } => {
            commands::cluster::list_nodes(&client_config, &selectors).await
          }
          ClusterCommand::Register { id, address } => {
            commands::cluster::register(&client_config, id, address).await
          }
        }
      })?;
    }
    Command::Setup { binary_name } => commands::setup::run(&binary_name)?,
  }

  Ok(())
}
