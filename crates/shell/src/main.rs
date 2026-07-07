#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use clap::Parser;

mod cli;
mod commands;
mod config;
mod error;

use cli::{Cli, ClusterCommand, Command};
use config::load_client_config;
use error::ShellError;

fn main() -> Result<(), ShellError> {
  lycoris_api::install_crypto_provider().map_err(ShellError::CryptoProvider)?;
  tracing_subscriber::fmt::init();

  let cli = Cli::parse();

  match cli.command {
    Command::Cluster(cluster) => {
      let runtime = tokio::runtime::Runtime::new().map_err(ShellError::RuntimeCreation)?;
      let client_config =
        load_client_config().map_err(|error| ShellError::ConfigLoad(error.to_string()))?;
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
