#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use std::path::PathBuf;

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
    Command::Cluster(cluster) => match cluster {
      ClusterCommand::Init { key } => commands::cluster::init_cluster(key)?,
      ClusterCommand::Key => commands::cluster::show_key()?,
      other => {
        let runtime = tokio::runtime::Runtime::new().map_err(ShellError::RuntimeCreation)?;
        let client_config =
          load_client_config().map_err(|error| ShellError::ConfigLoad(error.to_string()))?;
        runtime.block_on(async move {
          match other {
            ClusterCommand::Get {
              resource,
              name,
              selectors,
              scope,
            } => {
              commands::cluster::get_resources(&client_config, &resource, name, &selectors, scope)
                .await
            }
            ClusterCommand::Describe { resource, name } => {
              commands::cluster::describe_resource(&client_config, &resource, &name).await
            }
            ClusterCommand::Register { id, address } => {
              commands::cluster::register(&client_config, id, address).await
            }
            ClusterCommand::Join { peer, key } => {
              commands::cluster::join_cluster(&client_config, peer, key).await
            }
            ClusterCommand::Leave => commands::cluster::leave_cluster(&client_config).await,
            ClusterCommand::Init { .. } | ClusterCommand::Key => unreachable!(),
          }
        })?;
      }
    },
    Command::Daemon { config } => run_daemon(config)?,
    Command::Setup => commands::setup::run()?,
  }

  Ok(())
}

fn run_daemon(config: Option<PathBuf>) -> Result<(), ShellError> {
  let runtime = tokio::runtime::Runtime::new().map_err(ShellError::RuntimeCreation)?;
  let config_path = config
    .or_else(lycoris_core::paths::default_daemon_config_path)
    .ok_or(ShellError::ConfigNotFound)?;
  let daemon_config =
    lycoris_config::DaemonConfig::from_file(&config_path).map_err(ShellError::DaemonConfigLoad)?;

  runtime.block_on(async move {
    lycoris_daemon::runtime::run(daemon_config)
      .await
      .map_err(|error| ShellError::Setup(error.to_string()))
  })
}
