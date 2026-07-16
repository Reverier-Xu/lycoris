#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use clap::Parser;

pub(crate) mod cli;
pub(crate) mod commands;
pub(crate) mod config;
pub(crate) mod error;

use cli::{Cli, ClusterCommand, Command};
use config::load_client_config;
use error::ShellError;

fn main() -> Result<(), ShellError> {
  lycoris_tls::install_crypto_provider().map_err(ShellError::CryptoProvider)?;
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
            ClusterCommand::Register { id, address, key } => {
              commands::cluster::register(&client_config, id, address, key).await
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
    Command::Daemon { config } => {
      let runtime = tokio::runtime::Runtime::new().map_err(ShellError::RuntimeCreation)?;
      runtime.block_on(commands::daemon::run(config))?;
    }
    Command::Start { config } => {
      let child = commands::daemon::spawn(config)?;
      println!("daemon started with pid {}", child.id());
    }
    Command::Setup => commands::setup::run()?,
  }

  Ok(())
}
