#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use clap::Parser;

pub(crate) mod cli;
pub(crate) mod commands;
pub(crate) mod error;

use cli::{Cli, ClusterCommand, Command};
use error::ShellError;
use lycoris_config::ClientConfig;

fn main() {
  if let Err(error) = run() {
    eprintln!("error: {error}");
    std::process::exit(1);
  }
}

fn run() -> Result<(), ShellError> {
  lycoris_tls::install_crypto_provider().map_err(ShellError::CryptoProvider)?;
  tracing_subscriber::fmt::init();

  let command = Cli::parse().command;
  tokio::runtime::Runtime::new()
    .map_err(ShellError::RuntimeCreation)?
    .block_on(dispatch(command))
}

/// Single dispatch for every CLI command; synchronous commands simply
/// complete inline inside the async context.
async fn dispatch(command: Command) -> Result<(), ShellError> {
  match command {
    Command::Cluster(cluster) => dispatch_cluster(cluster).await,
    Command::Daemon(args) => commands::daemon::run(args.config).await,
    Command::Start(args) => {
      let child = commands::daemon::spawn(args.config)?;
      println!("daemon started with pid {}", child.id());
      Ok(())
    }
    Command::Setup => commands::setup::run(),
  }
}

async fn dispatch_cluster(command: ClusterCommand) -> Result<(), ShellError> {
  match command {
    ClusterCommand::Init { key } => commands::cluster::init_cluster(key),
    ClusterCommand::Key => commands::cluster::show_key(),
    ClusterCommand::Get {
      resource,
      name,
      selectors,
      scope,
    } => {
      let config = ClientConfig::load_default()?;
      commands::cluster::get_resources(&config, &resource, name, &selectors, scope).await
    }
    ClusterCommand::Register { id, address, key } => {
      let config = ClientConfig::load_default()?;
      commands::cluster::register(&config, id, address, key).await
    }
    ClusterCommand::Join { peer, key } => {
      let config = ClientConfig::load_default()?;
      commands::cluster::join_cluster(&config, peer, key).await
    }
    ClusterCommand::Leave => {
      let config = ClientConfig::load_default()?;
      commands::cluster::leave_cluster(&config).await
    }
  }
}
