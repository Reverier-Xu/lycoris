#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use clap::Parser;

pub(crate) mod cli;
pub(crate) mod commands;
pub(crate) mod error;

use cli::{Cli, ClusterCommand, Command, ExtCommand};
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
  // Logs go to stderr so stdout stays reserved for command output (which may
  // be piped). The default level is quiet; RUST_LOG overrides it.
  tracing_subscriber::fmt()
    .with_writer(std::io::stderr)
    .with_env_filter(
      tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
    )
    .init();

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
    ClusterCommand::Ext { command } => {
      let config = ClientConfig::load_default()?;
      match command {
        ExtCommand::Load { package } => commands::cluster::ext::ext_load(&config, &package).await,
        ExtCommand::Invoke {
          id,
          method,
          payload,
        } => commands::cluster::ext::ext_invoke(&config, &id, &method, payload).await,
      }
    }
  }
}
