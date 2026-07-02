use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
  name = "lycoris",
  version,
  about = "lycoris cluster command-line interface"
)]
pub struct Cli {
  #[command(subcommand)]
  pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
  /// Query cluster information.
  #[command(subcommand)]
  Cluster(ClusterCommand),
}

#[derive(Subcommand, Debug)]
pub enum ClusterCommand {
  /// List cluster nodes.
  Nodes {
    /// Label selector in the form `key=value`. Can be specified multiple
    /// times.
    #[arg(long = "selector", value_name = "KEY=VALUE")]
    selectors: Vec<String>,
  },
}
