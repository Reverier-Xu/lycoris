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

  /// Install lycoris-server as a user-mode systemd service.
  #[cfg(target_os = "linux")]
  Setup {
    /// Name of the server binary to look for next to the lycoris binary.
    #[arg(long, default_value = "lycoris-server")]
    binary_name: String,
  },
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
