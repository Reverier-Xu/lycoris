use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
  name = "lycoris",
  version,
  about = "lycoris cluster command-line interface"
)]
pub(crate) struct Cli {
  #[command(subcommand)]
  pub(crate) command: Command,
}

/// Shared argument for commands that take a daemon configuration file.
#[derive(Args, Debug)]
pub(crate) struct ConfigArg {
  /// Path to the daemon configuration file.
  #[arg(short, long)]
  pub(crate) config: Option<std::path::PathBuf>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
  /// Inspect and manage cluster resources and membership.
  #[command(subcommand)]
  Cluster(ClusterCommand),

  /// Run the lycoris daemon in the foreground.
  Daemon(ConfigArg),

  /// Install lycoris as a user-mode background service.
  Setup,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ClusterCommand {
  /// List or get cluster resources.
  Get {
    /// Resource kind, e.g. `nodes`, `skills`, `sessions`.
    resource: String,
    /// Optional resource id. If omitted, all matching resources are listed.
    name: Option<String>,
    /// Label selector in the form `key=value`. Can be specified multiple times.
    #[arg(short, long = "selector", value_name = "KEY=VALUE")]
    selectors: Vec<String>,
    /// Scope filter (`shared` or `local`).
    #[arg(long, value_name = "SCOPE")]
    scope: Option<String>,
  },

  /// Register a new node with the cluster.
  Register {
    /// Unique node id.
    #[arg(long)]
    id: String,
    /// Node address in host:port form.
    #[arg(long)]
    address: String,
    /// Cluster shared key in hex. Falls back to the local cluster key file
    /// when omitted, so the key does not have to appear on the command line.
    #[arg(long)]
    key: Option<String>,
  },

  /// Initialize this machine as a new cluster, generating or storing a
  /// cluster key.
  Init {
    /// Optional 32-byte cluster key in hex. If omitted, a random key is
    /// generated.
    #[arg(long)]
    key: Option<String>,
  },

  /// Join an existing cluster by contacting one of its members.
  Join {
    /// Address of an existing cluster member.
    #[arg(long)]
    peer: String,
    /// Cluster shared key in hex. Falls back to the local cluster key file
    /// when omitted, so the key does not have to appear on the command line.
    #[arg(long)]
    key: Option<String>,
  },

  /// Leave the cluster.
  Leave,

  /// Print the current cluster key.
  Key,
}

#[cfg(test)]
mod tests {
  use clap::CommandFactory;

  use super::Cli;

  #[test]
  fn cli_definition_is_valid() {
    Cli::command().debug_assert();
  }
}
