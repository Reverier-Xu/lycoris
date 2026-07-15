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

  /// Run the lycoris daemon in the foreground.
  Daemon {
    /// Path to the daemon configuration file.
    #[arg(short, long)]
    config: Option<std::path::PathBuf>,
  },

  /// Start the lycoris daemon as a background child process.
  Start {
    /// Path to the daemon configuration file.
    #[arg(short, long)]
    config: Option<std::path::PathBuf>,
  },

  /// Install lycoris as a user-mode background service.
  Setup,
}

#[derive(Subcommand, Debug)]
pub enum ClusterCommand {
  /// List or get cluster resources.
  Get {
    /// Resource kind, e.g. `nodes`, `skills`, `sessions`.
    resource: String,
    /// Optional resource id. If omitted, all matching resources are listed.
    name: Option<String>,
    /// Label selector in the form `key=value`. Can be specified multiple times.
    #[arg(short, long = "selector", value_name = "KEY=VALUE")]
    selectors: Vec<String>,
    /// Scope filter for skills and rules (`shared` or `local`).
    #[arg(long, value_name = "SCOPE")]
    scope: Option<String>,
  },

  /// Show detailed information about a single resource.
  Describe {
    /// Resource kind, e.g. `node`, `skill`, `session`.
    resource: String,
    /// Unique resource id.
    name: String,
  },

  /// Register a new node with the cluster.
  Register {
    /// Unique node id.
    #[arg(long)]
    id: String,
    /// Node address in host:port form.
    #[arg(long)]
    address: String,
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
    /// Cluster shared key in hex.
    #[arg(long)]
    key: String,
  },

  /// Leave the cluster.
  Leave,

  /// Print the current cluster key.
  Key,
}
