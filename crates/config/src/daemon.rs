use std::{fs, path::Path};

use lycoris_core::{
  paths::{default_data_dir, ensure_dir},
  validation::non_empty_string,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Node bootstrap configuration.
///
/// This file only contains information that is specific to the current node and
/// required to join the cluster (identity, listen address, TLS material
/// location, data directory). All dynamic runtime state such as peer list,
/// primary endpoint, node labels/annotations, and peer reachability is stored
/// in the SQLite database under `data_dir`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
  pub node: NodeConfig,
  pub cluster: ClusterConfig,
  pub tls: TlsConfig,
  #[serde(
    default = "default_data_dir_string",
    deserialize_with = "non_empty_string"
  )]
  pub data_dir: String,
}

fn default_data_dir_string() -> String {
  default_data_dir().to_string_lossy().to_string()
}

impl DaemonConfig {
  pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
    let content = fs::read_to_string(path.as_ref())?;
    let config: DaemonConfig = toml::from_str(&content)?;
    Ok(config)
  }

  /// Write the daemon configuration to a TOML file, creating parent directories
  /// if necessary.
  pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), ConfigError> {
    let parent = path.as_ref().parent();
    if let Some(parent) = parent {
      ensure_dir(parent)?;
    }
    fs::write(path.as_ref(), toml::to_string_pretty(self)?)?;
    Ok(())
  }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
  #[serde(deserialize_with = "non_empty_string")]
  pub id: String,
  #[serde(deserialize_with = "non_empty_string")]
  pub address: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterConfig {
  #[serde(deserialize_with = "non_empty_string")]
  pub listen_address: String,
  /// Optional list of peers to seed on first startup. After bootstrap, peer
  /// state is maintained in the database.
  pub bootstrap_peers: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TlsConfig {
  pub ca_cert: String,
  pub ca_key: String,
  pub cert: String,
  pub key: String,
}

#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("parse error: {0}")]
  Parse(#[from] toml::de::Error),
  #[error("serialize error: {0}")]
  Serialize(#[from] toml::ser::Error),
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parse_node_config() {
    let toml = r#"
            data_dir = "data"

            [node]
            id = "node-01"
            address = "127.0.0.1:5001"

            [cluster]
            listen_address = "0.0.0.0:5001"
            bootstrap_peers = ["https://127.0.0.1:5002"]

            [tls]
            ca_cert = "certs/ca.crt"
            ca_key = "certs/ca.key"
            cert = "certs/node.crt"
            key = "certs/node.key"
        "#;
    let cfg: DaemonConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.node.id, "node-01");
    assert_eq!(cfg.cluster.bootstrap_peers.len(), 1);
    assert_eq!(cfg.data_dir, "data");
  }

  #[test]
  fn reject_empty_node_id() {
    let toml = r#"
            data_dir = "data"

            [node]
            id = ""
            address = "127.0.0.1:5001"

            [cluster]
            listen_address = "0.0.0.0:5001"
            bootstrap_peers = []

            [tls]
            ca_cert = "c"
            ca_key = "ck"
            cert = "c"
            key = "k"
        "#;
    let result: Result<DaemonConfig, _> = toml::from_str(toml);
    assert!(result.is_err());
  }
}
