use std::{fs, path::Path};

use serde::Deserialize;
use thiserror::Error;

use crate::paths::default_data_dir;

/// Node bootstrap configuration.
///
/// This file only contains information that is specific to the current node and
/// required to join the cluster (identity, listen address, TLS material
/// location, data directory). All dynamic runtime state such as peer list,
/// primary endpoint, node labels/annotations, and peer reachability is stored
/// in the SQLite database under `data_dir`.
#[derive(Debug, Clone, Deserialize)]
pub struct DaemonConfig {
  pub node: NodeConfig,
  pub cluster: ClusterConfig,
  pub tls: TlsConfig,
  #[serde(default = "default_data_dir_string")]
  pub data_dir: String,
}

fn default_data_dir_string() -> String {
  default_data_dir().to_string_lossy().to_string()
}

impl DaemonConfig {
  pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
    let content = fs::read_to_string(path.as_ref())?;
    let config: DaemonConfig = toml::from_str(&content)?;
    config.validate()?;
    Ok(config)
  }

  fn validate(&self) -> Result<(), ConfigError> {
    if self.node.id.is_empty() {
      return Err(ConfigError::Invalid(
        "node.id must not be empty".to_string(),
      ));
    }
    if self.node.address.is_empty() {
      return Err(ConfigError::Invalid(
        "node.address must not be empty".to_string(),
      ));
    }
    if self.cluster.listen_address.is_empty() {
      return Err(ConfigError::Invalid(
        "cluster.listen_address must not be empty".to_string(),
      ));
    }
    if self.data_dir.is_empty() {
      return Err(ConfigError::Invalid(
        "data_dir must not be empty".to_string(),
      ));
    }
    Ok(())
  }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
  pub id: String,
  pub address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
  pub listen_address: String,
  /// Optional list of peers to seed on first startup. After bootstrap, peer
  /// state is maintained in the database.
  pub bootstrap_peers: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
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
  #[error("invalid config: {0}")]
  Invalid(String),
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
    let cfg: DaemonConfig = toml::from_str(toml).unwrap();
    assert!(cfg.validate().is_err());
  }
}
