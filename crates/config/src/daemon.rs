use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::{
  error::{ConfigError, InvalidAddressError},
  paths::{default_daemon_config_path, default_data_dir},
  validation::non_empty_string,
};

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
    config.validate()?;
    Ok(config)
  }

  /// Load the daemon configuration from an explicit `path`, or — when `None`
  /// — from the default configuration locations.
  ///
  /// A missing file at an explicit path surfaces as [`ConfigError::Io`]; a
  /// missing file in the default locations surfaces as
  /// [`ConfigError::NotFound`].
  pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
    match path {
      Some(path) => Self::from_file(path),
      None => {
        let path = default_daemon_config_path().ok_or(ConfigError::NotFound)?;
        if !path.is_file() {
          return Err(ConfigError::NotFound);
        }
        Self::from_file(&path)
      }
    }
  }

  fn validate(&self) -> Result<(), ConfigError> {
    validate_cluster_address(&self.node.address)?;
    for (index, peer) in self.cluster.bootstrap_peers.iter().enumerate() {
      validate_cluster_address(peer)
        .map_err(|source| ConfigError::InvalidPeerAddress { index, source })?;
    }
    Ok(())
  }

  /// Write the daemon configuration to a TOML file, creating parent directories
  /// if necessary.
  pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), ConfigError> {
    let parent = path.as_ref().parent();
    if let Some(parent) = parent {
      std::fs::create_dir_all(parent)?;
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
  #[serde(deserialize_with = "non_empty_string")]
  pub ca_cert: String,
  #[serde(deserialize_with = "non_empty_string")]
  pub ca_key: String,
  #[serde(deserialize_with = "non_empty_string")]
  pub cert: String,
  #[serde(deserialize_with = "non_empty_string")]
  pub key: String,
}

fn validate_cluster_address(address: &str) -> Result<(), InvalidAddressError> {
  if address.starts_with("https://") {
    Ok(())
  } else {
    Err(InvalidAddressError(address.to_string()))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  const VALID_TOML: &str = r#"
            data_dir = "data"

            [node]
            id = "node-01"
            address = "https://127.0.0.1:5001"

            [cluster]
            listen_address = "0.0.0.0:5001"
            bootstrap_peers = ["https://127.0.0.1:5002"]

            [tls]
            ca_cert = "certs/ca.crt"
            ca_key = "certs/ca.key"
            cert = "certs/node.crt"
            key = "certs/node.key"
        "#;

  #[test]
  fn parse_node_config() {
    let cfg: DaemonConfig = toml::from_str(VALID_TOML).unwrap();
    assert_eq!(cfg.node.id, "node-01");
    assert_eq!(cfg.cluster.bootstrap_peers.len(), 1);
    assert_eq!(cfg.data_dir, "data");
  }

  #[test]
  fn reject_empty_node_id() {
    let toml = VALID_TOML.replace("node-01", "");
    let result: Result<DaemonConfig, _> = toml::from_str(&toml);
    assert!(result.is_err());
  }

  #[test]
  fn reject_empty_tls_field() {
    let toml = VALID_TOML.replace("certs/ca.key", "");
    let result: Result<DaemonConfig, _> = toml::from_str(&toml);
    assert!(result.is_err());
  }

  #[test]
  fn reject_non_https_node_address() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("lycoris.toml");
    fs::write(
      &path,
      VALID_TOML.replace("https://127.0.0.1:5001\"", "http://127.0.0.1:5001\""),
    )
    .unwrap();
    let error = DaemonConfig::from_file(&path).unwrap_err();
    assert!(
      matches!(error, ConfigError::InvalidNodeAddress { .. }),
      "expected InvalidNodeAddress, got {error}"
    );
  }

  #[test]
  fn reject_non_https_bootstrap_peer_with_index() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("lycoris.toml");
    fs::write(
      &path,
      VALID_TOML.replace("https://127.0.0.1:5002", "http://127.0.0.1:5002"),
    )
    .unwrap();
    let error = DaemonConfig::from_file(&path).unwrap_err();
    match error {
      ConfigError::InvalidPeerAddress { index, source } => {
        assert_eq!(index, 0);
        assert_eq!(
          source.to_string(),
          "'http://127.0.0.1:5002' must start with https://"
        );
      }
      other => panic!("expected InvalidPeerAddress, got {other}"),
    }
  }
}
