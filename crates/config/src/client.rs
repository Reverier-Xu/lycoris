use std::{fs, path::Path};

use lycoris_core::non_empty_string;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::daemon::DaemonConfig;

fn daemon_cluster_key_path(daemon: &DaemonConfig) -> String {
  Path::new(&daemon.data_dir)
    .join("cluster.key")
    .to_string_lossy()
    .to_string()
}

/// Client configuration used by the `lycoris` CLI to talk to a daemon node.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
  /// gRPC API endpoint of the daemon node, e.g. `https://127.0.0.1:5001`.
  #[serde(deserialize_with = "non_empty_string")]
  pub api_address: String,
  /// Path to the CA certificate used to verify the daemon's TLS identity.
  #[serde(deserialize_with = "non_empty_string")]
  pub ca_cert: String,
  /// Path to the client certificate used for mutual TLS.
  #[serde(deserialize_with = "non_empty_string")]
  pub cert: String,
  /// Path to the client private key used for mutual TLS.
  #[serde(deserialize_with = "non_empty_string")]
  pub key: String,
  /// Path to the cluster key file used to authenticate `Cluster` RPCs.
  ///
  /// When present, the CLI loads the key from this path and attaches it as
  /// metadata on every `Cluster` request.
  pub cluster_key_path: Option<String>,
}

impl ClientConfig {
  pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ClientConfigError> {
    let content = fs::read_to_string(path.as_ref())?;
    let config: ClientConfig = toml::from_str(&content)?;
    Ok(config)
  }

  pub fn from_daemon_config(daemon: &DaemonConfig) -> Self {
    Self {
      api_address: daemon.node.address.clone(),
      ca_cert: daemon.tls.ca_cert.clone(),
      cert: daemon.tls.cert.clone(),
      key: daemon.tls.key.clone(),
      cluster_key_path: Some(daemon_cluster_key_path(daemon)),
    }
  }

  /// Write the client configuration to a TOML file, creating parent directories
  /// if necessary.
  pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), ClientConfigError> {
    if let Some(parent) = path.as_ref().parent() {
      fs::create_dir_all(parent)?;
    }
    fs::write(path.as_ref(), toml::to_string_pretty(self)?)?;
    Ok(())
  }
}

#[derive(Debug, Error)]
pub enum ClientConfigError {
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("parse error: {0}")]
  Parse(#[from] toml::de::Error),
  #[error("serialize error: {0}")]
  Serialize(#[from] toml::ser::Error),
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;
  use crate::daemon::{ClusterConfig, NodeConfig, TlsConfig};

  fn sample_daemon_config() -> DaemonConfig {
    DaemonConfig {
      node: NodeConfig {
        id: "node-1".to_string(),
        address: "https://127.0.0.1:5001".to_string(),
      },
      cluster: ClusterConfig {
        listen_address: "0.0.0.0:5001".to_string(),
        bootstrap_peers: vec![],
      },
      tls: TlsConfig {
        ca_cert: "ca.crt".to_string(),
        ca_key: "ca.key".to_string(),
        cert: "node.crt".to_string(),
        key: "node.key".to_string(),
      },
      data_dir: "/var/lib/lycoris".to_string(),
    }
  }

  #[test]
  fn client_config_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("lycoris.client.conf");
    let original = ClientConfig::from_daemon_config(&sample_daemon_config());
    original.write_to_file(&path).unwrap();
    let loaded = ClientConfig::from_file(&path).unwrap();
    assert_eq!(loaded.api_address, original.api_address);
    assert_eq!(loaded.ca_cert, original.ca_cert);
    assert_eq!(
      loaded.cluster_key_path,
      Some("/var/lib/lycoris/cluster.key".to_string())
    );
  }

  #[test]
  fn reject_empty_api_address() {
    let toml = r#"
      api_address = ""
      ca_cert = "ca.crt"
      cert = "node.crt"
      key = "node.key"
    "#;
    let result: Result<ClientConfig, _> = toml::from_str(toml);
    assert!(result.is_err());
  }
}
