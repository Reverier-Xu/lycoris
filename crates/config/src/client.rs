use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
  daemon::DaemonConfig,
  error::ConfigError,
  paths::default_client_config_path,
  validation::{non_empty_string, validate_https_address},
};

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
  pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
    let config: ClientConfig = crate::toml_file::read(path.as_ref())?;
    config.validate()?;
    Ok(config)
  }

  fn validate(&self) -> Result<(), ConfigError> {
    validate_https_address(&self.api_address)?;
    Ok(())
  }

  /// Load the client configuration from the default locations.
  ///
  /// 1. If a client configuration file exists in the user-specific
  ///    configuration directory, use it.
  /// 2. Otherwise derive one from the daemon configuration (same node address
  ///    and TLS material), so a machine that only runs a daemon still yields a
  ///    working CLI configuration.
  ///
  /// Returns [`ConfigError::NotFound`] when neither source exists. This is
  /// the single implementation of the load-or-derive policy, shared by the
  /// CLI and anything else that needs a client configuration.
  pub fn load_default() -> Result<Self, ConfigError> {
    Self::load_or_derive(default_client_config_path().as_deref(), None)
  }

  /// The load-or-derive policy with the lookup locations made explicit, so
  /// tests can exercise it without touching the real default directories.
  fn load_or_derive(
    client_path: Option<&Path>, daemon_path: Option<&Path>,
  ) -> Result<Self, ConfigError> {
    if let Some(path) = client_path
      && path.is_file()
    {
      return Self::from_file(path);
    }
    Ok(Self::from_daemon_config(&DaemonConfig::load(daemon_path)?))
  }

  /// Derive a client configuration from a daemon configuration: the daemon's
  /// own address and TLS material are what the CLI should use to reach it.
  pub fn from_daemon_config(daemon: &DaemonConfig) -> Self {
    Self {
      api_address: daemon.node.address.clone(),
      ca_cert: daemon.tls.ca_cert.clone(),
      cert: daemon.tls.cert.clone(),
      key: daemon.tls.key.clone(),
      cluster_key_path: Some(
        lycoris_core::cluster_key_path_in(Path::new(&daemon.data_dir))
          .to_string_lossy()
          .to_string(),
      ),
    }
  }

  /// Resolve the cluster key file to authenticate `Cluster` RPCs with.
  ///
  /// An explicit `cluster_key_path` wins when the file exists; otherwise the
  /// conventional key location in the default data directory is tried. The
  /// fallback covers the common flow where `lycoris cluster init` wrote the
  /// key to the default location before any client configuration existed, or
  /// where the daemon's `data_dir` points elsewhere.
  ///
  /// Note the deliberate asymmetry with the daemon: a daemon that found no
  /// key writes `cluster_key_path = None`, yet this lookup may still pick up
  /// a default-location key. That is safe — authorization is enforced by the
  /// server-side interceptor, so a mismatched key is rejected at RPC time —
  /// and convenient, since the key the user just initialized is found without
  /// extra configuration.
  pub fn resolve_cluster_key_path(&self) -> Option<PathBuf> {
    self.resolve_cluster_key_path_with(&lycoris_core::default_cluster_key_path())
  }

  /// [`Self::resolve_cluster_key_path`] with the fallback location made
  /// explicit, so tests can exercise the fallback without touching the real
  /// default data directory.
  fn resolve_cluster_key_path_with(&self, fallback: &Path) -> Option<PathBuf> {
    self
      .cluster_key_path
      .as_ref()
      .map(PathBuf::from)
      .filter(|path| path.is_file())
      .or_else(|| fallback.is_file().then(|| fallback.to_path_buf()))
  }

  /// Write the client configuration to a TOML file, creating parent directories
  /// if necessary.
  pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), ConfigError> {
    crate::toml_file::write(self, path.as_ref())
  }
}

#[cfg(test)]
mod tests {
  use std::fs;

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
      Some(
        std::path::Path::new("/var/lib/lycoris")
          .join("cluster.key")
          .to_string_lossy()
          .into_owned()
      )
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

  #[test]
  fn reject_non_https_api_address() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("lycoris.client.conf");
    fs::write(
      &path,
      r#"
        api_address = "http://127.0.0.1:5001"
        ca_cert = "ca.crt"
        cert = "node.crt"
        key = "node.key"
      "#,
    )
    .unwrap();
    let error = ClientConfig::from_file(&path).unwrap_err();
    assert!(
      matches!(error, ConfigError::InvalidNodeAddress { .. }),
      "expected InvalidNodeAddress, got {error}"
    );
  }

  #[test]
  fn resolve_cluster_key_path_prefers_explicit_path() {
    let dir = TempDir::new().unwrap();
    let key_path = dir.path().join("cluster.key");
    fs::write(&key_path, "aa").unwrap();

    let mut config = ClientConfig::from_daemon_config(&sample_daemon_config());
    config.cluster_key_path = Some(key_path.to_string_lossy().to_string());
    assert_eq!(config.resolve_cluster_key_path(), Some(key_path));
  }

  #[test]
  fn resolve_cluster_key_path_falls_back_to_default_location() {
    let dir = TempDir::new().unwrap();
    let fallback = dir.path().join("default/cluster.key");
    fs::create_dir_all(fallback.parent().unwrap()).unwrap();
    fs::write(&fallback, "aa").unwrap();

    // An explicit path that does not exist loses to the default location.
    let mut config = ClientConfig::from_daemon_config(&sample_daemon_config());
    config.cluster_key_path = Some(dir.path().join("missing.key").to_string_lossy().to_string());
    assert_eq!(
      config.resolve_cluster_key_path_with(&fallback),
      Some(fallback.clone())
    );

    // No explicit path at all falls back the same way.
    config.cluster_key_path = None;
    assert_eq!(
      config.resolve_cluster_key_path_with(&fallback),
      Some(fallback)
    );

    // Neither source exists: nothing to resolve.
    let missing = dir.path().join("none.key");
    assert_eq!(config.resolve_cluster_key_path_with(&missing), None);
  }

  #[test]
  fn load_or_derive_prefers_existing_client_file() {
    let dir = TempDir::new().unwrap();
    let client_path = dir.path().join("lycoris.client.conf");
    let original = ClientConfig::from_daemon_config(&sample_daemon_config());
    original.write_to_file(&client_path).unwrap();

    let loaded = ClientConfig::load_or_derive(Some(&client_path), None).unwrap();
    assert_eq!(loaded.api_address, original.api_address);
  }

  #[test]
  fn load_or_derive_falls_back_to_daemon_config() {
    let dir = TempDir::new().unwrap();
    let daemon_path = dir.path().join("lycoris.toml");
    sample_daemon_config().write_to_file(&daemon_path).unwrap();

    let missing_client = dir.path().join("no-client.conf");
    let loaded = ClientConfig::load_or_derive(Some(&missing_client), Some(&daemon_path)).unwrap();
    assert_eq!(loaded.api_address, "https://127.0.0.1:5001");
    assert_eq!(loaded.ca_cert, "ca.crt");

    // A missing client path slot behaves the same as a missing file.
    let loaded = ClientConfig::load_or_derive(None, Some(&daemon_path)).unwrap();
    assert_eq!(loaded.api_address, "https://127.0.0.1:5001");
  }

  #[test]
  fn load_or_derive_propagates_missing_daemon_config() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("missing.toml");
    let error = ClientConfig::load_or_derive(Some(&missing), Some(&missing)).unwrap_err();
    assert!(matches!(error, ConfigError::Io(_)));
  }
}
