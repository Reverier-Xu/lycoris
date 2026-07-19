use std::{collections::HashMap, path::Path};

use serde::{Deserialize, Serialize};

use crate::{
  error::ConfigError,
  paths::{default_daemon_config_path, default_data_dir},
  validation::{non_empty_string, validate_https_address},
};

/// Node bootstrap configuration.
///
/// This file only contains information that is specific to the current node and
/// required to join the cluster (identity, listen address, TLS material
/// location, data directory) plus the node's static scheduling labels. All
/// dynamic runtime state such as peer list, primary endpoint, node
/// annotations, and peer reachability is stored in the redb database under
/// `data_dir`; the configured labels are merged into that store at startup
/// (set semantics) so the local register and selector evaluation keep reading
/// labels from a single source.
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
  #[serde(default)]
  pub extensions: ExtensionsConfig,
}

fn default_data_dir_string() -> String {
  default_data_dir().to_string_lossy().to_string()
}

impl DaemonConfig {
  pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
    let config: DaemonConfig = crate::toml_file::read(path.as_ref())?;
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
    validate_https_address(&self.node.address)?;
    for (index, peer) in self.cluster.bootstrap_peers.iter().enumerate() {
      validate_https_address(peer)
        .map_err(|source| ConfigError::InvalidPeerAddress { index, source })?;
    }
    Ok(())
  }

  /// Write the daemon configuration to a TOML file, creating parent directories
  /// if necessary.
  pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), ConfigError> {
    crate::toml_file::write(self, path.as_ref())
  }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
  #[serde(deserialize_with = "non_empty_string")]
  pub id: String,
  #[serde(deserialize_with = "non_empty_string")]
  pub address: String,
  /// Static node labels exposed to the cluster for scheduling and extension
  /// selector activation. Merged into the node-local label store at startup
  /// (config keys overwrite stored values; other stored keys are kept).
  #[serde(default)]
  pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
  #[serde(deserialize_with = "non_empty_string")]
  pub listen_address: String,
  /// Optional list of peers to seed on first startup. After bootstrap, peer
  /// state is maintained in the database.
  pub bootstrap_peers: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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

/// Node-local extension engine budgets and per-extension overrides
/// (extension system design, section 9; llm-provider design, section 5).
///
/// Engine budgets live here because they are node-local. Everything
/// per-extension (selector, hooks, capabilities, baseline settings) lives in
/// the cluster-synced manifest — nodes differ only through labels, which is
/// what makes selector-based activation meaningful. The `local` table is the
/// deliberate exception: per-node overrides (API keys, base URLs, egress
/// allowlists) merged over the manifest settings at load time.
/// **Nothing in `local` ever leaves the node** — it is not part of any
/// synced record. The whole section is optional; each field falls back to
/// the design default.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionsConfig {
  /// Fuel a WASM guest may consume per invocation (deterministic timeout).
  #[serde(default = "default_wasm_fuel_per_call")]
  pub wasm_fuel_per_call: u64,
  /// Maximum linear memory a WASM guest may reach, in bytes.
  #[serde(default = "default_wasm_max_memory_bytes")]
  pub wasm_max_memory_bytes: u64,
  /// VM instructions a Lua script may execute per invocation.
  #[serde(default = "default_lua_instructions_per_call")]
  pub lua_instructions_per_call: u64,
  /// Maximum memory a Lua state may allocate, in bytes.
  #[serde(default = "default_lua_max_memory_bytes")]
  pub lua_max_memory_bytes: u64,
  /// Wall-clock deadline for a single invocation, in milliseconds.
  #[serde(default = "default_invoke_timeout_ms")]
  pub invoke_timeout_ms: u64,
  /// Per-extension node-local settings: `[extensions.local.<id>]` overlays
  /// the extension's manifest settings key by key at load time (local wins;
  /// values are written into the merged settings JSON as strings). Never
  /// leaves the node — secrets such as API keys belong here, never in the
  /// synced manifest.
  #[serde(default)]
  pub local: HashMap<String, HashMap<String, String>>,
}

impl Default for ExtensionsConfig {
  fn default() -> Self {
    Self {
      wasm_fuel_per_call: default_wasm_fuel_per_call(),
      wasm_max_memory_bytes: default_wasm_max_memory_bytes(),
      lua_instructions_per_call: default_lua_instructions_per_call(),
      lua_max_memory_bytes: default_lua_max_memory_bytes(),
      invoke_timeout_ms: default_invoke_timeout_ms(),
      local: HashMap::new(),
    }
  }
}

/// Sized for real guests, not toy WAT fixtures: the OpenAI provider guest
/// does serde_json-scale request/response work per `chat` call (parse the
/// wire request, build the upstream body, parse the completion), which the
/// old 5M budget — calibrated on bump-allocator echo fixtures — could not
/// sustain. 100M is the comfortable budget the wasm e2e suites measured
/// against the real guest.
fn default_wasm_fuel_per_call() -> u64 {
  100_000_000
}

fn default_wasm_max_memory_bytes() -> u64 {
  64 * 1024 * 1024
}

fn default_lua_instructions_per_call() -> u64 {
  1_000_000
}

fn default_lua_max_memory_bytes() -> u64 {
  32 * 1024 * 1024
}

fn default_invoke_timeout_ms() -> u64 {
  10_000
}

#[cfg(test)]
mod tests {
  use std::fs;

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
  fn node_labels_default_to_empty() {
    let cfg: DaemonConfig = toml::from_str(VALID_TOML).unwrap();
    assert!(cfg.node.labels.is_empty());
  }

  #[test]
  fn parse_node_labels() {
    let toml = VALID_TOML.replace(
      "address = \"https://127.0.0.1:5001\"",
      "address = \"https://127.0.0.1:5001\"\nlabels = { role = \"runner\", zone = \"eu\" }",
    );
    let cfg: DaemonConfig = toml::from_str(&toml).unwrap();
    assert_eq!(
      cfg.node.labels,
      HashMap::from([
        ("role".to_string(), "runner".to_string()),
        ("zone".to_string(), "eu".to_string()),
      ])
    );
  }

  #[test]
  fn reject_non_string_node_label_value() {
    let toml = VALID_TOML.replace(
      "address = \"https://127.0.0.1:5001\"",
      "address = \"https://127.0.0.1:5001\"\nlabels = { role = 1 }",
    );
    let result: Result<DaemonConfig, _> = toml::from_str(&toml);
    assert!(result.is_err());
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
  fn reject_unknown_field_in_nested_sections() {
    for (anchor, unknown) in [
      ("id = \"node-01\"", "node_typo = 1"),
      ("listen_address = \"0.0.0.0:5001\"", "peer = 1"),
      ("ca_cert = \"certs/ca.crt\"", "ca_certt = 1"),
    ] {
      let toml = VALID_TOML.replace(anchor, &format!("{anchor}\n{unknown}"));
      let result: Result<DaemonConfig, _> = toml::from_str(&toml);
      assert!(result.is_err(), "unknown field '{unknown}' was accepted");
    }
  }

  #[test]
  fn extensions_default_when_section_missing() {
    let cfg: DaemonConfig = toml::from_str(VALID_TOML).unwrap();
    assert_eq!(cfg.extensions, ExtensionsConfig::default());
    assert_eq!(cfg.extensions.wasm_fuel_per_call, 100_000_000);
    assert_eq!(cfg.extensions.wasm_max_memory_bytes, 64 * 1024 * 1024);
    assert_eq!(cfg.extensions.lua_instructions_per_call, 1_000_000);
    assert_eq!(cfg.extensions.lua_max_memory_bytes, 32 * 1024 * 1024);
    assert_eq!(cfg.extensions.invoke_timeout_ms, 10_000);
  }

  #[test]
  fn extensions_explicit_override_keeps_defaults_elsewhere() {
    let toml =
      format!("{VALID_TOML}\n[extensions]\nwasm_fuel_per_call = 42\ninvoke_timeout_ms = 1\n");
    let cfg: DaemonConfig = toml::from_str(&toml).unwrap();
    assert_eq!(cfg.extensions.wasm_fuel_per_call, 42);
    assert_eq!(cfg.extensions.invoke_timeout_ms, 1);
    assert_eq!(cfg.extensions.wasm_max_memory_bytes, 64 * 1024 * 1024);
    assert_eq!(cfg.extensions.lua_instructions_per_call, 1_000_000);
    assert_eq!(cfg.extensions.lua_max_memory_bytes, 32 * 1024 * 1024);
  }

  #[test]
  fn extensions_local_defaults_to_empty() {
    let cfg: DaemonConfig = toml::from_str(VALID_TOML).unwrap();
    assert!(cfg.extensions.local.is_empty());
  }

  #[test]
  fn extensions_local_parses_per_extension_tables() {
    let toml = format!(
      "{VALID_TOML}\n[extensions.local.openai]\napi_key = \"sk-test\"\nbase_url = \"https://api.openai.com/v1\"\nhttp_allow_hosts = \"[\\\"api.openai.com\\\"]\"\n"
    );
    let cfg: DaemonConfig = toml::from_str(&toml).unwrap();
    assert_eq!(
      cfg.extensions.local.get("openai"),
      Some(&HashMap::from([
        ("api_key".to_string(), "sk-test".to_string()),
        (
          "base_url".to_string(),
          "https://api.openai.com/v1".to_string()
        ),
        (
          "http_allow_hosts".to_string(),
          "[\"api.openai.com\"]".to_string()
        ),
      ]))
    );
  }

  #[test]
  fn reject_non_string_extensions_local_value() {
    let toml = format!("{VALID_TOML}\n[extensions.local.openai]\napi_key = 42\n");
    let result: Result<DaemonConfig, _> = toml::from_str(&toml);
    assert!(result.is_err());
  }

  #[test]
  fn reject_unknown_field_in_extensions_section() {
    let toml = format!("{VALID_TOML}\n[extensions]\nwasm_fuel = 1\n");
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
