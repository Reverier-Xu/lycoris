//! Extension manifest model and wire-format (de)serialization.
//!
//! On the wire a manifest rides inside a `map<string, string>` (design
//! document section 4); compound values are encoded as JSON strings:
//!
//! | key            | value                                        |
//! | -------------- | -------------------------------------------- |
//! | `semver`       | human-facing SemVer string (required)        |
//! | `capabilities` | JSON array of capability names             |
//! | `hooks`        | JSON array of `{point, on_error}` objects    |
//! | `selector`     | JSON object of string -> string label terms  |
//! | `settings`     | opaque JSON passed to the extension uninterpreted |
//!
//! `engine` and `entry` are *not* manifest keys: they ride as top-level
//! fields of the extension record next to this map (design document section 4).
//! Unknown keys are ignored so newer manifests stay readable by older nodes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{ExtensionError, Result};

const SEMVER_KEY: &str = "semver";
const CAPABILITIES_KEY: &str = "capabilities";
const HOOKS_KEY: &str = "hooks";
const SELECTOR_KEY: &str = "selector";
const SETTINGS_KEY: &str = "settings";

/// Capabilities the host currently knows how to honour. Anything else is
/// rejected (design document section 10).
const KNOWN_CAPABILITIES: [&str; 1] = ["log"];

/// What the hook dispatcher should do when a hook invocation fails.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HookErrorPolicy {
  /// Abort the surrounding workflow.
  #[default]
  Abort,
  /// Log and continue with the next hook.
  Ignore,
}

/// A single hook subscription declared in the manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookDecl {
  /// Hook point name, e.g. `skill.invoke.pre`.
  pub point: String,
  /// Failure policy; defaults to `abort` when omitted.
  #[serde(default)]
  pub on_error: HookErrorPolicy,
}

/// A validated extension manifest.
///
/// Carries only the keys of the wire manifest map; the engine kind and entry
/// point are top-level record fields and live on
/// [`ExtensionPackage`](crate::ExtensionPackage).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionManifest {
  /// Human-facing SemVer, validated at ingest.
  pub semver: semver::Version,
  /// Declared host-service capabilities; unknown names are rejected.
  pub capabilities: Vec<String>,
  /// Hook subscriptions, applied in declaration order.
  pub hooks: Vec<HookDecl>,
  /// Label selector deciding per-node activation.
  pub selector: BTreeMap<String, String>,
  /// Opaque JSON settings handed to the extension at load time.
  pub settings: String,
}

impl ExtensionManifest {
  /// Parse and validate a manifest from its wire representation.
  pub fn from_map(map: &BTreeMap<String, String>) -> Result<Self> {
    let semver_raw = required(map, SEMVER_KEY)?;
    let semver = semver::Version::parse(semver_raw)
      .map_err(|err| ExtensionError::Manifest(format!("invalid semver {semver_raw:?}: {err}")))?;

    let capabilities: Vec<String> = match map.get(CAPABILITIES_KEY) {
      Some(raw) => parse_json(raw, CAPABILITIES_KEY)?,
      None => Vec::new(),
    };
    for capability in &capabilities {
      if !KNOWN_CAPABILITIES.contains(&capability.as_str()) {
        return Err(ExtensionError::Manifest(format!(
          "unknown capability: {capability:?}"
        )));
      }
    }

    let hooks: Vec<HookDecl> = match map.get(HOOKS_KEY) {
      Some(raw) => parse_json(raw, HOOKS_KEY)?,
      None => Vec::new(),
    };
    for hook in &hooks {
      if hook.point.is_empty() {
        return Err(ExtensionError::Manifest(
          "hook point must not be empty".to_string(),
        ));
      }
    }

    let selector: BTreeMap<String, String> = match map.get(SELECTOR_KEY) {
      Some(raw) => parse_json(raw, SELECTOR_KEY)?,
      None => BTreeMap::new(),
    };

    let settings = match map.get(SETTINGS_KEY) {
      Some(raw) => {
        // Settings are opaque, but they must be valid JSON.
        let _: serde_json::Value = parse_json(raw, SETTINGS_KEY)?;
        raw.clone()
      }
      None => "{}".to_string(),
    };

    Ok(Self {
      semver,
      capabilities,
      hooks,
      selector,
      settings,
    })
  }

  /// Serialize back into the wire representation. The output is
  /// deterministic: selector keys are emitted in sorted order.
  pub fn to_map(&self) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    map.insert(SEMVER_KEY.to_string(), self.semver.to_string());
    map.insert(
      CAPABILITIES_KEY.to_string(),
      serde_json::to_string(&self.capabilities)
        .map_err(|err| ExtensionError::Manifest(format!("failed to encode capabilities: {err}")))?,
    );
    map.insert(
      HOOKS_KEY.to_string(),
      serde_json::to_string(&self.hooks)
        .map_err(|err| ExtensionError::Manifest(format!("failed to encode hooks: {err}")))?,
    );
    map.insert(
      SELECTOR_KEY.to_string(),
      serde_json::to_string(&self.selector)
        .map_err(|err| ExtensionError::Manifest(format!("failed to encode selector: {err}")))?,
    );
    map.insert(SETTINGS_KEY.to_string(), self.settings.clone());
    Ok(map)
  }
}

/// Extract a required key from the wire map.
fn required<'a>(map: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> {
  map
    .get(key)
    .map(String::as_str)
    .ok_or_else(|| ExtensionError::Manifest(format!("missing required key: {key:?}")))
}

/// Decode a JSON-encoded manifest value.
fn parse_json<T: serde::de::DeserializeOwned>(raw: &str, key: &str) -> Result<T> {
  serde_json::from_str(raw)
    .map_err(|err| ExtensionError::Manifest(format!("invalid JSON in {key:?}: {err}")))
}

#[cfg(test)]
mod tests {
  use super::*;

  fn full_map() -> BTreeMap<String, String> {
    BTreeMap::from([
      ("semver".to_string(), "1.2.3-rc.1".to_string()),
      ("capabilities".to_string(), r#"["log"]"#.to_string()),
      (
        "hooks".to_string(),
        r#"[{"point":"skill.invoke.pre","on_error":"ignore"},{"point":"llm.call.post"}]"#
          .to_string(),
      ),
      (
        "selector".to_string(),
        r#"{"gpu":"true","region":"eu"}"#.to_string(),
      ),
      (
        "settings".to_string(),
        r#"{"model":"gpt-x","retries":3}"#.to_string(),
      ),
    ])
  }

  #[test]
  fn parses_a_full_manifest() {
    let manifest = ExtensionManifest::from_map(&full_map()).unwrap();
    assert_eq!(manifest.semver.to_string(), "1.2.3-rc.1");
    assert_eq!(manifest.capabilities, vec!["log".to_string()]);
    assert_eq!(
      manifest.hooks,
      vec![
        HookDecl {
          point: "skill.invoke.pre".to_string(),
          on_error: HookErrorPolicy::Ignore,
        },
        HookDecl {
          point: "llm.call.post".to_string(),
          on_error: HookErrorPolicy::Abort,
        },
      ]
    );
    assert_eq!(
      manifest.selector,
      BTreeMap::from([
        ("gpu".to_string(), "true".to_string()),
        ("region".to_string(), "eu".to_string()),
      ])
    );
    assert_eq!(manifest.settings, r#"{"model":"gpt-x","retries":3}"#);
  }

  #[test]
  fn applies_defaults_for_missing_optional_keys() {
    let map = BTreeMap::from([("semver".to_string(), "0.1.0".to_string())]);
    let manifest = ExtensionManifest::from_map(&map).unwrap();
    assert!(manifest.capabilities.is_empty());
    assert!(manifest.hooks.is_empty());
    assert!(manifest.selector.is_empty());
    assert_eq!(manifest.settings, "{}");
  }

  #[test]
  fn ignores_unknown_keys() {
    // `engine` and `entry` are record fields, not manifest keys; leftover or
    // newer keys must not break parsing.
    let map = BTreeMap::from([
      ("engine".to_string(), "lua".to_string()),
      ("entry".to_string(), "handle".to_string()),
      ("future-key".to_string(), "anything".to_string()),
      ("semver".to_string(), "0.1.0".to_string()),
    ]);
    let manifest = ExtensionManifest::from_map(&map).unwrap();
    assert_eq!(manifest.semver.to_string(), "0.1.0");
  }

  #[test]
  fn rejects_a_missing_semver() {
    let map = BTreeMap::new();
    assert!(matches!(
      ExtensionManifest::from_map(&map),
      Err(ExtensionError::Manifest(_))
    ));
  }

  #[test]
  fn rejects_bad_semver() {
    for bad in ["1.0", "not-a-version", "1.2.3.4"] {
      let map = BTreeMap::from([("semver".to_string(), bad.to_string())]);
      let result = ExtensionManifest::from_map(&map);
      assert!(
        matches!(result, Err(ExtensionError::Manifest(_))),
        "expected manifest error for semver {bad:?}"
      );
    }
  }

  #[test]
  fn rejects_unknown_capabilities() {
    let map = BTreeMap::from([
      ("semver".to_string(), "0.1.0".to_string()),
      ("capabilities".to_string(), r#"["log","http"]"#.to_string()),
    ]);
    let result = ExtensionManifest::from_map(&map);
    assert!(matches!(result, Err(ExtensionError::Manifest(err)) if err.contains("http")));
  }

  #[test]
  fn rejects_malformed_capabilities_json() {
    let map = BTreeMap::from([
      ("semver".to_string(), "0.1.0".to_string()),
      ("capabilities".to_string(), "not json".to_string()),
    ]);
    assert!(matches!(
      ExtensionManifest::from_map(&map),
      Err(ExtensionError::Manifest(_))
    ));
  }

  #[test]
  fn rejects_bad_hooks_json() {
    for bad in [
      "not json",
      r#"[{"point":1}]"#,
      r#"[{"point":"x","on_error":"retry"}]"#,
    ] {
      let map = BTreeMap::from([
        ("semver".to_string(), "0.1.0".to_string()),
        ("hooks".to_string(), bad.to_string()),
      ]);
      let result = ExtensionManifest::from_map(&map);
      assert!(
        matches!(result, Err(ExtensionError::Manifest(_))),
        "expected manifest error for hooks {bad:?}"
      );
    }
  }

  #[test]
  fn rejects_a_selector_with_non_string_values() {
    let map = BTreeMap::from([
      ("semver".to_string(), "0.1.0".to_string()),
      ("selector".to_string(), r#"{"gpu":true}"#.to_string()),
    ]);
    assert!(matches!(
      ExtensionManifest::from_map(&map),
      Err(ExtensionError::Manifest(_))
    ));
  }

  #[test]
  fn rejects_non_json_settings() {
    let map = BTreeMap::from([
      ("semver".to_string(), "0.1.0".to_string()),
      ("settings".to_string(), "{broken".to_string()),
    ]);
    assert!(matches!(
      ExtensionManifest::from_map(&map),
      Err(ExtensionError::Manifest(_))
    ));
  }

  #[test]
  fn selector_and_settings_round_trip_byte_for_byte() {
    let map = full_map();
    let manifest = ExtensionManifest::from_map(&map).unwrap();
    let out = manifest.to_map().unwrap();
    assert_eq!(out.get("selector"), map.get("selector"));
    assert_eq!(out.get("settings"), map.get("settings"));
  }

  #[test]
  fn full_manifest_round_trips_semantically() {
    let manifest = ExtensionManifest::from_map(&full_map()).unwrap();
    let reparsed = ExtensionManifest::from_map(&manifest.to_map().unwrap()).unwrap();
    assert_eq!(manifest, reparsed);
  }
}
