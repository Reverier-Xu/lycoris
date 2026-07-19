//! Extension package files and the `cluster ext` command handlers.
//!
//! A package file is a TOML document describing one extension package:
//!
//! ```toml
//! id = "echo-ext"
//! name = "Echo"                        # optional, defaults to `id`
//! version = 1                          # monotonic, must strictly increase per id
//! engine = "lua"                       # or "wasm"
//! artifact = "./echo.lua"              # relative to the package file's directory
//! semver = "1.0.0"
//! # entry = "invoke"                   # optional override of the entry point
//! # capabilities = ["log"]
//! # selector = { role = "runner" }
//! # hooks = [{ point = "skill.invoke.pre", on_error = "ignore" }]
//! # [settings]                         # arbitrary values, forwarded as JSON
//! ```
//!
//! `engine` and `entry` ride as top-level record fields; everything else is
//! folded into the manifest map (extension system design, section 4) sent to
//! `Extension.RegisterExtension`.

use std::{
  collections::{BTreeMap, HashMap},
  path::{Path, PathBuf},
};

use lycoris_config::ClientConfig;
use lycoris_proto::node::RegisterExtensionRequest;
use owo_colors::OwoColorize;
use serde::{Deserialize, Serialize};

use crate::error::ShellError;

/// Engine spellings accepted by the package file, pre-validated locally so a
/// typo fails before any network round trip; the daemon re-validates.
const KNOWN_ENGINES: [&str; 2] = ["wasm", "lua"];

/// A hook subscription as written in a package file; `on_error` omitted means
/// the manifest default (`abort`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PackageHook {
  point: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  on_error: Option<String>,
}

/// The parsed package file; compound sections keep their TOML value until
/// they are folded into the manifest map as JSON strings.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackageFile {
  id: String,
  name: Option<String>,
  version: u64,
  engine: String,
  entry: Option<String>,
  artifact: PathBuf,
  semver: String,
  capabilities: Option<Vec<String>>,
  selector: Option<BTreeMap<String, String>>,
  hooks: Option<Vec<PackageHook>>,
  settings: Option<toml::Value>,
}

/// Parse and validate the package file at `path`, then read its artifact:
/// every local failure surfaces before a connection is opened.
fn load_package(path: &Path) -> Result<RegisterExtensionRequest, ShellError> {
  let raw = std::fs::read_to_string(path).map_err(|source| ShellError::PackageRead {
    path: path.to_path_buf(),
    source,
  })?;
  let package: PackageFile = toml::from_str(&raw).map_err(|source| ShellError::PackageParse {
    path: path.to_path_buf(),
    source,
  })?;

  if package.id.is_empty() {
    return Err(ShellError::PackageValidation(
      "package id must not be empty".to_string(),
    ));
  }
  if !KNOWN_ENGINES.contains(&package.engine.as_str()) {
    return Err(ShellError::PackageValidation(format!(
      "unknown engine {:?}, expected one of: {}",
      package.engine,
      KNOWN_ENGINES.join(", ")
    )));
  }
  semver::Version::parse(&package.semver).map_err(|error| {
    ShellError::PackageValidation(format!("invalid semver {:?}: {error}", package.semver))
  })?;

  // The artifact path resolves relative to the package file's directory, so
  // packages are relocatable.
  let artifact_path = path
    .parent()
    .unwrap_or_else(|| Path::new("."))
    .join(&package.artifact);
  let artifact = std::fs::read(&artifact_path).map_err(|source| ShellError::ArtifactRead {
    path: artifact_path.clone(),
    source,
  })?;
  if artifact.is_empty() {
    return Err(ShellError::PackageValidation(format!(
      "artifact {} is empty",
      artifact_path.display()
    )));
  }

  let manifest = manifest_map(&package)?;
  Ok(RegisterExtensionRequest {
    id: package.id.clone(),
    name: package.name.unwrap_or_else(|| package.id.clone()),
    version: package.version,
    engine: package.engine,
    entry: package.entry.unwrap_or_default(),
    artifact,
    manifest,
    labels: HashMap::new(),
  })
}

/// Fold the package's compound sections into the manifest wire map: values
/// are JSON strings (design section 4).
fn manifest_map(package: &PackageFile) -> Result<HashMap<String, String>, ShellError> {
  let mut manifest = HashMap::new();
  manifest.insert("semver".to_string(), package.semver.clone());
  if let Some(capabilities) = &package.capabilities {
    manifest.insert(
      "capabilities".to_string(),
      to_json(capabilities, "capabilities")?,
    );
  }
  if let Some(hooks) = &package.hooks {
    manifest.insert("hooks".to_string(), to_json(hooks, "hooks")?);
  }
  if let Some(selector) = &package.selector {
    manifest.insert("selector".to_string(), to_json(selector, "selector")?);
  }
  if let Some(settings) = &package.settings {
    manifest.insert("settings".to_string(), to_json(settings, "settings")?);
  }
  Ok(manifest)
}

/// Encode one manifest value as JSON; encoding these in-memory values cannot
/// realistically fail, but the error is mapped instead of unwrapped.
fn to_json(value: &impl Serialize, key: &str) -> Result<String, ShellError> {
  serde_json::to_string(value)
    .map_err(|error| ShellError::PackageValidation(format!("failed to encode {key}: {error}")))
}

/// `lycoris cluster ext load`: register the package on the connected node and
/// print the outcome; the package then converges cluster-wide.
pub(crate) async fn ext_load(
  client_config: &ClientConfig, package: &Path,
) -> Result<(), ShellError> {
  let request = load_package(package)?;
  let id = request.id.clone();
  let mut client = super::connect_extension(client_config).await?;
  let content_hash = client
    .register(request)
    .await
    .map_err(ShellError::RegisterExtension)?;
  println!("accepted: {}", "true".green());
  println!("extension:  {}", id.cyan());
  println!("content hash: {content_hash}");
  Ok(())
}

/// `lycoris cluster ext invoke`: call an extension method with a JSON
/// payload. The payload prints to stdout (pipeable); the routing decision
/// prints to stderr.
pub(crate) async fn ext_invoke(
  client_config: &ClientConfig, id: &str, method: &str, payload: Option<String>,
) -> Result<(), ShellError> {
  let payload = resolve_payload(payload)?;

  let mut client = super::connect_extension(client_config).await?;
  let response = client
    .invoke(id, method, payload, None)
    .await
    .map_err(ShellError::InvokeExtension)?;
  // Engines guarantee JSON output; print it verbatim instead of re-encoding.
  println!("{}", String::from_utf8_lossy(&response.payload));
  eprintln!("executed by: {}", response.executed_by);
  Ok(())
}

/// Resolve the optional payload argument into JSON bytes: absent means `{}`;
/// present must parse as JSON before any connection is opened.
fn resolve_payload(payload: Option<String>) -> Result<Vec<u8>, ShellError> {
  let payload = payload.unwrap_or_else(|| "{}".to_string());
  serde_json::from_str::<serde_json::Value>(&payload)
    .map_err(|error| ShellError::InvalidPayload(error.to_string()))?;
  Ok(payload.into_bytes())
}

#[cfg(test)]
mod tests {
  use super::*;

  const FULL_PACKAGE: &str = r##"
id = "echo-ext"
name = "Echo"
version = 3
engine = "lua"
artifact = "./echo.lua"
semver = "1.0.0-rc.1"
entry = "handle"
capabilities = ["log"]
selector = { role = "runner", zone = "eu" }
hooks = [{ point = "skill.invoke.pre", on_error = "ignore" }, { point = "llm.call.post" }]

[settings]
model = "gpt-x"
retries = 3
"##;

  fn write_package(dir: &tempfile::TempDir, package: &str, artifact: &[u8]) -> PathBuf {
    std::fs::write(dir.path().join("echo.lua"), artifact).unwrap();
    let path = dir.path().join("echo.pkg.toml");
    std::fs::write(&path, package).unwrap();
    path
  }

  #[test]
  fn full_package_parses_into_a_register_request() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = write_package(&dir, FULL_PACKAGE, b"lua-source");

    let request = load_package(&path).unwrap();

    assert_eq!(request.id, "echo-ext");
    assert_eq!(request.name, "Echo");
    assert_eq!(request.version, 3);
    assert_eq!(request.engine, "lua");
    assert_eq!(request.entry, "handle");
    assert_eq!(request.artifact, b"lua-source");
    assert_eq!(request.manifest.get("semver").unwrap(), "1.0.0-rc.1");
    assert_eq!(request.manifest.get("capabilities").unwrap(), r#"["log"]"#);
    assert_eq!(
      request.manifest.get("selector").unwrap(),
      r#"{"role":"runner","zone":"eu"}"#
    );
    let hooks: serde_json::Value =
      serde_json::from_str(request.manifest.get("hooks").unwrap()).unwrap();
    assert_eq!(
      hooks,
      serde_json::json!([
        {"point": "skill.invoke.pre", "on_error": "ignore"},
        {"point": "llm.call.post"}
      ])
    );
    let settings: serde_json::Value =
      serde_json::from_str(request.manifest.get("settings").unwrap()).unwrap();
    assert_eq!(
      settings,
      serde_json::json!({"model": "gpt-x", "retries": 3})
    );
    assert!(request.labels.is_empty());
  }

  #[test]
  fn minimal_package_defaults_name_and_entry_and_omits_optional_keys() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = write_package(
      &dir,
      r#"
id = "mini"
version = 1
engine = "wasm"
artifact = "./echo.lua"
semver = "0.1.0"
"#,
      b"wasm-bytes",
    );

    let request = load_package(&path).unwrap();

    assert_eq!(request.name, "mini");
    assert_eq!(request.entry, "");
    assert_eq!(request.manifest.len(), 1);
    assert_eq!(request.manifest.get("semver").unwrap(), "0.1.0");
  }

  #[test]
  fn artifact_resolves_relative_to_the_package_file() {
    let dir = tempfile::TempDir::new().unwrap();
    let nested = dir.path().join("nested");
    std::fs::create_dir(&nested).unwrap();
    std::fs::write(nested.join("ext.lua"), b"nested-source").unwrap();
    let path = nested.join("pkg.toml");
    std::fs::write(
      &path,
      r#"
id = "rel"
version = 1
engine = "lua"
artifact = "./ext.lua"
semver = "0.1.0"
"#,
    )
    .unwrap();

    let request = load_package(&path).unwrap();
    assert_eq!(request.artifact, b"nested-source");
  }

  #[test]
  fn unknown_engine_and_bad_semver_fail_before_any_io() {
    for (package, expected) in [
      (
        FULL_PACKAGE.replace(r#"engine = "lua""#, r#"engine = "python""#),
        "unknown engine",
      ),
      (
        FULL_PACKAGE.replace(r#"semver = "1.0.0-rc.1""#, r#"semver = "1.0""#),
        "invalid semver",
      ),
    ] {
      let dir = tempfile::TempDir::new().unwrap();
      let path = write_package(&dir, &package, b"x");
      let error = load_package(&path).unwrap_err();
      assert!(
        matches!(error, ShellError::PackageValidation(ref message) if message.contains(expected)),
        "expected {expected}, got {error}"
      );
    }
  }

  #[test]
  fn missing_and_empty_artifacts_are_rejected() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("pkg.toml");
    std::fs::write(&path, FULL_PACKAGE).unwrap();
    let error = load_package(&path).unwrap_err();
    assert!(
      matches!(error, ShellError::ArtifactRead { .. }),
      "expected ArtifactRead, got {error}"
    );

    let path = write_package(&dir, FULL_PACKAGE, b"");
    let error = load_package(&path).unwrap_err();
    assert!(
      matches!(error, ShellError::PackageValidation(ref message) if message.contains("empty")),
      "expected empty-artifact validation, got {error}"
    );
  }

  #[test]
  fn unknown_package_fields_are_rejected() {
    let dir = tempfile::TempDir::new().unwrap();
    let package = FULL_PACKAGE.replace(
      r#"semver = "1.0.0-rc.1""#,
      "semver = \"1.0.0-rc.1\"\nflavour = \"vanilla\"",
    );
    let path = write_package(&dir, &package, b"x");
    let error = load_package(&path).unwrap_err();
    assert!(
      matches!(error, ShellError::PackageParse { .. }),
      "expected PackageParse, got {error}"
    );
  }

  #[test]
  fn invoke_payload_resolution() {
    assert_eq!(resolve_payload(None).unwrap(), b"{}".to_vec());
    assert_eq!(
      resolve_payload(Some(r#"{"k":"v"}"#.to_string())).unwrap(),
      br#"{"k":"v"}"#.to_vec()
    );
    let error = resolve_payload(Some("{broken".to_string())).unwrap_err();
    assert!(
      matches!(error, ShellError::InvalidPayload(_)),
      "expected InvalidPayload, got {error}"
    );
  }
}
