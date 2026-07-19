//! Extension package model: the unit that crosses storage, sync and engines.
//!
//! A package pairs the generic resource metadata (id, name, monotonic
//! version) with the validated manifest and the raw artifact bytes. Artifact
//! integrity is anchored in a blake3 content hash, verified at ingest and
//! again at load time (design document sections 4 and 10).

use crate::{
  engine::EngineKind,
  error::{ExtensionError, Result},
  manifest::ExtensionManifest,
};

/// Entry point assumed when the record leaves `entry` empty.
pub const DEFAULT_ENTRY: &str = "invoke";

/// An extension package ready to be stored, synced or loaded.
#[derive(Debug, Clone)]
pub struct ExtensionPackage {
  /// Cluster-unique extension id.
  pub id: String,
  /// Human-facing extension name.
  pub name: String,
  /// Monotonic convergence version ordered by anti-entropy.
  pub version: u64,
  /// Execution engine for the artifact.
  pub engine: EngineKind,
  /// Exported entry point name (`invoke` unless overridden).
  pub entry: String,
  /// Validated manifest.
  pub manifest: ExtensionManifest,
  /// Raw artifact: a WASM module or Lua source.
  pub artifact: Vec<u8>,
  /// blake3 hex digest of `artifact`.
  pub content_hash: String,
}

impl ExtensionPackage {
  /// Build a package, computing the content hash of the artifact. An empty
  /// `entry` falls back to [`DEFAULT_ENTRY`].
  pub fn new(
    id: String, name: String, version: u64, engine: EngineKind, entry: String,
    manifest: ExtensionManifest, artifact: Vec<u8>,
  ) -> Self {
    let content_hash = hash_artifact(&artifact);
    let entry = if entry.is_empty() {
      DEFAULT_ENTRY.to_string()
    } else {
      entry
    };
    Self {
      id,
      name,
      version,
      engine,
      entry,
      manifest,
      artifact,
      content_hash,
    }
  }

  /// Verify the artifact against the declared content hash.
  pub fn verify(&self) -> Result<()> {
    let actual = hash_artifact(&self.artifact);
    if actual != self.content_hash {
      return Err(ExtensionError::ContentHashMismatch {
        expected: self.content_hash.clone(),
        actual,
      });
    }
    Ok(())
  }
}

/// Compute the canonical blake3 content hash of an artifact.
fn hash_artifact(artifact: &[u8]) -> String {
  blake3::hash(artifact).to_hex().to_string()
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use super::*;
  use crate::manifest::ExtensionManifest;

  fn manifest() -> ExtensionManifest {
    ExtensionManifest::from_map(&BTreeMap::from([(
      "semver".to_string(),
      "0.1.0".to_string(),
    )]))
    .unwrap()
  }

  fn package(artifact: &[u8]) -> ExtensionPackage {
    ExtensionPackage::new(
      "p1".to_string(),
      "echo".to_string(),
      1,
      EngineKind::Lua,
      String::new(),
      manifest(),
      artifact.to_vec(),
    )
  }

  #[test]
  fn new_computes_the_blake3_content_hash() {
    let package = package(b"return {}");
    assert_eq!(
      package.content_hash,
      blake3::hash(b"return {}").to_hex().to_string()
    );
  }

  #[test]
  fn new_defaults_an_empty_entry_to_invoke() {
    let package = package(b"return {}");
    assert_eq!(package.entry, DEFAULT_ENTRY);
  }

  #[test]
  fn verify_accepts_an_intact_artifact() {
    let package = package(b"x");
    assert!(package.verify().is_ok());
  }

  #[test]
  fn verify_rejects_a_tampered_artifact() {
    let mut package = package(b"x");
    package.artifact = b"y".to_vec();
    assert!(matches!(
      package.verify(),
      Err(ExtensionError::ContentHashMismatch { .. })
    ));
  }

  #[test]
  fn verify_rejects_a_forged_hash() {
    let mut package = package(b"x");
    package.content_hash = "0".repeat(64);
    assert!(matches!(
      package.verify(),
      Err(ExtensionError::ContentHashMismatch { .. })
    ));
  }
}
