//! Plugin package model: the unit that crosses storage, sync and engines.
//!
//! A package pairs the generic resource metadata (id, name, monotonic
//! version) with the validated manifest and the raw artifact bytes. Artifact
//! integrity is anchored in a blake3 content hash, verified at ingest and
//! again at load time (design document sections 4 and 10).

use crate::{
  error::{PluginError, Result},
  manifest::PluginManifest,
};

/// A plugin package ready to be stored, synced or loaded.
#[derive(Debug, Clone)]
pub struct PluginPackage {
  /// Cluster-unique plugin id.
  pub id: String,
  /// Human-facing plugin name.
  pub name: String,
  /// Monotonic convergence version ordered by anti-entropy.
  pub version: u64,
  /// Validated manifest.
  pub manifest: PluginManifest,
  /// Raw artifact: a WASM module or Lua source.
  pub artifact: Vec<u8>,
  /// blake3 hex digest of `artifact`.
  pub content_hash: String,
}

impl PluginPackage {
  /// Build a package, computing the content hash of the artifact.
  pub fn new(
    id: String, name: String, version: u64, manifest: PluginManifest, artifact: Vec<u8>,
  ) -> Self {
    let content_hash = hash_artifact(&artifact);
    Self {
      id,
      name,
      version,
      manifest,
      artifact,
      content_hash,
    }
  }

  /// Verify the artifact against the declared content hash.
  pub fn verify(&self) -> Result<()> {
    let actual = hash_artifact(&self.artifact);
    if actual != self.content_hash {
      return Err(PluginError::ContentHashMismatch {
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
  use crate::manifest::PluginManifest;

  fn manifest() -> PluginManifest {
    PluginManifest::from_map(&BTreeMap::from([
      ("engine".to_string(), "lua".to_string()),
      ("semver".to_string(), "0.1.0".to_string()),
    ]))
    .unwrap()
  }

  #[test]
  fn new_computes_the_blake3_content_hash() {
    let package = PluginPackage::new(
      "p1".to_string(),
      "echo".to_string(),
      1,
      manifest(),
      b"return {}".to_vec(),
    );
    assert_eq!(
      package.content_hash,
      blake3::hash(b"return {}").to_hex().to_string()
    );
  }

  #[test]
  fn verify_accepts_an_intact_artifact() {
    let package = PluginPackage::new(
      "p1".to_string(),
      "echo".to_string(),
      1,
      manifest(),
      b"x".to_vec(),
    );
    assert!(package.verify().is_ok());
  }

  #[test]
  fn verify_rejects_a_tampered_artifact() {
    let mut package = PluginPackage::new(
      "p1".to_string(),
      "echo".to_string(),
      1,
      manifest(),
      b"x".to_vec(),
    );
    package.artifact = b"y".to_vec();
    assert!(matches!(
      package.verify(),
      Err(PluginError::ContentHashMismatch { .. })
    ));
  }

  #[test]
  fn verify_rejects_a_forged_hash() {
    let mut package = PluginPackage::new(
      "p1".to_string(),
      "echo".to_string(),
      1,
      manifest(),
      b"x".to_vec(),
    );
    package.content_hash = "0".repeat(64);
    assert!(matches!(
      package.verify(),
      Err(PluginError::ContentHashMismatch { .. })
    ));
  }
}
