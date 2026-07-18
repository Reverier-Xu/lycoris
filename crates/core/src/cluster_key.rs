use std::{fs, io, path::Path};

use thiserror::Error;

use crate::paths::{cluster_key_path_in, default_data_dir};

const KEY_LENGTH: usize = 32;

/// A 32-byte cluster-shared key used to authorize new members joining the
/// cluster.
#[derive(Clone, Eq)]
pub struct ClusterKey([u8; KEY_LENGTH]);

/// Constant-time equality: keys are compared at the rpc admission boundary,
/// so the comparison must not leak key material through timing.
impl PartialEq for ClusterKey {
  fn eq(&self, other: &Self) -> bool {
    let mut diff = 0u8;
    for (left, right) in self.0.iter().zip(other.0.iter()) {
      diff |= left ^ right;
    }
    diff == 0
  }
}

/// Redacted output: the key is a 32-byte shared secret and must never reach
/// logs through `Debug` formatting.
impl std::fmt::Debug for ClusterKey {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.write_str("ClusterKey([redacted])")
  }
}

impl ClusterKey {
  /// Generate a new random cluster key.
  pub fn generate() -> Result<Self, ClusterKeyError> {
    let rng = ring::rand::SystemRandom::new();
    let mut bytes = [0u8; KEY_LENGTH];
    ring::rand::SecureRandom::fill(&rng, &mut bytes)
      .map_err(|_| ClusterKeyError::RandomGeneration)?;
    Ok(Self(bytes))
  }

  /// Parse a key from a hex string.
  pub fn from_hex(hex: &str) -> Result<Self, ClusterKeyError> {
    let bytes = hex::decode(hex).map_err(|_| ClusterKeyError::InvalidHex)?;
    let array: [u8; KEY_LENGTH] = bytes
      .try_into()
      .map_err(|_| ClusterKeyError::InvalidLength)?;
    Ok(Self(array))
  }

  /// Return the key as a hex string.
  pub fn to_hex(&self) -> String {
    hex::encode(self.0)
  }

  /// Load a key from a file. The file is expected to contain a single line of
  /// hex-encoded bytes.
  pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ClusterKeyError> {
    let content = fs::read_to_string(path.as_ref())?;
    let hex = content.trim();
    Self::from_hex(hex)
  }

  /// Save the key to a file with restricted permissions.
  pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), ClusterKeyError> {
    let content = format!("{}\n", self.to_hex());
    crate::fs::write_private_file(path, content.as_bytes())?;
    Ok(())
  }
}

/// Errors that can occur when working with a cluster key.
#[derive(Debug, Error)]
pub enum ClusterKeyError {
  #[error("failed to generate random key")]
  RandomGeneration,
  #[error("invalid hex encoding")]
  InvalidHex,
  #[error("key must be {KEY_LENGTH} bytes")]
  InvalidLength,
  #[error("io error: {0}")]
  Io(#[from] io::Error),
}

/// Return the default path to the cluster key file.
pub fn default_cluster_key_path() -> std::path::PathBuf {
  cluster_key_path_in(&default_data_dir())
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  #[test]
  fn generate_and_round_trip() {
    let key = ClusterKey::generate().unwrap();
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cluster.key");
    key.save(&path).unwrap();
    let loaded = ClusterKey::load(&path).unwrap();
    assert_eq!(key, loaded);
  }

  #[test]
  fn from_hex_accepts_valid_key() {
    let hex = "a".repeat(KEY_LENGTH * 2);
    let key = ClusterKey::from_hex(&hex).unwrap();
    assert_eq!(key.to_hex(), hex);
  }

  #[test]
  fn from_hex_rejects_invalid_length() {
    let hex = "a".repeat(KEY_LENGTH * 2 - 2);
    assert!(ClusterKey::from_hex(&hex).is_err());
  }

  #[test]
  fn debug_output_is_redacted() {
    let key = ClusterKey::generate().unwrap();
    let rendered = format!("{key:?}");
    assert_eq!(rendered, "ClusterKey([redacted])");
    assert!(!rendered.contains(&key.to_hex()));
  }

  #[test]
  fn equality_compares_every_byte() {
    let hex_of = |bytes: [u8; KEY_LENGTH]| hex::encode(bytes);
    let key = ClusterKey::from_hex(&hex_of([0xAA; KEY_LENGTH])).unwrap();
    assert_eq!(
      key,
      ClusterKey::from_hex(&hex_of([0xAA; KEY_LENGTH])).unwrap()
    );
    for index in [0, KEY_LENGTH - 1] {
      let mut bytes = [0xAA; KEY_LENGTH];
      bytes[index] ^= 0x01;
      assert_ne!(key, ClusterKey::from_hex(&hex_of(bytes)).unwrap());
    }
  }

  #[cfg(unix)]
  #[test]
  fn save_restricts_permissions_to_owner() {
    use std::os::unix::fs::PermissionsExt;

    let key = ClusterKey::generate().unwrap();
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cluster.key");
    key.save(&path).unwrap();

    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
  }

  #[test]
  fn load_rejects_bad_hex() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cluster.key");
    std::fs::write(&path, "not-hex\n").unwrap();

    let error = ClusterKey::load(&path).unwrap_err();
    assert!(
      matches!(error, ClusterKeyError::InvalidHex),
      "expected InvalidHex, got {error}"
    );
  }

  #[test]
  fn load_reports_io_error_for_missing_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("missing.key");

    let error = ClusterKey::load(&path).unwrap_err();
    assert!(
      matches!(error, ClusterKeyError::Io(_)),
      "expected Io, got {error}"
    );
  }
}
