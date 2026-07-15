use std::{
  fs,
  io::{self, Write},
  path::Path,
};

use thiserror::Error;

use crate::paths::default_data_dir;

const KEY_LENGTH: usize = 32;

/// A 32-byte cluster-shared key used to authorize new members joining the
/// cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterKey([u8; KEY_LENGTH]);

impl ClusterKey {
  /// Generate a new random cluster key.
  pub fn generate() -> Result<Self, ClusterKeyError> {
    let rng = ring::rand::SystemRandom::new();
    let mut bytes = [0u8; KEY_LENGTH];
    ring::rand::SecureRandom::fill(&rng, &mut bytes)
      .map_err(|_| ClusterKeyError::RandomGeneration)?;
    Ok(Self(bytes))
  }

  /// Construct a key from raw bytes.
  pub fn from_bytes(bytes: [u8; KEY_LENGTH]) -> Self {
    Self(bytes)
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

  /// Return the raw key bytes.
  pub fn as_bytes(&self) -> &[u8; KEY_LENGTH] {
    &self.0
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
    if let Some(parent) = path.as_ref().parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = fs::File::create(path.as_ref())?;
    #[cfg(unix)]
    {
      use std::os::unix::fs::PermissionsExt;
      let mut permissions = file.metadata()?.permissions();
      permissions.set_mode(0o600);
      file.set_permissions(permissions)?;
    }

    file.write_all(self.to_hex().as_bytes())?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
  }
}

impl Default for ClusterKey {
  fn default() -> Self {
    Self([0u8; KEY_LENGTH])
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
  default_data_dir().join("cluster.key")
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
    assert_eq!(key.as_bytes(), &[0xAA; KEY_LENGTH]);
  }

  #[test]
  fn from_hex_rejects_invalid_length() {
    let hex = "a".repeat(KEY_LENGTH * 2 - 2);
    assert!(ClusterKey::from_hex(&hex).is_err());
  }
}
