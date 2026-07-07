//! Simple filesystem-backed versioned content storage.
//!
//! Skill and rule bodies are stored as plain files. Each write creates a new
//! immutable snapshot; the latest snapshot is mirrored to a stable path for
//! easy reading. This is intentionally lightweight compared to a full DVCS.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Errors that can occur in workspace storage backends.
#[derive(Debug, thiserror::Error)]
pub enum WorkspaceStorageError {
  #[error("storage error: {0}")]
  Storage(#[from] crate::StorageError),
  #[error("workspace not found: {0}")]
  NotFound(String),
  #[error("corrupt version log: {0}")]
  CorruptVersionLog(String),
}

impl From<std::io::Error> for WorkspaceStorageError {
  fn from(error: std::io::Error) -> Self {
    Self::Storage(crate::StorageError::Io(error))
  }
}

impl From<toml::de::Error> for WorkspaceStorageError {
  fn from(error: toml::de::Error) -> Self {
    Self::CorruptVersionLog(error.to_string())
  }
}

impl From<toml::ser::Error> for WorkspaceStorageError {
  fn from(error: toml::ser::Error) -> Self {
    Self::CorruptVersionLog(error.to_string())
  }
}

/// A single version of a versioned resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInfo {
  pub hash: String,
  pub message: String,
  pub timestamp_ms: i64,
}

/// A content store backed by a version-control system.
pub trait VersionedContentStore: std::fmt::Debug + Send + Sync {
  /// Ensure the underlying repository exists and is initialized.
  fn initialize(&self) -> Result<(), WorkspaceStorageError>;

  /// Write a new version of `id` with the given content and commit message.
  ///
  /// Returns the hash of the recorded snapshot.
  fn write(&self, id: &str, content: &str, message: &str) -> Result<String, WorkspaceStorageError>;

  /// Read the latest version of `id`.
  fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError>;

  /// Return the hash of the latest recorded change for `id`, if any.
  fn latest_hash(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError>;

  /// Return the version history of `id`, most recent first.
  fn history(&self, id: &str) -> Result<Vec<VersionInfo>, WorkspaceStorageError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionEntry {
  hash: String,
  message: String,
  timestamp_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct VersionLog {
  versions: Vec<VersionEntry>,
}

/// A filesystem-backed content store.
///
/// Each store uses its own directory. Resources are stored as files under a
/// per-resource subdirectory; a TOML log keeps the ordered version metadata.
#[derive(Debug, Clone)]
pub struct SnapshotContentStore {
  pub(crate) repo_path: PathBuf,
}

impl SnapshotContentStore {
  pub fn new(repo_path: PathBuf) -> Self {
    Self { repo_path }
  }

  fn resource_dir(&self, id: &str) -> PathBuf {
    self.repo_path.join(id)
  }

  fn content_path(&self, id: &str, hash: &str) -> PathBuf {
    self.resource_dir(id).join(format!("{hash}.toml"))
  }

  fn latest_path(&self, id: &str) -> PathBuf {
    self.resource_dir(id).join("latest.toml")
  }

  fn log_path(&self, id: &str) -> PathBuf {
    self.resource_dir(id).join("versions.toml")
  }

  fn load_log(&self, id: &str) -> Result<VersionLog, WorkspaceStorageError> {
    let path = self.log_path(id);
    if !path.exists() {
      return Ok(VersionLog::default());
    }
    Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
  }

  fn save_log(&self, id: &str, log: &VersionLog) -> Result<(), WorkspaceStorageError> {
    std::fs::write(self.log_path(id), toml::to_string(log)?)?;
    Ok(())
  }
}

impl VersionedContentStore for SnapshotContentStore {
  fn initialize(&self) -> Result<(), WorkspaceStorageError> {
    std::fs::create_dir_all(&self.repo_path)?;
    Ok(())
  }

  fn write(&self, id: &str, content: &str, message: &str) -> Result<String, WorkspaceStorageError> {
    self.initialize()?;
    let dir = self.resource_dir(id);
    std::fs::create_dir_all(&dir)?;

    let timestamp_ms = jiff::Timestamp::now().as_millisecond();
    let hash = blake3::hash(content.as_bytes()).to_hex().to_string();
    std::fs::write(self.content_path(id, &hash), content)?;
    std::fs::write(self.latest_path(id), content)?;

    let mut log = self.load_log(id)?;
    log.versions.push(VersionEntry {
      hash: hash.clone(),
      message: message.to_string(),
      timestamp_ms,
    });
    self.save_log(id, &log)?;
    Ok(hash)
  }

  fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    let path = self.latest_path(id);
    if !path.exists() {
      return Ok(None);
    }
    Ok(Some(std::fs::read_to_string(path)?))
  }

  fn latest_hash(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    let log = self.load_log(id)?;
    Ok(log.versions.last().map(|entry| entry.hash.clone()))
  }

  fn history(&self, id: &str) -> Result<Vec<VersionInfo>, WorkspaceStorageError> {
    let log = self.load_log(id)?;
    Ok(
      log
        .versions
        .into_iter()
        .rev()
        .map(|entry| VersionInfo {
          hash: entry.hash,
          message: entry.message,
          timestamp_ms: entry.timestamp_ms,
        })
        .collect(),
    )
  }
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  fn test_store() -> (TempDir, SnapshotContentStore) {
    let dir = TempDir::new().unwrap();
    let store = SnapshotContentStore::new(dir.path().to_path_buf());
    (dir, store)
  }

  #[test]
  fn initialize_creates_repository() {
    let (dir, store) = test_store();
    store.initialize().unwrap();
    assert!(dir.path().exists());
  }

  #[test]
  fn write_and_read_version() {
    let (_dir, store) = test_store();
    store.write("skill-1", "v1", "initial").unwrap();
    store.write("skill-1", "v2", "update").unwrap();

    let latest = store.read("skill-1").unwrap().unwrap();
    assert_eq!(latest, "v2");
  }

  #[test]
  fn latest_hash_returns_most_recent_change() {
    let (_dir, store) = test_store();
    let first = store.write("skill-1", "v1", "initial").unwrap();
    let second = store.write("skill-1", "v2", "update").unwrap();

    let latest = store.latest_hash("skill-1").unwrap().unwrap();
    assert_eq!(latest, second);
    assert_ne!(latest, first);
  }

  #[test]
  fn latest_hash_missing_returns_none() {
    let (_dir, store) = test_store();
    assert!(store.latest_hash("missing").unwrap().is_none());
  }

  #[test]
  fn history_returns_versions_newest_first() {
    let (_dir, store) = test_store();
    store.write("skill-1", "v1", "initial").unwrap();
    store.write("skill-1", "v2", "update").unwrap();
    store.write("skill-1", "v3", "final").unwrap();

    let history = store.history("skill-1").unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].message, "final");
    assert_eq!(history[1].message, "update");
    assert_eq!(history[2].message, "initial");
    assert!(!history[0].hash.is_empty());
    assert!(history[0].timestamp_ms >= history[1].timestamp_ms);
    assert!(history[1].timestamp_ms >= history[2].timestamp_ms);
  }
}
