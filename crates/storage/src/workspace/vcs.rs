//! Version-controlled content storage for skills and rules.
//!
//! Skill and rule bodies are more than static prompts: they can contain
//! scripts, tool definitions, and other executable artifacts that evolve over
//! time. We therefore keep them in a real version-control system rather than a
//! simple file store.
//!
//! The current backend is **Pijul** via `pijul-core`. The abstraction is
//! intentionally small so that the storage layer does not depend directly on
//! Pijul's internal concepts outside this module.

use std::path::PathBuf;

use pijul_core::{
  Hash, MutTxnTExt, TxnTExt, apply,
  change::{Change, ChangeHeader},
  changestore::ChangeStore,
  pristine::{Base32, ChannelRef, Position, sanakirja::SanakirjaError},
  record::{Algorithm, Builder},
  small_string::SmallString,
  working_copy::WorkingCopy,
};

/// Errors that can occur in workspace storage backends.
#[derive(Debug, thiserror::Error)]
pub enum WorkspaceStorageError {
  #[error("storage error: {0}")]
  Storage(#[from] crate::StorageError),
  #[error("workspace not found: {0}")]
  NotFound(String),
  #[error("pijul error: {0}")]
  Pijul(String),
  #[error("pijul change error: {0}")]
  Change(#[from] pijul_core::change::ChangeError),
  #[error("corrupt state in database: {0}")]
  CorruptState(String),
}

impl From<std::io::Error> for WorkspaceStorageError {
  fn from(error: std::io::Error) -> Self {
    Self::Storage(crate::StorageError::Io(error))
  }
}

impl From<pijul_core::changestore::filesystem::Error> for WorkspaceStorageError {
  fn from(error: pijul_core::changestore::filesystem::Error) -> Self {
    Self::Pijul(error.to_string())
  }
}

impl From<SanakirjaError> for WorkspaceStorageError {
  fn from(error: SanakirjaError) -> Self {
    Self::Pijul(error.to_string())
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
  /// Returns the hash of the recorded change.
  fn write(&self, id: &str, content: &str, message: &str) -> Result<String, WorkspaceStorageError>;

  /// Read the latest version of `id`.
  fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError>;

  /// Return the hash of the latest recorded change for `id`, if any.
  fn latest_hash(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError>;

  /// Return the version history of `id`, most recent first.
  fn history(&self, id: &str) -> Result<Vec<VersionInfo>, WorkspaceStorageError>;
}

/// A `pijul-core`-backed content store.
///
/// Each store uses its own Pijul repository located at `repo_path`. Resources
/// are stored as files at the repository root, one file per resource id.
#[derive(Debug, Clone)]
pub struct PijulContentStore {
  pub(crate) repo_path: PathBuf,
}

impl PijulContentStore {
  pub fn new(repo_path: PathBuf) -> Self {
    Self { repo_path }
  }

  fn resource_path(&self, id: &str) -> PathBuf {
    self.repo_path.join(format!("{id}.toml"))
  }

  fn pristine_path(&self) -> PathBuf {
    self.repo_path.join(".pijul").join("pristine")
  }

  fn changes_dir(&self) -> PathBuf {
    self.repo_path.join(".pijul").join("changes")
  }

  fn working_copy(&self) -> pijul_core::working_copy::filesystem::FileSystem {
    pijul_core::working_copy::filesystem::FileSystem::from_root(&self.repo_path)
  }

  fn file_position<T: pijul_core::pristine::TreeTxnT>(
    &self, txn: &T, id: &str,
  ) -> Result<Option<Position<pijul_core::pristine::ChangeId>>, WorkspaceStorageError> {
    let vertex = pijul_core::fs::get_vertex(txn, &format!("{id}.toml")).map_err(pijul_err)?;
    if !vertex.remaining {
      return Ok(None);
    }
    Ok(vertex.result)
  }

  fn change_store(&self) -> pijul_core::changestore::filesystem::FileSystem {
    pijul_core::changestore::filesystem::FileSystem::from_changes(self.changes_dir(), 100)
  }

  fn pristine(&self) -> Result<pijul_core::pristine::sanakirja::Pristine, WorkspaceStorageError> {
    let path = self.pristine_path();
    if let Some(parent) = path.parent() {
      std::fs::create_dir_all(parent)?;
    }
    Ok(pijul_core::pristine::sanakirja::Pristine::new(path)?)
  }
}

impl VersionedContentStore for PijulContentStore {
  fn initialize(&self) -> Result<(), WorkspaceStorageError> {
    std::fs::create_dir_all(&self.repo_path)?;
    let pristine = self.pristine()?;
    let _ = pristine.mut_txn_begin()?;
    Ok(())
  }

  fn write(&self, id: &str, content: &str, message: &str) -> Result<String, WorkspaceStorageError> {
    self.initialize()?;
    let path = self.resource_path(id);
    std::fs::write(&path, content)?;

    let pristine = self.pristine()?;
    let txn = pristine.arc_txn_begin()?;
    let path_str = format!("{id}.toml");

    {
      let txn_read = txn.read();
      if !txn_read.is_tracked(&path_str).map_err(pijul_err)? {
        drop(txn_read);
        let mut txn_write = txn.write();
        txn_write.add_file(&path_str, 0).map_err(pijul_err)?;
      }
    }

    let channel = open_channel(&txn).map_err(pijul_err)?;
    let working_copy = self.working_copy();
    let change_store = self.change_store();

    let hash = record_change(&working_copy, &change_store, &txn, &channel, "", message)
      .map_err(pijul_err)?;

    txn.commit().map_err(pijul_err)?;
    Ok(format!("{:?}", hash))
  }

  fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    let path = self.resource_path(id);
    if !path.exists() {
      return Ok(None);
    }
    Ok(Some(std::fs::read_to_string(path)?))
  }

  fn latest_hash(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    let pristine = self.pristine()?;
    let txn = pristine.arc_txn_begin()?;
    let channel = open_channel(&txn).map_err(pijul_err)?;
    let txn_guard = txn.read();
    let channel_guard = channel.read();
    let Some(position) = self.file_position(&*txn_guard, id)? else {
      return Ok(None);
    };
    let log = txn_guard
      .log_for_path(&*channel_guard, position, 0)
      .map_err(pijul_err)?;
    let entries: Vec<_> = log.collect::<Result<Vec<_>, _>>().map_err(pijul_err)?;
    Ok(
      entries
        .last()
        .map(|hash: &pijul_core::pristine::Hash| hash.to_base32()),
    )
  }

  fn history(&self, id: &str) -> Result<Vec<VersionInfo>, WorkspaceStorageError> {
    let change_store = self.change_store();
    let pristine = self.pristine()?;
    let txn = pristine.arc_txn_begin()?;
    let channel = open_channel(&txn).map_err(pijul_err)?;
    let txn_guard = txn.read();
    let channel_guard = channel.read();
    let Some(position) = self.file_position(&*txn_guard, id)? else {
      return Ok(Vec::new());
    };
    let log = txn_guard
      .log_for_path(&*channel_guard, position, 0)
      .map_err(pijul_err)?;
    let entries: Vec<_> = log.collect::<Result<Vec<_>, _>>().map_err(pijul_err)?;
    let mut history = Vec::with_capacity(entries.len());
    for hash in entries.into_iter().rev() {
      let header = change_store
        .get_header(&hash)
        .map_err(|error| WorkspaceStorageError::Pijul(error.to_string()))?;
      history.push(VersionInfo {
        hash: hash.to_base32(),
        message: header.message,
        timestamp_ms: header.timestamp.as_millisecond(),
      });
    }
    Ok(history)
  }
}

fn pijul_err(error: impl std::fmt::Display) -> WorkspaceStorageError {
  WorkspaceStorageError::Pijul(error.to_string())
}

fn open_channel<T: pijul_core::pristine::MutTxnT>(
  txn: &pijul_core::pristine::ArcTxn<T>,
) -> Result<ChannelRef<T>, T::GraphError> {
  let name = SmallString::from_str("main");
  txn.write().open_or_create_channel(&name)
}

fn record_change<
  T: pijul_core::pristine::MutTxnT + Send + Sync + 'static,
  W: WorkingCopy + Clone + Send + Sync + 'static,
  C: ChangeStore + Clone + Send + 'static,
>(
  working_copy: &W, change_store: &C, txn: &pijul_core::pristine::ArcTxn<T>,
  channel: &ChannelRef<T>, prefix: &str, message: &str,
) -> Result<Hash, WorkspaceStorageError>
where
  W::Error: Send + Sync + 'static,
  C::Error: std::error::Error + Send + Sync + 'static,
  WorkspaceStorageError: From<C::Error>, {
  let mut state = Builder::new();
  state
    .record(
      txn.clone(),
      Algorithm::default(),
      false,
      &pijul_core::DEFAULT_SEPARATOR,
      channel.clone(),
      working_copy,
      change_store,
      prefix,
      1,
    )
    .map_err(pijul_err)?;

  let rec = state.finish();
  let actions: Vec<_> = rec
    .actions
    .into_iter()
    .map(|rec| rec.globalize(&*txn.read()).map_err(pijul_err))
    .collect::<Result<Vec<_>, _>>()?;

  let mut change = Change::make_change(
    &*txn.read(),
    channel,
    actions,
    std::mem::take(&mut *rec.contents.lock()),
    ChangeHeader {
      message: message.to_string(),
      authors: vec![],
      description: None,
      timestamp: jiff::Timestamp::now(),
    },
    Vec::new(),
  )
  .map_err(pijul_err)?;

  let hash = change_store
    .save_change(&mut change, |_, _| Ok::<(), WorkspaceStorageError>(()))
    .map_err(pijul_err)?;

  apply::apply_local_change(&mut *txn.write(), channel, &change, &hash, &rec.updatables)
    .map_err(pijul_err)?;

  Ok(hash)
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  fn test_store() -> (TempDir, PijulContentStore) {
    let dir = TempDir::new().unwrap();
    let store = PijulContentStore::new(dir.path().to_path_buf());
    (dir, store)
  }

  #[test]
  fn initialize_creates_pijul_repository() {
    let (dir, store) = test_store();
    store.initialize().unwrap();
    assert!(dir.path().join(".pijul").join("pristine").exists());
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
