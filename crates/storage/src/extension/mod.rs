//! Extension package storage.
//!
//! This domain persists cluster-synchronized extension packages (extension
//! system design, section 4):
//! - [`ExtensionRecord`] metadata lives in redb, serialized with postcard; the
//!   manifest is a `BTreeMap` so the encoding stays deterministic across
//!   processes (same lesson as `WorkspaceRecord`).
//! - Artifact bytes live in a plain filesystem blob store — not in git:
//!   artifacts are immutable, content-addressed bytes; history lives in the
//!   version sequence, not in a VCS.
//!
//! The record implements [`VersionedRecord`], so the shared apply pipeline
//! rules (`should_apply_versioned`, per-domain mutex, content-before-metadata
//! ordering) are reused unchanged.

use std::{
  collections::BTreeMap,
  path::PathBuf,
  sync::{Arc, Mutex},
};

use lycoris_core::ResourceScope;
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::{bytes::Bytes, resource_id, table::RedbTableStorage, versioned::VersionedRecord};

/// redb table holding extension metadata records.
pub(crate) const EXTENSIONS: TableDefinition<&str, Bytes> = TableDefinition::new("extensions");

/// Persistent metadata for an extension package.
///
/// The artifact itself is stored in the [`ExtensionBlobStore`]; this record
/// carries everything needed for lookup, synchronization and version
/// conflict resolution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtensionRecord {
  pub id: String,
  pub name: String,
  /// Monotonic convergence version ordered by anti-entropy.
  pub version: u64,
  /// Execution engine for the artifact (`"wasm"` or `"lua"`).
  pub engine: String,
  /// Exported entry point name (`invoke` unless overridden).
  pub entry: String,
  /// blake3 hex digest of the artifact, verified at ingest.
  pub content_hash: String,
  pub scope: ResourceScope,
  /// `None` means this resource originated on the local node.
  pub source_node_id: Option<String>,
  /// Creation time of the first version: set by the origin node when the
  /// resource is first written and preserved across updates. Anti-entropy
  /// applies take the wire value as authoritative.
  pub created_at_ms: i64,
  pub updated_at_ms: i64,
  /// Extension configuration (`semver`, `capabilities`, `hooks`, `selector`,
  /// `settings`); `BTreeMap` keeps the postcard encoding deterministic.
  pub manifest: BTreeMap<String, String>,
  /// Generic metadata labels matched by list selectors (same role as
  /// `VersionedResource::metadata`); `BTreeMap` keeps the postcard encoding
  /// deterministic across processes.
  pub labels: BTreeMap<String, String>,
}

impl VersionedRecord for ExtensionRecord {
  fn version(&self) -> u64 {
    self.version
  }

  fn updated_at_ms(&self) -> i64 {
    self.updated_at_ms
  }

  fn content_hash(&self) -> &str {
    &self.content_hash
  }

  fn scope(&self) -> ResourceScope {
    self.scope
  }
}

/// Filesystem-backed store for extension artifact bytes.
///
/// Artifacts are kept as raw bytes under `<data_dir>/extensions/blobs/<id>`;
/// the id whitelist shared with the other content stores keeps remote-supplied
/// ids inside the directory.
#[derive(Debug, Clone)]
pub struct ExtensionBlobStore {
  dir: PathBuf,
}

impl ExtensionBlobStore {
  pub(crate) fn new(dir: PathBuf) -> Self {
    Self { dir }
  }

  /// Store the artifact bytes of an extension, replacing any previous artifact.
  pub fn write(&self, id: &str, artifact: &[u8]) -> Result<(), ExtensionStorageError> {
    resource_id::validate(id)?;
    std::fs::create_dir_all(&self.dir)?;
    std::fs::write(self.dir.join(id), artifact)?;
    Ok(())
  }

  /// Read the artifact bytes of an extension, if any are stored.
  pub fn read(&self, id: &str) -> Result<Option<Vec<u8>>, ExtensionStorageError> {
    resource_id::validate(id)?;
    match std::fs::read(self.dir.join(id)) {
      Ok(artifact) => Ok(Some(artifact)),
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
      Err(error) => Err(error.into()),
    }
  }
}

/// Errors that can occur in extension storage backends.
#[derive(Debug, thiserror::Error)]
pub enum ExtensionStorageError {
  #[error("storage error: {0}")]
  Storage(#[from] crate::StorageError),
  #[error("content hash mismatch")]
  HashMismatch(#[from] crate::versioned::ContentHashMismatch),
  /// A resource id that is not safe to use as a blob-store file name.
  #[error(transparent)]
  InvalidResourceId(#[from] resource_id::InvalidResourceId),
}

impl From<std::io::Error> for ExtensionStorageError {
  fn from(error: std::io::Error) -> Self {
    Self::Storage(crate::StorageError::Io(error))
  }
}

/// Extension storage facade.
///
/// Provides access to extension metadata records and the artifact blob store.
/// The underlying redb database is shared with the other storage domains via
/// an `Arc`. Records enter through [`Self::apply_remote_extension`]: the
/// anti-entropy apply pipeline calls it for synced resources, and the
/// admission-side registration path (`ExtensionManager::register` in the
/// daemon) reuses it so both write paths share one ordering and validation.
#[derive(Debug, Clone)]
pub struct ExtensionDomain {
  records: RedbTableStorage<ExtensionRecord>,
  blobs: ExtensionBlobStore,
  /// Serializes the read-check-write apply pipeline so concurrent applies of
  /// the same extension cannot interleave and let an older version win the
  /// final write. The critical sections are fully synchronous, so a standard
  /// mutex is sufficient.
  apply_lock: Arc<Mutex<()>>,
}

impl ExtensionDomain {
  pub(crate) fn new(db: Arc<Database>, data_dir: PathBuf) -> Self {
    Self {
      records: RedbTableStorage::new(db, EXTENSIONS),
      blobs: ExtensionBlobStore::new(data_dir.join("extensions").join("blobs")),
      apply_lock: Arc::new(Mutex::new(())),
    }
  }

  /// Access the filesystem-backed artifact blob store.
  pub fn blobs(&self) -> &ExtensionBlobStore {
    &self.blobs
  }

  /// Return the extension record with the given id, if any.
  pub fn get(&self, id: &str) -> Result<Option<ExtensionRecord>, ExtensionStorageError> {
    Ok(self.records.get(id)?)
  }

  /// Return all extension records.
  pub fn list(&self) -> Result<Vec<ExtensionRecord>, ExtensionStorageError> {
    Ok(self.records.list()?)
  }

  /// Return extensions whose scope is `ClusterShared`.
  pub fn list_shared(&self) -> Result<Vec<ExtensionRecord>, ExtensionStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|record| record.scope == ResourceScope::ClusterShared)
        .collect(),
    )
  }

  /// Return extensions whose scope is `NodeLocal`.
  pub fn list_local(&self) -> Result<Vec<ExtensionRecord>, ExtensionStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|record| record.scope == ResourceScope::NodeLocal)
        .collect(),
    )
  }

  /// Delete an extension metadata record. The artifact blob is left in place,
  /// matching the content stores of the other domains, which keep history.
  pub fn delete(&self, id: &str) -> Result<(), ExtensionStorageError> {
    Ok(self.records.delete(id)?)
  }

  /// Apply a remote extension if it wins the version/scope conflict check.
  ///
  /// The artifact integrity is verified against the declared content hash
  /// first; the blob is then written *before* the metadata record (same
  /// failure atomicity as the workspace domain): if the blob write fails, the
  /// stored metadata still points at the previous content hash and a retry of
  /// the same record is not skipped, so the pipeline converges.
  ///
  /// Returns `true` when the extension was stored, `false` when it was skipped.
  pub fn apply_remote_extension(
    &self, record: ExtensionRecord, artifact: &[u8],
  ) -> Result<bool, ExtensionStorageError> {
    let _guard = self.lock_apply();
    if artifact.is_empty() {
      return Ok(false);
    }
    crate::versioned::verify_content_hash(&crate::hash_content(artifact), &record.content_hash)?;
    let local = self.get(&record.id)?;
    if !crate::versioned::should_apply_versioned(local.as_ref(), &record) {
      return Ok(false);
    }
    if local
      .as_ref()
      .is_none_or(|local| local.content_hash != record.content_hash)
    {
      self.blobs.write(&record.id, artifact)?;
    }
    self.records.upsert(&record.id, &record)?;
    Ok(true)
  }

  /// Serialize the whole apply pipeline (read local, decide, write back).
  ///
  /// Poisoning only means a previous apply panicked mid-write; the stores are
  /// left in their last committed state, so applying may safely continue.
  fn lock_apply(&self) -> std::sync::MutexGuard<'_, ()> {
    self
      .apply_lock
      .lock()
      .unwrap_or_else(std::sync::PoisonError::into_inner)
  }
}

#[cfg(test)]
mod tests {
  use lycoris_core::now_ms;
  use tempfile::TempDir;

  use super::*;
  use crate::Storage;

  fn test_domain() -> (TempDir, ExtensionDomain) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("extension.redb")).unwrap();
    (dir, storage.extensions().clone())
  }

  fn extension_record(id: &str, scope: ResourceScope) -> ExtensionRecord {
    ExtensionRecord {
      id: id.to_string(),
      name: format!("extension-{id}"),
      version: 1,
      engine: "lua".to_string(),
      entry: "invoke".to_string(),
      content_hash: String::new(),
      scope,
      source_node_id: None,
      created_at_ms: now_ms(),
      updated_at_ms: now_ms(),
      manifest: [("semver".to_string(), "0.1.0".to_string())]
        .into_iter()
        .collect(),
      labels: BTreeMap::new(),
    }
  }

  fn shared_extension(id: &str, artifact: &[u8], version: u64) -> ExtensionRecord {
    let mut record = extension_record(id, ResourceScope::ClusterShared);
    record.content_hash = crate::hash_content(artifact);
    record.version = version;
    record
  }

  #[test]
  fn extension_record_round_trip() {
    let (_dir, domain) = test_domain();
    let record = extension_record("p-1", ResourceScope::NodeLocal);

    domain.records.upsert(&record.id, &record).unwrap();
    let loaded = domain.get("p-1").unwrap().unwrap();

    assert_eq!(loaded, record);
  }

  #[test]
  fn extension_scope_filtering() {
    let (_dir, domain) = test_domain();
    let shared = extension_record("shared-p", ResourceScope::ClusterShared);
    let local = extension_record("local-p", ResourceScope::NodeLocal);
    domain.records.upsert(&shared.id, &shared).unwrap();
    domain.records.upsert(&local.id, &local).unwrap();

    let shared_list = domain.list_shared().unwrap();
    assert_eq!(shared_list.len(), 1);
    assert_eq!(shared_list[0].id, "shared-p");

    let local_list = domain.list_local().unwrap();
    assert_eq!(local_list.len(), 1);
    assert_eq!(local_list[0].id, "local-p");

    domain.delete("shared-p").unwrap();
    assert!(domain.list_shared().unwrap().is_empty());
    assert_eq!(domain.list().unwrap().len(), 1);
  }

  #[test]
  fn blob_write_and_read_round_trip() {
    let (_dir, domain) = test_domain();
    domain.blobs().write("p-1", b"\0asm-bytes").unwrap();
    assert_eq!(domain.blobs().read("p-1").unwrap().unwrap(), b"\0asm-bytes");
    assert_eq!(domain.blobs().read("missing").unwrap(), None);
  }

  #[test]
  fn blob_store_rejects_ids_that_escape_the_directory() {
    let (dir, domain) = test_domain();
    for id in ["../escape", "a/b", "", ".hidden", "a..b", "a\\b", "a b"] {
      let error = domain.blobs().write(id, b"x").unwrap_err();
      assert!(
        matches!(error, ExtensionStorageError::InvalidResourceId(_)),
        "write id: {id:?}"
      );
      let error = domain.blobs().read(id).unwrap_err();
      assert!(
        matches!(error, ExtensionStorageError::InvalidResourceId(_)),
        "read id: {id:?}"
      );
    }
    // No file may have been created anywhere for the rejected ids.
    assert!(!dir.path().join("extensions").join("escape").exists());
  }

  #[test]
  fn apply_remote_extension_stores_new_extension_and_artifact() {
    let (_dir, domain) = test_domain();
    let artifact = b"return {}";
    let record = shared_extension("remote-p", artifact, 1);

    let applied = domain.apply_remote_extension(record, artifact).unwrap();
    assert!(applied);

    let loaded = domain.get("remote-p").unwrap().unwrap();
    assert_eq!(loaded.version, 1);
    assert_eq!(loaded.engine, "lua");
    assert_eq!(domain.blobs().read("remote-p").unwrap().unwrap(), artifact);
  }

  #[test]
  fn apply_remote_extension_rejects_hash_mismatch_without_persisting() {
    let (_dir, domain) = test_domain();
    let artifact = b"real artifact";
    let mut record = shared_extension("p-hash", artifact, 1);
    record.content_hash = "wrong-hash".to_string();

    let error = domain.apply_remote_extension(record, artifact).unwrap_err();
    assert!(matches!(error, ExtensionStorageError::HashMismatch(_)));
    assert!(domain.get("p-hash").unwrap().is_none());
    assert_eq!(domain.blobs().read("p-hash").unwrap(), None);
  }

  #[test]
  fn apply_remote_extension_rejects_empty_artifact() {
    let (_dir, domain) = test_domain();
    let record = shared_extension("p-empty", b"", 1);

    let applied = domain.apply_remote_extension(record, b"").unwrap();
    assert!(!applied);
    assert!(domain.get("p-empty").unwrap().is_none());
  }

  #[test]
  fn apply_remote_extension_skips_older_version() {
    let (_dir, domain) = test_domain();
    let local_artifact = b"local v2";
    let local = shared_extension("p-conflict", local_artifact, 2);
    domain
      .apply_remote_extension(local, local_artifact)
      .unwrap();

    let remote_artifact = b"remote v1";
    let remote = shared_extension("p-conflict", remote_artifact, 1);
    let applied = domain
      .apply_remote_extension(remote, remote_artifact)
      .unwrap();
    assert!(!applied);

    let loaded = domain.get("p-conflict").unwrap().unwrap();
    assert_eq!(loaded.version, 2);
    assert_eq!(
      domain.blobs().read("p-conflict").unwrap().unwrap(),
      local_artifact
    );
  }

  #[test]
  fn apply_remote_extension_skips_node_local_scope() {
    let (_dir, domain) = test_domain();
    let artifact = b"local artifact";
    let mut record = extension_record("p-local", ResourceScope::NodeLocal);
    record.content_hash = crate::hash_content(artifact);

    let applied = domain.apply_remote_extension(record, artifact).unwrap();
    assert!(!applied);
    assert!(domain.get("p-local").unwrap().is_none());
  }

  #[test]
  fn apply_remote_extension_does_not_rewrite_unchanged_artifact() {
    let (_dir, domain) = test_domain();
    let artifact = b"stable artifact";
    let local = shared_extension("p-stable", artifact, 1);
    domain
      .apply_remote_extension(local.clone(), artifact)
      .unwrap();

    let mut remote = shared_extension("p-stable", artifact, 2);
    remote.updated_at_ms = local.updated_at_ms + 1;

    let applied = domain.apply_remote_extension(remote, artifact).unwrap();
    assert!(applied);
    let loaded = domain.get("p-stable").unwrap().unwrap();
    assert_eq!(loaded.version, 2);
    assert_eq!(domain.blobs().read("p-stable").unwrap().unwrap(), artifact);
  }

  #[test]
  fn apply_remote_extension_persists_no_metadata_when_blob_write_fails() {
    let (dir, domain) = test_domain();
    let artifact = b"fragile artifact";
    let record = shared_extension("p-fragile", artifact, 1);
    // A directory where the blob file should be makes the write fail.
    std::fs::create_dir_all(
      dir
        .path()
        .join("extensions")
        .join("blobs")
        .join("p-fragile"),
    )
    .unwrap();

    let error = domain.apply_remote_extension(record, artifact).unwrap_err();
    assert!(matches!(error, ExtensionStorageError::Storage(_)));

    // The metadata must not point at an artifact that was never written; a
    // retry of the same record has to be applied instead of skipped.
    assert!(domain.get("p-fragile").unwrap().is_none());
  }

  #[test]
  fn concurrent_applies_converge_to_highest_version() {
    let (_dir, domain) = test_domain();
    let mut handles = Vec::new();
    for version in 1..=8_u64 {
      let domain = domain.clone();
      handles.push(std::thread::spawn(move || {
        let artifact = format!("artifact v{version}").into_bytes();
        let record = shared_extension("p-race", &artifact, version);
        domain.apply_remote_extension(record, &artifact).unwrap();
      }));
    }
    for handle in handles {
      handle.join().unwrap();
    }

    let loaded = domain.get("p-race").unwrap().unwrap();
    assert_eq!(loaded.version, 8);
    assert_eq!(
      domain.blobs().read("p-race").unwrap().unwrap(),
      b"artifact v8"
    );
  }
}
