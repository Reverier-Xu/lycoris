//! Plugin package storage.
//!
//! This domain persists cluster-synchronized plugin packages (plugin system
//! design, section 4):
//! - [`PluginRecord`] metadata lives in redb, serialized with postcard; the
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

/// redb table holding plugin metadata records.
pub(crate) const PLUGINS: TableDefinition<&str, Bytes> = TableDefinition::new("plugins");

/// Persistent metadata for a plugin package.
///
/// The artifact itself is stored in the [`PluginBlobStore`]; this record
/// carries everything needed for lookup, synchronization and version
/// conflict resolution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginRecord {
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
  /// Plugin configuration (`semver`, `capabilities`, `hooks`, `selector`,
  /// `settings`); `BTreeMap` keeps the postcard encoding deterministic.
  pub manifest: BTreeMap<String, String>,
}

impl VersionedRecord for PluginRecord {
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

/// Filesystem-backed store for plugin artifact bytes.
///
/// Artifacts are kept as raw bytes under `<data_dir>/plugins/blobs/<id>`; the
/// id whitelist shared with the other content stores keeps remote-supplied
/// ids inside the directory.
#[derive(Debug, Clone)]
pub struct PluginBlobStore {
  dir: PathBuf,
}

impl PluginBlobStore {
  pub(crate) fn new(dir: PathBuf) -> Self {
    Self { dir }
  }

  /// Store the artifact bytes of a plugin, replacing any previous artifact.
  pub fn write(&self, id: &str, artifact: &[u8]) -> Result<(), PluginStorageError> {
    resource_id::validate(id)?;
    std::fs::create_dir_all(&self.dir)?;
    std::fs::write(self.dir.join(id), artifact)?;
    Ok(())
  }

  /// Read the artifact bytes of a plugin, if any are stored.
  pub fn read(&self, id: &str) -> Result<Option<Vec<u8>>, PluginStorageError> {
    resource_id::validate(id)?;
    match std::fs::read(self.dir.join(id)) {
      Ok(artifact) => Ok(Some(artifact)),
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
      Err(error) => Err(error.into()),
    }
  }
}

/// Errors that can occur in plugin storage backends.
#[derive(Debug, thiserror::Error)]
pub enum PluginStorageError {
  #[error("storage error: {0}")]
  Storage(#[from] crate::StorageError),
  #[error("content hash mismatch")]
  HashMismatch(#[from] crate::versioned::ContentHashMismatch),
  /// A resource id that is not safe to use as a blob-store file name.
  #[error(transparent)]
  InvalidResourceId(#[from] resource_id::InvalidResourceId),
}

impl From<std::io::Error> for PluginStorageError {
  fn from(error: std::io::Error) -> Self {
    Self::Storage(crate::StorageError::Io(error))
  }
}

/// Plugin storage facade.
///
/// Provides access to plugin metadata records and the artifact blob store.
/// The underlying redb database is shared with the other storage domains via
/// an `Arc`. The write path for locally created plugins is deliberately left
/// to a future change; records currently enter only through the anti-entropy
/// apply pipeline.
#[derive(Debug, Clone)]
pub struct PluginDomain {
  records: RedbTableStorage<PluginRecord>,
  blobs: PluginBlobStore,
  /// Serializes the read-check-write apply pipeline so concurrent applies of
  /// the same plugin cannot interleave and let an older version win the final
  /// write. The critical sections are fully synchronous, so a standard mutex
  /// is sufficient.
  apply_lock: Arc<Mutex<()>>,
}

impl PluginDomain {
  pub(crate) fn new(db: Arc<Database>, data_dir: PathBuf) -> Self {
    Self {
      records: RedbTableStorage::new(db, PLUGINS),
      blobs: PluginBlobStore::new(data_dir.join("plugins").join("blobs")),
      apply_lock: Arc::new(Mutex::new(())),
    }
  }

  /// Access the filesystem-backed artifact blob store.
  pub fn blobs(&self) -> &PluginBlobStore {
    &self.blobs
  }

  /// Return the plugin record with the given id, if any.
  pub fn get(&self, id: &str) -> Result<Option<PluginRecord>, PluginStorageError> {
    Ok(self.records.get(id)?)
  }

  /// Return all plugin records.
  pub fn list(&self) -> Result<Vec<PluginRecord>, PluginStorageError> {
    Ok(self.records.list()?)
  }

  /// Return plugins whose scope is `ClusterShared`.
  pub fn list_shared(&self) -> Result<Vec<PluginRecord>, PluginStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|record| record.scope == ResourceScope::ClusterShared)
        .collect(),
    )
  }

  /// Return plugins whose scope is `NodeLocal`.
  pub fn list_local(&self) -> Result<Vec<PluginRecord>, PluginStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|record| record.scope == ResourceScope::NodeLocal)
        .collect(),
    )
  }

  /// Delete a plugin metadata record. The artifact blob is left in place,
  /// matching the content stores of the other domains, which keep history.
  pub fn delete(&self, id: &str) -> Result<(), PluginStorageError> {
    Ok(self.records.delete(id)?)
  }

  /// Apply a remote plugin if it wins the version/scope conflict check.
  ///
  /// The artifact integrity is verified against the declared content hash
  /// first; the blob is then written *before* the metadata record (same
  /// failure atomicity as the workspace domain): if the blob write fails, the
  /// stored metadata still points at the previous content hash and a retry of
  /// the same record is not skipped, so the pipeline converges.
  ///
  /// Returns `true` when the plugin was stored, `false` when it was skipped.
  pub fn apply_remote_plugin(
    &self, record: PluginRecord, artifact: &[u8],
  ) -> Result<bool, PluginStorageError> {
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

  fn test_domain() -> (TempDir, PluginDomain) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("plugin.redb")).unwrap();
    (dir, storage.plugins().clone())
  }

  fn plugin_record(id: &str, scope: ResourceScope) -> PluginRecord {
    PluginRecord {
      id: id.to_string(),
      name: format!("plugin-{id}"),
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
    }
  }

  fn shared_plugin(id: &str, artifact: &[u8], version: u64) -> PluginRecord {
    let mut record = plugin_record(id, ResourceScope::ClusterShared);
    record.content_hash = crate::hash_content(artifact);
    record.version = version;
    record
  }

  #[test]
  fn plugin_record_round_trip() {
    let (_dir, domain) = test_domain();
    let record = plugin_record("p-1", ResourceScope::NodeLocal);

    domain.records.upsert(&record.id, &record).unwrap();
    let loaded = domain.get("p-1").unwrap().unwrap();

    assert_eq!(loaded, record);
  }

  #[test]
  fn plugin_scope_filtering() {
    let (_dir, domain) = test_domain();
    let shared = plugin_record("shared-p", ResourceScope::ClusterShared);
    let local = plugin_record("local-p", ResourceScope::NodeLocal);
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
        matches!(error, PluginStorageError::InvalidResourceId(_)),
        "write id: {id:?}"
      );
      let error = domain.blobs().read(id).unwrap_err();
      assert!(
        matches!(error, PluginStorageError::InvalidResourceId(_)),
        "read id: {id:?}"
      );
    }
    // No file may have been created anywhere for the rejected ids.
    assert!(!dir.path().join("plugins").join("escape").exists());
  }

  #[test]
  fn apply_remote_plugin_stores_new_plugin_and_artifact() {
    let (_dir, domain) = test_domain();
    let artifact = b"return {}";
    let record = shared_plugin("remote-p", artifact, 1);

    let applied = domain.apply_remote_plugin(record, artifact).unwrap();
    assert!(applied);

    let loaded = domain.get("remote-p").unwrap().unwrap();
    assert_eq!(loaded.version, 1);
    assert_eq!(loaded.engine, "lua");
    assert_eq!(domain.blobs().read("remote-p").unwrap().unwrap(), artifact);
  }

  #[test]
  fn apply_remote_plugin_rejects_hash_mismatch_without_persisting() {
    let (_dir, domain) = test_domain();
    let artifact = b"real artifact";
    let mut record = shared_plugin("p-hash", artifact, 1);
    record.content_hash = "wrong-hash".to_string();

    let error = domain.apply_remote_plugin(record, artifact).unwrap_err();
    assert!(matches!(error, PluginStorageError::HashMismatch(_)));
    assert!(domain.get("p-hash").unwrap().is_none());
    assert_eq!(domain.blobs().read("p-hash").unwrap(), None);
  }

  #[test]
  fn apply_remote_plugin_rejects_empty_artifact() {
    let (_dir, domain) = test_domain();
    let record = shared_plugin("p-empty", b"", 1);

    let applied = domain.apply_remote_plugin(record, b"").unwrap();
    assert!(!applied);
    assert!(domain.get("p-empty").unwrap().is_none());
  }

  #[test]
  fn apply_remote_plugin_skips_older_version() {
    let (_dir, domain) = test_domain();
    let local_artifact = b"local v2";
    let local = shared_plugin("p-conflict", local_artifact, 2);
    domain.apply_remote_plugin(local, local_artifact).unwrap();

    let remote_artifact = b"remote v1";
    let remote = shared_plugin("p-conflict", remote_artifact, 1);
    let applied = domain.apply_remote_plugin(remote, remote_artifact).unwrap();
    assert!(!applied);

    let loaded = domain.get("p-conflict").unwrap().unwrap();
    assert_eq!(loaded.version, 2);
    assert_eq!(
      domain.blobs().read("p-conflict").unwrap().unwrap(),
      local_artifact
    );
  }

  #[test]
  fn apply_remote_plugin_skips_node_local_scope() {
    let (_dir, domain) = test_domain();
    let artifact = b"local artifact";
    let mut record = plugin_record("p-local", ResourceScope::NodeLocal);
    record.content_hash = crate::hash_content(artifact);

    let applied = domain.apply_remote_plugin(record, artifact).unwrap();
    assert!(!applied);
    assert!(domain.get("p-local").unwrap().is_none());
  }

  #[test]
  fn apply_remote_plugin_does_not_rewrite_unchanged_artifact() {
    let (_dir, domain) = test_domain();
    let artifact = b"stable artifact";
    let local = shared_plugin("p-stable", artifact, 1);
    domain.apply_remote_plugin(local.clone(), artifact).unwrap();

    let mut remote = shared_plugin("p-stable", artifact, 2);
    remote.updated_at_ms = local.updated_at_ms + 1;

    let applied = domain.apply_remote_plugin(remote, artifact).unwrap();
    assert!(applied);
    let loaded = domain.get("p-stable").unwrap().unwrap();
    assert_eq!(loaded.version, 2);
    assert_eq!(domain.blobs().read("p-stable").unwrap().unwrap(), artifact);
  }

  #[test]
  fn apply_remote_plugin_persists_no_metadata_when_blob_write_fails() {
    let (dir, domain) = test_domain();
    let artifact = b"fragile artifact";
    let record = shared_plugin("p-fragile", artifact, 1);
    // A directory where the blob file should be makes the write fail.
    std::fs::create_dir_all(dir.path().join("plugins").join("blobs").join("p-fragile")).unwrap();

    let error = domain.apply_remote_plugin(record, artifact).unwrap_err();
    assert!(matches!(error, PluginStorageError::Storage(_)));

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
        let record = shared_plugin("p-race", &artifact, version);
        domain.apply_remote_plugin(record, &artifact).unwrap();
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
