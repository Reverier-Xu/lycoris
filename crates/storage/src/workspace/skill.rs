use std::{collections::HashMap, path::Path, sync::Arc};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::{ResourceScope, WorkspaceStorageError, vcs::VersionedContentStore};
use crate::{
  bytes::{Bytes, decode, encode},
  error::{is_missing_table, redb_err},
};

const SKILLS: TableDefinition<&str, Bytes> = TableDefinition::new("skills");

/// Persistent metadata for a reusable skill.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillRecord {
  pub id: String,
  pub name: String,
  pub version: u64,
  pub content_hash: String,
  pub scope: ResourceScope,
  /// `None` means this skill originated on the local node.
  pub source_node_id: Option<String>,
  pub updated_at_ms: i64,
  pub metadata: HashMap<String, String>,
}

/// Storage for skill metadata.
///
/// Skill *content* is versioned as immutable snapshots; this store only
/// persists the metadata and content hash needed for synchronization and
/// lookup.
pub trait SkillStorage: std::fmt::Debug + Send + Sync {
  /// Insert or update a skill record.
  fn upsert(&self, skill: &SkillRecord) -> Result<(), WorkspaceStorageError>;

  /// Return the skill record with the given id, if any.
  fn get(&self, id: &str) -> Result<Option<SkillRecord>, WorkspaceStorageError>;

  /// Return all skill records.
  fn list(&self) -> Result<Vec<SkillRecord>, WorkspaceStorageError>;

  /// Return skills whose scope is `ClusterShared`.
  fn list_shared(&self) -> Result<Vec<SkillRecord>, WorkspaceStorageError>;

  /// Return skills whose scope is `NodeLocal`.
  fn list_local(&self) -> Result<Vec<SkillRecord>, WorkspaceStorageError>;

  /// Delete a skill record.
  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError>;
}

/// Filesystem-backed content store for skill bodies.
///
/// Skill content is versioned as immutable snapshots. This store only exposes
/// the small surface needed by the rest of the storage layer.
#[derive(Debug, Clone)]
pub struct SkillContentStore {
  inner: super::vcs::SnapshotContentStore,
}

impl SkillContentStore {
  pub fn new(repo_path: std::path::PathBuf) -> Self {
    Self {
      inner: super::vcs::SnapshotContentStore::new(repo_path),
    }
  }

  /// Return the directory of the underlying snapshot repository.
  pub fn repo_path(&self) -> &Path {
    &self.inner.repo_path
  }

  /// Read the latest content of a skill.
  pub fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    self.inner.read(id)
  }

  /// Write a new version of a skill, recording it as a snapshot.
  ///
  /// Returns the content hash of the recorded snapshot.
  pub fn write(
    &self, id: &str, content: &str, message: &str,
  ) -> Result<String, WorkspaceStorageError> {
    self.inner.write(id, content, message)
  }

  /// Return the hash of the latest recorded version, if any.
  pub fn latest_hash(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    self.inner.latest_hash(id)
  }
}

/// redb-backed implementation of `SkillStorage`.
#[derive(Debug, Clone)]
pub struct RedbSkillStorage {
  db: Arc<Database>,
}

impl RedbSkillStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self { db }
  }
}

impl SkillStorage for RedbSkillStorage {
  fn upsert(&self, skill: &SkillRecord) -> Result<(), WorkspaceStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(SKILLS).map_err(redb_err)?;
      table
        .insert(skill.id.as_str(), Bytes(encode(skill)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  fn get(&self, id: &str) -> Result<Option<SkillRecord>, WorkspaceStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(SKILLS).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if is_missing_table(&e) => return Ok(None),
      Err(e) => return Err(e.into()),
    };

    table
      .get(id)
      .map_err(redb_err)?
      .map(|guard| decode::<SkillRecord>(&guard.value().0))
      .transpose()
      .map_err(Into::into)
  }

  fn list(&self) -> Result<Vec<SkillRecord>, WorkspaceStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(SKILLS).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if is_missing_table(&e) => return Ok(Vec::new()),
      Err(e) => return Err(e.into()),
    };

    table
      .iter()
      .map_err(redb_err)?
      .map(|row| {
        let (_, value) = row.map_err(redb_err)?;
        decode::<SkillRecord>(&value.value().0)
      })
      .collect::<Result<Vec<_>, _>>()
      .map_err(Into::into)
  }

  fn list_shared(&self) -> Result<Vec<SkillRecord>, WorkspaceStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|skill| skill.scope == ResourceScope::ClusterShared)
        .collect(),
    )
  }

  fn list_local(&self) -> Result<Vec<SkillRecord>, WorkspaceStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|skill| skill.scope == ResourceScope::NodeLocal)
        .collect(),
    )
  }

  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(SKILLS).map_err(redb_err)?;
      table.remove(id).map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }
}
