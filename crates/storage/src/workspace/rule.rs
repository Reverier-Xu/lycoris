use std::{collections::HashMap, path::Path, sync::Arc};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::{ResourceScope, WorkspaceStorageError, vcs::VersionedContentStore};
use crate::{
  bytes::{Bytes, decode, encode},
  error::{is_missing_table, redb_err},
};

const RULES: TableDefinition<&str, Bytes> = TableDefinition::new("rules");

/// Persistent metadata for a reusable rule.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuleRecord {
  pub id: String,
  pub name: String,
  pub version: u64,
  pub content_hash: String,
  pub scope: ResourceScope,
  /// `None` means this rule originated on the local node.
  pub source_node_id: Option<String>,
  pub updated_at_ms: i64,
  pub metadata: HashMap<String, String>,
}

/// Storage for rule metadata.
///
/// Rule *content* is versioned as immutable snapshots; this store only
/// persists the metadata and content hash needed for synchronization and
/// lookup.
pub trait RuleStorage: std::fmt::Debug + Send + Sync {
  /// Insert or update a rule record.
  fn upsert(&self, rule: &RuleRecord) -> Result<(), WorkspaceStorageError>;

  /// Return the rule record with the given id, if any.
  fn get(&self, id: &str) -> Result<Option<RuleRecord>, WorkspaceStorageError>;

  /// Return all rule records.
  fn list(&self) -> Result<Vec<RuleRecord>, WorkspaceStorageError>;

  /// Return rules whose scope is `ClusterShared`.
  fn list_shared(&self) -> Result<Vec<RuleRecord>, WorkspaceStorageError>;

  /// Return rules whose scope is `NodeLocal`.
  fn list_local(&self) -> Result<Vec<RuleRecord>, WorkspaceStorageError>;

  /// Delete a rule record.
  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError>;
}

/// Git-backed content store for rule bodies.
#[derive(Debug, Clone)]
pub struct RuleContentStore {
  inner: super::vcs::ContentStore,
}

impl RuleContentStore {
  pub fn new(repo_path: std::path::PathBuf) -> Self {
    Self {
      inner: super::vcs::ContentStore::new(repo_path),
    }
  }

  /// Return the directory of the underlying git repository.
  pub fn repo_path(&self) -> &Path {
    &self.inner.repo_path
  }

  /// Read the latest content of a rule.
  pub fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    self.inner.read(id)
  }

  /// Write a new version of a rule, recording it in git.
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

/// redb-backed implementation of `RuleStorage`.
#[derive(Debug, Clone)]
pub struct RedbRuleStorage {
  db: Arc<Database>,
}

impl RedbRuleStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self { db }
  }
}

impl RuleStorage for RedbRuleStorage {
  fn upsert(&self, rule: &RuleRecord) -> Result<(), WorkspaceStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(RULES).map_err(redb_err)?;
      table
        .insert(rule.id.as_str(), Bytes(encode(rule)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  fn get(&self, id: &str) -> Result<Option<RuleRecord>, WorkspaceStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(RULES).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if is_missing_table(&e) => return Ok(None),
      Err(e) => return Err(e.into()),
    };

    table
      .get(id)
      .map_err(redb_err)?
      .map(|guard| decode::<RuleRecord>(&guard.value().0))
      .transpose()
      .map_err(Into::into)
  }

  fn list(&self) -> Result<Vec<RuleRecord>, WorkspaceStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(RULES).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if is_missing_table(&e) => return Ok(Vec::new()),
      Err(e) => return Err(e.into()),
    };

    table
      .iter()
      .map_err(redb_err)?
      .map(|row| {
        let (_, value) = row.map_err(redb_err)?;
        decode::<RuleRecord>(&value.value().0)
      })
      .collect::<Result<Vec<_>, _>>()
      .map_err(Into::into)
  }

  fn list_shared(&self) -> Result<Vec<RuleRecord>, WorkspaceStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|rule| rule.scope == ResourceScope::ClusterShared)
        .collect(),
    )
  }

  fn list_local(&self) -> Result<Vec<RuleRecord>, WorkspaceStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|rule| rule.scope == ResourceScope::NodeLocal)
        .collect(),
    )
  }

  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(RULES).map_err(redb_err)?;
      table.remove(id).map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }
}
