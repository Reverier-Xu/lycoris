//! Generic versioned resource storage.
//!
//! Skills and rules are both versioned, scoped resources that differ only in
//! user-facing name. This module implements the shared metadata record and
//! storage trait once; `skill.rs` and `rule.rs` re-export aliases so callers
//! keep their domain vocabulary.

use std::{collections::HashMap, sync::Arc};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use serde::{Deserialize, Serialize};

use super::WorkspaceStorageError;
use crate::{
  bytes::{Bytes, decode, encode},
  error::{is_missing_table, redb_err},
};

/// Persistent metadata for a versioned, scopeable resource.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionedResource {
  pub id: String,
  pub name: String,
  pub version: u64,
  pub content_hash: String,
  pub scope: super::ResourceScope,
  /// `None` means this resource originated on the local node.
  pub source_node_id: Option<String>,
  pub updated_at_ms: i64,
  pub metadata: HashMap<String, String>,
}

/// Storage for versioned resource metadata.
///
/// Content is versioned as immutable snapshots in a [`ContentStore`]; this
/// trait only persists the metadata and content hash needed for
/// synchronization and lookup.
pub trait VersionedStorage: std::fmt::Debug + Send + Sync {
  /// Insert or update a resource record.
  fn upsert(&self, resource: &VersionedResource) -> Result<(), WorkspaceStorageError>;

  /// Return the resource record with the given id, if any.
  fn get(&self, id: &str) -> Result<Option<VersionedResource>, WorkspaceStorageError>;

  /// Return all resource records.
  fn list(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError>;

  /// Return resources whose scope is `ClusterShared`.
  fn list_shared(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError>;

  /// Return resources whose scope is `NodeLocal`.
  fn list_local(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError>;

  /// Delete a resource record.
  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError>;
}

/// redb-backed implementation of `VersionedStorage`.
pub struct RedbVersionedStorage {
  db: Arc<Database>,
  table: TableDefinition<'static, &'static str, Bytes>,
}

impl std::fmt::Debug for RedbVersionedStorage {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("RedbVersionedStorage")
      .field("table", &self.table.name())
      .finish_non_exhaustive()
  }
}

impl Clone for RedbVersionedStorage {
  fn clone(&self) -> Self {
    Self {
      db: self.db.clone(),
      table: self.table,
    }
  }
}

impl RedbVersionedStorage {
  /// Create a storage backend backed by the given table.
  pub(crate) fn new(
    db: Arc<Database>, table: TableDefinition<'static, &'static str, Bytes>,
  ) -> Self {
    Self { db, table }
  }
}

impl VersionedStorage for RedbVersionedStorage {
  fn upsert(&self, resource: &VersionedResource) -> Result<(), WorkspaceStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(self.table).map_err(redb_err)?;
      table
        .insert(resource.id.as_str(), Bytes(encode(resource)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  fn get(&self, id: &str) -> Result<Option<VersionedResource>, WorkspaceStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(self.table).map_err(redb_err) {
      Ok(table) => table,
      Err(error) if is_missing_table(&error) => return Ok(None),
      Err(error) => return Err(error.into()),
    };

    table
      .get(id)
      .map_err(redb_err)?
      .map(|guard| decode::<VersionedResource>(&guard.value().0))
      .transpose()
      .map_err(Into::into)
  }

  fn list(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(self.table).map_err(redb_err) {
      Ok(table) => table,
      Err(error) if is_missing_table(&error) => return Ok(Vec::new()),
      Err(error) => return Err(error.into()),
    };

    table
      .iter()
      .map_err(redb_err)?
      .map(|row| {
        let (_, value) = row.map_err(redb_err)?;
        decode::<VersionedResource>(&value.value().0)
      })
      .collect::<Result<Vec<_>, _>>()
      .map_err(Into::into)
  }

  fn list_shared(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|resource| resource.scope == super::ResourceScope::ClusterShared)
        .collect(),
    )
  }

  fn list_local(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError> {
    Ok(
      self
        .list()?
        .into_iter()
        .filter(|resource| resource.scope == super::ResourceScope::NodeLocal)
        .collect(),
    )
  }

  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(self.table).map_err(redb_err)?;
      table.remove(id).map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }
}
