use std::{collections::HashMap, path::PathBuf, sync::Arc};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::WorkspaceStorageError;
use crate::{
  bytes::{Bytes, decode, encode},
  error::{is_missing_table, redb_err},
};

const WORKSPACES: TableDefinition<&str, Bytes> = TableDefinition::new("workspaces");

/// Persistent record for a workspace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceRecord {
  pub id: String,
  /// Root directory of the workspace on the local filesystem.
  pub root: PathBuf,
  /// Sessions currently associated with this workspace.
  pub session_ids: Vec<String>,
  pub metadata: HashMap<String, String>,
  pub created_at_ms: i64,
  pub updated_at_ms: i64,
}

/// Storage for workspace metadata.
///
/// Workspaces may reference large files on disk; this store only persists the
/// metadata needed for discovery and association with agent sessions.
pub trait WorkspaceMetadataStorage: std::fmt::Debug + Send + Sync {
  /// Create or update a workspace record.
  fn upsert(&self, workspace: &WorkspaceRecord) -> Result<(), WorkspaceStorageError>;

  /// Return the workspace record with the given id, if any.
  fn get(&self, id: &str) -> Result<Option<WorkspaceRecord>, WorkspaceStorageError>;

  /// Return all workspace records.
  fn list(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError>;

  /// Delete a workspace record.
  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError>;
}

/// redb-backed implementation of `WorkspaceMetadataStorage`.
#[derive(Debug, Clone)]
pub struct RedbWorkspaceStorage {
  db: Arc<Database>,
}

impl RedbWorkspaceStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self { db }
  }
}

impl WorkspaceMetadataStorage for RedbWorkspaceStorage {
  fn upsert(&self, workspace: &WorkspaceRecord) -> Result<(), WorkspaceStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(WORKSPACES).map_err(redb_err)?;
      table
        .insert(workspace.id.as_str(), Bytes(encode(workspace)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  fn get(&self, id: &str) -> Result<Option<WorkspaceRecord>, WorkspaceStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(WORKSPACES).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if is_missing_table(&e) => return Ok(None),
      Err(e) => return Err(e.into()),
    };

    table
      .get(id)
      .map_err(redb_err)?
      .map(|guard| decode::<WorkspaceRecord>(&guard.value().0))
      .transpose()
      .map_err(Into::into)
  }

  fn list(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(WORKSPACES).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if is_missing_table(&e) => return Ok(Vec::new()),
      Err(e) => return Err(e.into()),
    };

    table
      .iter()
      .map_err(redb_err)?
      .map(|row| {
        let (_, value) = row.map_err(redb_err)?;
        decode::<WorkspaceRecord>(&value.value().0)
      })
      .collect::<Result<Vec<_>, _>>()
      .map_err(Into::into)
  }

  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(WORKSPACES).map_err(redb_err)?;
      table.remove(id).map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }
}
