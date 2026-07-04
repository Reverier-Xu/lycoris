use std::{collections::HashMap, sync::Arc};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::{
  StorageError,
  bytes::{Bytes, decode, encode},
  error::redb_err,
};

const CLUSTER_NODES: TableDefinition<&str, Bytes> = TableDefinition::new("cluster_nodes");

/// Persistent record for a cluster member as seen by this node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeRecord {
  pub id: String,
  pub address: String,
  pub last_heartbeat_ms: i64,
  pub state: NodeState,
  pub labels: HashMap<String, String>,
  pub annotations: HashMap<String, String>,
}

/// Coarse-grained state of a node from this node's point of view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
  Alive,
  Offline,
}

/// Storage for the synced cluster node registry.
#[derive(Debug, Clone)]
pub struct ClusterNodeStorage {
  db: Arc<Database>,
}

impl ClusterNodeStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self { db }
  }

  /// Insert or update a cluster node record.
  pub fn upsert(&self, node: &ClusterNodeRecord) -> Result<(), StorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(CLUSTER_NODES).map_err(redb_err)?;
      table
        .insert(node.id.as_str(), Bytes(encode(node)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  /// Return all cluster node records.
  pub fn list(&self) -> Result<Vec<ClusterNodeRecord>, StorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(CLUSTER_NODES).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if crate::error::is_missing_table(&e) => return Ok(Vec::new()),
      Err(e) => return Err(e),
    };

    table
      .iter()
      .map_err(redb_err)?
      .map(|row| {
        let (_, value) = row.map_err(redb_err)?;
        decode::<ClusterNodeRecord>(&value.value().0)
      })
      .collect()
  }

  /// Mark alive nodes with a heartbeat older than `cutoff_ms` as offline.
  pub fn cleanup_offline(&self, cutoff_ms: i64) -> Result<(), StorageError> {
    let to_update: Vec<ClusterNodeRecord> = {
      let read_txn = self.db.begin_read().map_err(redb_err)?;
      let table = match read_txn.open_table(CLUSTER_NODES).map_err(redb_err) {
        Ok(table) => table,
        Err(e) if crate::error::is_missing_table(&e) => return Ok(()),
        Err(e) => return Err(e),
      };
      table
        .iter()
        .map_err(redb_err)?
        .filter_map(|row| {
          let (_, value) = row.map_err(redb_err).ok()?;
          let mut record: ClusterNodeRecord = decode(&value.value().0).ok()?;
          if record.state == NodeState::Alive && record.last_heartbeat_ms < cutoff_ms {
            record.state = NodeState::Offline;
            Some(record)
          } else {
            None
          }
        })
        .collect()
    };

    if to_update.is_empty() {
      return Ok(());
    }

    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(CLUSTER_NODES).map_err(redb_err)?;
      for record in to_update {
        table
          .insert(record.id.as_str(), Bytes(encode(&record)?))
          .map_err(redb_err)?;
      }
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }
}
