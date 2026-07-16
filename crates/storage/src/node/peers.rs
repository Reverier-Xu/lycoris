use std::sync::Arc;

use lycoris_core::now_ms;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::{
  StorageError,
  bytes::{Bytes, decode, encode},
  error::redb_err,
};

const PEERS: TableDefinition<&str, Bytes> = TableDefinition::new("peers");
const PRIMARY: TableDefinition<&str, Bytes> = TableDefinition::new("primary");
const PRIMARY_KEY: &str = "primary";

/// Persistent record describing a known peer endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
  pub address: String,
  pub is_primary: bool,
  pub online: bool,
  pub last_seen_ms: Option<i64>,
  pub last_attempt_ms: Option<i64>,
}

/// Storage for known peer endpoints and the current primary.
#[derive(Debug, Clone)]
pub struct PeerStorage {
  db: Arc<Database>,
}

impl PeerStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self { db }
  }

  /// Insert a bootstrap peer if it is not already known.
  pub fn seed(&self, address: &str) -> Result<(), StorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(PEERS).map_err(redb_err)?;
      if table.get(address).map_err(redb_err)?.is_none() {
        let record = PeerRecord {
          address: address.to_string(),
          is_primary: false,
          online: false,
          last_seen_ms: None,
          last_attempt_ms: None,
        };
        table
          .insert(address, Bytes(encode(&record)?))
          .map_err(redb_err)?;
      }
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  /// Record that a peer was reachable at the given timestamp.
  pub fn mark_seen(&self, address: &str, seen_ms: i64) -> Result<(), StorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(PEERS).map_err(redb_err)?;
      let record = table
        .get(address)
        .map_err(redb_err)?
        .map(|guard| decode::<PeerRecord>(&guard.value().0))
        .transpose()?
        .unwrap_or_else(|| PeerRecord {
          address: address.to_string(),
          is_primary: false,
          online: false,
          last_seen_ms: None,
          last_attempt_ms: None,
        });
      let updated = PeerRecord {
        address: record.address,
        is_primary: record.is_primary,
        online: true,
        last_seen_ms: Some(seen_ms),
        last_attempt_ms: Some(seen_ms),
      };
      table
        .insert(address, Bytes(encode(&updated)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  /// Record a communication attempt now.
  pub fn mark_attempt(&self, address: &str, online: bool) -> Result<(), StorageError> {
    let now = now_ms();
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(PEERS).map_err(redb_err)?;
      let record = table
        .get(address)
        .map_err(redb_err)?
        .map(|guard| decode::<PeerRecord>(&guard.value().0))
        .transpose()?
        .unwrap_or_else(|| PeerRecord {
          address: address.to_string(),
          is_primary: false,
          online,
          last_seen_ms: None,
          last_attempt_ms: None,
        });
      let updated = PeerRecord {
        address: record.address,
        is_primary: record.is_primary,
        online,
        last_seen_ms: record.last_seen_ms,
        last_attempt_ms: Some(now),
      };
      table
        .insert(address, Bytes(encode(&updated)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  /// Promote a peer to the primary communication endpoint.
  pub fn set_primary(&self, address: &str) -> Result<(), StorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(PRIMARY).map_err(redb_err)?;
      table
        .insert(PRIMARY_KEY, Bytes(encode(address)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  /// Get the current primary endpoint, if any.
  pub fn get_primary(&self) -> Result<Option<String>, StorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(PRIMARY).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if crate::error::is_missing_table(&e) => return Ok(None),
      Err(e) => return Err(e),
    };
    table
      .get(PRIMARY_KEY)
      .map_err(redb_err)?
      .map(|guard| decode::<String>(&guard.value().0))
      .transpose()
  }

  /// Return candidate peer addresses excluding the current primary.
  pub fn fallback_addresses(&self) -> Result<Vec<String>, StorageError> {
    let primary = self.get_primary()?;
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(PEERS).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if crate::error::is_missing_table(&e) => return Ok(Vec::new()),
      Err(e) => return Err(e),
    };

    let mut addresses = Vec::new();
    for row in table.iter().map_err(redb_err)? {
      let (key, _) = row.map_err(redb_err)?;
      let address = key.value().to_string();
      if primary.as_ref() != Some(&address) {
        addresses.push(address);
      }
    }
    Ok(addresses)
  }
}
