use std::sync::Arc;

use redb::{Database, TableDefinition};

use crate::{StorageError, bytes::Bytes, table::RedbTableStorage};

const META: TableDefinition<&str, Bytes> = TableDefinition::new("node_meta");

/// Node-local key/value metadata.
///
/// Stores the small monotonic counters that must survive process restarts —
/// the gossip sequence and the local register's incarnation (P5b). Values are
/// plain strings (counters are decimal-encoded); interpretation lives with
/// the callers.
#[derive(Debug, Clone)]
pub struct MetaStorage {
  table: RedbTableStorage<String>,
}

impl MetaStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self {
      table: RedbTableStorage::new(db, META),
    }
  }

  /// Return the value stored under `key`, if any.
  pub fn get(&self, key: &str) -> Result<Option<String>, StorageError> {
    self.table.get(key)
  }

  /// Store `value` under `key`, overwriting any existing value.
  pub fn set(&self, key: &str, value: &str) -> Result<(), StorageError> {
    self.table.upsert(key, &value.to_string())
  }
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  fn test_meta() -> (TempDir, MetaStorage) {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("test.redb")).unwrap());
    (dir, MetaStorage::new(db))
  }

  #[test]
  fn set_get_round_trip() {
    let (_dir, meta) = test_meta();

    assert_eq!(meta.get("counter").unwrap(), None);
    meta.set("counter", "1").unwrap();
    meta.set("counter", "2").unwrap();

    assert_eq!(meta.get("counter").unwrap().as_deref(), Some("2"));
  }

  #[test]
  fn values_survive_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.redb");
    {
      let db = Arc::new(Database::create(&path).unwrap());
      MetaStorage::new(db).set("counter", "41").unwrap();
    }

    // Reopening the same database file simulates a process restart.
    let db = Arc::new(Database::create(&path).unwrap());
    assert_eq!(
      MetaStorage::new(db).get("counter").unwrap().as_deref(),
      Some("41")
    );
  }
}
