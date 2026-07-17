//! Generic redb table storage.
//!
//! [`RedbTableStorage`] carries the transaction boilerplate for the common
//! "string key → serialized record" access pattern shared by every redb-backed
//! domain storage. Domain traits (`SessionStorage`, `WorkspaceMetadataStorage`,
//! `VersionedStorage`, ...) are implemented as thin adapters over these
//! primitives.

use std::{marker::PhantomData, sync::Arc};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
  StorageError,
  bytes::{Bytes, decode, encode},
  error::{is_missing_table, redb_err},
};

/// Generic redb-backed storage for records keyed by string ids.
///
/// Records are serialized with `postcard` and wrapped in [`Bytes`]. Reads
/// treat a not-yet-created table as empty; writes create it on first use.
pub struct RedbTableStorage<T> {
  db: Arc<Database>,
  table: TableDefinition<'static, &'static str, Bytes>,
  _marker: PhantomData<T>,
}

impl<T> Clone for RedbTableStorage<T> {
  fn clone(&self) -> Self {
    Self {
      db: self.db.clone(),
      table: self.table,
      _marker: PhantomData,
    }
  }
}

impl<T> std::fmt::Debug for RedbTableStorage<T> {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("RedbTableStorage")
      .field("table", &self.table.name())
      .finish_non_exhaustive()
  }
}

impl<T: Serialize + DeserializeOwned> RedbTableStorage<T> {
  pub(crate) fn new(
    db: Arc<Database>, table: TableDefinition<'static, &'static str, Bytes>,
  ) -> Self {
    Self {
      db,
      table,
      _marker: PhantomData,
    }
  }

  /// Insert or overwrite the record with the given id.
  pub fn upsert(&self, id: &str, record: &T) -> Result<(), StorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(self.table).map_err(redb_err)?;
      table.insert(id, Bytes(encode(record)?)).map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  /// Return the record with the given id, if any.
  pub fn get(&self, id: &str) -> Result<Option<T>, StorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(self.table).map_err(redb_err) {
      Ok(table) => table,
      Err(error) if is_missing_table(&error) => return Ok(None),
      Err(error) => return Err(error),
    };

    table
      .get(id)
      .map_err(redb_err)?
      .map(|guard| decode::<T>(&guard.value().0))
      .transpose()
  }

  /// Return all records.
  pub fn list(&self) -> Result<Vec<T>, StorageError> {
    Ok(
      self
        .entries()?
        .into_iter()
        .map(|(_, record)| record)
        .collect(),
    )
  }

  /// Return all id/record pairs.
  pub fn entries(&self) -> Result<Vec<(String, T)>, StorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(self.table).map_err(redb_err) {
      Ok(table) => table,
      Err(error) if is_missing_table(&error) => return Ok(Vec::new()),
      Err(error) => return Err(error),
    };

    table
      .iter()
      .map_err(redb_err)?
      .map(|row| {
        let (key, value) = row.map_err(redb_err)?;
        Ok((key.value().to_string(), decode::<T>(&value.value().0)?))
      })
      .collect()
  }

  /// Return all record ids.
  pub fn keys(&self) -> Result<Vec<String>, StorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(self.table).map_err(redb_err) {
      Ok(table) => table,
      Err(error) if is_missing_table(&error) => return Ok(Vec::new()),
      Err(error) => return Err(error),
    };

    table
      .iter()
      .map_err(redb_err)?
      .map(|row| {
        let (key, _) = row.map_err(redb_err)?;
        Ok(key.value().to_string())
      })
      .collect()
  }

  /// Read-modify-write the record with the given id in a single transaction.
  ///
  /// When no record exists yet, `default` provides the initial value. `mutate`
  /// is applied to the resulting record before it is written back.
  pub fn update(
    &self, id: &str, default: impl FnOnce() -> T, mutate: impl FnOnce(&mut T),
  ) -> Result<(), StorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(self.table).map_err(redb_err)?;
      let mut record = table
        .get(id)
        .map_err(redb_err)?
        .map(|guard| decode::<T>(&guard.value().0))
        .transpose()?
        .unwrap_or_else(default);
      mutate(&mut record);
      table
        .insert(id, Bytes(encode(&record)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  /// Delete the record with the given id.
  pub fn delete(&self, id: &str) -> Result<(), StorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(self.table).map_err(redb_err)?;
      table.remove(id).map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use serde::{Deserialize, Serialize};
  use tempfile::TempDir;

  use super::*;

  const RECORDS: TableDefinition<&str, Bytes> = TableDefinition::new("records");

  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  struct TestRecord {
    id: String,
    value: u32,
  }

  fn test_storage() -> (TempDir, RedbTableStorage<TestRecord>) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("test.redb")).unwrap();
    (dir, RedbTableStorage::new(Arc::new(db), RECORDS))
  }

  fn record(id: &str, value: u32) -> TestRecord {
    TestRecord {
      id: id.to_string(),
      value,
    }
  }

  #[test]
  fn upsert_get_list_delete_round_trip() {
    let (_dir, storage) = test_storage();

    storage.upsert("a", &record("a", 1)).unwrap();
    storage.upsert("b", &record("b", 2)).unwrap();
    storage.upsert("a", &record("a", 3)).unwrap();

    assert_eq!(storage.get("a").unwrap(), Some(record("a", 3)));
    assert_eq!(storage.get("missing").unwrap(), None);
    assert_eq!(storage.list().unwrap().len(), 2);
    assert_eq!(
      storage.keys().unwrap(),
      vec!["a".to_string(), "b".to_string()]
    );

    storage.delete("a").unwrap();
    assert_eq!(storage.get("a").unwrap(), None);
    assert_eq!(storage.list().unwrap().len(), 1);
  }

  #[test]
  fn missing_table_reads_as_empty() {
    let (_dir, storage) = test_storage();

    assert_eq!(storage.get("a").unwrap(), None);
    assert!(storage.list().unwrap().is_empty());
    assert!(storage.entries().unwrap().is_empty());
    assert!(storage.keys().unwrap().is_empty());
  }

  #[test]
  fn update_inserts_default_then_mutates() {
    let (_dir, storage) = test_storage();

    storage
      .update("a", || record("a", 0), |record| record.value += 1)
      .unwrap();
    storage
      .update("a", || record("a", 0), |record| record.value += 1)
      .unwrap();

    assert_eq!(storage.get("a").unwrap(), Some(record("a", 2)));
  }

  #[test]
  fn entries_returns_id_record_pairs() {
    let (_dir, storage) = test_storage();

    storage.upsert("a", &record("a", 1)).unwrap();
    storage.upsert("b", &record("b", 2)).unwrap();

    let entries = storage.entries().unwrap();
    assert_eq!(
      entries,
      vec![
        ("a".to_string(), record("a", 1)),
        ("b".to_string(), record("b", 2)),
      ]
    );
  }
}
