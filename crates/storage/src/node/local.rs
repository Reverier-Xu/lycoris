use std::{collections::HashMap, sync::Arc};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::{
  StorageError,
  bytes::{Bytes, decode, encode},
  error::redb_err,
};

const LOCAL_LABELS: TableDefinition<&str, Bytes> = TableDefinition::new("local_labels");
const LOCAL_ANNOTATIONS: TableDefinition<&str, Bytes> = TableDefinition::new("local_annotations");

/// Local node attributes stored in the node-local database.
///
/// Labels are exposed to the cluster for scheduling/selectors; annotations are
/// opaque local metadata.
#[derive(Debug, Clone)]
pub struct LocalStorage {
  db: Arc<Database>,
}

impl LocalStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self { db }
  }

  /// Set a local label, overwriting any existing value.
  pub fn set_label(&self, key: &str, value: &str) -> Result<(), StorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(LOCAL_LABELS).map_err(redb_err)?;
      table.insert(key, Bytes(encode(value)?)).map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  /// Set a local annotation, overwriting any existing value.
  pub fn set_annotation(&self, key: &str, value: &str) -> Result<(), StorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(LOCAL_ANNOTATIONS).map_err(redb_err)?;
      table.insert(key, Bytes(encode(value)?)).map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  /// Return all local labels.
  pub fn labels(&self) -> Result<HashMap<String, String>, StorageError> {
    self.read_map(LOCAL_LABELS)
  }

  /// Return all local annotations.
  pub fn annotations(&self) -> Result<HashMap<String, String>, StorageError> {
    self.read_map(LOCAL_ANNOTATIONS)
  }

  fn read_map(
    &self, table_def: TableDefinition<&str, Bytes>,
  ) -> Result<HashMap<String, String>, StorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(table_def).map_err(redb_err) {
      Ok(table) => table,
      Err(e) if crate::error::is_missing_table(&e) => return Ok(HashMap::new()),
      Err(e) => return Err(e),
    };

    let mut map = HashMap::new();
    for row in table.iter().map_err(redb_err)? {
      let (key, value) = row.map_err(redb_err)?;
      let value: String = decode(&value.value().0)?;
      map.insert(key.value().to_string(), value);
    }
    Ok(map)
  }
}
