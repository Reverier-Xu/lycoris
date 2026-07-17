use std::{collections::HashMap, sync::Arc};

use redb::{Database, TableDefinition};

use crate::{StorageError, bytes::Bytes, table::RedbTableStorage};

const LOCAL_LABELS: TableDefinition<&str, Bytes> = TableDefinition::new("local_labels");
const LOCAL_ANNOTATIONS: TableDefinition<&str, Bytes> = TableDefinition::new("local_annotations");

/// Local node attributes stored in the node-local database.
///
/// Labels are exposed to the cluster for scheduling/selectors; annotations are
/// opaque local metadata.
#[derive(Debug, Clone)]
pub struct LocalStorage {
  labels: RedbTableStorage<String>,
  annotations: RedbTableStorage<String>,
}

impl LocalStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self {
      labels: RedbTableStorage::new(db.clone(), LOCAL_LABELS),
      annotations: RedbTableStorage::new(db, LOCAL_ANNOTATIONS),
    }
  }

  /// Set a local label, overwriting any existing value.
  pub fn set_label(&self, key: &str, value: &str) -> Result<(), StorageError> {
    self.labels.upsert(key, &value.to_string())
  }

  /// Set a local annotation, overwriting any existing value.
  pub fn set_annotation(&self, key: &str, value: &str) -> Result<(), StorageError> {
    self.annotations.upsert(key, &value.to_string())
  }

  /// Return all local labels.
  pub fn labels(&self) -> Result<HashMap<String, String>, StorageError> {
    Ok(self.labels.entries()?.into_iter().collect())
  }

  /// Return all local annotations.
  pub fn annotations(&self) -> Result<HashMap<String, String>, StorageError> {
    Ok(self.annotations.entries()?.into_iter().collect())
  }
}
