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

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  fn test_local() -> (TempDir, LocalStorage) {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("test.redb")).unwrap());
    (dir, LocalStorage::new(db))
  }

  #[test]
  fn labels_and_annotations_start_empty() {
    let (_dir, local) = test_local();

    assert!(local.labels().unwrap().is_empty());
    assert!(local.annotations().unwrap().is_empty());
  }

  #[test]
  fn labels_round_trip_and_overwrite() {
    let (_dir, local) = test_local();

    local.set_label("role", "worker").unwrap();
    local.set_label("zone", "a").unwrap();
    // Setting an existing key overwrites instead of duplicating.
    local.set_label("role", "scheduler").unwrap();

    let labels = local.labels().unwrap();
    assert_eq!(labels.len(), 2);
    assert_eq!(labels.get("role").map(String::as_str), Some("scheduler"));
    assert_eq!(labels.get("zone").map(String::as_str), Some("a"));
  }

  #[test]
  fn annotations_round_trip_independently_of_labels() {
    let (_dir, local) = test_local();

    local.set_annotation("note", "hello").unwrap();
    // The same key in the label table must not leak into annotations.
    local.set_label("note", "label-value").unwrap();

    let annotations = local.annotations().unwrap();
    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations.get("note").map(String::as_str), Some("hello"));
    assert_eq!(
      local.labels().unwrap().get("note").map(String::as_str),
      Some("label-value")
    );
  }
}
