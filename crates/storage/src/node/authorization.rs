use std::sync::Arc;

use lycoris_overlay::AuthorizationRecord;
use redb::{Database, TableDefinition};

use crate::{StorageError, bytes::Bytes, table::RedbTableStorage};

const AUTHORIZATION_RECORDS: TableDefinition<&str, Bytes> =
  TableDefinition::new("authorization_records");

#[derive(Debug, Clone)]
pub struct AuthorizationStorage {
  records: RedbTableStorage<AuthorizationRecord>,
}

impl AuthorizationStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self {
      records: RedbTableStorage::new(db, AUTHORIZATION_RECORDS),
    }
  }

  pub fn put(&self, record: &AuthorizationRecord) -> Result<(), StorageError> {
    self.records.upsert(&record.id().to_string(), record)
  }

  pub fn records(&self) -> Result<Vec<AuthorizationRecord>, StorageError> {
    self.records.list()
  }

  /// Atomically replace the persisted authorization checkpoint.
  pub fn replace(&self, records: &[AuthorizationRecord]) -> Result<(), StorageError> {
    self.records.replace_all(
      records
        .iter()
        .cloned()
        .map(|record| (record.id().to_string(), record))
        .collect(),
    )
  }
}

#[cfg(test)]
mod tests {
  use lycoris_overlay::{AuthorizationRecord, NodeIdentity};
  use tempfile::TempDir;

  use crate::Storage;

  #[test]
  fn replacement_removes_the_standalone_genesis() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let storage = Storage::open(dir.path().join("lycoris.redb"))?;
    let local = NodeIdentity::generate();
    let (_, local_genesis) = AuthorizationRecord::genesis(&local)?;
    storage.node().authorization().put(&local_genesis)?;

    let sponsor = NodeIdentity::generate();
    let (_, sponsor_genesis) = AuthorizationRecord::genesis(&sponsor)?;
    storage
      .node()
      .authorization()
      .replace(std::slice::from_ref(&sponsor_genesis))?;

    let records = storage.node().authorization().records()?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id(), sponsor_genesis.id());
    Ok(())
  }

  #[test]
  fn complete_authorization_set_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("lycoris.redb");
    let genesis_identity = NodeIdentity::generate();
    let (cluster_id, genesis) = AuthorizationRecord::genesis(&genesis_identity).unwrap();
    let member_identity = NodeIdentity::generate();
    let admission = AuthorizationRecord::admit(
      cluster_id,
      &member_identity.public_identity(),
      &genesis,
      &genesis,
      &genesis_identity,
    )
    .unwrap();

    {
      let storage = Storage::open(&path).unwrap();
      storage.node().authorization().put(&genesis).unwrap();
      storage.node().authorization().put(&admission).unwrap();
    }

    let storage = Storage::open(path).unwrap();
    let mut ids: Vec<_> = storage
      .node()
      .authorization()
      .records()
      .unwrap()
      .into_iter()
      .map(|record| record.id())
      .collect();
    ids.sort();
    let mut expected = vec![genesis.id(), admission.id()];
    expected.sort();
    assert_eq!(ids, expected);
  }
}
