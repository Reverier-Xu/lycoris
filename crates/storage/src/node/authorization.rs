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
}

#[cfg(test)]
mod tests {
  use lycoris_overlay::{AuthorizationRecord, NodeIdentity};
  use tempfile::TempDir;

  use crate::Storage;

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
