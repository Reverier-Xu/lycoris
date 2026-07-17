use std::sync::Arc;

use lycoris_core::now_ms;
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::{StorageError, bytes::Bytes, table::RedbTableStorage};

const PEERS: TableDefinition<&str, Bytes> = TableDefinition::new("peers");
const PRIMARY: TableDefinition<&str, Bytes> = TableDefinition::new("primary");
const PRIMARY_KEY: &str = "primary";

/// Persistent record describing a known peer endpoint.
///
/// The `PRIMARY` table is the single authority on which endpoint is primary;
/// the record deliberately carries no `is_primary` flag (I5). The health
/// fields (`online`, `last_seen_ms`, `last_attempt_ms`) feed the daemon's
/// peer-selection policy: recency of `last_seen_ms` ranks candidates, and a
/// recent failed `last_attempt_ms` triggers failure backoff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
  pub address: String,
  pub online: bool,
  pub last_seen_ms: Option<i64>,
  pub last_attempt_ms: Option<i64>,
}

impl PeerRecord {
  fn new(address: &str) -> Self {
    Self {
      address: address.to_string(),
      online: false,
      last_seen_ms: None,
      last_attempt_ms: None,
    }
  }
}

/// Storage for known peer endpoints and the current primary.
#[derive(Debug, Clone)]
pub struct PeerStorage {
  peers: RedbTableStorage<PeerRecord>,
  primary: RedbTableStorage<String>,
}

impl PeerStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self {
      peers: RedbTableStorage::new(db.clone(), PEERS),
      primary: RedbTableStorage::new(db, PRIMARY),
    }
  }

  /// Insert a bootstrap peer if it is not already known.
  pub fn seed(&self, address: &str) -> Result<(), StorageError> {
    self
      .peers
      .update(address, || PeerRecord::new(address), |_| {})
  }

  /// Record that a peer was reachable at the given timestamp.
  pub fn mark_seen(&self, address: &str, seen_ms: i64) -> Result<(), StorageError> {
    self.peers.update(
      address,
      || PeerRecord::new(address),
      |record| {
        record.online = true;
        record.last_seen_ms = Some(seen_ms);
        record.last_attempt_ms = Some(seen_ms);
      },
    )
  }

  /// Record a communication attempt now.
  pub fn mark_attempt(&self, address: &str, online: bool) -> Result<(), StorageError> {
    let now = now_ms();
    self.peers.update(
      address,
      || PeerRecord::new(address),
      |record| {
        record.online = online;
        record.last_attempt_ms = Some(now);
      },
    )
  }

  /// Promote a peer to the primary communication endpoint.
  ///
  /// The "never point the primary at the local node" rule lives here in the
  /// node domain (D8), not at the rpc layer. The node domain does not hold
  /// the local identity itself, so callers pass `local_address` in for the
  /// check.
  pub fn set_primary(&self, address: &str, local_address: &str) -> Result<(), StorageError> {
    if address == local_address {
      return Err(StorageError::SelfPrimary);
    }
    self.primary.upsert(PRIMARY_KEY, &address.to_string())
  }

  /// Get the current primary endpoint, if any.
  pub fn get_primary(&self) -> Result<Option<String>, StorageError> {
    self.primary.get(PRIMARY_KEY)
  }

  /// Return candidate peer addresses excluding the current primary.
  pub fn fallback_addresses(&self) -> Result<Vec<String>, StorageError> {
    let primary = self.get_primary()?;
    Ok(
      self
        .peers
        .keys()?
        .into_iter()
        .filter(|address| primary.as_ref() != Some(address))
        .collect(),
    )
  }

  /// Return all known peer records, including the health fields consumed by
  /// the peer-selection policy.
  pub fn records(&self) -> Result<Vec<PeerRecord>, StorageError> {
    self.peers.list()
  }

  /// Return every known peer address, including the current primary (which
  /// may have no peer record of its own).
  pub fn known_addresses(&self) -> Result<Vec<String>, StorageError> {
    let mut addresses = self.fallback_addresses()?;
    if let Some(primary) = self.get_primary()? {
      addresses.push(primary);
    }
    Ok(addresses)
  }
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  fn test_peers() -> (TempDir, PeerStorage) {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("test.redb")).unwrap());
    (dir, PeerStorage::new(db))
  }

  #[test]
  fn set_primary_rejects_local_address() {
    let (_dir, peers) = test_peers();

    let error = peers.set_primary("a:1", "a:1").unwrap_err();

    assert!(matches!(error, StorageError::SelfPrimary));
    assert_eq!(peers.get_primary().unwrap(), None);
  }

  #[test]
  fn set_primary_accepts_remote_address() {
    let (_dir, peers) = test_peers();

    peers.set_primary("b:1", "a:1").unwrap();

    assert_eq!(peers.get_primary().unwrap().as_deref(), Some("b:1"));
  }

  #[test]
  fn known_addresses_includes_primary_without_record() {
    let (_dir, peers) = test_peers();
    peers.seed("b:1").unwrap();
    peers.set_primary("c:1", "a:1").unwrap();

    let mut known = peers.known_addresses().unwrap();
    known.sort();

    assert_eq!(known, vec!["b:1".to_string(), "c:1".to_string()]);
  }

  #[test]
  fn health_marks_round_trip() {
    let (_dir, peers) = test_peers();

    peers.mark_seen("b:1", 1_000).unwrap();
    let record = &peers.records().unwrap()[0];
    assert!(record.online);
    assert_eq!(record.last_seen_ms, Some(1_000));
    assert_eq!(record.last_attempt_ms, Some(1_000));

    peers.mark_attempt("b:1", false).unwrap();
    let record = &peers.records().unwrap()[0];
    assert!(!record.online);
    assert_eq!(record.last_seen_ms, Some(1_000));
    assert!(record.last_attempt_ms.is_some());
  }
}
