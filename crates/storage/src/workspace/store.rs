use std::{collections::BTreeMap, path::PathBuf};

use lycoris_core::ResourceScope;
use redb::TableDefinition;
use serde::{Deserialize, Serialize};

use super::WorkspaceStorageError;
use crate::{bytes::Bytes, table::RedbTableStorage};

pub(crate) const WORKSPACES: TableDefinition<&str, Bytes> = TableDefinition::new("workspaces");

/// Persistent record for a workspace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceRecord {
  pub id: String,
  /// Root directory of the workspace on the local filesystem.
  pub root: PathBuf,
  /// Sessions currently associated with this workspace.
  pub session_ids: Vec<String>,
  /// `BTreeMap` keeps the postcard encoding deterministic so
  /// [`Self::compute_content_hash`] is stable across processes.
  pub metadata: BTreeMap<String, String>,
  pub scope: ResourceScope,
  /// `None` means this workspace originated on the local node.
  pub source_node_id: Option<String>,
  pub version: u64,
  pub content_hash: String,
  pub created_at_ms: i64,
  pub updated_at_ms: i64,
}

impl WorkspaceRecord {
  /// Compute a stable content hash from the meaningful fields of the record.
  ///
  /// `updated_at_ms` and `content_hash` are excluded so the hash does not
  /// change when only synchronization metadata is updated.
  pub fn compute_content_hash(&self) -> Result<String, WorkspaceStorageError> {
    let mut canonical = self.clone();
    canonical.updated_at_ms = 0;
    canonical.content_hash = String::new();
    let bytes = crate::bytes::encode(&canonical)?;
    Ok(crate::hash_content(&bytes))
  }
}

impl crate::versioned::VersionedRecord for WorkspaceRecord {
  fn version(&self) -> u64 {
    self.version
  }

  fn updated_at_ms(&self) -> i64 {
    self.updated_at_ms
  }

  fn scope(&self) -> ResourceScope {
    self.scope
  }
}

/// Storage for workspace metadata.
///
/// Workspaces may reference large files on disk; this store only persists the
/// metadata needed for discovery and association with agent sessions.
pub trait WorkspaceMetadataStorage: std::fmt::Debug + Send + Sync {
  /// Create or update a workspace record.
  fn upsert(&self, workspace: &WorkspaceRecord) -> Result<(), WorkspaceStorageError>;

  /// Return the workspace record with the given id, if any.
  fn get(&self, id: &str) -> Result<Option<WorkspaceRecord>, WorkspaceStorageError>;

  /// Return all workspace records.
  fn list(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError>;

  /// Return workspace records whose scope is `ClusterShared`.
  fn list_shared(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError>;

  /// Return workspace records whose scope is `NodeLocal`.
  fn list_local(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError>;

  /// Delete a workspace record.
  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError>;
}

/// redb-backed implementation of `WorkspaceMetadataStorage`.
pub type RedbWorkspaceStorage = RedbTableStorage<WorkspaceRecord>;

impl WorkspaceMetadataStorage for RedbTableStorage<WorkspaceRecord> {
  fn upsert(&self, workspace: &WorkspaceRecord) -> Result<(), WorkspaceStorageError> {
    let mut workspace = workspace.clone();
    workspace.content_hash = workspace.compute_content_hash()?;
    RedbTableStorage::upsert(self, &workspace.id, &workspace).map_err(Into::into)
  }

  fn get(&self, id: &str) -> Result<Option<WorkspaceRecord>, WorkspaceStorageError> {
    RedbTableStorage::get(self, id).map_err(Into::into)
  }

  fn list(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError> {
    RedbTableStorage::list(self).map_err(Into::into)
  }

  fn list_shared(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError> {
    Ok(
      RedbTableStorage::list(self)?
        .into_iter()
        .filter(|workspace| workspace.scope == ResourceScope::ClusterShared)
        .collect(),
    )
  }

  fn list_local(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError> {
    Ok(
      RedbTableStorage::list(self)?
        .into_iter()
        .filter(|workspace| workspace.scope == ResourceScope::NodeLocal)
        .collect(),
    )
  }

  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError> {
    RedbTableStorage::delete(self, id).map_err(Into::into)
  }
}

#[cfg(test)]
mod tests {
  use lycoris_core::ResourceScope;

  use super::*;

  fn workspace_with_metadata(entries: &[(&str, &str)]) -> WorkspaceRecord {
    WorkspaceRecord {
      id: "ws-1".to_string(),
      root: PathBuf::from("/tmp/ws-1"),
      session_ids: vec!["session-1".to_string()],
      metadata: entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect(),
      scope: ResourceScope::ClusterShared,
      source_node_id: Some("node-1".to_string()),
      version: 7,
      content_hash: String::new(),
      created_at_ms: 1000,
      updated_at_ms: 2000,
    }
  }

  #[test]
  fn content_hash_is_stable_across_metadata_insertion_orders() {
    let forward = workspace_with_metadata(&[("a", "1"), ("b", "2"), ("c", "3")]);
    let reverse = workspace_with_metadata(&[("c", "3"), ("b", "2"), ("a", "1")]);

    let forward_hash = forward.compute_content_hash().unwrap();
    let reverse_hash = reverse.compute_content_hash().unwrap();

    assert!(!forward_hash.is_empty());
    assert_eq!(forward_hash, reverse_hash);
  }
}
