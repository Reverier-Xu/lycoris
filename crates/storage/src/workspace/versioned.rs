//! Generic versioned resource storage.
//!
//! Skills and rules are both versioned, scoped resources that differ only in
//! user-facing name. This module implements the shared metadata record and
//! storage trait once; `skill.rs` and `rule.rs` re-export aliases so callers
//! keep their domain vocabulary.

use std::collections::BTreeMap;

use lycoris_core::ResourceScope;
use serde::{Deserialize, Serialize};

use super::WorkspaceStorageError;
use crate::table::RedbTableStorage;

/// Persistent metadata for a versioned, scopeable resource.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionedResource {
  pub id: String,
  pub name: String,
  pub version: u64,
  pub content_hash: String,
  pub scope: ResourceScope,
  /// `None` means this resource originated on the local node.
  pub source_node_id: Option<String>,
  /// Creation time of the first version: set by the origin node when the
  /// resource is first written and preserved across updates. Anti-entropy
  /// applies take the wire value as authoritative.
  pub created_at_ms: i64,
  pub updated_at_ms: i64,
  /// `BTreeMap` keeps the postcard encoding deterministic across processes.
  pub metadata: BTreeMap<String, String>,
}

impl crate::versioned::VersionedRecord for VersionedResource {
  fn version(&self) -> u64 {
    self.version
  }

  fn updated_at_ms(&self) -> i64 {
    self.updated_at_ms
  }

  fn content_hash(&self) -> &str {
    &self.content_hash
  }

  fn scope(&self) -> ResourceScope {
    self.scope
  }
}

/// Storage for versioned resource metadata.
///
/// Content is versioned as immutable snapshots in a [`GitContentStore`]; this
/// trait only persists the metadata and content hash needed for
/// synchronization and lookup.
pub trait VersionedStorage: std::fmt::Debug + Send + Sync {
  /// Insert or update a resource record.
  fn upsert(&self, resource: &VersionedResource) -> Result<(), WorkspaceStorageError>;

  /// Return the resource record with the given id, if any.
  fn get(&self, id: &str) -> Result<Option<VersionedResource>, WorkspaceStorageError>;

  /// Return all resource records.
  fn list(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError>;

  /// Return resources whose scope is `ClusterShared`.
  fn list_shared(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError>;

  /// Return resources whose scope is `NodeLocal`.
  fn list_local(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError>;

  /// Delete a resource record.
  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError>;
}

/// redb-backed implementation of `VersionedStorage`.
pub type RedbVersionedStorage = RedbTableStorage<VersionedResource>;

impl VersionedStorage for RedbTableStorage<VersionedResource> {
  fn upsert(&self, resource: &VersionedResource) -> Result<(), WorkspaceStorageError> {
    RedbTableStorage::upsert(self, &resource.id, resource).map_err(Into::into)
  }

  fn get(&self, id: &str) -> Result<Option<VersionedResource>, WorkspaceStorageError> {
    RedbTableStorage::get(self, id).map_err(Into::into)
  }

  fn list(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError> {
    RedbTableStorage::list(self).map_err(Into::into)
  }

  fn list_shared(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError> {
    Ok(
      RedbTableStorage::list(self)?
        .into_iter()
        .filter(|resource| resource.scope == ResourceScope::ClusterShared)
        .collect(),
    )
  }

  fn list_local(&self) -> Result<Vec<VersionedResource>, WorkspaceStorageError> {
    Ok(
      RedbTableStorage::list(self)?
        .into_iter()
        .filter(|resource| resource.scope == ResourceScope::NodeLocal)
        .collect(),
    )
  }

  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError> {
    RedbTableStorage::delete(self, id).map_err(Into::into)
  }
}
