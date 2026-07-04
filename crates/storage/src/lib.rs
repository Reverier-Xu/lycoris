#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod agent;
pub mod cluster;
pub mod error;
pub mod workspace;

use std::path::Path;

pub use cluster::{ClusterNodeRecord, ClusterStorage, NodeState, PeerRecord};
pub use error::StorageError;

/// Unified storage facade.
///
/// `Storage` is the top-level entry point for all persistent state. It
/// currently exposes the cluster node domain; agent and workspace domains are
/// reserved for future expansion.
#[derive(Debug, Clone)]
pub struct Storage {
  cluster: ClusterStorage,
}

impl Storage {
  /// Open or create the SQLite database at the given path.
  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
    Ok(Self {
      cluster: ClusterStorage::open(path)?,
    })
  }

  /// Access the cluster node storage domain.
  pub fn cluster(&self) -> &ClusterStorage {
    &self.cluster
  }
}
