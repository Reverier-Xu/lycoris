#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod agent;
mod bytes;
mod error;
mod extension;
mod node;
mod resource_id;
mod table;
mod versioned;
mod workspace;

use std::{path::Path, sync::Arc};

pub use agent::{
  AgentDomain, AgentStorageError, DEFAULT_EMBEDDING_DIM, MemoryEntry, MemoryStorage, Session,
  SessionStorage,
};
pub use error::StorageError;
pub use extension::{ExtensionBlobStore, ExtensionDomain, ExtensionRecord, ExtensionStorageError};
pub use lycoris_core::ResourceScope;
pub use node::{LocalStorage, MetaStorage, NodeDomain, PeerRecord, PeerStorage};
use redb::Database;
pub use resource_id::{InvalidResourceId, validate as validate_resource_id};
pub use table::RedbTableStorage;
pub use versioned::{ContentHashMismatch, VersionedRecord, should_apply_versioned};
pub use workspace::{
  RuleRecord, RuleStorage, SkillRecord, SkillStorage, VersionedContentStore, VersionedResource,
  VersionedStorage, WorkspaceDomain, WorkspaceMetadataStorage, WorkspaceRecord,
  WorkspaceStorageError,
};

/// `Storage` is the top-level entry point for all persistent state. The
/// underlying `redb::Database` is shared (via `Arc`) by lightweight, cloneable
/// domain handles for node-local state, agent orchestration state, workspace
/// state, and extension packages.
#[derive(Debug, Clone)]
pub struct Storage {
  node: NodeDomain,
  agent: AgentDomain,
  workspace: WorkspaceDomain,
  extensions: ExtensionDomain,
}

impl Storage {
  /// Open or create the storage at `db_path`.
  ///
  /// The workspace domain stores skill/rule content in a subdirectory of the
  /// database's parent directory.
  pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self, StorageError> {
    let db_path = db_path.as_ref().to_path_buf();
    let data_dir = db_path
      .parent()
      .map(std::path::Path::to_path_buf)
      .unwrap_or_else(|| std::path::PathBuf::from("."));
    Self::open_with_data_dir(&db_path, data_dir)
  }

  /// Open or create the storage at `db_path`, storing domain files in
  /// `data_dir`.
  fn open_with_data_dir<P: AsRef<Path>, Q: AsRef<Path>>(
    db_path: P, data_dir: Q,
  ) -> Result<Self, StorageError> {
    let db_path = db_path.as_ref().to_path_buf();
    let data_dir = data_dir.as_ref().to_path_buf();

    if let Some(parent) = db_path.parent() {
      std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir_all(&data_dir)?;

    let db = Database::create(&db_path).map_err(crate::error::redb_err)?;
    let db = Arc::new(db);

    Ok(Self {
      node: NodeDomain::new(db.clone()),
      agent: AgentDomain::new(db.clone(), data_dir.clone()),
      workspace: WorkspaceDomain::new(db.clone(), data_dir.clone()),
      extensions: ExtensionDomain::new(db, data_dir),
    })
  }

  /// Access the node-local storage domain.
  pub fn node(&self) -> &NodeDomain {
    &self.node
  }

  /// Access the agent orchestration storage domain.
  pub fn agent(&self) -> &AgentDomain {
    &self.agent
  }

  /// Access the workspace storage domain.
  pub fn workspace(&self) -> &WorkspaceDomain {
    &self.workspace
  }

  /// Access the extension storage domain.
  pub fn extensions(&self) -> &ExtensionDomain {
    &self.extensions
  }
}

/// Compute the canonical blake3 content hash used across all storage domains.
pub fn hash_content(content: &[u8]) -> String {
  blake3::hash(content).to_hex().to_string()
}
