#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod agent;
mod bytes;
mod error;
mod node;
mod versioned;
mod workspace;

use std::{path::Path, sync::Arc};

pub use agent::{
  AgentDomain, AgentStorageError, MemoryEntry, MemoryStorage, Session, SessionStorage,
};
pub use error::StorageError;
pub use lycoris_core::ResourceScope;
pub use node::{LocalStorage, NodeDomain, PeerRecord, PeerStorage};
use redb::Database;
pub use versioned::{VersionedRecord, should_apply_versioned};
pub use workspace::{
  RedbRuleStorage, RedbSkillStorage, RedbWorkspaceStorage, RuleRecord, RuleStorage, SkillRecord,
  SkillStorage, VersionedContentStore, VersionedResource, WorkspaceDomain,
  WorkspaceMetadataStorage, WorkspaceRecord, WorkspaceStorageError,
};

/// `Storage` is the top-level entry point for all persistent state. It owns a
/// single `redb::Database` and hands out lightweight, cloneable domain handles
/// for node-local state, agent orchestration state, and workspace state.
#[derive(Debug, Clone)]
pub struct Storage {
  #[allow(dead_code)]
  db: Arc<Database>,
  node: NodeDomain,
  agent: AgentDomain,
  workspace: WorkspaceDomain,
}

impl Storage {
  /// Open or create the storage at `db_path`.
  ///
  /// The workspace domain stores skill/rule content in a subdirectory of the
  /// database's parent directory. To place content elsewhere, use
  /// [`Storage::open_with_data_dir`].
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
  pub fn open_with_data_dir<P: AsRef<Path>, Q: AsRef<Path>>(
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
      db: db.clone(),
      node: NodeDomain::new(db.clone()),
      agent: AgentDomain::new(db.clone(), data_dir.clone()),
      workspace: WorkspaceDomain::new(db, data_dir)?,
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
}
