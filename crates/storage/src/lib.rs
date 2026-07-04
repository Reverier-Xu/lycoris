#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod agent;
pub mod bytes;
pub mod error;
pub mod node;
pub mod workspace;

use std::{path::Path, sync::Arc};

pub use agent::{
  AgentDomain, AgentStorageError, MemoryEntry, MemoryStorage, Session, SessionStorage,
};
pub use error::StorageError;
pub use node::{
  ClusterNodeRecord, ClusterNodeStorage, LocalNode, LocalStorage, NodeDomain, NodeRegistry,
  NodeState, PeerRecord, PeerStorage,
};
use redb::Database;
use tokio::sync::Notify;
pub use workspace::{Workspace, WorkspaceDomain, WorkspaceStorage, WorkspaceStorageError};

/// Unified storage facade.
///
/// `Storage` is the top-level entry point for all persistent state. It owns a
/// single `redb::Database` and hands out lightweight, cloneable domain handles
/// for node-local state, agent orchestration state, and workspace state.
#[derive(Debug, Clone)]
pub struct Storage {
  db: Arc<Database>,
  notify: Arc<Notify>,
  agent: AgentDomain,
  workspace: WorkspaceDomain,
}

impl Storage {
  /// Open or create the redb database at the given path.
  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
    if let Some(parent) = path.as_ref().parent() {
      std::fs::create_dir_all(parent)?;
    }
    let db = Database::create(path).map_err(crate::error::redb_err)?;
    Ok(Self {
      db: Arc::new(db),
      notify: Arc::new(Notify::new()),
      agent: AgentDomain::new(),
      workspace: WorkspaceDomain::new(),
    })
  }

  /// Access the node-local storage domain.
  pub fn node(&self) -> NodeDomain {
    NodeDomain::new(self.db.clone(), self.notify.clone())
  }

  /// Access the agent orchestration storage domain.
  pub fn agent(&self) -> AgentDomain {
    self.agent.clone()
  }

  /// Access the workspace storage domain.
  pub fn workspace(&self) -> WorkspaceDomain {
    self.workspace.clone()
  }
}
