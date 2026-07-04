//! Workspace storage.
//!
//! Planned responsibilities:
//! - Project/workspace metadata and configuration.
//! - File index, artifact cache, and versioned snapshots.
//! - Shared state between local tools and remote workers.
//!
//! The current implementation is an intentional placeholder. Versioning is
//! expected to be handled by Pijul once the workspace abstraction is designed,
//! while lightweight metadata may remain in the node-local redb database.

use std::{collections::HashMap, fmt::Debug, sync::Arc};

/// A workspace descriptor.
#[derive(Debug, Clone)]
pub struct Workspace {
  pub id: String,
  pub root: std::path::PathBuf,
  pub metadata: HashMap<String, String>,
}

/// Storage for workspace metadata and versioned snapshots.
pub trait WorkspaceStorage: Debug + Send + Sync {
  fn create(&self, workspace: &Workspace) -> Result<(), WorkspaceStorageError>;
  fn get(&self, id: &str) -> Result<Option<Workspace>, WorkspaceStorageError>;
  fn list(&self) -> Result<Vec<Workspace>, WorkspaceStorageError>;
  fn delete(&self, id: &str) -> Result<(), WorkspaceStorageError>;
}

/// Errors that can occur in workspace storage backends.
#[derive(Debug, thiserror::Error)]
pub enum WorkspaceStorageError {
  #[error("workspace storage not implemented")]
  NotImplemented,
  #[error("backend error: {0}")]
  Backend(String),
}

/// No-op placeholder implementation.
#[derive(Debug, Clone, Default)]
pub struct NoopWorkspaceStorage;

impl WorkspaceStorage for NoopWorkspaceStorage {
  fn create(&self, _workspace: &Workspace) -> Result<(), WorkspaceStorageError> {
    Ok(())
  }

  fn get(&self, _id: &str) -> Result<Option<Workspace>, WorkspaceStorageError> {
    Ok(None)
  }

  fn list(&self) -> Result<Vec<Workspace>, WorkspaceStorageError> {
    Ok(Vec::new())
  }

  fn delete(&self, _id: &str) -> Result<(), WorkspaceStorageError> {
    Ok(())
  }
}

/// Workspace storage facade.
#[derive(Debug, Clone)]
pub struct WorkspaceDomain {
  inner: Arc<dyn WorkspaceStorage>,
}

impl WorkspaceDomain {
  pub(crate) fn new() -> Self {
    Self {
      inner: Arc::new(NoopWorkspaceStorage),
    }
  }

  pub fn storage(&self) -> &dyn WorkspaceStorage {
    self.inner.as_ref()
  }
}
