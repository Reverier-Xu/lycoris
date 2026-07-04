//! Agent orchestration storage.
//!
//! Planned responsibilities:
//! - Session storage: persisting active agent sessions, turn history, and
//!   session-level metadata.
//! - Short-term memory (STM): recent context window / working memory for
//!   ongoing interactions.
//! - Long-term memory (LTM): episodic and semantic memory retrieval, likely
//!   backed by embeddings + a vector store in addition to node-local metadata.
//!
//! The current implementation is intentionally a set of placeholder traits and
//! a no-op implementation. The expected future backend for multimodal vector
//! memory is LanceDB, while session/STM metadata may remain in the node-local
//! redb database.

use std::{collections::HashMap, fmt::Debug, sync::Arc};

/// A stored agent session.
#[derive(Debug, Clone)]
pub struct Session {
  pub id: String,
  pub metadata: HashMap<String, String>,
}

/// A memory entry (short- or long-term).
#[derive(Debug, Clone)]
pub struct MemoryEntry {
  pub id: String,
  pub content: Vec<u8>,
  pub metadata: HashMap<String, String>,
}

/// Storage for active agent sessions.
pub trait SessionStorage: Debug + Send + Sync {
  fn create(&self, session: &Session) -> Result<(), AgentStorageError>;
  fn get(&self, id: &str) -> Result<Option<Session>, AgentStorageError>;
  fn list(&self) -> Result<Vec<Session>, AgentStorageError>;
  fn delete(&self, id: &str) -> Result<(), AgentStorageError>;
}

/// Storage for agent memory.
pub trait MemoryStorage: Debug + Send + Sync {
  fn store(&self, entry: &MemoryEntry) -> Result<(), AgentStorageError>;
  fn recall(&self, query: &[usize], limit: usize) -> Result<Vec<MemoryEntry>, AgentStorageError>;
}

/// Errors that can occur in agent storage backends.
#[derive(Debug, thiserror::Error)]
pub enum AgentStorageError {
  #[error("agent storage not implemented")]
  NotImplemented,
  #[error("backend error: {0}")]
  Backend(String),
}

/// No-op placeholder implementation.
#[derive(Debug, Clone, Default)]
pub struct NoopAgentStorage;

impl SessionStorage for NoopAgentStorage {
  fn create(&self, _session: &Session) -> Result<(), AgentStorageError> {
    Ok(())
  }

  fn get(&self, _id: &str) -> Result<Option<Session>, AgentStorageError> {
    Ok(None)
  }

  fn list(&self) -> Result<Vec<Session>, AgentStorageError> {
    Ok(Vec::new())
  }

  fn delete(&self, _id: &str) -> Result<(), AgentStorageError> {
    Ok(())
  }
}

impl MemoryStorage for NoopAgentStorage {
  fn store(&self, _entry: &MemoryEntry) -> Result<(), AgentStorageError> {
    Ok(())
  }

  fn recall(&self, _query: &[usize], _limit: usize) -> Result<Vec<MemoryEntry>, AgentStorageError> {
    Ok(Vec::new())
  }
}

/// Agent storage facade.
#[derive(Debug, Clone)]
pub struct AgentDomain {
  sessions: Arc<dyn SessionStorage>,
  memory: Arc<dyn MemoryStorage>,
}

impl AgentDomain {
  pub(crate) fn new() -> Self {
    let noop = Arc::new(NoopAgentStorage);
    Self {
      sessions: noop.clone(),
      memory: noop,
    }
  }

  pub fn sessions(&self) -> &dyn SessionStorage {
    self.sessions.as_ref()
  }

  pub fn memory(&self) -> &dyn MemoryStorage {
    self.memory.as_ref()
  }
}
