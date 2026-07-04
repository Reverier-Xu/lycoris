//! Agent orchestration storage.
//!
//! Planned responsibilities:
//! - Session storage: persisting active agent sessions, turn history, and
//!   session-level metadata.
//! - Short-term memory (STM): recent context window / working memory for
//!   ongoing interactions.
//! - Long-term memory (LTM): episodic and semantic memory retrieval, likely
//!   backed by embeddings + a vector store in addition to SQLite metadata.
//!
//! This module is intentionally a placeholder until the agent runtime is
//! designed.
