//! Plugin error types.
//!
//! Every failure that crosses an engine boundary is reported as a structured
//! [`PluginError`]; the host never panics on guest misbehaviour.

use std::time::Duration;

use thiserror::Error;

/// Errors produced by the plugin engine layer.
#[derive(Debug, Error)]
pub enum PluginError {
  /// The plugin manifest failed validation.
  #[error("invalid manifest: {0}")]
  Manifest(String),
  /// The engine itself (wasmtime runtime / Lua VM setup) failed, as opposed
  /// to guest code misbehaving.
  #[error("engine error: {0}")]
  Engine(String),
  /// A WASM guest trapped during instantiation or invocation.
  #[error("guest trapped: {0}")]
  GuestTrap(String),
  /// A Lua script raised an error during load or invocation.
  #[error("script error: {0}")]
  Script(String),
  /// The invocation exceeded its wall-clock deadline.
  #[error("invocation timed out after {0:?}")]
  Timeout(Duration),
  /// The guest exceeded a configured resource budget (instruction count or
  /// memory).
  #[error("resource budget exceeded: {0}")]
  BudgetExceeded(String),
  /// A payload crossing the engine boundary is not well-formed JSON.
  #[error("invalid payload: {0}")]
  InvalidPayload(String),
  /// The artifact bytes do not match the declared blake3 content hash.
  #[error("content hash mismatch: expected {expected}, got {actual}")]
  ContentHashMismatch { expected: String, actual: String },
}

/// Convenience alias for plugin results.
pub type Result<T> = std::result::Result<T, PluginError>;
