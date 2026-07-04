use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
  #[error("sqlite error: {0}")]
  Sqlite(#[from] rusqlite::Error),
  #[error("storage lock poisoned")]
  LockPoisoned,
  #[error("corrupt node state in database: {0}")]
  CorruptState(String),
}
