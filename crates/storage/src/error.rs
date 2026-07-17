use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
  #[error("redb error: {0}")]
  Redb(#[from] redb::Error),
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("serialization error: {0}")]
  Serialization(#[from] postcard::Error),
  #[error("cannot set the local node's own address as primary")]
  SelfPrimary,
}

/// Returns true when the error indicates that a redb table has not been
/// created yet. Read helpers use this to treat missing tables as empty.
pub fn is_missing_table(err: &StorageError) -> bool {
  matches!(err, StorageError::Redb(redb::Error::TableDoesNotExist(_)))
}

/// Convert any redb sub-error into a `StorageError::Redb`.
///
/// redb exposes many error types (`DatabaseError`, `TransactionError`,
/// `TableError`, `CommitError`, etc.) that all convert into the superset
/// `redb::Error`. This helper keeps `StorageError` small while still allowing
/// the `?` operator to be used after a `.map_err`.
pub fn redb_err<E: Into<redb::Error>>(error: E) -> StorageError {
  StorageError::Redb(error.into())
}
