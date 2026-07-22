mod record;
mod registry;

pub use record::{AuthorizationKind, AuthorizationRecord, KeyState};
pub use registry::{AuthorizationRegistry, AuthorizationStatus};
use thiserror::Error;

use crate::{ClusterId, IdentityError, RecordId};

#[derive(Debug, Error)]
pub enum AuthorizationError {
  #[error(transparent)]
  Identity(#[from] IdentityError),
  #[error("authorization serialization failed: {0}")]
  Serialization(#[from] postcard::Error),
  #[error("authorization record belongs to cluster {actual}, expected {expected}")]
  ForeignCluster {
    expected: ClusterId,
    actual: ClusterId,
  },
  #[error("authorization record {0} has an invalid signature")]
  InvalidSignature(RecordId),
  #[error("authorization record is invalid: {0}")]
  InvalidRecord(&'static str),
  #[error("authorization record depends on missing record {0}")]
  MissingDependency(RecordId),
  #[error("authorization actions for identity record {0} are conflicted")]
  ConflictedAuthorizer(RecordId),
  #[error("identity record {0} has retired its authorization key")]
  RetiredAuthorizer(RecordId),
}
