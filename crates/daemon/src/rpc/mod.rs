use tonic::Status;

pub mod cluster;
pub mod extension;
pub mod interceptor;
pub mod resource;
pub mod server;

use crate::resource::MapperError;

/// The single `MapperError` → gRPC status mapping.
///
/// Wire-shape and data problems reported by the caller/peer map to
/// `invalid_argument`; domain preconditions to `failed_precondition`; genuine
/// server-side failures to `internal`.
impl From<MapperError> for Status {
  fn from(error: MapperError) -> Self {
    match error {
      MapperError::MissingMetadata
      | MapperError::UnknownKind(_)
      | MapperError::UnknownScope(_)
      | MapperError::MissingBody { .. }
      | MapperError::KindBodyMismatch { .. } => Status::invalid_argument(error.to_string()),
      MapperError::NotShared { .. } | MapperError::NotSynchronized { .. } => {
        Status::failed_precondition(error.to_string())
      }
      MapperError::NotFound(_) => Status::not_found(error.to_string()),
      MapperError::Agent { context, source } => agent_storage_status(context, source),
      MapperError::Workspace { context, source } => workspace_storage_status(context, source),
      MapperError::Extension { context, source } => extension_storage_status(context, source),
    }
  }
}

/// Map an agent-domain storage failure to a gRPC status at the RPC boundary.
///
/// A content hash mismatch or an embedding of the wrong dimension means the
/// *peer* sent a record this node cannot accept, so both map to
/// `invalid_argument`; genuine server-side failures map to `internal`.
pub(crate) fn agent_storage_status(
  context: &str, error: lycoris_storage::AgentStorageError,
) -> Status {
  use lycoris_storage::AgentStorageError as Error;
  match &error {
    Error::HashMismatch(_) | Error::InvalidEmbeddingDim { .. } => {
      Status::invalid_argument(format!("{context}: {error}"))
    }
    Error::Backend(_) => Status::internal(format!("{context}: {error}")),
  }
}

/// Map a workspace-domain storage failure to a gRPC status; see
/// [`agent_storage_status`] for the mapping rationale.
pub(crate) fn workspace_storage_status(
  context: &str, error: lycoris_storage::WorkspaceStorageError,
) -> Status {
  use lycoris_storage::WorkspaceStorageError as Error;
  match &error {
    Error::HashMismatch(_) | Error::InvalidResourceId(_) => {
      Status::invalid_argument(format!("{context}: {error}"))
    }
    Error::Storage(_) | Error::GitCommandFailed(_) => {
      Status::internal(format!("{context}: {error}"))
    }
  }
}

/// Map an extension-domain storage failure to a gRPC status; see
/// [`agent_storage_status`] for the mapping rationale.
pub(crate) fn extension_storage_status(
  context: &str, error: lycoris_storage::ExtensionStorageError,
) -> Status {
  use lycoris_storage::ExtensionStorageError as Error;
  match &error {
    Error::HashMismatch(_) | Error::InvalidResourceId(_) => {
      Status::invalid_argument(format!("{context}: {error}"))
    }
    Error::Storage(_) => Status::internal(format!("{context}: {error}")),
  }
}

/// Map a node-domain storage failure to a gRPC status. The node domain never
/// validates peer-supplied records, so every failure is genuinely internal.
pub(crate) fn storage_status(context: &str, error: lycoris_storage::StorageError) -> Status {
  Status::internal(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
  use lycoris_proto::node::ResourceKind;
  use tonic::Code;

  use super::*;

  #[test]
  fn mapper_error_status_mapping() {
    let cases = [
      (MapperError::MissingMetadata, Code::InvalidArgument),
      (MapperError::UnknownKind(42), Code::InvalidArgument),
      (MapperError::UnknownScope(42), Code::InvalidArgument),
      (
        MapperError::MissingBody {
          kind: ResourceKind::Memory,
          id: "m".to_string(),
        },
        Code::InvalidArgument,
      ),
      (
        MapperError::KindBodyMismatch {
          kind: ResourceKind::Skill,
        },
        Code::InvalidArgument,
      ),
      (
        MapperError::NotShared {
          id: "m".to_string(),
          scope: lycoris_core::ResourceScope::NodeLocal,
        },
        Code::FailedPrecondition,
      ),
      (
        MapperError::NotSynchronized {
          kind: ResourceKind::Node,
        },
        Code::FailedPrecondition,
      ),
      (MapperError::NotFound("m".to_string()), Code::NotFound),
      (
        MapperError::Agent {
          context: "failed to apply remote memory",
          source: lycoris_storage::AgentStorageError::Backend("boom".to_string()),
        },
        Code::Internal,
      ),
      (
        MapperError::Extension {
          context: "failed to apply remote extension",
          source: lycoris_storage::ExtensionStorageError::HashMismatch(
            lycoris_storage::ContentHashMismatch,
          ),
        },
        Code::InvalidArgument,
      ),
    ];
    for (error, expected) in cases {
      assert_eq!(Status::from(error).code(), expected, "error: {expected:?}");
    }
  }

  #[test]
  fn peer_data_problems_map_to_invalid_argument() {
    let hash_mismatch = agent_storage_status(
      "ctx",
      lycoris_storage::AgentStorageError::HashMismatch(lycoris_storage::ContentHashMismatch),
    );
    assert_eq!(hash_mismatch.code(), Code::InvalidArgument);

    let dim_mismatch = agent_storage_status(
      "ctx",
      lycoris_storage::AgentStorageError::InvalidEmbeddingDim {
        expected: 384,
        actual: 3,
      },
    );
    assert_eq!(dim_mismatch.code(), Code::InvalidArgument);
  }

  #[test]
  fn extension_storage_status_mapping() {
    let hash_mismatch = extension_storage_status(
      "ctx",
      lycoris_storage::ExtensionStorageError::HashMismatch(lycoris_storage::ContentHashMismatch),
    );
    assert_eq!(hash_mismatch.code(), Code::InvalidArgument);

    let invalid_id = extension_storage_status(
      "ctx",
      lycoris_storage::ExtensionStorageError::InvalidResourceId(
        lycoris_storage::InvalidResourceId("a/b".to_string()),
      ),
    );
    assert_eq!(invalid_id.code(), Code::InvalidArgument);

    let storage = extension_storage_status(
      "ctx",
      lycoris_storage::ExtensionStorageError::Storage(lycoris_storage::StorageError::Io(
        std::io::Error::other("boom"),
      )),
    );
    assert_eq!(storage.code(), Code::Internal);
  }
}
