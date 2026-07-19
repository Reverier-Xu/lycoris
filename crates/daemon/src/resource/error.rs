//! The typed failure enum of the resource facade.
//!
//! The rpc boundary maps these onto gRPC statuses (`crate::rpc`); the
//! anti-entropy task logs them directly, so every variant names the failing
//! operation precisely.

use lycoris_core::ResourceScope;
use lycoris_proto::node::ResourceKind;
use lycoris_storage::{AgentStorageError, ExtensionStorageError, WorkspaceStorageError};

/// Errors produced by the resource facade.
#[derive(Debug, thiserror::Error)]
pub enum MapperError {
  /// The request carried no metadata block.
  #[error("missing resource metadata")]
  MissingMetadata,
  /// The metadata kind field did not decode.
  #[error("unknown resource kind: {0}")]
  UnknownKind(i32),
  /// The metadata scope field did not decode.
  #[error("unknown resource scope: {0}")]
  UnknownScope(i32),
  /// An applied resource was not cluster-shared (D8: never silently drop).
  #[error("only cluster-shared resources can be applied; resource '{id}' is {scope}")]
  NotShared { id: String, scope: ResourceScope },
  /// The resource body was absent.
  #[error("missing body for {kind:?} resource '{id}'")]
  MissingBody { kind: ResourceKind, id: String },
  /// The declared kind and the body variant disagree.
  #[error("resource kind {kind:?} does not match its body")]
  KindBodyMismatch { kind: ResourceKind },
  /// Nodes and sessions do not participate in resource synchronization.
  #[error("{kind:?} resources are not synchronized")]
  NotSynchronized { kind: ResourceKind },
  /// No resource with the requested id exists.
  #[error("resource not found: {0}")]
  NotFound(String),
  /// Agent-domain storage failure, with the failing operation as context.
  #[error("{context}: {source}")]
  Agent {
    context: &'static str,
    source: AgentStorageError,
  },
  /// Workspace-domain storage failure, with the failing operation as context.
  #[error("{context}: {source}")]
  Workspace {
    context: &'static str,
    source: WorkspaceStorageError,
  },
  /// Extension-domain storage failure, with the failing operation as context.
  #[error("{context}: {source}")]
  Extension {
    context: &'static str,
    source: ExtensionStorageError,
  },
}

impl MapperError {
  pub(crate) fn agent(context: &'static str) -> impl Fn(AgentStorageError) -> Self {
    move |source| Self::Agent { context, source }
  }

  pub(crate) fn workspace(context: &'static str) -> impl Fn(WorkspaceStorageError) -> Self {
    move |source| Self::Workspace { context, source }
  }

  pub(crate) fn extension(context: &'static str) -> impl Fn(ExtensionStorageError) -> Self {
    move |source| Self::Extension { context, source }
  }
}
