//! Tonic wiring for the admission-guarded `Extension` service.
//!
//! The service is a thin shell (same shape as `rpc::cluster`): it validates
//! the wire shape — non-empty id and method, a well-formed JSON payload — and
//! delegates routing and execution to [`ExtensionManager::invoke`], so the
//! manager never sees a tonic type. Forwarded calls arrive with
//! `origin_node_id` set and are executed locally or rejected by the manager's
//! hop-limit rule (extension system design, section 7).
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use lycoris_extension::ExtensionError;
use lycoris_proto::node::{
  ExtensionInvokeRequest, ExtensionInvokeResponse, extension_server::Extension,
};
use tonic::{Request, Response, Status};

use crate::extension::{ExtensionManager, ExtensionManagerError};

/// The tonic shell of the `Extension` service.
#[derive(Clone)]
pub struct ExtensionService {
  manager: Arc<ExtensionManager>,
}

impl ExtensionService {
  pub fn new(manager: Arc<ExtensionManager>) -> Self {
    Self { manager }
  }
}

#[tonic::async_trait]
#[allow(clippy::result_large_err)]
impl Extension for ExtensionService {
  async fn invoke(
    &self, request: Request<ExtensionInvokeRequest>,
  ) -> Result<Response<ExtensionInvokeResponse>, Status> {
    let request = request.into_inner();
    if request.extension_id.is_empty() {
      return Err(Status::invalid_argument("extension id must not be empty"));
    }
    if request.method.is_empty() {
      return Err(Status::invalid_argument("method must not be empty"));
    }
    // Payloads are JSON end to end (design section 5.1); reject malformed
    // bytes at the boundary instead of letting them reach an engine.
    if let Err(error) = serde_json::from_slice::<serde_json::Value>(&request.payload) {
      return Err(Status::invalid_argument(format!(
        "payload is not valid JSON: {error}"
      )));
    }
    // An empty origin marks a direct call; a set origin marks a forwarded
    // one (hop limit 1).
    let origin = if request.origin_node_id.is_empty() {
      None
    } else {
      Some(request.origin_node_id)
    };

    let outcome = self
      .manager
      .invoke(
        &request.extension_id,
        &request.method,
        &request.payload,
        origin,
      )
      .await?;
    Ok(Response::new(ExtensionInvokeResponse {
      payload: outcome.payload,
      executed_by: outcome.executed_by,
    }))
  }
}

/// The single `ExtensionManagerError` → gRPC status mapping.
///
/// An extension that is nowhere runnable is `not_found`; a second forwarding
/// hop is a violated routing precondition; a payload an engine rejects is the
/// caller's `invalid_argument`; an engine deadline maps to
/// `deadline_exceeded`; unreachable candidates and forwarding transport
/// failures are transient `unavailable`; everything else (guest traps, script
/// errors, storage failures) is a genuine server-side `internal`.
impl From<ExtensionManagerError> for Status {
  fn from(error: ExtensionManagerError) -> Self {
    use ExtensionManagerError as Error;
    match error {
      Error::NotRunning(_) | Error::NotFound(_) => Status::not_found(error.to_string()),
      Error::AlreadyForwarded(_) => Status::failed_precondition(error.to_string()),
      Error::Extension(ExtensionError::InvalidPayload(_)) => {
        Status::invalid_argument(error.to_string())
      }
      Error::Extension(ExtensionError::Timeout(_)) => Status::deadline_exceeded(error.to_string()),
      Error::Unavailable { .. } | Error::Forwarded(_) => Status::unavailable(error.to_string()),
      Error::Extension(_) | Error::MissingArtifact(_) | Error::Storage(_) => {
        Status::internal(error.to_string())
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{collections::BTreeMap, sync::Arc, time::Duration};

  use lycoris_config::ExtensionsConfig;
  use lycoris_membership::SwimConfig;
  use lycoris_storage::{ExtensionRecord, ResourceScope, Storage};
  use tempfile::TempDir;
  use tonic::Code;

  use super::*;
  use crate::membership::{MemberRegister, MembershipService};

  const ECHO_SOURCE: &[u8] = b"function invoke(method, payload) return payload end";

  /// Build a service backed by temporary storage, with the echo extension
  /// loaded locally when `with_echo` is set.
  async fn test_service(with_echo: bool) -> (TempDir, ExtensionService) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    let membership = Arc::new(MembershipService::new(
      "local",
      SwimConfig::default(),
      MemberRegister::new("local", "127.0.0.1:1", 1, 0),
    ));
    let manager =
      ExtensionManager::new(&ExtensionsConfig::default(), storage.clone(), membership).unwrap();
    if with_echo {
      let record = ExtensionRecord {
        id: "ext-echo".to_string(),
        name: "extension-ext-echo".to_string(),
        version: 1,
        engine: "lua".to_string(),
        entry: "invoke".to_string(),
        content_hash: blake3::hash(ECHO_SOURCE).to_hex().to_string(),
        scope: ResourceScope::ClusterShared,
        source_node_id: None,
        created_at_ms: 0,
        updated_at_ms: 0,
        manifest: BTreeMap::from([("semver".to_string(), "1.0.0".to_string())]),
        labels: BTreeMap::new(),
      };
      storage
        .extensions()
        .apply_remote_extension(record, ECHO_SOURCE)
        .unwrap();
      manager.reconcile().await;
    }
    (dir, ExtensionService::new(Arc::new(manager)))
  }

  fn invoke_request(extension_id: &str, method: &str, payload: &[u8]) -> ExtensionInvokeRequest {
    ExtensionInvokeRequest {
      extension_id: extension_id.to_string(),
      method: method.to_string(),
      payload: payload.to_vec(),
      origin_node_id: String::new(),
    }
  }

  #[tokio::test]
  async fn invoke_rejects_an_empty_extension_id() {
    let (_dir, service) = test_service(false).await;
    let status = service
      .invoke(Request::new(invoke_request("", "m", b"{}")))
      .await
      .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("extension id"));
  }

  #[tokio::test]
  async fn invoke_rejects_an_empty_method() {
    let (_dir, service) = test_service(false).await;
    let status = service
      .invoke(Request::new(invoke_request("ext-echo", "", b"{}")))
      .await
      .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("method"));
  }

  #[tokio::test]
  async fn invoke_rejects_a_non_json_payload() {
    let (_dir, service) = test_service(false).await;
    let status = service
      .invoke(Request::new(invoke_request("ext-echo", "m", b"{broken")))
      .await
      .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("not valid JSON"));
  }

  #[tokio::test]
  async fn invoke_executes_a_local_extension() {
    let (_dir, service) = test_service(true).await;
    let response = service
      .invoke(Request::new(invoke_request(
        "ext-echo",
        "echo",
        br#"{"a":1}"#,
      )))
      .await
      .unwrap()
      .into_inner();
    assert_eq!(response.payload, br#"{"a":1}"#.to_vec());
    assert_eq!(response.executed_by, "local");
  }

  #[tokio::test]
  async fn invoke_reports_an_unknown_extension_as_not_found() {
    let (_dir, service) = test_service(false).await;
    let status = service
      .invoke(Request::new(invoke_request("ghost", "m", b"{}")))
      .await
      .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
    assert!(status.message().contains("ghost"));
  }

  #[test]
  fn manager_error_status_mapping() {
    let cases = [
      (
        ExtensionManagerError::NotRunning("x".to_string()),
        Code::NotFound,
      ),
      (
        ExtensionManagerError::NotFound("x".to_string()),
        Code::NotFound,
      ),
      (
        ExtensionManagerError::AlreadyForwarded("x".to_string()),
        Code::FailedPrecondition,
      ),
      (
        ExtensionManagerError::Extension(ExtensionError::InvalidPayload("x".to_string())),
        Code::InvalidArgument,
      ),
      (
        ExtensionManagerError::Extension(ExtensionError::Timeout(Duration::from_secs(1))),
        Code::DeadlineExceeded,
      ),
      (
        ExtensionManagerError::Extension(ExtensionError::GuestTrap("x".to_string())),
        Code::Internal,
      ),
      (
        ExtensionManagerError::Extension(ExtensionError::Script("x".to_string())),
        Code::Internal,
      ),
      (
        ExtensionManagerError::Unavailable {
          id: "x".to_string(),
          candidates: 2,
          message: "boom".to_string(),
        },
        Code::Unavailable,
      ),
      (
        ExtensionManagerError::Forwarded(lycoris_client::ClientError::Timeout("x")),
        Code::Unavailable,
      ),
      (
        ExtensionManagerError::MissingArtifact("x".to_string()),
        Code::Internal,
      ),
      (
        ExtensionManagerError::Storage(lycoris_storage::ExtensionStorageError::Storage(
          lycoris_storage::StorageError::Io(std::io::Error::other("boom")),
        )),
        Code::Internal,
      ),
    ];
    for (error, expected) in cases {
      assert_eq!(Status::from(error).code(), expected, "error: {expected:?}");
    }
  }
}
