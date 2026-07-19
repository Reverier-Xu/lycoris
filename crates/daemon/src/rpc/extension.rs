//! Tonic wiring for the admission-guarded `Extension` service.
//!
//! The service is a thin shell (same shape as `rpc::cluster`): for invocation
//! it validates the wire shape — non-empty id and method, a well-formed JSON
//! payload — and delegates routing and execution to
//! [`ExtensionManager::invoke`], so the manager never sees a tonic type.
//! Forwarded calls arrive with `origin_node_id` set and are executed locally
//! or rejected by the manager's hop-limit rule (extension system design,
//! section 7). Registration maps the wire request onto an
//! [`ExtensionRegistration`] and delegates to [`ExtensionManager::register`]:
//! validation failures return in-band rejections (`accepted = false` with the
//! reason), while a version that does not strictly increase is a
//! `FAILED_PRECONDITION` status (extension system design, section 4).
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use lycoris_extension::ExtensionError;
use lycoris_proto::node::{
  ExtensionInvokeRequest, ExtensionInvokeResponse, RegisterExtensionRequest,
  RegisterExtensionResponse, extension_server::Extension,
};
use tonic::{Request, Response, Status};

use crate::extension::{
  ExtensionManager, ExtensionManagerError, ExtensionRegistration, RegisterExtensionError,
};

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

  async fn register_extension(
    &self, request: Request<RegisterExtensionRequest>,
  ) -> Result<Response<RegisterExtensionResponse>, Status> {
    let request = request.into_inner();
    let registration = ExtensionRegistration {
      id: request.id,
      name: request.name,
      version: request.version,
      engine: request.engine,
      entry: request.entry,
      artifact: request.artifact,
      manifest: request.manifest.into_iter().collect(),
      labels: request.labels.into_iter().collect(),
    };
    match self.manager.register(registration) {
      Ok(content_hash) => Ok(Response::new(RegisterExtensionResponse {
        accepted: true,
        reason: String::new(),
        content_hash,
      })),
      // Validation failures are in-band rejections: the caller fixes the
      // package and retries; the version sequence is untouched.
      Err(
        error @ (RegisterExtensionError::InvalidId(_)
        | RegisterExtensionError::Manifest(_)
        | RegisterExtensionError::EmptyArtifact),
      ) => Ok(Response::new(RegisterExtensionResponse {
        accepted: false,
        reason: error.to_string(),
        content_hash: String::new(),
      })),
      Err(error) => Err(Status::from(error)),
    }
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

/// The `RegisterExtensionError` → gRPC status mapping for rejections that do
/// not travel in-band: a version that fails the strictly-increasing rule is a
/// violated precondition; storage failures are server-side `internal`. The
/// validation variants are listed for completeness — the registration handler
/// returns them as in-band rejections instead.
impl From<RegisterExtensionError> for Status {
  fn from(error: RegisterExtensionError) -> Self {
    use RegisterExtensionError as Error;
    match error {
      Error::InvalidId(_) | Error::Manifest(_) | Error::EmptyArtifact => {
        Status::invalid_argument(error.to_string())
      }
      Error::VersionNotIncreasing { .. } => Status::failed_precondition(error.to_string()),
      Error::Storage(_) => Status::internal(error.to_string()),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
  };

  use lycoris_config::ExtensionsConfig;
  use lycoris_membership::SwimConfig;
  use lycoris_storage::{ExtensionRecord, ResourceScope, Storage};
  use tempfile::TempDir;
  use tonic::Code;

  use super::*;
  use crate::membership::{MemberRegister, MembershipService};

  const ECHO_SOURCE: &[u8] = b"function invoke(method, payload) return payload end";

  /// Build a service backed by temporary storage plus the handles its tests
  /// need to inspect storage and drive the reconcile loop directly.
  async fn test_handles(
    with_echo: bool,
  ) -> (TempDir, Storage, Arc<ExtensionManager>, ExtensionService) {
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
    let manager = Arc::new(manager);
    (
      dir,
      storage,
      manager.clone(),
      ExtensionService::new(manager),
    )
  }

  /// Build a service backed by temporary storage, with the echo extension
  /// loaded locally when `with_echo` is set.
  async fn test_service(with_echo: bool) -> (TempDir, ExtensionService) {
    let (dir, _storage, _manager, service) = test_handles(with_echo).await;
    (dir, service)
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

  fn register_request(id: &str, version: u64) -> RegisterExtensionRequest {
    RegisterExtensionRequest {
      id: id.to_string(),
      name: format!("extension-{id}"),
      version,
      engine: "lua".to_string(),
      entry: String::new(),
      artifact: ECHO_SOURCE.to_vec(),
      manifest: HashMap::from([("semver".to_string(), "1.0.0".to_string())]),
      labels: HashMap::new(),
    }
  }

  #[tokio::test]
  async fn register_extension_accepts_a_valid_package() {
    let (_dir, storage, manager, service) = test_handles(false).await;

    let response = service
      .register_extension(Request::new(register_request("ext-new", 1)))
      .await
      .unwrap()
      .into_inner();

    assert!(response.accepted);
    assert!(response.reason.is_empty());
    assert_eq!(
      response.content_hash,
      blake3::hash(ECHO_SOURCE).to_hex().to_string()
    );

    // The stored record is cluster-shared with this node as the source; the
    // empty entry defaulted to the contract entry point.
    let record = storage
      .extensions()
      .get("ext-new")
      .unwrap()
      .expect("registered record");
    assert_eq!(record.scope, ResourceScope::ClusterShared);
    assert_eq!(record.source_node_id.as_deref(), Some("local"));
    assert_eq!(record.entry, "invoke");
    assert_eq!(record.engine, "lua");
    assert!(record.created_at_ms > 0);
    assert_eq!(record.created_at_ms, record.updated_at_ms);
    assert_eq!(
      storage.extensions().blobs().read("ext-new").unwrap(),
      Some(ECHO_SOURCE.to_vec())
    );

    // The reconcile notify fired, so the extension loads without waiting for
    // a sync round (no selector in the manifest matches every node).
    tokio::time::timeout(Duration::from_millis(50), manager.notify().notified())
      .await
      .expect("register must fire the reconcile notify");
    manager.reconcile().await;
    let output = manager
      .invoke_local("ext-new", "echo", br#"{"a":1}"#)
      .await
      .unwrap();
    assert_eq!(output, br#"{"a":1}"#.to_vec());
  }

  #[tokio::test]
  async fn register_extension_rejects_a_non_increasing_version() {
    let (_dir, storage, _manager, service) = test_handles(false).await;
    service
      .register_extension(Request::new(register_request("ext-versioned", 2)))
      .await
      .unwrap();
    let created = storage
      .extensions()
      .get("ext-versioned")
      .unwrap()
      .expect("record")
      .created_at_ms;

    // Re-registering the same version is rejected, not idempotent; a lower
    // version is rejected the same way.
    for version in [2, 1] {
      let status = service
        .register_extension(Request::new(register_request("ext-versioned", version)))
        .await
        .unwrap_err();
      assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "version: {version}"
      );
      assert!(status.message().contains("ext-versioned"));
    }
    // The failed attempts left the stored record untouched.
    assert_eq!(
      storage
        .extensions()
        .get("ext-versioned")
        .unwrap()
        .expect("record")
        .version,
      2
    );

    // A strictly higher version is accepted and preserves the creation time.
    let response = service
      .register_extension(Request::new(register_request("ext-versioned", 3)))
      .await
      .unwrap()
      .into_inner();
    assert!(response.accepted);
    let record = storage
      .extensions()
      .get("ext-versioned")
      .unwrap()
      .expect("record");
    assert_eq!(record.version, 3);
    assert_eq!(record.created_at_ms, created);
    assert!(record.updated_at_ms >= created);
  }

  #[tokio::test]
  async fn register_extension_rejects_invalid_packages_in_band() {
    let (_dir, storage, _manager, service) = test_handles(false).await;

    let cases: Vec<(RegisterExtensionRequest, &str)> = vec![
      // An id outside the resource-id whitelist.
      (
        RegisterExtensionRequest {
          id: "../escape".to_string(),
          ..register_request("ignored", 1)
        },
        "invalid extension id",
      ),
      // An unknown engine.
      (
        RegisterExtensionRequest {
          engine: "python".to_string(),
          ..register_request("ext-engine", 1)
        },
        "unknown engine",
      ),
      // A manifest without the required semver.
      (
        RegisterExtensionRequest {
          manifest: HashMap::new(),
          ..register_request("ext-manifest", 1)
        },
        "semver",
      ),
      // A manifest with an unparseable semver.
      (
        RegisterExtensionRequest {
          manifest: HashMap::from([("semver".to_string(), "1.0".to_string())]),
          ..register_request("ext-semver", 1)
        },
        "invalid semver",
      ),
      // An empty artifact.
      (
        RegisterExtensionRequest {
          artifact: Vec::new(),
          ..register_request("ext-artifact", 1)
        },
        "artifact",
      ),
    ];

    for (request, expected) in cases {
      let id = request.id.clone();
      let response = service
        .register_extension(Request::new(request))
        .await
        .unwrap()
        .into_inner();
      assert!(!response.accepted, "case: {expected}");
      assert!(
        response.reason.contains(expected),
        "reason {:?} must mention {expected:?}",
        response.reason
      );
      // A rejected registration persists nothing.
      assert!(
        storage.extensions().get(&id).unwrap().is_none(),
        "case: {expected}"
      );
    }
  }

  #[tokio::test]
  async fn register_extension_persists_nothing_for_a_rejected_package() {
    let (_dir, storage, _manager, service) = test_handles(false).await;
    let request = RegisterExtensionRequest {
      manifest: HashMap::new(),
      ..register_request("ext-rejected", 1)
    };
    let response = service
      .register_extension(Request::new(request))
      .await
      .unwrap()
      .into_inner();
    assert!(!response.accepted);
    assert!(storage.extensions().get("ext-rejected").unwrap().is_none());
    assert_eq!(
      storage.extensions().blobs().read("ext-rejected").unwrap(),
      None
    );
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
