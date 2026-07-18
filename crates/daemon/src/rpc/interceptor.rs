use std::sync::Arc;

use lycoris_core::{ClusterKey, ClusterKeyError};
use lycoris_proto::CLUSTER_KEY_HEADER;
use tonic::{Request, Status};

/// Build a tonic interceptor that validates the `x-lycoris-cluster-key` header.
///
/// The interceptor rejects requests when:
/// - this node has not initialized a cluster key (`failed_precondition`: the
///   server-side precondition is unmet, unlike the caller-side auth failures
///   below), or
/// - the caller did not supply a cluster key, or
/// - the supplied key cannot be used: malformed hex, or a header value that is
///   not a valid header string — both are "supplied but unusable" and take the
///   same `permission_denied` path, never the missing-key path, or
/// - the supplied key does not match the expected key.
#[allow(clippy::result_large_err)]
pub fn cluster_key_interceptor(
  expected: Option<Arc<ClusterKey>>,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
  move |request: Request<()>| {
    let provided = request.metadata().get(CLUSTER_KEY_HEADER).map(|value| {
      value
        .to_str()
        .map_err(|_| ClusterKeyError::InvalidHex)
        .and_then(ClusterKey::from_hex)
    });

    match (&expected, provided) {
      (Some(expected), Some(Ok(provided))) if provided == **expected => Ok(request),
      (Some(_), Some(Ok(_))) => Err(Status::permission_denied("cluster key mismatch")),
      (Some(_), Some(Err(_))) => Err(Status::permission_denied("invalid cluster key format")),
      (Some(_), None) => Err(Status::unauthenticated("missing cluster key")),
      (None, _) => Err(Status::failed_precondition(
        "this node has not initialized a cluster key; run 'lycoris cluster init' first",
      )),
    }
  }
}

#[cfg(test)]
mod tests {
  use tonic::{Code, metadata::MetadataValue};

  use super::*;

  fn key() -> ClusterKey {
    ClusterKey::from_bytes([0xAB; 32])
  }

  fn request_with_key(value: &str) -> Request<()> {
    let mut request = Request::new(());
    request
      .metadata_mut()
      .insert(CLUSTER_KEY_HEADER, MetadataValue::try_from(value).unwrap());
    request
  }

  #[test]
  fn accepts_matching_key() {
    let expected = Arc::new(key());
    let interceptor = cluster_key_interceptor(Some(expected.clone()));
    assert!(interceptor(request_with_key(&expected.to_hex())).is_ok());
  }

  #[test]
  fn rejects_mismatched_key() {
    let interceptor = cluster_key_interceptor(Some(Arc::new(key())));
    let other = ClusterKey::from_bytes([0xCD; 32]);
    let status = interceptor(request_with_key(&other.to_hex())).unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied);
  }

  #[test]
  fn rejects_malformed_key() {
    let interceptor = cluster_key_interceptor(Some(Arc::new(key())));
    let status = interceptor(request_with_key("not-hex")).unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied);
  }

  #[test]
  fn rejects_missing_key() {
    let interceptor = cluster_key_interceptor(Some(Arc::new(key())));
    let status = interceptor(Request::new(())).unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
  }

  #[test]
  fn rejects_when_node_has_no_key() {
    let interceptor = cluster_key_interceptor(None);
    let status = interceptor(request_with_key(&key().to_hex())).unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
  }
}
