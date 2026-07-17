use std::sync::Arc;

use lycoris_core::ClusterKey;
use lycoris_proto::CLUSTER_KEY_HEADER;
use tonic::{Request, Status};

/// Build a tonic interceptor that validates the `x-lycoris-cluster-key` header.
///
/// The interceptor rejects requests when:
/// - this node has not initialized a cluster key (`failed_precondition`: the
///   server-side precondition is unmet, unlike the caller-side auth failures
///   below), or
/// - the caller did not supply a cluster key, or
/// - the supplied key is malformed, or
/// - the supplied key does not match the expected key.
#[allow(clippy::result_large_err)]
pub fn cluster_key_interceptor(
  expected: Option<Arc<ClusterKey>>,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
  move |request: Request<()>| {
    let provided = request
      .metadata()
      .get(CLUSTER_KEY_HEADER)
      .and_then(|value| value.to_str().ok())
      .map(ClusterKey::from_hex);

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
