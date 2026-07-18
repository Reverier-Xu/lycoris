#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

// The generated code needs these allowances; hand-written code in this crate
// does not get them.
#[allow(clippy::large_enum_variant, clippy::result_large_err)]
pub mod node {
  tonic::include_proto!("lycoris.daemon");
}

use node::{NodeInfo, NodeState};

/// Metadata header carrying the cluster key that authorizes `Cluster` RPCs.
///
/// This is the single source of truth for the header name; clients attach the
/// key under this header and the daemon interceptor reads it back.
pub const CLUSTER_KEY_HEADER: &str = "x-lycoris-cluster-key";

impl NodeInfo {
  /// Construct a fresh registration payload for a node: `active` state at
  /// incarnation 1 with heartbeat 0 and the current time as the last
  /// heartbeat.
  ///
  /// This is the single construction point for ad-hoc registrations (CLI
  /// register/join, examples, tests). Daemons exchange full registers via the
  /// membership layer and never build `NodeInfo` from scratch.
  pub fn new(
    id: impl Into<String>, address: impl Into<String>,
    labels: std::collections::HashMap<String, String>,
    annotations: std::collections::HashMap<String, String>,
  ) -> Self {
    let last_heartbeat_unix_ms = std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .map(|elapsed| elapsed.as_millis() as i64)
      .unwrap_or(0);
    Self {
      id: id.into(),
      address: address.into(),
      labels,
      annotations,
      last_heartbeat_unix_ms,
      state: NodeState::Active as i32,
      incarnation: 1,
      heartbeat: 0,
    }
  }
}
