#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

// The generated code needs these allowances; hand-written code in this crate
// does not get them.
#[allow(clippy::large_enum_variant, clippy::result_large_err)]
pub mod node {
  tonic::include_proto!("lycoris.daemon");
}

use lycoris_core::{ResourceScope, now_ms};
use node::{NodeInfo, NodeState, ResourceScope as ProtoResourceScope};

/// Metadata header carrying the cluster key that authorizes `Cluster` RPCs.
///
/// This is the single source of truth for the header name; clients attach the
/// key under this header and the daemon interceptor reads it back.
pub const CLUSTER_KEY_HEADER: &str = "x-lycoris-cluster-key";

/// Map a domain scope to its wire representation.
///
/// This is the single domain-to-wire scope mapping; the inverse is
/// [`scope_from_proto`].
pub fn scope_to_proto(scope: ResourceScope) -> ProtoResourceScope {
  match scope {
    ResourceScope::ClusterShared => ProtoResourceScope::ClusterShared,
    ResourceScope::NodeLocal => ProtoResourceScope::NodeLocal,
  }
}

/// Map a wire scope to the domain scope.
///
/// `UNSPECIFIED` normalizes to `NodeLocal`: an unscoped resource must never be
/// synchronized.
pub fn scope_from_proto(scope: ProtoResourceScope) -> ResourceScope {
  match scope {
    ProtoResourceScope::ClusterShared => ResourceScope::ClusterShared,
    ProtoResourceScope::NodeLocal | ProtoResourceScope::Unspecified => ResourceScope::NodeLocal,
  }
}

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
    Self {
      id: id.into(),
      address: address.into(),
      labels,
      annotations,
      last_heartbeat_unix_ms: now_ms(),
      state: NodeState::Active as i32,
      incarnation: 1,
      heartbeat: 0,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn scope_mapping_round_trip() {
    for (proto, domain) in [
      (
        ProtoResourceScope::ClusterShared,
        ResourceScope::ClusterShared,
      ),
      (ProtoResourceScope::NodeLocal, ResourceScope::NodeLocal),
      (ProtoResourceScope::Unspecified, ResourceScope::NodeLocal),
    ] {
      assert_eq!(scope_from_proto(proto), domain);
    }
    for domain in [ResourceScope::ClusterShared, ResourceScope::NodeLocal] {
      assert_eq!(scope_from_proto(scope_to_proto(domain)), domain);
    }
  }
}
