use serde::{Deserialize, Serialize};

/// Visibility scope for a reusable resource.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceScope {
  /// Visible only on the node where it was created; never synchronized.
  NodeLocal,
  /// Synchronized across the cluster via the resource anti-entropy protocol.
  ClusterShared,
}
