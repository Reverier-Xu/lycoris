use serde::{Deserialize, Serialize};

/// Visibility scope for a reusable resource.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceScope {
  /// Visible only on the node where it was created; never synchronized.
  NodeLocal,
  /// Synchronized across the cluster via the resource anti-entropy protocol.
  ClusterShared,
}

impl ResourceScope {
  /// Stable string encoding shared by storage backends and wire mappings.
  ///
  /// This is the single source of truth for the `"local"` / `"shared"`
  /// spellings; decode with [`ResourceScope::from_str`].
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::NodeLocal => "local",
      Self::ClusterShared => "shared",
    }
  }
}

impl std::fmt::Display for ResourceScope {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.write_str(self.as_str())
  }
}

/// Error returned when decoding an unknown scope string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown resource scope '{0}', expected 'shared' or 'local'")]
pub struct UnknownResourceScope(pub String);

impl std::str::FromStr for ResourceScope {
  type Err = UnknownResourceScope;

  fn from_str(raw: &str) -> Result<Self, Self::Err> {
    match raw {
      "local" => Ok(Self::NodeLocal),
      "shared" => Ok(Self::ClusterShared),
      other => Err(UnknownResourceScope(other.to_string())),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn scope_string_round_trip() {
    for scope in [ResourceScope::NodeLocal, ResourceScope::ClusterShared] {
      let encoded = scope.as_str();
      assert_eq!(encoded.parse::<ResourceScope>(), Ok(scope));
      assert_eq!(scope.to_string(), encoded);
    }
  }

  #[test]
  fn unknown_scope_is_rejected() {
    assert_eq!(
      "cluster".parse::<ResourceScope>(),
      Err(UnknownResourceScope("cluster".to_string()))
    );
    assert!("".parse::<ResourceScope>().is_err());
  }
}
