//! Versioned record utilities.
//!
//! Provides a shared model for resources that participate in anti-entropy
//! synchronization. The helper functions capture the conflict-resolution rules
//! that will later drive `apply_remote` across all resource kinds.

use lycoris_core::ResourceScope;

/// A record that carries the fields needed for versioned conflict resolution.
pub trait VersionedRecord {
  /// Logical version used as the primary tiebreaker.
  fn version(&self) -> u64;

  /// Wall-clock update timestamp used as a secondary tiebreaker.
  fn updated_at_ms(&self) -> i64;

  /// Resource scope (shared resources participate in cluster sync).
  fn scope(&self) -> ResourceScope;
}

/// Determine whether a remote record should overwrite a local one.
///
/// Local `NodeLocal` resources are never overwritten by remote shared
/// resources. Otherwise, the higher version wins; if versions are equal, the
/// more recent `updated_at_ms` wins.
pub fn should_apply_versioned(
  local: Option<&dyn VersionedRecord>, remote: &dyn VersionedRecord,
) -> bool {
  if remote.scope() != ResourceScope::ClusterShared {
    return false;
  }
  match local {
    None => true,
    Some(local) if local.scope() == ResourceScope::NodeLocal => false,
    Some(local) if remote.version() != local.version() => remote.version() > local.version(),
    Some(local) => remote.updated_at_ms() > local.updated_at_ms(),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[derive(Debug, Clone)]
  struct TestRecord {
    version: u64,
    updated_at_ms: i64,
    scope: ResourceScope,
  }

  impl VersionedRecord for TestRecord {
    fn version(&self) -> u64 {
      self.version
    }

    fn updated_at_ms(&self) -> i64 {
      self.updated_at_ms
    }

    fn scope(&self) -> ResourceScope {
      self.scope
    }
  }

  fn record(version: u64, updated_at_ms: i64, scope: ResourceScope) -> TestRecord {
    TestRecord {
      version,
      updated_at_ms,
      scope,
    }
  }

  #[test]
  fn applies_when_local_missing() {
    let remote = record(1, 100, ResourceScope::ClusterShared);
    assert!(should_apply_versioned(None, &remote));
  }

  #[test]
  fn does_not_apply_when_remote_local() {
    let remote = record(2, 200, ResourceScope::NodeLocal);
    let local = record(1, 100, ResourceScope::ClusterShared);
    assert!(!should_apply_versioned(Some(&local), &remote));
  }

  #[test]
  fn does_not_apply_when_local_nodelocal() {
    let remote = record(2, 200, ResourceScope::ClusterShared);
    let local = record(1, 100, ResourceScope::NodeLocal);
    assert!(!should_apply_versioned(Some(&local), &remote));
  }

  #[test]
  fn applies_when_remote_version_higher() {
    let remote = record(2, 50, ResourceScope::ClusterShared);
    let local = record(1, 100, ResourceScope::ClusterShared);
    assert!(should_apply_versioned(Some(&local), &remote));
  }

  #[test]
  fn does_not_apply_when_remote_version_lower() {
    let remote = record(1, 200, ResourceScope::ClusterShared);
    let local = record(2, 100, ResourceScope::ClusterShared);
    assert!(!should_apply_versioned(Some(&local), &remote));
  }

  #[test]
  fn applies_when_version_equal_and_remote_newer() {
    let remote = record(1, 200, ResourceScope::ClusterShared);
    let local = record(1, 100, ResourceScope::ClusterShared);
    assert!(should_apply_versioned(Some(&local), &remote));
  }

  #[test]
  fn does_not_apply_when_version_equal_and_remote_older() {
    let remote = record(1, 50, ResourceScope::ClusterShared);
    let local = record(1, 100, ResourceScope::ClusterShared);
    assert!(!should_apply_versioned(Some(&local), &remote));
  }
}
