//! Versioned record utilities.
//!
//! Provides a shared model for resources that participate in anti-entropy
//! synchronization. The helper functions capture the integrity-verification
//! and conflict-resolution rules that drive `apply_remote` across all
//! resource kinds.

use lycoris_core::ResourceScope;

/// A record that carries the fields needed for versioned conflict resolution.
pub trait VersionedRecord {
  /// Logical version used as the primary tiebreaker.
  fn version(&self) -> u64;

  /// Wall-clock update timestamp used as a secondary tiebreaker.
  fn updated_at_ms(&self) -> i64;

  /// Content hash used as the final tiebreaker when version and timestamp
  /// are equal, so concurrent writes of the same version converge.
  fn content_hash(&self) -> &str;

  /// Resource scope (shared resources participate in cluster sync).
  fn scope(&self) -> ResourceScope;
}

/// Error returned when remote content fails integrity verification.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("content hash mismatch")]
pub struct ContentHashMismatch;

/// Verify that the freshly computed `actual_hash` matches the `expected_hash`
/// declared by a remote record.
pub(crate) fn verify_content_hash(
  actual_hash: &str, expected_hash: &str,
) -> Result<(), ContentHashMismatch> {
  if actual_hash != expected_hash {
    return Err(ContentHashMismatch);
  }
  Ok(())
}

/// Determine whether a remote record should overwrite a local one.
///
/// Local `NodeLocal` resources are never overwritten by remote shared
/// resources. Otherwise, the higher version wins; if versions are equal, the
/// more recent `updated_at_ms` wins; if both are equal, the greater
/// `content_hash` wins so concurrent writes of the same version converge
/// deterministically. A fully identical record is rejected, keeping repeated
/// applications idempotent.
pub fn should_apply_versioned<R: VersionedRecord>(local: Option<&R>, remote: &R) -> bool {
  if remote.scope() != ResourceScope::ClusterShared {
    return false;
  }
  match local {
    None => true,
    Some(local) if local.scope() == ResourceScope::NodeLocal => false,
    Some(local) if remote.version() != local.version() => remote.version() > local.version(),
    Some(local) if remote.updated_at_ms() != local.updated_at_ms() => {
      remote.updated_at_ms() > local.updated_at_ms()
    }
    Some(local) => remote.content_hash() > local.content_hash(),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[derive(Debug, Clone)]
  struct TestRecord {
    version: u64,
    updated_at_ms: i64,
    content_hash: String,
    scope: ResourceScope,
  }

  impl VersionedRecord for TestRecord {
    fn version(&self) -> u64 {
      self.version
    }

    fn updated_at_ms(&self) -> i64 {
      self.updated_at_ms
    }

    fn content_hash(&self) -> &str {
      &self.content_hash
    }

    fn scope(&self) -> ResourceScope {
      self.scope
    }
  }

  fn record(version: u64, updated_at_ms: i64, scope: ResourceScope) -> TestRecord {
    record_with_hash(version, updated_at_ms, "hash", scope)
  }

  fn record_with_hash(
    version: u64, updated_at_ms: i64, content_hash: &str, scope: ResourceScope,
  ) -> TestRecord {
    TestRecord {
      version,
      updated_at_ms,
      content_hash: content_hash.to_string(),
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

  #[test]
  fn applies_when_version_and_timestamp_equal_and_remote_hash_greater() {
    let remote = record_with_hash(1, 100, "bbb", ResourceScope::ClusterShared);
    let local = record_with_hash(1, 100, "aaa", ResourceScope::ClusterShared);
    assert!(should_apply_versioned(Some(&local), &remote));
  }

  #[test]
  fn does_not_apply_when_version_and_timestamp_equal_and_remote_hash_smaller() {
    let remote = record_with_hash(1, 100, "aaa", ResourceScope::ClusterShared);
    let local = record_with_hash(1, 100, "bbb", ResourceScope::ClusterShared);
    assert!(!should_apply_versioned(Some(&local), &remote));
  }

  #[test]
  fn does_not_apply_identical_record() {
    let remote = record_with_hash(1, 100, "hash", ResourceScope::ClusterShared);
    let local = record_with_hash(1, 100, "hash", ResourceScope::ClusterShared);
    assert!(!should_apply_versioned(Some(&local), &remote));
  }
}
