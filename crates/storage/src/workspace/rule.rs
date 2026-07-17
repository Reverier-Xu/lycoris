use redb::TableDefinition;

use crate::{
  bytes::Bytes,
  workspace::{
    vcs::GitContentStore,
    versioned::{RedbVersionedStorage, VersionedResource},
  },
};

/// redb table holding rule metadata records.
pub(crate) const RULES: TableDefinition<&str, Bytes> = TableDefinition::new("rules");

/// Persistent metadata for a reusable rule.
pub type RuleRecord = VersionedResource;

/// Storage for rule metadata.
pub use super::versioned::VersionedStorage as RuleStorage;

/// redb-backed implementation of `RuleStorage`.
pub type RedbRuleStorage = RedbVersionedStorage;

/// Git-backed content store for rule bodies.
pub type RuleContentStore = GitContentStore;
