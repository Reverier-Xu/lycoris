use std::sync::Arc;

use redb::{Database, TableDefinition};

use crate::{
  bytes::Bytes,
  workspace::{
    vcs::ContentStore,
    versioned::{RedbVersionedStorage, VersionedResource},
  },
};

const RULES: TableDefinition<&str, Bytes> = TableDefinition::new("rules");

/// Persistent metadata for a reusable rule.
pub type RuleRecord = VersionedResource;

/// Storage for rule metadata.
pub use super::versioned::VersionedStorage as RuleStorage;

/// redb-backed implementation of `RuleStorage`.
pub type RedbRuleStorage = RedbVersionedStorage;

/// Git-backed content store for rule bodies.
pub type RuleContentStore = ContentStore;

/// Create a redb-backed rule metadata storage.
pub(crate) fn new_rule_storage(db: Arc<Database>) -> RedbRuleStorage {
  RedbVersionedStorage::new(db, RULES)
}
