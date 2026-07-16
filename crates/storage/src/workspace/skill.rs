use std::sync::Arc;

use redb::{Database, TableDefinition};

use crate::{
  bytes::Bytes,
  workspace::{
    vcs::GitContentStore,
    versioned::{RedbVersionedStorage, VersionedResource},
  },
};

const SKILLS: TableDefinition<&str, Bytes> = TableDefinition::new("skills");

/// Persistent metadata for a reusable skill.
pub type SkillRecord = VersionedResource;

/// Storage for skill metadata.
pub use super::versioned::VersionedStorage as SkillStorage;

/// redb-backed implementation of `SkillStorage`.
pub type RedbSkillStorage = RedbVersionedStorage;

/// Git-backed content store for skill bodies.
pub type SkillContentStore = GitContentStore;

/// Create a redb-backed skill metadata storage.
pub(crate) fn new_skill_storage(db: Arc<Database>) -> RedbSkillStorage {
  RedbVersionedStorage::new(db, SKILLS)
}
