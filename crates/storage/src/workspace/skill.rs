use redb::TableDefinition;

use crate::{
  bytes::Bytes,
  workspace::{
    vcs::GitContentStore,
    versioned::{RedbVersionedStorage, VersionedResource},
  },
};

/// redb table holding skill metadata records.
pub(crate) const SKILLS: TableDefinition<&str, Bytes> = TableDefinition::new("skills");

/// Persistent metadata for a reusable skill.
pub type SkillRecord = VersionedResource;

/// Storage for skill metadata.
pub use super::versioned::VersionedStorage as SkillStorage;

/// redb-backed implementation of `SkillStorage`.
pub type RedbSkillStorage = RedbVersionedStorage;

/// Git-backed content store for skill bodies.
pub type SkillContentStore = GitContentStore;
