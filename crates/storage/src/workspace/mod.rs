//! Workspace and stability storage.
//!
//! This domain is responsible for:
//! - Workspace metadata and configuration.
//! - Reusable skills and rules, scoped as either cluster-shared or node-local.
//!
//! Skill and rule *content* is stored on the filesystem; redb only holds the
//! metadata and content hashes required for lookup and synchronization.

use std::{path::PathBuf, sync::Arc};

use redb::Database;

pub mod rule;
pub mod skill;
pub mod store;
pub mod vcs;
pub mod versioned;

pub use rule::{RedbRuleStorage, RuleContentStore, RuleRecord, RuleStorage};
pub use skill::{RedbSkillStorage, SkillContentStore, SkillRecord, SkillStorage};
pub use store::{RedbWorkspaceStorage, WorkspaceMetadataStorage, WorkspaceRecord};
pub use vcs::VersionedContentStore;
pub use versioned::VersionedResource;

/// Errors that can occur in workspace storage backends.
#[derive(Debug, thiserror::Error)]
pub enum WorkspaceStorageError {
  #[error("storage error: {0}")]
  Storage(#[from] crate::StorageError),
  #[error("content hash mismatch")]
  HashMismatch(#[from] crate::versioned::ContentHashMismatch),
  #[error("git command failed: {0}")]
  GitCommandFailed(String),
  /// A resource id that is not safe to use as a content-store file name.
  #[error("invalid resource id: {0:?}")]
  InvalidResourceId(String),
}

impl From<std::io::Error> for WorkspaceStorageError {
  fn from(error: std::io::Error) -> Self {
    Self::Storage(crate::StorageError::Io(error))
  }
}

impl From<std::string::FromUtf8Error> for WorkspaceStorageError {
  fn from(error: std::string::FromUtf8Error) -> Self {
    Self::GitCommandFailed(format!("invalid utf-8 in git output: {error}"))
  }
}

/// Workspace storage facade.
///
/// Provides access to workspace metadata, skills, and rules. The underlying
/// redb database is shared with the other storage domains via an `Arc`.
#[derive(Debug, Clone)]
pub struct WorkspaceDomain {
  workspaces: Arc<dyn WorkspaceMetadataStorage>,
  skills: Arc<dyn SkillStorage>,
  rules: Arc<dyn RuleStorage>,
  skill_content: SkillContentStore,
  rule_content: RuleContentStore,
}

impl WorkspaceDomain {
  pub(crate) fn new(db: Arc<Database>, data_dir: PathBuf) -> Self {
    Self {
      workspaces: Arc::new(RedbWorkspaceStorage::new(db.clone(), store::WORKSPACES)),
      skills: Arc::new(RedbSkillStorage::new(db.clone(), skill::SKILLS)),
      rules: Arc::new(RedbRuleStorage::new(db, rule::RULES)),
      skill_content: SkillContentStore::new(data_dir.join("skills")),
      rule_content: RuleContentStore::new(data_dir.join("rules")),
    }
  }

  /// Access workspace metadata storage.
  pub fn workspaces(&self) -> &dyn WorkspaceMetadataStorage {
    self.workspaces.as_ref()
  }

  /// Access skill metadata storage.
  pub fn skills(&self) -> &dyn SkillStorage {
    self.skills.as_ref()
  }

  /// Access rule metadata storage.
  pub fn rules(&self) -> &dyn RuleStorage {
    self.rules.as_ref()
  }

  /// Access the filesystem-backed skill content store.
  pub fn skill_content(&self) -> &SkillContentStore {
    &self.skill_content
  }

  /// Access the filesystem-backed rule content store.
  pub fn rule_content(&self) -> &RuleContentStore {
    &self.rule_content
  }

  /// Apply a remote skill if it wins the version/scope conflict check.
  ///
  /// Returns `true` when the skill was stored, `false` when it was skipped.
  pub fn apply_remote_skill(
    &self, record: VersionedResource, content: &str,
  ) -> Result<bool, WorkspaceStorageError> {
    apply_remote_resource(&record, content, self.skills(), self.skill_content())
  }

  /// Apply a remote rule if it wins the version/scope conflict check.
  ///
  /// Returns `true` when the rule was stored, `false` when it was skipped.
  pub fn apply_remote_rule(
    &self, record: VersionedResource, content: &str,
  ) -> Result<bool, WorkspaceStorageError> {
    apply_remote_resource(&record, content, self.rules(), self.rule_content())
  }

  /// Apply a remote workspace if it wins the version/scope conflict check.
  ///
  /// Returns `true` when the workspace was stored, `false` when it was skipped.
  pub fn apply_remote_workspace(
    &self, record: WorkspaceRecord,
  ) -> Result<bool, WorkspaceStorageError> {
    crate::versioned::verify_content_hash(&record.compute_content_hash()?, &record.content_hash)?;
    let local = self.workspaces().get(&record.id)?;
    if !crate::versioned::should_apply_versioned(local.as_ref(), &record) {
      return Ok(false);
    }
    self.workspaces().upsert(&record)?;
    Ok(true)
  }
}

/// Shared apply pipeline for content-backed versioned resources (skills and
/// rules): verify content integrity, win the version/scope conflict check,
/// then persist the metadata record and rewrite the content only when it
/// actually changed.
fn apply_remote_resource(
  record: &VersionedResource, content: &str, storage: &dyn versioned::VersionedStorage,
  content_store: &dyn VersionedContentStore,
) -> Result<bool, WorkspaceStorageError> {
  if content.is_empty() {
    return Ok(false);
  }
  crate::versioned::verify_content_hash(
    &crate::hash_content(content.as_bytes()),
    &record.content_hash,
  )?;
  let local = storage.get(&record.id)?;
  if !crate::versioned::should_apply_versioned(local.as_ref(), record) {
    return Ok(false);
  }
  storage.upsert(record)?;
  if local
    .as_ref()
    .is_none_or(|local| local.content_hash != record.content_hash)
  {
    content_store.write(&record.id, content, &record.content_hash)?;
  }
  Ok(true)
}

#[cfg(test)]
mod tests {
  use lycoris_core::{ResourceScope, now_ms};
  use tempfile::TempDir;

  use super::*;
  use crate::Storage;

  fn test_domain() -> (TempDir, WorkspaceDomain) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("workspace.redb")).unwrap();
    (dir, storage.workspace().clone())
  }

  fn workspace_record(id: &str, scope: ResourceScope) -> WorkspaceRecord {
    WorkspaceRecord {
      id: id.to_string(),
      root: PathBuf::from(format!("/tmp/{id}")),
      session_ids: vec![],
      metadata: [("project".to_string(), "lycoris".to_string())]
        .into_iter()
        .collect(),
      scope,
      source_node_id: None,
      version: 1,
      content_hash: String::new(),
      created_at_ms: now_ms(),
      updated_at_ms: now_ms(),
    }
  }

  fn skill_record(id: &str, scope: ResourceScope) -> SkillRecord {
    SkillRecord {
      id: id.to_string(),
      name: format!("skill-{id}"),
      version: 1,
      content_hash: format!("hash-{id}"),
      scope,
      source_node_id: None,
      updated_at_ms: now_ms(),
      metadata: [("lang".to_string(), "rust".to_string())]
        .into_iter()
        .collect(),
    }
  }

  fn rule_record(id: &str, scope: ResourceScope) -> RuleRecord {
    RuleRecord {
      id: id.to_string(),
      name: format!("rule-{id}"),
      version: 1,
      content_hash: format!("hash-{id}"),
      scope,
      source_node_id: None,
      updated_at_ms: now_ms(),
      metadata: [("severity".to_string(), "high".to_string())]
        .into_iter()
        .collect(),
    }
  }

  #[test]
  fn workspace_round_trip() {
    let (_dir, domain) = test_domain();
    let record = workspace_record("ws-1", ResourceScope::NodeLocal);

    domain.workspaces().upsert(&record).unwrap();
    let loaded = domain.workspaces().get("ws-1").unwrap().unwrap();

    assert_eq!(loaded.id, "ws-1");
    assert_eq!(loaded.root, PathBuf::from("/tmp/ws-1"));
    assert_eq!(loaded.metadata.get("project"), Some(&"lycoris".to_string()));
    assert_eq!(loaded.scope, ResourceScope::NodeLocal);
    assert!(!loaded.content_hash.is_empty());
  }

  #[test]
  fn workspace_list_and_delete() {
    let (_dir, domain) = test_domain();
    domain
      .workspaces()
      .upsert(&workspace_record("ws-a", ResourceScope::NodeLocal))
      .unwrap();
    domain
      .workspaces()
      .upsert(&workspace_record("ws-b", ResourceScope::ClusterShared))
      .unwrap();

    let list = domain.workspaces().list().unwrap();
    assert_eq!(list.len(), 2);

    domain.workspaces().delete("ws-a").unwrap();
    let list = domain.workspaces().list().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, "ws-b");
  }

  #[test]
  fn workspace_scope_filtering() {
    let (_dir, domain) = test_domain();
    domain
      .workspaces()
      .upsert(&workspace_record("shared-ws", ResourceScope::ClusterShared))
      .unwrap();
    domain
      .workspaces()
      .upsert(&workspace_record("local-ws", ResourceScope::NodeLocal))
      .unwrap();

    let shared = domain.workspaces().list_shared().unwrap();
    assert_eq!(shared.len(), 1);
    assert_eq!(shared[0].id, "shared-ws");

    let local = domain.workspaces().list_local().unwrap();
    assert_eq!(local.len(), 1);
    assert_eq!(local[0].id, "local-ws");
  }

  #[test]
  fn skill_scope_filtering() {
    let (_dir, domain) = test_domain();
    domain
      .skills()
      .upsert(&skill_record("shared-skill", ResourceScope::ClusterShared))
      .unwrap();
    domain
      .skills()
      .upsert(&skill_record("local-skill", ResourceScope::NodeLocal))
      .unwrap();

    let shared = domain.skills().list_shared().unwrap();
    assert_eq!(shared.len(), 1);
    assert_eq!(shared[0].id, "shared-skill");

    let local = domain.skills().list_local().unwrap();
    assert_eq!(local.len(), 1);
    assert_eq!(local[0].id, "local-skill");
  }

  #[test]
  fn rule_scope_filtering() {
    let (_dir, domain) = test_domain();
    domain
      .rules()
      .upsert(&rule_record("shared-rule", ResourceScope::ClusterShared))
      .unwrap();
    domain
      .rules()
      .upsert(&rule_record("local-rule", ResourceScope::NodeLocal))
      .unwrap();

    let shared = domain.rules().list_shared().unwrap();
    assert_eq!(shared.len(), 1);
    assert_eq!(shared[0].id, "shared-rule");

    let local = domain.rules().list_local().unwrap();
    assert_eq!(local.len(), 1);
    assert_eq!(local[0].id, "local-rule");
  }

  #[test]
  fn skill_content_round_trip() {
    let (_dir, domain) = test_domain();
    domain
      .skill_content()
      .write("skill-1", "name = 'example'", "initial")
      .unwrap();

    let content = domain.skill_content().read("skill-1").unwrap().unwrap();
    assert_eq!(content, "name = 'example'");
  }

  #[test]
  fn rule_content_round_trip() {
    let (_dir, domain) = test_domain();
    domain
      .rule_content()
      .write("rule-1", "match = 'always'", "initial")
      .unwrap();

    let content = domain.rule_content().read("rule-1").unwrap().unwrap();
    assert_eq!(content, "match = 'always'");
  }

  fn shared_skill(id: &str, content: &str, version: u64) -> VersionedResource {
    let mut record = skill_record(id, ResourceScope::ClusterShared);
    record.content_hash = crate::hash_content(content.as_bytes());
    record.version = version;
    record
  }

  fn shared_rule(id: &str, content: &str, version: u64) -> VersionedResource {
    let mut record = rule_record(id, ResourceScope::ClusterShared);
    record.content_hash = crate::hash_content(content.as_bytes());
    record.version = version;
    record
  }

  #[test]
  fn apply_remote_skill_stores_new_skill_and_content() {
    let (_dir, domain) = test_domain();
    let content = "name = 'remote-skill'";
    let record = shared_skill("remote-skill", content, 1);

    let applied = domain.apply_remote_skill(record, content).unwrap();
    assert!(applied);

    let loaded = domain.skills().get("remote-skill").unwrap().unwrap();
    assert_eq!(loaded.version, 1);
    assert_eq!(
      domain
        .skill_content()
        .read("remote-skill")
        .unwrap()
        .unwrap(),
      content
    );
  }

  #[test]
  fn apply_remote_skill_skips_older_version() {
    let (_dir, domain) = test_domain();
    let local_content = "local skill";
    let local_record = shared_skill("skill-conflict", local_content, 2);
    domain.skills().upsert(&local_record).unwrap();
    domain
      .skill_content()
      .write("skill-conflict", local_content, &local_record.content_hash)
      .unwrap();

    let remote_content = "remote skill";
    let remote_record = shared_skill("skill-conflict", remote_content, 1);

    let applied = domain
      .apply_remote_skill(remote_record, remote_content)
      .unwrap();
    assert!(!applied);

    let loaded = domain
      .skill_content()
      .read("skill-conflict")
      .unwrap()
      .unwrap();
    assert_eq!(loaded, local_content);
  }

  #[test]
  fn apply_remote_skill_does_not_rewrite_unchanged_content() {
    let (_dir, domain) = test_domain();
    let content = "stable skill";
    let local = shared_skill("skill-stable", content, 1);
    domain.skills().upsert(&local).unwrap();
    domain
      .skill_content()
      .write("skill-stable", content, &local.content_hash)
      .unwrap();

    let mut remote = shared_skill("skill-stable", content, 2);
    remote.updated_at_ms = local.updated_at_ms + 1;

    let applied = domain.apply_remote_skill(remote, content).unwrap();
    assert!(applied);
    assert_eq!(
      domain
        .skill_content()
        .read("skill-stable")
        .unwrap()
        .unwrap(),
      content
    );
    let loaded = domain.skills().get("skill-stable").unwrap().unwrap();
    assert_eq!(loaded.version, 2);
  }

  #[test]
  fn apply_remote_skill_rejects_hash_mismatch() {
    let (_dir, domain) = test_domain();
    let content = "real skill";
    let mut record = shared_skill("skill-hash", content, 1);
    record.content_hash = "wrong-hash".to_string();

    let error = domain.apply_remote_skill(record, content).unwrap_err();
    assert!(error.to_string().contains("content hash mismatch"));
  }

  #[test]
  fn apply_remote_skill_rejects_empty_content() {
    let (_dir, domain) = test_domain();
    let record = shared_skill("skill-empty", "", 1);

    let applied = domain.apply_remote_skill(record, "").unwrap();
    assert!(!applied);
  }

  #[test]
  fn apply_remote_rule_stores_new_rule_and_content() {
    let (_dir, domain) = test_domain();
    let content = "match = 'remote-rule'";
    let record = shared_rule("remote-rule", content, 1);

    let applied = domain.apply_remote_rule(record, content).unwrap();
    assert!(applied);

    let loaded = domain.rules().get("remote-rule").unwrap().unwrap();
    assert_eq!(loaded.version, 1);
    assert_eq!(
      domain.rule_content().read("remote-rule").unwrap().unwrap(),
      content
    );
  }

  #[test]
  fn apply_remote_rule_skips_local_scope() {
    let (_dir, domain) = test_domain();
    let content = "local rule";
    let mut record = rule_record("rule-local", ResourceScope::NodeLocal);
    record.content_hash = crate::hash_content(content.as_bytes());
    record.version = 1;

    let applied = domain.apply_remote_rule(record, content).unwrap();
    assert!(!applied);
  }

  #[test]
  fn apply_remote_workspace_stores_new_shared_workspace() {
    let (_dir, domain) = test_domain();
    let mut record = workspace_record("remote-ws", ResourceScope::ClusterShared);
    record.content_hash = record.compute_content_hash().unwrap();

    let applied = domain.apply_remote_workspace(record.clone()).unwrap();
    assert!(applied);

    let loaded = domain.workspaces().get("remote-ws").unwrap().unwrap();
    assert_eq!(loaded.version, record.version);
  }

  #[test]
  fn apply_remote_workspace_skips_older_version() {
    let (_dir, domain) = test_domain();
    let mut local = workspace_record("ws-conflict", ResourceScope::ClusterShared);
    local.version = 2;
    local.content_hash = local.compute_content_hash().unwrap();
    domain.workspaces().upsert(&local).unwrap();

    let mut remote = workspace_record("ws-conflict", ResourceScope::ClusterShared);
    remote.version = 1;
    remote.content_hash = remote.compute_content_hash().unwrap();

    let applied = domain.apply_remote_workspace(remote).unwrap();
    assert!(!applied);

    let loaded = domain.workspaces().get("ws-conflict").unwrap().unwrap();
    assert_eq!(loaded.version, 2);
  }

  #[test]
  fn apply_remote_workspace_rejects_hash_mismatch() {
    let (_dir, domain) = test_domain();
    let mut record = workspace_record("ws-hash", ResourceScope::ClusterShared);
    record.content_hash = "wrong-hash".to_string();

    let error = domain.apply_remote_workspace(record).unwrap_err();
    assert!(error.to_string().contains("content hash mismatch"));
  }
}
