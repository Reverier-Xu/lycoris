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
use serde::{Deserialize, Serialize};

use crate::StorageError;

pub mod rule;
pub mod skill;
pub mod store;
pub mod vcs;

pub use rule::{RedbRuleStorage, RuleContentStore, RuleRecord, RuleStorage};
pub use skill::{RedbSkillStorage, SkillContentStore, SkillRecord, SkillStorage};
pub use store::{RedbWorkspaceStorage, WorkspaceMetadataStorage, WorkspaceRecord};
pub use vcs::{PijulContentStore, VersionInfo, VersionedContentStore, WorkspaceStorageError};

/// Visibility scope for a reusable resource.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceScope {
  /// Visible only on the node where it was created; never synchronized.
  NodeLocal,
  /// Synchronized across the cluster via the resource anti-entropy protocol.
  ClusterShared,
}

/// A workspace descriptor.
///
/// This is a lightweight convenience wrapper around `WorkspaceRecord` used by
/// callers that do not need the full record type.
#[derive(Debug, Clone)]
pub struct Workspace {
  pub id: String,
  pub root: PathBuf,
  pub metadata: std::collections::HashMap<String, String>,
}

impl From<WorkspaceRecord> for Workspace {
  fn from(record: WorkspaceRecord) -> Self {
    Self {
      id: record.id,
      root: record.root,
      metadata: record.metadata,
    }
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
  pub(crate) fn new(db: Arc<Database>, data_dir: PathBuf) -> Result<Self, StorageError> {
    std::fs::create_dir_all(&data_dir)?;

    Ok(Self {
      workspaces: Arc::new(RedbWorkspaceStorage::new(db.clone())),
      skills: Arc::new(RedbSkillStorage::new(db.clone())),
      rules: Arc::new(RedbRuleStorage::new(db)),
      skill_content: SkillContentStore::new(data_dir.join("skills.pijul")),
      rule_content: RuleContentStore::new(data_dir.join("rules.pijul")),
    })
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
}

#[cfg(test)]
mod tests {
  use lycoris_config::time::now_ms;
  use tempfile::TempDir;

  use super::*;
  use crate::Storage;

  fn test_domain() -> (TempDir, WorkspaceDomain) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("workspace.redb")).unwrap();
    (dir, storage.workspace().clone())
  }

  fn workspace_record(id: &str) -> WorkspaceRecord {
    WorkspaceRecord {
      id: id.to_string(),
      root: PathBuf::from(format!("/tmp/{id}")),
      session_ids: vec![],
      metadata: [("project".to_string(), "lycoris".to_string())]
        .into_iter()
        .collect(),
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
    let record = workspace_record("ws-1");

    domain.workspaces().upsert(&record).unwrap();
    let loaded = domain.workspaces().get("ws-1").unwrap().unwrap();

    assert_eq!(loaded.id, "ws-1");
    assert_eq!(loaded.root, PathBuf::from("/tmp/ws-1"));
    assert_eq!(loaded.metadata.get("project"), Some(&"lycoris".to_string()));
  }

  #[test]
  fn workspace_list_and_delete() {
    let (_dir, domain) = test_domain();
    domain
      .workspaces()
      .upsert(&workspace_record("ws-a"))
      .unwrap();
    domain
      .workspaces()
      .upsert(&workspace_record("ws-b"))
      .unwrap();

    let list = domain.workspaces().list().unwrap();
    assert_eq!(list.len(), 2);

    domain.workspaces().delete("ws-a").unwrap();
    let list = domain.workspaces().list().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, "ws-b");
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
}
