#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod agent;
pub mod bytes;
pub mod error;
pub mod node;
pub mod workspace;

use std::{path::Path, sync::Arc};

pub use agent::{
  AgentDomain, AgentStorageError, DEFAULT_EMBEDDING_DIM, MemoryEntry, MemoryStorage, Session,
  SessionStorage,
};
pub use error::StorageError;
pub use node::{
  ClusterNodeRecord, ClusterNodeStorage, LocalNode, LocalStorage, NodeDomain, NodeRegistry,
  NodeState, PeerRecord, PeerStorage,
};
use redb::Database;
use tokio::sync::Notify;
pub use workspace::{
  RedbRuleStorage, RedbSkillStorage, RedbWorkspaceStorage, ResourceScope, RuleContentStore,
  RuleRecord, RuleStorage, SkillContentStore, SkillRecord, SkillStorage, Workspace,
  WorkspaceDomain, WorkspaceMetadataStorage, WorkspaceRecord, WorkspaceStorageError,
};

/// Unified storage facade.
///
/// `Storage` is the top-level entry point for all persistent state. It owns a
/// single `redb::Database` and hands out lightweight, cloneable domain handles
/// for node-local state, agent orchestration state, and workspace state.
#[derive(Debug, Clone)]
pub struct Storage {
  db: Arc<Database>,
  notify: Arc<Notify>,
  agent: AgentDomain,
  workspace: WorkspaceDomain,
}

impl Storage {
  /// Open or create the storage at `db_path`.
  ///
  /// The workspace domain stores skill/rule content in a subdirectory of the
  /// database's parent directory. To place content elsewhere, use
  /// [`Storage::open_with_data_dir`].
  pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self, StorageError> {
    let db_path = db_path.as_ref().to_path_buf();
    let data_dir = db_path
      .parent()
      .map(std::path::Path::to_path_buf)
      .unwrap_or_else(|| std::path::PathBuf::from("."));
    Self::open_with_data_dir(&db_path, data_dir)
  }

  /// Open or create the storage at `db_path`, storing domain files in
  /// `data_dir`.
  pub fn open_with_data_dir<P: AsRef<Path>, Q: AsRef<Path>>(
    db_path: P, data_dir: Q,
  ) -> Result<Self, StorageError> {
    let db_path = db_path.as_ref().to_path_buf();
    let data_dir = data_dir.as_ref().to_path_buf();

    if let Some(parent) = db_path.parent() {
      std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir_all(&data_dir)?;

    let db = Database::create(&db_path).map_err(crate::error::redb_err)?;
    let db = Arc::new(db);

    Ok(Self {
      db: db.clone(),
      notify: Arc::new(Notify::new()),
      agent: AgentDomain::new(db.clone(), data_dir.clone()),
      workspace: WorkspaceDomain::new(db, data_dir)?,
    })
  }

  /// Access the node-local storage domain.
  pub fn node(&self) -> NodeDomain {
    NodeDomain::new(self.db.clone(), self.notify.clone())
  }

  /// Access the agent orchestration storage domain.
  pub fn agent(&self) -> AgentDomain {
    self.agent.clone()
  }

  /// Access the workspace storage domain.
  pub fn workspace(&self) -> &WorkspaceDomain {
    &self.workspace
  }

  /// Build a sync manifest for the local node.
  ///
  /// The manifest contains:
  /// - Cluster-shared skills and rules (to be replicated).
  /// - Node-local skills and rules (for scheduling visibility only).
  /// - Session titles and their hosting node (for dispatch decisions).
  ///
  /// Memory contents and full session histories are intentionally excluded.
  pub fn sync_manifest(&self, node_id: &str) -> Result<ResourceSyncManifest, StorageError> {
    let shared_skills = self
      .workspace
      .skills()
      .list_shared()
      .map_err(|error| StorageError::Workspace(error.to_string()))?;
    let shared_rules = self
      .workspace
      .rules()
      .list_shared()
      .map_err(|error| StorageError::Workspace(error.to_string()))?;
    let local_skills = self
      .workspace
      .skills()
      .list_local()
      .map_err(|error| StorageError::Workspace(error.to_string()))?;
    let local_rules = self
      .workspace
      .rules()
      .list_local()
      .map_err(|error| StorageError::Workspace(error.to_string()))?;

    let sessions = self
      .agent
      .sessions()
      .list()
      .map_err(|error| StorageError::Agent(error.to_string()))?
      .into_iter()
      .map(|session| SessionHostRecord {
        id: session.id,
        title: session.metadata.get("title").cloned(),
        host_node_id: session.metadata.get("host_node_id").cloned(),
      })
      .collect();

    Ok(ResourceSyncManifest {
      node_id: node_id.to_string(),
      shared_skills,
      shared_rules,
      local_skills,
      local_rules,
      sessions,
    })
  }

  /// Apply a remote sync manifest with last-write-wins semantics.
  ///
  /// Only `ClusterShared` skills/rules and session title/host metadata are
  /// merged. Node-local resources from the remote node are ignored.
  pub fn apply_sync_manifest(&self, manifest: &ResourceSyncManifest) -> Result<(), StorageError> {
    for skill in &manifest.shared_skills {
      let mut merged = skill.clone();
      merged.source_node_id = Some(manifest.node_id.clone());
      if should_merge_skill(self.workspace.skills(), &merged)? {
        self
          .workspace
          .skills()
          .upsert(&merged)
          .map_err(|error| StorageError::Workspace(error.to_string()))?;
      }
    }

    for rule in &manifest.shared_rules {
      let mut merged = rule.clone();
      merged.source_node_id = Some(manifest.node_id.clone());
      if should_merge_rule(self.workspace.rules(), &merged)? {
        self
          .workspace
          .rules()
          .upsert(&merged)
          .map_err(|error| StorageError::Workspace(error.to_string()))?;
      }
    }

    for session in &manifest.sessions {
      let mut metadata = std::collections::HashMap::new();
      if let Some(title) = &session.title {
        metadata.insert("title".to_string(), title.clone());
      }
      if let Some(host) = &session.host_node_id {
        metadata.insert("host_node_id".to_string(), host.clone());
      }
      self
        .agent
        .sessions()
        .upsert(&Session {
          id: session.id.clone(),
          metadata,
        })
        .map_err(|error| StorageError::Agent(error.to_string()))?;
    }

    Ok(())
  }
}

/// A lightweight description of a session for cluster sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionHostRecord {
  pub id: String,
  pub title: Option<String>,
  pub host_node_id: Option<String>,
}

/// Manifest of resources offered by a node for cluster anti-entropy.
#[derive(Debug, Clone)]
pub struct ResourceSyncManifest {
  pub node_id: String,
  pub shared_skills: Vec<SkillRecord>,
  pub shared_rules: Vec<RuleRecord>,
  pub local_skills: Vec<SkillRecord>,
  pub local_rules: Vec<RuleRecord>,
  pub sessions: Vec<SessionHostRecord>,
}

fn should_merge_skill(
  storage: &dyn SkillStorage, incoming: &SkillRecord,
) -> Result<bool, StorageError> {
  Ok(
    match storage
      .get(&incoming.id)
      .map_err(|error| StorageError::Workspace(error.to_string()))?
    {
      Some(local) => incoming.updated_at_ms >= local.updated_at_ms,
      None => true,
    },
  )
}

fn should_merge_rule(
  storage: &dyn RuleStorage, incoming: &RuleRecord,
) -> Result<bool, StorageError> {
  Ok(
    match storage
      .get(&incoming.id)
      .map_err(|error| StorageError::Workspace(error.to_string()))?
    {
      Some(local) => incoming.updated_at_ms >= local.updated_at_ms,
      None => true,
    },
  )
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use tempfile::TempDir;

  use super::*;

  fn test_storage() -> (TempDir, Storage) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("sync.redb")).unwrap();
    (dir, storage)
  }

  fn skill(id: &str, scope: ResourceScope, updated_at_ms: i64) -> SkillRecord {
    SkillRecord {
      id: id.to_string(),
      name: id.to_string(),
      version: 1,
      content_hash: format!("hash-{id}"),
      scope,
      source_node_id: None,
      updated_at_ms,
      metadata: HashMap::new(),
    }
  }

  fn rule(id: &str, scope: ResourceScope, updated_at_ms: i64) -> RuleRecord {
    RuleRecord {
      id: id.to_string(),
      name: id.to_string(),
      version: 1,
      content_hash: format!("hash-{id}"),
      scope,
      source_node_id: None,
      updated_at_ms,
      metadata: HashMap::new(),
    }
  }

  #[test]
  fn sync_manifest_includes_shared_and_local_resources() {
    let (_dir, storage) = test_storage();
    storage
      .workspace()
      .skills()
      .upsert(&skill("shared-skill", ResourceScope::ClusterShared, 1))
      .unwrap();
    storage
      .workspace()
      .skills()
      .upsert(&skill("local-skill", ResourceScope::NodeLocal, 1))
      .unwrap();
    storage
      .workspace()
      .rules()
      .upsert(&rule("shared-rule", ResourceScope::ClusterShared, 1))
      .unwrap();

    storage
      .agent()
      .sessions()
      .create(&Session {
        id: "session-1".to_string(),
        metadata: [
          ("title".to_string(), "hello".to_string()),
          ("host_node_id".to_string(), "node-a".to_string()),
        ]
        .into_iter()
        .collect(),
      })
      .unwrap();

    let manifest = storage.sync_manifest("node-a").unwrap();
    assert_eq!(manifest.shared_skills.len(), 1);
    assert_eq!(manifest.shared_skills[0].id, "shared-skill");
    assert_eq!(manifest.local_skills.len(), 1);
    assert_eq!(manifest.shared_rules.len(), 1);
    assert_eq!(manifest.sessions.len(), 1);
    assert_eq!(manifest.sessions[0].title.as_deref(), Some("hello"));
  }

  #[test]
  fn apply_sync_manifest_merges_shared_resources() {
    let (_dir, storage) = test_storage();
    storage
      .workspace()
      .skills()
      .upsert(&skill("skill-1", ResourceScope::ClusterShared, 100))
      .unwrap();

    let remote = skill("skill-1", ResourceScope::ClusterShared, 200);
    let manifest = ResourceSyncManifest {
      node_id: "node-b".to_string(),
      shared_skills: vec![remote],
      shared_rules: vec![],
      local_skills: vec![],
      local_rules: vec![],
      sessions: vec![],
    };

    storage.apply_sync_manifest(&manifest).unwrap();
    let merged = storage
      .workspace()
      .skills()
      .get("skill-1")
      .unwrap()
      .unwrap();
    assert_eq!(merged.updated_at_ms, 200);
    assert_eq!(merged.source_node_id, Some("node-b".to_string()));
  }

  #[test]
  fn apply_sync_manifest_ignores_older_shared_resources() {
    let (_dir, storage) = test_storage();
    storage
      .workspace()
      .skills()
      .upsert(&skill("skill-1", ResourceScope::ClusterShared, 200))
      .unwrap();

    let remote = skill("skill-1", ResourceScope::ClusterShared, 100);
    let manifest = ResourceSyncManifest {
      node_id: "node-b".to_string(),
      shared_skills: vec![remote],
      shared_rules: vec![],
      local_skills: vec![],
      local_rules: vec![],
      sessions: vec![],
    };

    storage.apply_sync_manifest(&manifest).unwrap();
    let local = storage
      .workspace()
      .skills()
      .get("skill-1")
      .unwrap()
      .unwrap();
    assert_eq!(local.updated_at_ms, 200);
  }

  #[test]
  fn apply_sync_manifest_merges_session_hosts() {
    let (_dir, storage) = test_storage();
    storage
      .agent()
      .sessions()
      .create(&Session {
        id: "session-1".to_string(),
        metadata: HashMap::new(),
      })
      .unwrap();

    let manifest = ResourceSyncManifest {
      node_id: "node-b".to_string(),
      shared_skills: vec![],
      shared_rules: vec![],
      local_skills: vec![],
      local_rules: vec![],
      sessions: vec![SessionHostRecord {
        id: "session-1".to_string(),
        title: Some("remote-title".to_string()),
        host_node_id: Some("node-b".to_string()),
      }],
    };

    storage.apply_sync_manifest(&manifest).unwrap();
    let session = storage
      .agent()
      .sessions()
      .get("session-1")
      .unwrap()
      .unwrap();
    assert_eq!(
      session.metadata.get("title"),
      Some(&"remote-title".to_string())
    );
    assert_eq!(
      session.metadata.get("host_node_id"),
      Some(&"node-b".to_string())
    );
  }
}
