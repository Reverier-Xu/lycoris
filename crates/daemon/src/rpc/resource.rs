#![allow(clippy::result_large_err)]

use std::{collections::HashMap, sync::Arc};

use lycoris_core::{DEFAULT_EMBEDDING_DIM, matches_selector};
use lycoris_proto::node::{
  MemoryBody, NodeBody, Resource, ResourceKind, ResourceMetadata, RuleBody, SessionBody, SkillBody,
  WorkspaceBody, resource::Body,
};
use lycoris_storage::{
  MemoryEntry, ResourceScope, RuleRecord, Session, SkillRecord, Storage, WorkspaceRecord,
  workspace::VersionedContentStore,
};
use tonic::Status;

use crate::membership::MembershipService;

/// Maps between stored domain types and the public `Resource` proto.
#[derive(Debug, Clone)]
pub struct ResourceMapper {
  storage: Storage,
  service: Arc<MembershipService>,
}

impl ResourceMapper {
  pub fn new(storage: Storage, service: Arc<MembershipService>) -> Self {
    Self { storage, service }
  }

  pub async fn list(
    &self, kind: ResourceKind, selector: HashMap<String, String>, scope: Option<ResourceScope>,
  ) -> Result<Vec<Resource>, Status> {
    match kind {
      ResourceKind::Node => {
        let nodes = self.service.list_nodes(&selector).await;
        Ok(nodes.into_iter().map(node_to_resource).collect())
      }
      ResourceKind::Session => {
        let sessions = self
          .storage
          .agent()
          .sessions()
          .list()
          .map_err(|error| Status::internal(format!("failed to list sessions: {error}")))?;
        Ok(
          sessions
            .into_iter()
            .filter(|session| matches_selector(&session.metadata, &selector))
            .map(session_to_resource)
            .collect(),
        )
      }
      ResourceKind::Memory => {
        let entries = self
          .recall_memories(100)
          .await
          .map_err(|error| Status::internal(format!("failed to list memories: {error}")))?;
        Ok(
          entries
            .into_iter()
            .filter(|entry| matches_selector(&entry.metadata, &selector))
            .map(|entry| memory_to_resource(entry, false))
            .collect(),
        )
      }
      ResourceKind::Skill => {
        let skills = self
          .list_skills(scope)
          .map_err(|error| Status::internal(format!("failed to list skills: {error}")))?;
        Ok(
          skills
            .into_iter()
            .filter(|skill| matches_selector(&skill.metadata, &selector))
            .map(|skill| skill_to_resource(skill, None))
            .collect(),
        )
      }
      ResourceKind::Rule => {
        let rules = self
          .list_rules(scope)
          .map_err(|error| Status::internal(format!("failed to list rules: {error}")))?;
        Ok(
          rules
            .into_iter()
            .filter(|rule| matches_selector(&rule.metadata, &selector))
            .map(|rule| rule_to_resource(rule, None))
            .collect(),
        )
      }
      ResourceKind::Workspace => {
        let workspaces = self
          .storage
          .workspace()
          .workspaces()
          .list()
          .map_err(|error| Status::internal(format!("failed to list workspaces: {error}")))?;
        Ok(
          workspaces
            .into_iter()
            .filter(|workspace| matches_selector(&workspace.metadata, &selector))
            .map(workspace_to_resource)
            .collect(),
        )
      }
    }
  }

  pub async fn get(&self, kind: ResourceKind, id: &str) -> Result<Resource, Status> {
    match kind {
      ResourceKind::Node => self
        .service
        .list_nodes(&HashMap::new())
        .await
        .into_iter()
        .find(|node| node.id == id)
        .map(node_to_resource)
        .ok_or_else(|| not_found(id)),
      ResourceKind::Session => self
        .storage
        .agent()
        .sessions()
        .get(id)
        .map_err(|error| Status::internal(format!("failed to get session: {error}")))?
        .map(session_to_resource)
        .ok_or_else(|| not_found(id)),
      ResourceKind::Memory => {
        let entries = self
          .recall_memories(1000)
          .await
          .map_err(|error| Status::internal(format!("failed to get memory: {error}")))?;
        entries
          .into_iter()
          .find(|entry| entry.id == id)
          .map(|entry| memory_to_resource(entry, true))
          .ok_or_else(|| not_found(id))
      }
      ResourceKind::Skill => {
        let skill = self
          .storage
          .workspace()
          .skills()
          .get(id)
          .map_err(|error| Status::internal(format!("failed to get skill: {error}")))?;
        match skill {
          Some(skill) => {
            let content = self
              .storage
              .workspace()
              .skill_content()
              .read(id)
              .map_err(|error| {
                Status::internal(format!("failed to read skill content: {error}"))
              })?;
            Ok(skill_to_resource(skill, content))
          }
          None => Err(not_found(id)),
        }
      }
      ResourceKind::Rule => {
        let rule = self
          .storage
          .workspace()
          .rules()
          .get(id)
          .map_err(|error| Status::internal(format!("failed to get rule: {error}")))?;
        match rule {
          Some(rule) => {
            let content = self
              .storage
              .workspace()
              .rule_content()
              .read(id)
              .map_err(|error| Status::internal(format!("failed to read rule content: {error}")))?;
            Ok(rule_to_resource(rule, content))
          }
          None => Err(not_found(id)),
        }
      }
      ResourceKind::Workspace => self
        .storage
        .workspace()
        .workspaces()
        .get(id)
        .map_err(|error| Status::internal(format!("failed to get workspace: {error}")))?
        .map(workspace_to_resource)
        .ok_or_else(|| not_found(id)),
    }
  }

  fn list_skills(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<SkillRecord>, lycoris_storage::WorkspaceStorageError> {
    let skills = self.storage.workspace().skills();
    match scope {
      Some(ResourceScope::ClusterShared) => skills.list_shared(),
      Some(ResourceScope::NodeLocal) => skills.list_local(),
      None => skills.list(),
    }
  }

  fn list_rules(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<RuleRecord>, lycoris_storage::WorkspaceStorageError> {
    let rules = self.storage.workspace().rules();
    match scope {
      Some(ResourceScope::ClusterShared) => rules.list_shared(),
      Some(ResourceScope::NodeLocal) => rules.list_local(),
      None => rules.list(),
    }
  }

  async fn recall_memories(
    &self, limit: usize,
  ) -> Result<Vec<MemoryEntry>, lycoris_storage::AgentStorageError> {
    let query = vec![0.0f32; DEFAULT_EMBEDDING_DIM];
    self.storage.agent().memory().recall(query, limit).await
  }
}

fn node_to_resource(node: lycoris_proto::node::NodeInfo) -> Resource {
  Resource {
    metadata: Some(ResourceMetadata {
      id: node.id.clone(),
      name: node.id.clone(),
      kind: ResourceKind::Node as i32,
      labels: node.labels.clone(),
      annotations: node.annotations.clone(),
      scope: String::new(),
      source_node_id: String::new(),
      created_at_ms: 0,
      updated_at_ms: node.last_heartbeat_unix_ms,
    }),
    body: Some(Body::Node(NodeBody { node: Some(node) })),
  }
}

fn session_to_resource(session: Session) -> Resource {
  let title = session.metadata.get("title").cloned().unwrap_or_default();
  let host_node_id = session
    .metadata
    .get("host_node_id")
    .cloned()
    .unwrap_or_default();

  Resource {
    metadata: Some(ResourceMetadata {
      id: session.id.clone(),
      name: session.id.clone(),
      kind: ResourceKind::Session as i32,
      labels: HashMap::new(),
      annotations: HashMap::new(),
      scope: String::new(),
      source_node_id: String::new(),
      created_at_ms: 0,
      updated_at_ms: 0,
    }),
    body: Some(Body::Session(SessionBody {
      title,
      host_node_id,
      metadata: session.metadata,
    })),
  }
}

fn memory_to_resource(entry: MemoryEntry, full: bool) -> Resource {
  let content = if full { entry.content } else { Vec::new() };

  Resource {
    metadata: Some(ResourceMetadata {
      id: entry.id.clone(),
      name: entry.id.clone(),
      kind: ResourceKind::Memory as i32,
      labels: entry.metadata.clone(),
      annotations: HashMap::new(),
      scope: String::new(),
      source_node_id: String::new(),
      created_at_ms: 0,
      updated_at_ms: 0,
    }),
    body: Some(Body::Memory(MemoryBody {
      content,
      metadata: entry.metadata,
    })),
  }
}

fn skill_to_resource(skill: SkillRecord, content: Option<String>) -> Resource {
  Resource {
    metadata: Some(ResourceMetadata {
      id: skill.id.clone(),
      name: skill.name.clone(),
      kind: ResourceKind::Skill as i32,
      labels: skill.metadata.clone(),
      annotations: HashMap::new(),
      scope: scope_to_string(skill.scope),
      source_node_id: skill.source_node_id.unwrap_or_default(),
      created_at_ms: skill.updated_at_ms,
      updated_at_ms: skill.updated_at_ms,
    }),
    body: Some(Body::Skill(SkillBody {
      version: skill.version,
      content_hash: skill.content_hash,
      content: content.unwrap_or_default(),
      metadata: skill.metadata,
    })),
  }
}

fn rule_to_resource(rule: RuleRecord, content: Option<String>) -> Resource {
  Resource {
    metadata: Some(ResourceMetadata {
      id: rule.id.clone(),
      name: rule.name.clone(),
      kind: ResourceKind::Rule as i32,
      labels: rule.metadata.clone(),
      annotations: HashMap::new(),
      scope: scope_to_string(rule.scope),
      source_node_id: rule.source_node_id.unwrap_or_default(),
      created_at_ms: rule.updated_at_ms,
      updated_at_ms: rule.updated_at_ms,
    }),
    body: Some(Body::Rule(RuleBody {
      version: rule.version,
      content_hash: rule.content_hash,
      content: content.unwrap_or_default(),
      metadata: rule.metadata,
    })),
  }
}

fn workspace_to_resource(workspace: WorkspaceRecord) -> Resource {
  Resource {
    metadata: Some(ResourceMetadata {
      id: workspace.id.clone(),
      name: workspace.id.clone(),
      kind: ResourceKind::Workspace as i32,
      labels: workspace.metadata.clone(),
      annotations: HashMap::new(),
      scope: String::new(),
      source_node_id: String::new(),
      created_at_ms: workspace.created_at_ms,
      updated_at_ms: workspace.updated_at_ms,
    }),
    body: Some(Body::Workspace(WorkspaceBody {
      root: workspace.root.to_string_lossy().to_string(),
      session_ids: workspace.session_ids,
      metadata: workspace.metadata,
    })),
  }
}

fn scope_to_string(scope: ResourceScope) -> String {
  match scope {
    ResourceScope::ClusterShared => "shared".to_string(),
    ResourceScope::NodeLocal => "local".to_string(),
  }
}

fn not_found(id: &str) -> Status {
  Status::not_found(format!("resource not found: {id}"))
}

pub fn parse_kind(raw: i32) -> Result<ResourceKind, Status> {
  ResourceKind::try_from(raw)
    .map_err(|_| Status::invalid_argument(format!("unknown resource kind: {raw}")))
}

pub fn parse_scope(raw: &str) -> Result<Option<ResourceScope>, Status> {
  if raw.is_empty() {
    return Ok(None);
  }
  match raw {
    "shared" => Ok(Some(ResourceScope::ClusterShared)),
    "local" => Ok(Some(ResourceScope::NodeLocal)),
    _ => Err(Status::invalid_argument(format!(
      "invalid scope '{raw}'; expected 'shared' or 'local'"
    ))),
  }
}
