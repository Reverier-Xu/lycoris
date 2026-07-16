#![allow(clippy::result_large_err)]

use std::{collections::HashMap, sync::Arc};

use lycoris_core::matches_selector;
use lycoris_proto::node::{
  MemoryBody, NodeBody, Resource, ResourceKind, ResourceMetadata, RuleBody, SessionBody, SkillBody,
  WorkspaceBody, resource::Body,
};
use lycoris_storage::{
  MemoryEntry, ResourceScope, RuleRecord, Session, SkillRecord, Storage, WorkspaceRecord,
  workspace::{VersionedContentStore, versioned::VersionedResource},
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
          .list_memories(scope)
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
          .list_workspaces(scope)
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
        let entry = self
          .storage
          .agent()
          .memory()
          .get(id)
          .await
          .map_err(|error| Status::internal(format!("failed to get memory: {error}")))?;
        entry
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
      ResourceKind::Workspace => {
        let workspace = self
          .storage
          .workspace()
          .workspaces()
          .get(id)
          .map_err(|error| Status::internal(format!("failed to get workspace: {error}")))?;
        workspace
          .map(workspace_to_resource)
          .ok_or_else(|| not_found(id))
      }
    }
  }

  async fn list_memories(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<MemoryEntry>, lycoris_storage::AgentStorageError> {
    let agent = self.storage.agent();
    let memory = agent.memory();
    match scope {
      Some(ResourceScope::ClusterShared) => memory.list_shared().await,
      Some(ResourceScope::NodeLocal) => memory.list_local().await,
      None => memory.list().await,
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

  fn list_workspaces(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<WorkspaceRecord>, lycoris_storage::WorkspaceStorageError> {
    let workspaces = self.storage.workspace().workspaces();
    match scope {
      Some(ResourceScope::ClusterShared) => workspaces.list_shared(),
      Some(ResourceScope::NodeLocal) => workspaces.list_local(),
      None => workspaces.list(),
    }
  }

  /// Merge an incoming shared resource into local storage.
  ///
  /// Only `ClusterShared` resources are accepted. The content body is written
  /// to the filesystem-backed content store when present.
  pub async fn apply_resource(&self, resource: &Resource) -> Result<(), Status> {
    let metadata = resource
      .metadata
      .as_ref()
      .ok_or_else(|| Status::invalid_argument("missing resource metadata"))?;
    let kind = parse_kind(metadata.kind)?;
    let scope = parse_scope(&metadata.scope)?;

    if scope != Some(ResourceScope::ClusterShared) {
      return Ok(());
    }

    match (kind, resource.body.as_ref()) {
      (ResourceKind::Memory, Some(Body::Memory(body))) => {
        if body.content.is_empty() {
          return Ok(());
        }
        let record = resource_to_memory(metadata, body)?;
        verify_content_hash_bytes(&body.content, &body.content_hash)?;
        let local = self
          .storage
          .agent()
          .memory()
          .get(&record.id)
          .await
          .map_err(|error| Status::internal(format!("failed to read memory: {error}")))?;
        if !should_apply_versioned(
          local.as_ref().map(|local| local.version()),
          local.as_ref().map(|local| local.updated_at_ms),
          local.as_ref().map(|local| local.scope),
          0,
          record.updated_at_ms,
          ResourceScope::ClusterShared,
        ) {
          return Ok(());
        }
        self
          .storage
          .agent()
          .memory()
          .store(&record)
          .await
          .map_err(|error| Status::internal(format!("failed to store memory: {error}")))?;
      }
      (ResourceKind::Skill, Some(Body::Skill(body))) => {
        if body.content.is_empty() {
          return Ok(());
        }
        let record = resource_to_skill(metadata, body)?;
        verify_content_hash(&body.content, &body.content_hash)?;
        let local = self
          .storage
          .workspace()
          .skills()
          .get(&record.id)
          .map_err(|error| Status::internal(format!("failed to read skill: {error}")))?;
        if !should_apply_versioned(
          local.as_ref().map(|local| local.version),
          local.as_ref().map(|local| local.updated_at_ms),
          local.as_ref().map(|local| local.scope),
          record.version,
          record.updated_at_ms,
          ResourceScope::ClusterShared,
        ) {
          return Ok(());
        }
        self
          .storage
          .workspace()
          .skills()
          .upsert(&record)
          .map_err(|error| Status::internal(format!("failed to upsert skill: {error}")))?;
        if should_write_content(local.as_ref(), body.content.is_empty(), &body.content_hash) {
          self
            .storage
            .workspace()
            .skill_content()
            .write(&record.id, &body.content, &record.content_hash)
            .map_err(|error| Status::internal(format!("failed to write skill content: {error}")))?;
        }
      }
      (ResourceKind::Rule, Some(Body::Rule(body))) => {
        if body.content.is_empty() {
          return Ok(());
        }
        let record = resource_to_rule(metadata, body)?;
        verify_content_hash(&body.content, &body.content_hash)?;
        let local = self
          .storage
          .workspace()
          .rules()
          .get(&record.id)
          .map_err(|error| Status::internal(format!("failed to read rule: {error}")))?;
        if !should_apply_versioned(
          local.as_ref().map(|local| local.version),
          local.as_ref().map(|local| local.updated_at_ms),
          local.as_ref().map(|local| local.scope),
          record.version,
          record.updated_at_ms,
          ResourceScope::ClusterShared,
        ) {
          return Ok(());
        }
        self
          .storage
          .workspace()
          .rules()
          .upsert(&record)
          .map_err(|error| Status::internal(format!("failed to upsert rule: {error}")))?;
        if should_write_content(local.as_ref(), body.content.is_empty(), &body.content_hash) {
          self
            .storage
            .workspace()
            .rule_content()
            .write(&record.id, &body.content, &record.content_hash)
            .map_err(|error| Status::internal(format!("failed to write rule content: {error}")))?;
        }
      }
      (ResourceKind::Workspace, Some(Body::Workspace(body))) => {
        let record = resource_to_workspace(metadata, body)?;
        let computed_hash = record.compute_content_hash().map_err(|error| {
          Status::internal(format!("failed to compute workspace content hash: {error}"))
        })?;
        if computed_hash != body.content_hash {
          return Err(Status::invalid_argument(format!(
            "workspace content hash mismatch: expected {}, got {}",
            body.content_hash, computed_hash
          )));
        }
        let local = self
          .storage
          .workspace()
          .workspaces()
          .get(&record.id)
          .map_err(|error| Status::internal(format!("failed to read workspace: {error}")))?;
        if !should_apply_versioned(
          local.as_ref().map(|local| local.version),
          local.as_ref().map(|local| local.updated_at_ms),
          local.as_ref().map(|local| local.scope),
          record.version,
          record.updated_at_ms,
          ResourceScope::ClusterShared,
        ) {
          return Ok(());
        }
        self
          .storage
          .workspace()
          .workspaces()
          .upsert(&record)
          .map_err(|error| Status::internal(format!("failed to upsert workspace: {error}")))?;
      }
      _ => {}
    }

    Ok(())
  }

  /// Return all cluster-shared resources as `Resource` protos.
  pub async fn local_shared_resources(&self) -> Result<Vec<Resource>, Status> {
    let mut resources = Vec::new();

    let memories = self
      .storage
      .agent()
      .memory()
      .list_shared()
      .await
      .map_err(|error| Status::internal(format!("failed to list shared memories: {error}")))?;
    for entry in memories {
      resources.push(memory_to_resource(entry, true));
    }

    let workspaces = self
      .storage
      .workspace()
      .workspaces()
      .list_shared()
      .map_err(|error| Status::internal(format!("failed to list shared workspaces: {error}")))?;
    for workspace in workspaces {
      resources.push(workspace_to_resource(workspace));
    }

    let skills = self
      .storage
      .workspace()
      .skills()
      .list_shared()
      .map_err(|error| Status::internal(format!("failed to list shared skills: {error}")))?;
    for skill in skills {
      let content = self
        .storage
        .workspace()
        .skill_content()
        .read(&skill.id)
        .map_err(|error| Status::internal(format!("failed to read skill content: {error}")))?;
      resources.push(skill_to_resource(skill, content));
    }

    let rules = self
      .storage
      .workspace()
      .rules()
      .list_shared()
      .map_err(|error| Status::internal(format!("failed to list shared rules: {error}")))?;
    for rule in rules {
      let content = self
        .storage
        .workspace()
        .rule_content()
        .read(&rule.id)
        .map_err(|error| Status::internal(format!("failed to read rule content: {error}")))?;
      resources.push(rule_to_resource(rule, content));
    }

    Ok(resources)
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
  let content = if full {
    entry.content.clone()
  } else {
    Vec::new()
  };
  let embedding = if full {
    entry.embedding.clone()
  } else {
    Vec::new()
  };

  Resource {
    metadata: Some(ResourceMetadata {
      id: entry.id.clone(),
      name: entry.id.clone(),
      kind: ResourceKind::Memory as i32,
      labels: entry.metadata.clone(),
      annotations: HashMap::new(),
      scope: scope_to_string(entry.scope),
      source_node_id: entry.source_node_id.clone().unwrap_or_default(),
      created_at_ms: 0,
      updated_at_ms: entry.updated_at_ms,
    }),
    body: Some(Body::Memory(MemoryBody {
      content,
      metadata: entry.metadata,
      scope: scope_to_string(entry.scope),
      source_node_id: entry.source_node_id.unwrap_or_default(),
      updated_at_ms: entry.updated_at_ms,
      content_hash: entry.content_hash,
      embedding,
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
      source_node_id: skill.source_node_id.clone().unwrap_or_default(),
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
      source_node_id: rule.source_node_id.clone().unwrap_or_default(),
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
      scope: scope_to_string(workspace.scope),
      source_node_id: workspace.source_node_id.clone().unwrap_or_default(),
      created_at_ms: workspace.created_at_ms,
      updated_at_ms: workspace.updated_at_ms,
    }),
    body: Some(Body::Workspace(WorkspaceBody {
      root: workspace.root.to_string_lossy().to_string(),
      session_ids: workspace.session_ids,
      metadata: workspace.metadata,
      scope: scope_to_string(workspace.scope),
      source_node_id: workspace.source_node_id.unwrap_or_default(),
      version: workspace.version,
      content_hash: workspace.content_hash,
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

fn resource_to_memory(
  metadata: &ResourceMetadata, body: &MemoryBody,
) -> Result<MemoryEntry, Status> {
  Ok(MemoryEntry {
    id: metadata.id.clone(),
    content: body.content.clone(),
    embedding: body.embedding.clone(),
    metadata: body.metadata.clone(),
    scope: ResourceScope::ClusterShared,
    source_node_id: Some(metadata.source_node_id.clone()).filter(|s| !s.is_empty()),
    updated_at_ms: metadata.updated_at_ms,
    content_hash: body.content_hash.clone(),
  })
}

fn resource_to_skill(metadata: &ResourceMetadata, body: &SkillBody) -> Result<SkillRecord, Status> {
  Ok(SkillRecord {
    id: metadata.id.clone(),
    name: metadata.name.clone(),
    version: body.version,
    content_hash: body.content_hash.clone(),
    scope: ResourceScope::ClusterShared,
    source_node_id: Some(metadata.source_node_id.clone()).filter(|s| !s.is_empty()),
    updated_at_ms: metadata.updated_at_ms,
    metadata: metadata.labels.clone(),
  })
}

fn resource_to_rule(metadata: &ResourceMetadata, body: &RuleBody) -> Result<RuleRecord, Status> {
  Ok(RuleRecord {
    id: metadata.id.clone(),
    name: metadata.name.clone(),
    version: body.version,
    content_hash: body.content_hash.clone(),
    scope: ResourceScope::ClusterShared,
    source_node_id: Some(metadata.source_node_id.clone()).filter(|s| !s.is_empty()),
    updated_at_ms: metadata.updated_at_ms,
    metadata: metadata.labels.clone(),
  })
}

fn resource_to_workspace(
  metadata: &ResourceMetadata, body: &WorkspaceBody,
) -> Result<WorkspaceRecord, Status> {
  Ok(WorkspaceRecord {
    id: metadata.id.clone(),
    root: body.root.clone().into(),
    session_ids: body.session_ids.clone(),
    metadata: body.metadata.clone(),
    scope: ResourceScope::ClusterShared,
    source_node_id: Some(metadata.source_node_id.clone()).filter(|s| !s.is_empty()),
    version: body.version,
    content_hash: body.content_hash.clone(),
    created_at_ms: metadata.created_at_ms,
    updated_at_ms: metadata.updated_at_ms,
  })
}

fn verify_content_hash(content: &str, expected: &str) -> Result<(), Status> {
  let actual = blake3::hash(content.as_bytes()).to_hex().to_string();
  if actual != expected {
    return Err(Status::invalid_argument(format!(
      "content hash mismatch: expected {expected}, got {actual}"
    )));
  }
  Ok(())
}

fn verify_content_hash_bytes(content: &[u8], expected: &str) -> Result<(), Status> {
  let actual = blake3::hash(content).to_hex().to_string();
  if actual != expected {
    return Err(Status::invalid_argument(format!(
      "content hash mismatch: expected {expected}, got {actual}"
    )));
  }
  Ok(())
}

/// Determine whether a remote resource should overwrite a local one.
///
/// Local `NodeLocal` resources are never overwritten by remote shared
/// resources. Otherwise, the higher version wins; if versions are equal, the
/// more recent `updated_at_ms` wins.
fn should_apply_versioned(
  local_version: Option<u64>, local_updated_at_ms: Option<i64>, local_scope: Option<ResourceScope>,
  remote_version: u64, remote_updated_at_ms: i64, remote_scope: ResourceScope,
) -> bool {
  if remote_scope != ResourceScope::ClusterShared {
    return false;
  }
  match (local_version, local_updated_at_ms, local_scope) {
    (None, ..) => true,
    (_, _, Some(ResourceScope::NodeLocal)) => false,
    (Some(local_version), ..) if remote_version != local_version => remote_version > local_version,
    (Some(_), Some(local_updated_at_ms), _) => remote_updated_at_ms > local_updated_at_ms,
    (Some(_), None, _) => true,
  }
}

fn should_write_content(
  local: Option<&VersionedResource>, remote_content_empty: bool, remote_content_hash: &str,
) -> bool {
  if remote_content_empty {
    return false;
  }
  match local {
    None => true,
    Some(local) => local.content_hash != remote_content_hash,
  }
}
