//! Daemon-level resource facade.
//!
//! `ResourceMapper` maps between stored domain types and the public
//! `Resource` proto. It is the single entry point shared by the rpc handlers
//! (`crate::rpc::server`) and the resource anti-entropy task
//! (`crate::sync::resource`). Failures are reported as the typed
//! [`MapperError`]; the rpc boundary maps it onto gRPC statuses
//! (`crate::rpc`), keeping this module free of transport concerns.

use std::{collections::HashMap, sync::Arc};

use lycoris_core::ResourceScope;
use lycoris_proto::node::{
  MemoryBody, NodeBody, NodeInfo, Resource, ResourceKind, ResourceMetadata,
  ResourceScope as ProtoResourceScope, RuleBody, SessionBody, SkillBody, WorkspaceBody,
  resource::Body,
};
use lycoris_storage::{
  AgentStorageError, MemoryEntry, RuleRecord, Session, SkillRecord, Storage, VersionedContentStore,
  VersionedResource, WorkspaceRecord, WorkspaceStorageError,
};

use crate::{
  membership::{MembershipService, convert::register_to_proto},
  selector::matches_selector,
};

/// Errors produced by the resource facade.
///
/// The rpc boundary maps these onto gRPC statuses (`crate::rpc`); the
/// anti-entropy task logs them directly, so every variant names the failing
/// operation precisely.
#[derive(Debug, thiserror::Error)]
pub enum MapperError {
  /// The request carried no metadata block.
  #[error("missing resource metadata")]
  MissingMetadata,
  /// The metadata kind field did not decode.
  #[error("unknown resource kind: {0}")]
  UnknownKind(i32),
  /// The metadata scope field did not decode.
  #[error("unknown resource scope: {0}")]
  UnknownScope(i32),
  /// An applied resource was not cluster-shared (D8: never silently drop).
  #[error("only cluster-shared resources can be applied; resource '{id}' is {scope}")]
  NotShared { id: String, scope: ResourceScope },
  /// The resource body was absent.
  #[error("missing body for {kind:?} resource '{id}'")]
  MissingBody { kind: ResourceKind, id: String },
  /// The declared kind and the body variant disagree.
  #[error("resource kind {kind:?} does not match its body")]
  KindBodyMismatch { kind: ResourceKind },
  /// Nodes and sessions do not participate in resource synchronization.
  #[error("{kind:?} resources are not synchronized")]
  NotSynchronized { kind: ResourceKind },
  /// No resource with the requested id exists.
  #[error("resource not found: {0}")]
  NotFound(String),
  /// Agent-domain storage failure, with the failing operation as context.
  #[error("{context}: {source}")]
  Agent {
    context: &'static str,
    source: AgentStorageError,
  },
  /// Workspace-domain storage failure, with the failing operation as context.
  #[error("{context}: {source}")]
  Workspace {
    context: &'static str,
    source: WorkspaceStorageError,
  },
}

impl MapperError {
  fn agent(context: &'static str) -> impl Fn(AgentStorageError) -> Self {
    move |source| Self::Agent { context, source }
  }

  fn workspace(context: &'static str) -> impl Fn(WorkspaceStorageError) -> Self {
    move |source| Self::Workspace { context, source }
  }
}

/// Decode a wire resource kind; the single raw-`i32` decoding point.
pub(crate) fn decode_kind(raw: i32) -> Result<ResourceKind, MapperError> {
  ResourceKind::try_from(raw).map_err(|_| MapperError::UnknownKind(raw))
}

/// The single wire-to-domain scope mapping: `UNSPECIFIED` normalizes to
/// `NodeLocal`, because an unscoped resource must never be synchronized.
pub(crate) fn scope_from_proto(scope: ProtoResourceScope) -> ResourceScope {
  match scope {
    ProtoResourceScope::ClusterShared => ResourceScope::ClusterShared,
    ProtoResourceScope::Unspecified | ProtoResourceScope::NodeLocal => ResourceScope::NodeLocal,
  }
}

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
  ) -> Result<Vec<Resource>, MapperError> {
    match kind {
      ResourceKind::Node => {
        let nodes = self.service.list_nodes(&selector).await;
        Ok(
          nodes
            .into_iter()
            .map(|register| node_to_resource(register_to_proto(&register)))
            .collect(),
        )
      }
      ResourceKind::Session => {
        let sessions = self
          .storage
          .agent()
          .sessions()
          .list()
          .map_err(MapperError::agent("failed to list sessions"))?;
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
          .map_err(MapperError::agent("failed to list memories"))?;
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
          .map_err(MapperError::workspace("failed to list skills"))?;
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
          .map_err(MapperError::workspace("failed to list rules"))?;
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
          .map_err(MapperError::workspace("failed to list workspaces"))?;
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

  pub async fn get(&self, kind: ResourceKind, id: &str) -> Result<Resource, MapperError> {
    match kind {
      ResourceKind::Node => self
        .service
        .list_nodes(&HashMap::new())
        .await
        .into_iter()
        .find(|register| register.node_id() == id)
        .map(|register| node_to_resource(register_to_proto(&register)))
        .ok_or_else(|| MapperError::NotFound(id.to_string())),
      ResourceKind::Session => self
        .storage
        .agent()
        .sessions()
        .get(id)
        .map_err(MapperError::agent("failed to get session"))?
        .map(session_to_resource)
        .ok_or_else(|| MapperError::NotFound(id.to_string())),
      ResourceKind::Memory => {
        let entry = self
          .storage
          .agent()
          .memory()
          .get(id)
          .await
          .map_err(MapperError::agent("failed to get memory"))?;
        entry
          .map(|entry| memory_to_resource(entry, true))
          .ok_or_else(|| MapperError::NotFound(id.to_string()))
      }
      ResourceKind::Skill => {
        let skill = self
          .storage
          .workspace()
          .skills()
          .get(id)
          .map_err(MapperError::workspace("failed to get skill"))?;
        match skill {
          Some(skill) => {
            let content = self
              .storage
              .workspace()
              .skill_content()
              .read(id)
              .map_err(MapperError::workspace("failed to read skill content"))?;
            Ok(skill_to_resource(skill, content))
          }
          None => Err(MapperError::NotFound(id.to_string())),
        }
      }
      ResourceKind::Rule => {
        let rule = self
          .storage
          .workspace()
          .rules()
          .get(id)
          .map_err(MapperError::workspace("failed to get rule"))?;
        match rule {
          Some(rule) => {
            let content = self
              .storage
              .workspace()
              .rule_content()
              .read(id)
              .map_err(MapperError::workspace("failed to read rule content"))?;
            Ok(rule_to_resource(rule, content))
          }
          None => Err(MapperError::NotFound(id.to_string())),
        }
      }
      ResourceKind::Workspace => {
        let workspace = self
          .storage
          .workspace()
          .workspaces()
          .get(id)
          .map_err(MapperError::workspace("failed to get workspace"))?;
        workspace
          .map(workspace_to_resource)
          .ok_or_else(|| MapperError::NotFound(id.to_string()))
      }
    }
  }

  async fn list_memories(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<MemoryEntry>, AgentStorageError> {
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
  ) -> Result<Vec<SkillRecord>, WorkspaceStorageError> {
    let skills = self.storage.workspace().skills();
    match scope {
      Some(ResourceScope::ClusterShared) => skills.list_shared(),
      Some(ResourceScope::NodeLocal) => skills.list_local(),
      None => skills.list(),
    }
  }

  fn list_rules(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<RuleRecord>, WorkspaceStorageError> {
    let rules = self.storage.workspace().rules();
    match scope {
      Some(ResourceScope::ClusterShared) => rules.list_shared(),
      Some(ResourceScope::NodeLocal) => rules.list_local(),
      None => rules.list(),
    }
  }

  fn list_workspaces(
    &self, scope: Option<ResourceScope>,
  ) -> Result<Vec<WorkspaceRecord>, WorkspaceStorageError> {
    let workspaces = self.storage.workspace().workspaces();
    match scope {
      Some(ResourceScope::ClusterShared) => workspaces.list_shared(),
      Some(ResourceScope::NodeLocal) => workspaces.list_local(),
      None => workspaces.list(),
    }
  }

  /// Merge an incoming shared resource into local storage.
  ///
  /// Only `ClusterShared` resources whose body matches their declared kind
  /// are accepted; anything else is rejected explicitly (D8) instead of being
  /// silently dropped. The content body is written to the filesystem-backed
  /// content store when present.
  pub async fn apply_resource(&self, resource: &Resource) -> Result<(), MapperError> {
    let metadata = resource
      .metadata
      .as_ref()
      .ok_or(MapperError::MissingMetadata)?;
    let kind = decode_kind(metadata.kind)?;
    let scope = metadata_scope(metadata)?;
    if scope != ResourceScope::ClusterShared {
      return Err(MapperError::NotShared {
        id: metadata.id.clone(),
        scope,
      });
    }

    match (kind, resource.body.as_ref()) {
      (ResourceKind::Memory, Some(Body::Memory(body))) => {
        let record = resource_to_memory(metadata, body)?;
        self
          .storage
          .agent()
          .apply_remote_memory(record, &body.content)
          .await
          .map_err(MapperError::agent("failed to apply remote memory"))?;
      }
      (ResourceKind::Skill, Some(Body::Skill(body))) => {
        let record = resource_to_skill(metadata, body)?;
        self
          .storage
          .workspace()
          .apply_remote_skill(record, &body.content)
          .map_err(MapperError::workspace("failed to apply remote skill"))?;
      }
      (ResourceKind::Rule, Some(Body::Rule(body))) => {
        let record = resource_to_rule(metadata, body)?;
        self
          .storage
          .workspace()
          .apply_remote_rule(record, &body.content)
          .map_err(MapperError::workspace("failed to apply remote rule"))?;
      }
      (ResourceKind::Workspace, Some(Body::Workspace(body))) => {
        let record = resource_to_workspace(metadata, body)?;
        self
          .storage
          .workspace()
          .apply_remote_workspace(record)
          .map_err(MapperError::workspace("failed to apply remote workspace"))?;
      }
      (ResourceKind::Node | ResourceKind::Session, _) => {
        return Err(MapperError::NotSynchronized { kind });
      }
      (_, None) => {
        return Err(MapperError::MissingBody {
          kind,
          id: metadata.id.clone(),
        });
      }
      _ => {
        return Err(MapperError::KindBodyMismatch { kind });
      }
    }

    Ok(())
  }

  /// Return all cluster-shared resources as `Resource` protos.
  pub async fn local_shared_resources(&self) -> Result<Vec<Resource>, MapperError> {
    let mut resources = Vec::new();

    let memories = self
      .storage
      .agent()
      .memory()
      .list_shared()
      .await
      .map_err(MapperError::agent("failed to list shared memories"))?;
    for entry in memories {
      resources.push(memory_to_resource(entry, true));
    }

    let workspaces = self
      .storage
      .workspace()
      .workspaces()
      .list_shared()
      .map_err(MapperError::workspace("failed to list shared workspaces"))?;
    for workspace in workspaces {
      resources.push(workspace_to_resource(workspace));
    }

    let skills = self
      .storage
      .workspace()
      .skills()
      .list_shared()
      .map_err(MapperError::workspace("failed to list shared skills"))?;
    for skill in skills {
      let content = self
        .storage
        .workspace()
        .skill_content()
        .read(&skill.id)
        .map_err(MapperError::workspace("failed to read skill content"))?;
      resources.push(skill_to_resource(skill, content));
    }

    let rules = self
      .storage
      .workspace()
      .rules()
      .list_shared()
      .map_err(MapperError::workspace("failed to list shared rules"))?;
    for rule in rules {
      let content = self
        .storage
        .workspace()
        .rule_content()
        .read(&rule.id)
        .map_err(MapperError::workspace("failed to read rule content"))?;
      resources.push(rule_to_resource(rule, content));
    }

    Ok(resources)
  }
}

/// Scaffolding for the metadata block of an outgoing `Resource`.
///
/// Scope, source node, and timestamps have their single copy here; body
/// payloads do not repeat them. Unscoped resources (nodes, sessions) simply
/// never call [`MetadataBuilder::scope`], leaving `RESOURCE_SCOPE_UNSPECIFIED`.
struct MetadataBuilder(ResourceMetadata);

impl MetadataBuilder {
  fn new(id: &str, name: &str, kind: ResourceKind) -> Self {
    Self(ResourceMetadata {
      id: id.to_string(),
      name: name.to_string(),
      kind: kind as i32,
      ..ResourceMetadata::default()
    })
  }

  fn labels(mut self, labels: HashMap<String, String>) -> Self {
    self.0.labels = labels;
    self
  }

  fn scope(mut self, scope: ResourceScope, source_node_id: Option<&str>) -> Self {
    self.0.scope = scope_to_proto(scope) as i32;
    self.0.source_node_id = source_node_id.unwrap_or_default().to_string();
    self
  }

  fn timestamps(mut self, created_at_ms: i64, updated_at_ms: i64) -> Self {
    self.0.created_at_ms = created_at_ms;
    self.0.updated_at_ms = updated_at_ms;
    self
  }

  fn build(self) -> ResourceMetadata {
    self.0
  }
}

fn node_to_resource(node: NodeInfo) -> Resource {
  // The node's labels and annotations live on the `NodeInfo` payload only;
  // metadata carries the generic resource scaffold.
  let metadata = MetadataBuilder::new(&node.id, &node.id, ResourceKind::Node)
    .timestamps(0, node.last_heartbeat_unix_ms)
    .build();
  Resource {
    metadata: Some(metadata),
    body: Some(Body::Node(NodeBody { node: Some(node) })),
  }
}

fn session_to_resource(session: Session) -> Resource {
  let title = session
    .metadata
    .get(Session::META_TITLE)
    .cloned()
    .unwrap_or_default();
  let host_node_id = session
    .metadata
    .get(Session::META_HOST_NODE_ID)
    .cloned()
    .unwrap_or_default();

  Resource {
    metadata: Some(MetadataBuilder::new(&session.id, &session.id, ResourceKind::Session).build()),
    body: Some(Body::Session(SessionBody {
      title,
      host_node_id,
      metadata: session.metadata,
    })),
  }
}

fn memory_to_resource(entry: MemoryEntry, full: bool) -> Resource {
  let (content, embedding) = if full {
    (entry.content.clone(), entry.embedding.clone())
  } else {
    (Vec::new(), Vec::new())
  };

  Resource {
    metadata: Some(
      MetadataBuilder::new(&entry.id, &entry.id, ResourceKind::Memory)
        .labels(entry.metadata.clone())
        .scope(entry.scope, entry.source_node_id.as_deref())
        .timestamps(0, entry.updated_at_ms)
        .build(),
    ),
    body: Some(Body::Memory(MemoryBody {
      content,
      metadata: entry.metadata,
      content_hash: entry.content_hash,
      embedding,
      version: entry.version,
    })),
  }
}

/// Shared metadata scaffold for skills and rules (both are `VersionedResource`
/// records).
fn versioned_metadata(record: &VersionedResource, kind: ResourceKind) -> ResourceMetadata {
  MetadataBuilder::new(&record.id, &record.name, kind)
    .labels(record.metadata.clone().into_iter().collect())
    .scope(record.scope, record.source_node_id.as_deref())
    .timestamps(record.updated_at_ms, record.updated_at_ms)
    .build()
}

fn skill_to_resource(skill: SkillRecord, content: Option<String>) -> Resource {
  Resource {
    metadata: Some(versioned_metadata(&skill, ResourceKind::Skill)),
    body: Some(Body::Skill(SkillBody {
      version: skill.version,
      content_hash: skill.content_hash,
      content: content.unwrap_or_default(),
      metadata: skill.metadata.into_iter().collect(),
    })),
  }
}

fn rule_to_resource(rule: RuleRecord, content: Option<String>) -> Resource {
  Resource {
    metadata: Some(versioned_metadata(&rule, ResourceKind::Rule)),
    body: Some(Body::Rule(RuleBody {
      version: rule.version,
      content_hash: rule.content_hash,
      content: content.unwrap_or_default(),
      metadata: rule.metadata.into_iter().collect(),
    })),
  }
}

fn workspace_to_resource(workspace: WorkspaceRecord) -> Resource {
  Resource {
    metadata: Some(
      MetadataBuilder::new(&workspace.id, &workspace.id, ResourceKind::Workspace)
        .labels(workspace.metadata.clone().into_iter().collect())
        .scope(workspace.scope, workspace.source_node_id.as_deref())
        .timestamps(workspace.created_at_ms, workspace.updated_at_ms)
        .build(),
    ),
    body: Some(Body::Workspace(WorkspaceBody {
      root: workspace.root.to_string_lossy().to_string(),
      session_ids: workspace.session_ids,
      metadata: workspace.metadata.into_iter().collect(),
      version: workspace.version,
      content_hash: workspace.content_hash,
    })),
  }
}

/// Decode the scope carried by resource metadata into the domain type.
fn metadata_scope(metadata: &ResourceMetadata) -> Result<ResourceScope, MapperError> {
  let scope = ProtoResourceScope::try_from(metadata.scope)
    .map_err(|_| MapperError::UnknownScope(metadata.scope))?;
  Ok(scope_from_proto(scope))
}

/// The single domain-to-wire scope mapping.
fn scope_to_proto(scope: ResourceScope) -> ProtoResourceScope {
  match scope {
    ResourceScope::ClusterShared => ProtoResourceScope::ClusterShared,
    ResourceScope::NodeLocal => ProtoResourceScope::NodeLocal,
  }
}

/// Normalize the optional source node id carried by resource metadata.
fn metadata_source_node_id(metadata: &ResourceMetadata) -> Option<String> {
  Some(metadata.source_node_id.clone()).filter(|id| !id.is_empty())
}

fn resource_to_memory(
  metadata: &ResourceMetadata, body: &MemoryBody,
) -> Result<MemoryEntry, MapperError> {
  Ok(MemoryEntry {
    id: metadata.id.clone(),
    content: body.content.clone(),
    embedding: body.embedding.clone(),
    metadata: body.metadata.clone(),
    scope: metadata_scope(metadata)?,
    source_node_id: metadata_source_node_id(metadata),
    updated_at_ms: metadata.updated_at_ms,
    content_hash: body.content_hash.clone(),
    version: body.version,
  })
}

/// Shared conversion for skills and rules (both are `VersionedResource`
/// records).
fn resource_to_versioned(
  metadata: &ResourceMetadata, version: u64, content_hash: &str,
) -> Result<VersionedResource, MapperError> {
  Ok(VersionedResource {
    id: metadata.id.clone(),
    name: metadata.name.clone(),
    version,
    content_hash: content_hash.to_string(),
    scope: metadata_scope(metadata)?,
    source_node_id: metadata_source_node_id(metadata),
    updated_at_ms: metadata.updated_at_ms,
    metadata: metadata.labels.clone().into_iter().collect(),
  })
}

fn resource_to_skill(
  metadata: &ResourceMetadata, body: &SkillBody,
) -> Result<SkillRecord, MapperError> {
  resource_to_versioned(metadata, body.version, &body.content_hash)
}

fn resource_to_rule(
  metadata: &ResourceMetadata, body: &RuleBody,
) -> Result<RuleRecord, MapperError> {
  resource_to_versioned(metadata, body.version, &body.content_hash)
}

fn resource_to_workspace(
  metadata: &ResourceMetadata, body: &WorkspaceBody,
) -> Result<WorkspaceRecord, MapperError> {
  Ok(WorkspaceRecord {
    id: metadata.id.clone(),
    root: body.root.clone().into(),
    session_ids: body.session_ids.clone(),
    metadata: body.metadata.clone().into_iter().collect(),
    scope: metadata_scope(metadata)?,
    source_node_id: metadata_source_node_id(metadata),
    version: body.version,
    content_hash: body.content_hash.clone(),
    created_at_ms: metadata.created_at_ms,
    updated_at_ms: metadata.updated_at_ms,
  })
}

#[cfg(test)]
mod tests {
  use lycoris_membership::SwimConfig;
  use lycoris_storage::DEFAULT_EMBEDDING_DIM;
  use tempfile::TempDir;

  use super::*;
  use crate::membership::MemberRegister;

  fn test_mapper() -> (TempDir, ResourceMapper) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    let service = Arc::new(MembershipService::new(
      "local",
      SwimConfig::default(),
      MemberRegister::new("local", "127.0.0.1:1", 1, 0),
    ));
    (dir, ResourceMapper::new(storage, service))
  }

  fn memory_resource(scope: ProtoResourceScope) -> Resource {
    let content = b"shared memory".to_vec();
    Resource {
      metadata: Some(ResourceMetadata {
        id: "mem-1".to_string(),
        name: "mem-1".to_string(),
        kind: ResourceKind::Memory as i32,
        scope: scope as i32,
        source_node_id: "peer".to_string(),
        updated_at_ms: 1,
        ..ResourceMetadata::default()
      }),
      body: Some(Body::Memory(MemoryBody {
        content_hash: MemoryEntry::compute_content_hash(b"shared memory"),
        content,
        embedding: vec![0.0; DEFAULT_EMBEDDING_DIM],
        version: 1,
        ..MemoryBody::default()
      })),
    }
  }

  #[test]
  fn scope_mapping_round_trip() {
    for (proto, domain) in [
      (
        ProtoResourceScope::ClusterShared,
        ResourceScope::ClusterShared,
      ),
      (ProtoResourceScope::NodeLocal, ResourceScope::NodeLocal),
      (ProtoResourceScope::Unspecified, ResourceScope::NodeLocal),
    ] {
      assert_eq!(scope_from_proto(proto), domain);
    }
    for domain in [ResourceScope::ClusterShared, ResourceScope::NodeLocal] {
      assert_eq!(scope_from_proto(scope_to_proto(domain)), domain);
    }
  }

  #[tokio::test]
  async fn apply_resource_rejects_missing_metadata() {
    let (_dir, mapper) = test_mapper();
    let resource = Resource {
      metadata: None,
      body: None,
    };
    let error = mapper.apply_resource(&resource).await.unwrap_err();
    assert!(matches!(error, MapperError::MissingMetadata));
  }

  #[tokio::test]
  async fn apply_resource_rejects_non_shared_scope() {
    let (_dir, mapper) = test_mapper();
    for scope in [
      ProtoResourceScope::NodeLocal,
      ProtoResourceScope::Unspecified,
    ] {
      let error = mapper
        .apply_resource(&memory_resource(scope))
        .await
        .unwrap_err();
      assert!(
        matches!(error, MapperError::NotShared { .. }),
        "scope: {scope:?}"
      );
    }
  }

  #[tokio::test]
  async fn apply_resource_rejects_kind_body_mismatch() {
    let (_dir, mapper) = test_mapper();
    let mut resource = memory_resource(ProtoResourceScope::ClusterShared);
    resource.body = Some(Body::Workspace(WorkspaceBody::default()));
    let error = mapper.apply_resource(&resource).await.unwrap_err();
    assert!(matches!(error, MapperError::KindBodyMismatch { .. }));
  }

  #[tokio::test]
  async fn apply_resource_rejects_missing_body() {
    let (_dir, mapper) = test_mapper();
    let mut resource = memory_resource(ProtoResourceScope::ClusterShared);
    resource.body = None;
    let error = mapper.apply_resource(&resource).await.unwrap_err();
    assert!(matches!(error, MapperError::MissingBody { .. }));
  }

  #[tokio::test]
  async fn apply_resource_stores_valid_shared_memory() {
    let (_dir, mapper) = test_mapper();
    mapper
      .apply_resource(&memory_resource(ProtoResourceScope::ClusterShared))
      .await
      .unwrap();
    let stored = mapper
      .get(ResourceKind::Memory, "mem-1")
      .await
      .expect("get memory");
    assert!(matches!(stored.body, Some(Body::Memory(_))));
  }
}
