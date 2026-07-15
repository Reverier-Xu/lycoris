#![allow(clippy::result_large_err)]

use std::{collections::HashMap, sync::Arc};

use lycoris_api::{
  ClusterRpcClient,
  proto::{
    DescribeResourceRequest, GetOutDegreeRequest, GetOutDegreeResponse, GetResourceRequest,
    JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, ListResourcesRequest,
    ListResourcesResponse, MemoryBody, NodeBody, NodeInfo as ProtoNodeInfo, RegisterRequest,
    RegisterResponse, Resource, ResourceKind, ResourceMetadata, RuleBody, SessionBody,
    SetPrimaryEndpointRequest, SetPrimaryEndpointResponse, SkillBody, WorkspaceBody,
    cluster_server::{Cluster, ClusterServer},
    resource::Body,
  },
};
use lycoris_core::{ClusterKey, DEFAULT_EMBEDDING_DIM};
use lycoris_storage::{
  MemoryEntry, ResourceScope, RuleRecord, Session, SkillRecord, Storage, WorkspaceRecord,
  workspace::VersionedContentStore,
};
use tokio::sync::watch;
use tonic::{Request, Response, Status, transport::ClientTlsConfig};

use crate::{cluster_sync::ClusterSync, membership::MembershipService};

pub type ClusterServerHandle = ClusterServer<ClusterService>;

#[derive(Debug, Clone)]
pub struct ClusterService {
  service: Arc<MembershipService>,
  storage: Storage,
  cluster_sync: Option<ClusterSync>,
  cluster_key: Option<ClusterKey>,
  shutdown: Option<watch::Sender<bool>>,
  tls: ClientTlsConfig,
}

impl ClusterService {
  pub fn new(service: Arc<MembershipService>, storage: Storage, tls: ClientTlsConfig) -> Self {
    Self {
      service,
      storage,
      cluster_sync: None,
      cluster_key: None,
      shutdown: None,
      tls,
    }
  }

  pub fn with_cluster_sync(mut self, cluster_sync: ClusterSync) -> Self {
    self.cluster_sync = Some(cluster_sync);
    self
  }

  pub fn with_cluster_key(mut self, cluster_key: Option<ClusterKey>) -> Self {
    self.cluster_key = cluster_key;
    self
  }

  pub fn with_shutdown(mut self, shutdown: watch::Sender<bool>) -> Self {
    self.shutdown = Some(shutdown);
    self
  }

  pub fn into_server(self) -> ClusterServerHandle {
    ClusterServer::new(self)
  }

  fn local_node_id(&self) -> &str {
    self.service.local_node_id()
  }

  /// Return the node id and address of this node's primary peer, if any.
  fn local_out_degree(&self, nodes: &[ProtoNodeInfo]) -> Result<Option<(String, String)>, Status> {
    let primary = self
      .storage
      .node()
      .peers
      .get_primary()
      .map_err(|error| Status::internal(format!("failed to read primary peer: {error}")))?;

    Ok(primary.and_then(|address| {
      nodes
        .iter()
        .find(|node| node.address == address)
        .map(|node| (node.id.clone(), node.address.clone()))
    }))
  }

  /// Query every known node for its out-degree and build a map from node id to
  /// the node id it points to. Empty vectors are used for unreachable nodes.
  async fn collect_out_degrees(
    &self, nodes: &[ProtoNodeInfo],
  ) -> Result<HashMap<String, Vec<String>>, Status> {
    let mut out_degrees: HashMap<String, Vec<String>> = HashMap::new();
    let local_id = self.local_node_id().to_string();

    if let Some((target_id, _)) = self.local_out_degree(nodes)? {
      out_degrees
        .entry(local_id.clone())
        .or_default()
        .push(target_id);
    }

    for node in nodes {
      if node.id == local_id {
        continue;
      }

      match ClusterRpcClient::connect_with_tls(&node.address, self.tls.clone()).await {
        Ok(mut client) => match client.get_out_degree().await {
          Ok(Some(response)) if !response.node_id.is_empty() => {
            out_degrees
              .entry(node.id.clone())
              .or_default()
              .push(response.node_id);
          }
          Ok(_) => {
            tracing::debug!(%node.id, %node.address, "peer returned empty out-degree");
          }
          Err(error) => {
            tracing::warn!(%node.id, %node.address, %error, "failed to query out-degree");
          }
        },
        Err(error) => {
          tracing::warn!(%node.id, %node.address, %error, "failed to connect for out-degree query");
        }
      }
    }

    Ok(out_degrees)
  }

  /// Decorate nodes with in-degree and out-degree arrays by asking each node
  /// for its single out-degree.
  async fn decorate_degrees(
    &self, mut nodes: Vec<ProtoNodeInfo>,
  ) -> Result<Vec<ProtoNodeInfo>, Status> {
    let out_degrees = self.collect_out_degrees(&nodes).await?;

    let mut in_degree_map: HashMap<String, Vec<String>> = HashMap::new();
    for (from_id, targets) in &out_degrees {
      for target_id in targets {
        in_degree_map
          .entry(target_id.clone())
          .or_default()
          .push(from_id.clone());
      }
    }

    for node in &mut nodes {
      node.out_degree = out_degrees.get(&node.id).cloned().unwrap_or_default();
      node.in_degree = in_degree_map.get(&node.id).cloned().unwrap_or_default();
    }

    Ok(nodes)
  }
}

// ---------------------------------------------------------------------------
// Resource conversion
// ---------------------------------------------------------------------------

fn node_to_resource(node: ProtoNodeInfo) -> Resource {
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

fn matches_selector(
  metadata: &HashMap<String, String>, selector: &HashMap<String, String>,
) -> bool {
  if selector.is_empty() {
    return true;
  }
  selector
    .iter()
    .all(|(key, value)| metadata.get(key) == Some(value))
}

fn parse_kind(raw: i32) -> Result<ResourceKind, Status> {
  ResourceKind::try_from(raw)
    .map_err(|_| Status::invalid_argument(format!("unknown resource kind: {raw}")))
}

fn parse_scope(raw: &str) -> Result<Option<ResourceScope>, Status> {
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

fn not_found(id: &str) -> Status {
  Status::not_found(format!("resource not found: {id}"))
}

// ---------------------------------------------------------------------------
// Resource handlers
// ---------------------------------------------------------------------------

impl ClusterService {
  async fn list_resources_inner(
    &self, kind: ResourceKind, selector: HashMap<String, String>, scope: Option<ResourceScope>,
  ) -> Result<Vec<Resource>, Status> {
    match kind {
      ResourceKind::Node => {
        let nodes = self.service.list_nodes(&selector).await;
        let nodes = self.decorate_degrees(nodes).await?;
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

  async fn get_resource_inner(&self, kind: ResourceKind, id: &str) -> Result<Resource, Status> {
    match kind {
      ResourceKind::Node => {
        let mut nodes = self.service.list_nodes(&HashMap::new()).await;
        nodes = self.decorate_degrees(nodes).await?;
        nodes
          .into_iter()
          .find(|node| node.id == id)
          .map(node_to_resource)
          .ok_or_else(|| not_found(id))
      }
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

#[tonic::async_trait]
#[allow(clippy::result_large_err)]
impl Cluster for ClusterService {
  async fn register(
    &self, request: Request<RegisterRequest>,
  ) -> Result<Response<RegisterResponse>, Status> {
    let info = request
      .into_inner()
      .info
      .ok_or_else(|| Status::invalid_argument("missing node info"))?;

    if info.id.is_empty() {
      return Ok(Response::new(RegisterResponse {
        accepted: false,
        reason: "node id must not be empty".to_string(),
      }));
    }

    let actions = self.service.register(&info).await;
    if let Some(cluster_sync) = &self.cluster_sync {
      cluster_sync.dispatch(actions).await;
      cluster_sync.push_change(info.clone()).await;
    }

    Ok(Response::new(RegisterResponse {
      accepted: true,
      reason: String::new(),
    }))
  }

  async fn get_out_degree(
    &self, _request: Request<GetOutDegreeRequest>,
  ) -> Result<Response<GetOutDegreeResponse>, Status> {
    let nodes = self.service.list_nodes(&HashMap::new()).await;
    let target = self.local_out_degree(&nodes)?;

    Ok(Response::new(GetOutDegreeResponse {
      node_id: target
        .as_ref()
        .map(|(id, _)| id.clone())
        .unwrap_or_default(),
      address: target.map(|(_, address)| address).unwrap_or_default(),
    }))
  }

  async fn set_primary_endpoint(
    &self, request: Request<SetPrimaryEndpointRequest>,
  ) -> Result<Response<SetPrimaryEndpointResponse>, Status> {
    let address = request.into_inner().address;
    if address.is_empty() {
      return Ok(Response::new(SetPrimaryEndpointResponse {
        accepted: false,
        reason: "address must not be empty".to_string(),
      }));
    }

    let local_address = self
      .service
      .member_address(self.local_node_id())
      .await
      .unwrap_or_default();
    if address == local_address {
      return Ok(Response::new(SetPrimaryEndpointResponse {
        accepted: false,
        reason: "cannot set the local node's own address as primary".to_string(),
      }));
    }

    self
      .storage
      .node()
      .peers
      .set_primary(&address)
      .map_err(|error| Status::internal(format!("failed to set primary endpoint: {error}")))?;

    Ok(Response::new(SetPrimaryEndpointResponse {
      accepted: true,
      reason: String::new(),
    }))
  }

  async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
    let join = request.into_inner();
    let info = join
      .info
      .ok_or_else(|| Status::invalid_argument("missing node info"))?;

    if info.id.is_empty() {
      return Ok(Response::new(JoinResponse {
        accepted: false,
        reason: "node id must not be empty".to_string(),
      }));
    }

    if let Some(expected) = &self.cluster_key {
      let provided = ClusterKey::from_hex(&join.cluster_key)
        .map_err(|_| Status::permission_denied("invalid cluster key format"))?;
      if provided != *expected {
        return Ok(Response::new(JoinResponse {
          accepted: false,
          reason: "cluster key mismatch".to_string(),
        }));
      }
    } else {
      return Ok(Response::new(JoinResponse {
        accepted: false,
        reason: "this node has not initialized a cluster key; run 'lycoris cluster init' first"
          .to_string(),
      }));
    }

    let _ = self.storage.node().peers.seed(&info.address);
    let actions = self.service.register(&info).await;
    if let Some(cluster_sync) = &self.cluster_sync {
      cluster_sync.dispatch(actions).await;
      cluster_sync.push_change(info.clone()).await;
    }

    Ok(Response::new(JoinResponse {
      accepted: true,
      reason: String::new(),
    }))
  }

  async fn leave(&self, request: Request<LeaveRequest>) -> Result<Response<LeaveResponse>, Status> {
    let node_id = request.into_inner().node_id;
    let local_id = self.local_node_id().to_string();

    if node_id != local_id {
      let actions = self
        .service
        .leave(&node_id, lycoris_core::time::now_ms())
        .await;
      if let Some(cluster_sync) = &self.cluster_sync {
        cluster_sync.dispatch(actions).await;
      }
      return Ok(Response::new(LeaveResponse {
        accepted: true,
        reason: String::new(),
      }));
    }

    let actions = self
      .service
      .leave(&local_id, lycoris_core::time::now_ms())
      .await;
    if let Some(cluster_sync) = &self.cluster_sync {
      cluster_sync.dispatch(actions).await;
    }

    if let Some(shutdown) = &self.shutdown {
      let _ = shutdown.send(true);
    }

    Ok(Response::new(LeaveResponse {
      accepted: true,
      reason: String::new(),
    }))
  }

  async fn list_resources(
    &self, request: Request<ListResourcesRequest>,
  ) -> Result<Response<ListResourcesResponse>, Status> {
    let request = request.into_inner();
    let kind = parse_kind(request.kind)?;
    let scope = parse_scope(&request.scope)?;
    let resources = self
      .list_resources_inner(kind, request.selector, scope)
      .await?;
    Ok(Response::new(ListResourcesResponse { resources }))
  }

  async fn get_resource(
    &self, request: Request<GetResourceRequest>,
  ) -> Result<Response<Resource>, Status> {
    let request = request.into_inner();
    let kind = parse_kind(request.kind)?;
    let resource = self.get_resource_inner(kind, &request.id).await?;
    Ok(Response::new(resource))
  }

  async fn describe_resource(
    &self, request: Request<DescribeResourceRequest>,
  ) -> Result<Response<Resource>, Status> {
    let request = request.into_inner();
    let kind = parse_kind(request.kind)?;
    let resource = self.get_resource_inner(kind, &request.id).await?;
    Ok(Response::new(resource))
  }
}
