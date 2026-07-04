use std::sync::Arc;

use lycoris_api::proto::{
  HeartbeatRequest, HeartbeatResponse, ListNodesRequest, ListNodesResponse,
  NodeInfo as ProtoNodeInfo, RegisterRequest, RegisterResponse, SetPrimaryEndpointRequest,
  SetPrimaryEndpointResponse,
  cluster_server::{Cluster, ClusterServer},
};
use lycoris_storage::{ClusterNodeRecord, NodeDomain, NodeState};
use tonic::{Request, Response, Status};

use crate::{cluster_sync::ClusterSync, membership::MembershipService};

pub type ClusterServerHandle = ClusterServer<ClusterService>;

#[derive(Debug, Clone)]
pub struct ClusterService {
  service: Arc<MembershipService>,
  storage: NodeDomain,
  cluster_sync: Option<ClusterSync>,
}

impl ClusterService {
  pub fn new(service: Arc<MembershipService>, storage: NodeDomain) -> Self {
    Self {
      service,
      storage,
      cluster_sync: None,
    }
  }

  pub fn with_cluster_sync(mut self, cluster_sync: ClusterSync) -> Self {
    self.cluster_sync = Some(cluster_sync);
    self
  }

  pub fn into_server(self) -> ClusterServerHandle {
    ClusterServer::new(self)
  }
}

fn persist_node_info(storage: &NodeDomain, info: &ProtoNodeInfo) {
  let record = ClusterNodeRecord {
    id: info.id.clone(),
    address: info.address.clone(),
    last_heartbeat_ms: info.last_heartbeat_unix_ms,
    state: proto_state_to_storage(&info.state),
    labels: info.labels.clone(),
    annotations: info.annotations.clone(),
  };
  if let Err(error) = storage.cluster.upsert(&record) {
    tracing::warn!(%error, node_id = %info.id, "failed to persist node info");
  }
}

fn proto_state_to_storage(state: &str) -> NodeState {
  match state {
    "offline" | "leaving" => NodeState::Offline,
    _ => NodeState::Alive,
  }
}

#[tonic::async_trait]
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

    persist_node_info(&self.storage, &info);
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

  async fn heartbeat(
    &self, request: Request<HeartbeatRequest>,
  ) -> Result<Response<HeartbeatResponse>, Status> {
    let info = request
      .into_inner()
      .info
      .ok_or_else(|| Status::invalid_argument("missing node info"))?;

    if info.id.is_empty() {
      return Ok(Response::new(HeartbeatResponse {
        accepted: false,
        reason: "node id must not be empty".to_string(),
      }));
    }

    persist_node_info(&self.storage, &info);
    let actions = self.service.heartbeat(&info).await;
    if let Some(cluster_sync) = &self.cluster_sync {
      cluster_sync.dispatch(actions).await;
      cluster_sync.push_change(info.clone()).await;
    }

    Ok(Response::new(HeartbeatResponse {
      accepted: true,
      reason: String::new(),
    }))
  }

  async fn list_nodes(
    &self, request: Request<ListNodesRequest>,
  ) -> Result<Response<ListNodesResponse>, Status> {
    let selector = request.into_inner().selector;
    let nodes = self.service.list_nodes(&selector).await;
    Ok(Response::new(ListNodesResponse { nodes }))
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

    self
      .storage
      .peers
      .set_primary(&address)
      .map_err(|error| Status::internal(format!("failed to set primary endpoint: {error}")))?;

    Ok(Response::new(SetPrimaryEndpointResponse {
      accepted: true,
      reason: String::new(),
    }))
  }
}
