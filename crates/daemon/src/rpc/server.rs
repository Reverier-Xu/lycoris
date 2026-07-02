use lycoris_api::proto::{
  HeartbeatRequest, HeartbeatResponse, ListNodesRequest, ListNodesResponse,
  NodeInfo as ProtoNodeInfo, RegisterRequest, RegisterResponse, SetPrimaryEndpointRequest,
  SetPrimaryEndpointResponse,
  cluster_server::{Cluster, ClusterServer},
};
use tonic::{Request, Response, Status};

use crate::{gossip::Gossip, node::registry::NodeRegistry, storage::Storage};

pub type ClusterServerHandle = ClusterServer<ClusterService>;

#[derive(Debug, Clone)]
pub struct ClusterService {
  registry: NodeRegistry,
  storage: Storage,
  gossip: Option<Gossip>,
}

impl ClusterService {
  pub fn new(registry: NodeRegistry, storage: Storage) -> Self {
    Self {
      registry,
      storage,
      gossip: None,
    }
  }

  pub fn with_gossip(mut self, gossip: Gossip) -> Self {
    self.gossip = Some(gossip);
    self
  }

  pub fn into_server(self) -> ClusterServerHandle {
    ClusterServer::new(self)
  }

  async fn propagate_change(&self, info: ProtoNodeInfo) {
    if let Some(gossip) = &self.gossip {
      gossip.push_change(info).await;
    }
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

    self.registry.register_or_update(&info);
    self.propagate_change(info).await;

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

    self.registry.heartbeat(&info);
    self.propagate_change(info).await;

    Ok(Response::new(HeartbeatResponse {
      accepted: true,
      reason: String::new(),
    }))
  }

  async fn list_nodes(
    &self, request: Request<ListNodesRequest>,
  ) -> Result<Response<ListNodesResponse>, Status> {
    let selector = request.into_inner().selector;
    let nodes = self.registry.list_alive(&selector);
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
      .set_primary(&address)
      .map_err(|error| Status::internal(format!("failed to set primary endpoint: {error}")))?;

    Ok(Response::new(SetPrimaryEndpointResponse {
      accepted: true,
      reason: String::new(),
    }))
  }
}
