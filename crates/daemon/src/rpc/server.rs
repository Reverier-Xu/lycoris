#![allow(clippy::result_large_err)]

use std::{collections::HashMap, sync::Arc};

use lycoris_api::proto::{
  DescribeResourceRequest, GetOutDegreeRequest, GetOutDegreeResponse, GetResourceRequest,
  JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, ListResourcesRequest,
  ListResourcesResponse, NodeInfo as ProtoNodeInfo, RegisterRequest, RegisterResponse, Resource,
  SetPrimaryEndpointRequest, SetPrimaryEndpointResponse,
  cluster_server::{Cluster, ClusterServer},
};
use lycoris_core::ClusterKey;
use lycoris_storage::Storage;
use tokio::sync::watch;
use tonic::{Request, Response, Status};

use crate::{
  cluster_sync::ClusterSync, membership::MembershipService, rpc::resource::ResourceMapper,
};

pub type ClusterServerHandle = ClusterServer<ClusterService>;

#[derive(Debug, Clone)]
pub struct ClusterService {
  service: Arc<MembershipService>,
  storage: Storage,
  mapper: ResourceMapper,
  cluster_sync: Option<ClusterSync>,
  cluster_key: Option<ClusterKey>,
  shutdown: Option<watch::Sender<bool>>,
}

impl ClusterService {
  pub fn new(service: Arc<MembershipService>, storage: Storage, mapper: ResourceMapper) -> Self {
    Self {
      service,
      storage,
      mapper,
      cluster_sync: None,
      cluster_key: None,
      shutdown: None,
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

    let actions = self
      .service
      .leave(&node_id, lycoris_core::time::now_ms())
      .await;
    if let Some(cluster_sync) = &self.cluster_sync {
      cluster_sync.dispatch(actions).await;
    }

    if node_id == local_id
      && let Some(shutdown) = &self.shutdown
    {
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
    let kind = crate::rpc::resource::parse_kind(request.kind)?;
    let scope = crate::rpc::resource::parse_scope(&request.scope)?;
    let resources = self.mapper.list(kind, request.selector, scope).await?;
    Ok(Response::new(ListResourcesResponse { resources }))
  }

  async fn get_resource(
    &self, request: Request<GetResourceRequest>,
  ) -> Result<Response<Resource>, Status> {
    let request = request.into_inner();
    let kind = crate::rpc::resource::parse_kind(request.kind)?;
    let resource = self.mapper.get(kind, &request.id).await?;
    Ok(Response::new(resource))
  }

  async fn describe_resource(
    &self, request: Request<DescribeResourceRequest>,
  ) -> Result<Response<Resource>, Status> {
    let request = request.into_inner();
    let kind = crate::rpc::resource::parse_kind(request.kind)?;
    let resource = self.mapper.get(kind, &request.id).await?;
    Ok(Response::new(resource))
  }
}
