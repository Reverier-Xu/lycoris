#![allow(clippy::result_large_err)]

use std::sync::Arc;

use lycoris_proto::node::{
  GetResourceRequest, JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, ListResourcesRequest,
  ListResourcesResponse, NodeInfo as ProtoNodeInfo, RegisterRequest, RegisterResponse, Resource,
  SetPrimaryEndpointRequest, SetPrimaryEndpointResponse, cluster_server::Cluster,
};
use lycoris_storage::Storage;
use tokio::sync::watch;
use tonic::{Request, Response, Status};

use crate::{
  membership::{MembershipService, convert::proto_to_register},
  resource::ResourceMapper,
  sync::ClusterSync,
};

#[derive(Debug, Clone)]
pub struct ClusterService {
  service: Arc<MembershipService>,
  storage: Storage,
  mapper: ResourceMapper,
  cluster_sync: Option<ClusterSync>,
  shutdown: Option<watch::Sender<bool>>,
}

impl ClusterService {
  pub fn new(service: Arc<MembershipService>, storage: Storage, mapper: ResourceMapper) -> Self {
    Self {
      service,
      storage,
      mapper,
      cluster_sync: None,
      shutdown: None,
    }
  }

  pub fn with_cluster_sync(mut self, cluster_sync: ClusterSync) -> Self {
    self.cluster_sync = Some(cluster_sync);
    self
  }

  pub fn with_shutdown(mut self, shutdown: watch::Sender<bool>) -> Self {
    self.shutdown = Some(shutdown);
    self
  }

  fn local_node_id(&self) -> &str {
    self.service.local_node_id()
  }

  /// Shared admission path for `register` and `join`: validate the payload,
  /// merge the register into membership, then dispatch and gossip the
  /// resulting actions. An empty node id is an application-level rejection
  /// (`Ok(Err(reason))`); a missing payload is a protocol error.
  async fn admit(
    &self, info: Option<ProtoNodeInfo>,
  ) -> Result<Result<ProtoNodeInfo, String>, Status> {
    let info = info.ok_or_else(|| Status::invalid_argument("missing node info"))?;
    if info.id.is_empty() {
      return Ok(Err("node id must not be empty".to_string()));
    }

    let actions = self.service.register(proto_to_register(&info)).await;
    if let Some(cluster_sync) = &self.cluster_sync {
      cluster_sync.dispatch(actions).await;
      cluster_sync.push_change(info.clone()).await;
    }

    Ok(Ok(info))
  }
}

#[tonic::async_trait]
#[allow(clippy::result_large_err)]
impl Cluster for ClusterService {
  async fn register(
    &self, request: Request<RegisterRequest>,
  ) -> Result<Response<RegisterResponse>, Status> {
    match self.admit(request.into_inner().info).await? {
      Ok(_) => Ok(Response::new(RegisterResponse {
        accepted: true,
        reason: String::new(),
      })),
      Err(reason) => Ok(Response::new(RegisterResponse {
        accepted: false,
        reason,
      })),
    }
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

    // The self-primary rule is enforced by the storage node domain (D8); the
    // rpc layer only translates the domain error into the response reason.
    // That check compares against the local node's own address, so a missing
    // local register must fail the request outright: falling back to an empty
    // address would silently disable the check.
    let local_address = self
      .service
      .member_address(self.local_node_id())
      .await
      .ok_or_else(|| Status::failed_precondition("local node is not registered in membership"))?;
    match self
      .storage
      .node()
      .peers()
      .set_primary(&address, &local_address)
    {
      Ok(()) => Ok(Response::new(SetPrimaryEndpointResponse {
        accepted: true,
        reason: String::new(),
      })),
      Err(error @ lycoris_storage::StorageError::SelfPrimary) => {
        Ok(Response::new(SetPrimaryEndpointResponse {
          accepted: false,
          reason: error.to_string(),
        }))
      }
      Err(error) => Err(crate::rpc::storage_status(
        "failed to set primary endpoint",
        error,
      )),
    }
  }

  async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
    let info = match self.admit(request.into_inner().info).await? {
      Ok(info) => info,
      Err(reason) => {
        return Ok(Response::new(JoinResponse {
          accepted: false,
          reason,
        }));
      }
    };

    // Seeding is bookkeeping for future outbound sync; a failure must not
    // reject a join that membership already accepted, but it is never silent.
    if let Err(error) = self.storage.node().peers().seed(&info.address) {
      tracing::warn!(%error, address = %info.address, "failed to seed joined peer");
    }

    Ok(Response::new(JoinResponse {
      accepted: true,
      reason: String::new(),
    }))
  }

  async fn leave(&self, request: Request<LeaveRequest>) -> Result<Response<LeaveResponse>, Status> {
    let node_id = request.into_inner().node_id;
    let local_id = self.local_node_id().to_string();

    let actions = self.service.leave(&node_id, lycoris_core::now_ms()).await;
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
    let scope = crate::rpc::resource::parse_scope_filter(request.scope)?;
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
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;
  use crate::membership::{MemberRegister, SwimConfig};

  /// Build a cluster service whose local node id is `local_id` while
  /// membership only seeds `seed_id`; differing ids simulate a local register
  /// that never made it into membership.
  fn test_service(local_id: &str, seed_id: &str) -> (TempDir, ClusterService) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("lycoris.redb")).unwrap();
    let service = Arc::new(MembershipService::new(
      local_id,
      SwimConfig::default(),
      MemberRegister::new(seed_id, "127.0.0.1:1", 1, 0),
    ));
    let mapper = ResourceMapper::new(storage.clone(), service.clone());
    (dir, ClusterService::new(service, storage, mapper))
  }

  #[tokio::test]
  async fn set_primary_endpoint_fails_when_local_address_unknown() {
    // The local register is missing, so the self-primary check has no local
    // address to compare against; the request must fail instead of slipping
    // past the check on an empty address.
    let (_dir, service) = test_service("ghost", "local");
    let status = service
      .set_primary_endpoint(Request::new(SetPrimaryEndpointRequest {
        address: "https://127.0.0.1:2".to_string(),
      }))
      .await
      .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("not registered"));
  }
}
