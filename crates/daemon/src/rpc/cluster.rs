//! Tonic wiring for the peer-facing `Sync` and `Membership` services.
//!
//! All business logic lives in `crate::sync` (see `ClusterSync`); this module
//! is the thin shell that unwraps requests, validates wire shape, and wraps
//! responses, so that `crate::sync` never sees a tonic type.
#![allow(clippy::result_large_err)]

use lycoris_proto::node::{
  FetchRegistersRequest, FetchRegistersResponse, MerkleNodesRequest, MerkleNodesResponse,
  MerkleRootRequest, MerkleRootResponse, ProbeRequest, ProbeResponse, PushNodeRequest,
  PushNodeResponse, PushRegistersRequest, PushRegistersResponse, StateMessage, StateResponse,
  SyncNodesRequest, SyncNodesResponse, SyncResourcesRequest, SyncResourcesResponse,
  membership_server::{Membership, MembershipServer},
  sync_server::{Sync, SyncServer},
};
use tonic::{Request, Response, Status};

use crate::sync::ClusterSync;

impl ClusterSync {
  /// Build the tonic server handles serving this instance.
  pub fn servers(&self) -> (SyncServer<ClusterSync>, MembershipServer<ClusterSync>) {
    (
      SyncServer::new(self.clone())
        .max_decoding_message_size(lycoris_client::MAX_RPC_MESSAGE_BYTES)
        .max_encoding_message_size(lycoris_client::MAX_RPC_MESSAGE_BYTES),
      MembershipServer::new(self.clone())
        .max_decoding_message_size(lycoris_client::MAX_RPC_MESSAGE_BYTES)
        .max_encoding_message_size(lycoris_client::MAX_RPC_MESSAGE_BYTES),
    )
  }
}

#[tonic::async_trait]
#[allow(clippy::result_large_err)]
impl Sync for ClusterSync {
  async fn sync_nodes(
    &self, request: Request<SyncNodesRequest>,
  ) -> Result<Response<SyncNodesResponse>, Status> {
    let nodes = self.serve_sync_nodes(request.into_inner().nodes).await;
    Ok(Response::new(SyncNodesResponse { nodes }))
  }

  async fn push_node(
    &self, request: Request<PushNodeRequest>,
  ) -> Result<Response<PushNodeResponse>, Status> {
    let push = request.into_inner();
    let info = push
      .info
      .ok_or_else(|| Status::invalid_argument("missing node info"))?;
    self
      .serve_push_node(info, push.origin_node_id, push.sequence)
      .await;
    Ok(Response::new(PushNodeResponse { accepted: true }))
  }

  async fn sync_resources(
    &self, request: Request<SyncResourcesRequest>,
  ) -> Result<Response<SyncResourcesResponse>, Status> {
    let resources = self
      .resources()
      .merge_and_list_shared(request.into_inner().resources)
      .await;
    Ok(Response::new(SyncResourcesResponse { resources }))
  }
}

#[tonic::async_trait]
#[allow(clippy::result_large_err)]
impl Membership for ClusterSync {
  async fn merkle_root(
    &self, _request: Request<MerkleRootRequest>,
  ) -> Result<Response<MerkleRootResponse>, Status> {
    let root_hash = self.serve_merkle_root().await.to_vec();
    Ok(Response::new(MerkleRootResponse { root_hash }))
  }

  async fn merkle_nodes(
    &self, request: Request<MerkleNodesRequest>,
  ) -> Result<Response<MerkleNodesResponse>, Status> {
    let results = self.serve_merkle_nodes(request.into_inner().nodes).await;
    Ok(Response::new(MerkleNodesResponse { results }))
  }

  async fn fetch_registers(
    &self, request: Request<FetchRegistersRequest>,
  ) -> Result<Response<FetchRegistersResponse>, Status> {
    let registers = self
      .serve_fetch_registers(request.into_inner().node_ids)
      .await;
    Ok(Response::new(FetchRegistersResponse { registers }))
  }

  async fn push_registers(
    &self, request: Request<PushRegistersRequest>,
  ) -> Result<Response<PushRegistersResponse>, Status> {
    self
      .serve_push_registers(request.into_inner().registers)
      .await;
    Ok(Response::new(PushRegistersResponse {}))
  }

  async fn probe(&self, request: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
    let probe = request.into_inner();
    let ack = self.serve_probe(probe.seq).await;
    Ok(Response::new(ProbeResponse {
      ack,
      seq: probe.seq,
    }))
  }

  async fn state(&self, request: Request<StateMessage>) -> Result<Response<StateResponse>, Status> {
    self.serve_state_message(request.into_inner()).await;
    Ok(Response::new(StateResponse { accepted: true }))
  }
}
