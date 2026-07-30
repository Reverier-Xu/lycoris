//! Membership-plane transport over the overlay (`OverlayPool`).
//!
//! Mirrors the membership-facing surface of the legacy gRPC `PeerClient` so
//! the sync modules (`swim`, `gossip`, `antientropy`) keep their call shapes
//! while every request rides a routed overlay envelope. Requests are
//! prost-encoded [`NodeRequest`]s; responses decode the matching
//! [`NodeResponse`] variant. There is no per-peer connection state: the
//! overlay owns routing, deduplication, and backpressure.

use std::sync::Arc;

use lycoris_client::ClientError;
use lycoris_overlay::{
  AuthorizationStatus, ClusterId, Enrollment, Envelope, EnvelopeHeader, LinkHandle, MessageKind,
  NodeId, PROTOCOL_VERSION, ProtocolId,
};
use lycoris_proto::node::{
  FetchRegistersRequest, FetchRegistersResponse, MerkleNodesRequest, MerkleNodesResponse,
  MerkleRootRequest, MerkleRootResponse, NodeInfo as ProtoNodeInfo, NodeRequest, NodeResponse,
  ProbeRequest, ProbeResponse, PushNodeRequest, PushNodeResponse, PushRegistersRequest,
  PushRegistersResponse, Resource, StateMessage, StateResponse, SyncNodesRequest,
  SyncNodesResponse, SyncResourcesRequest, SyncResourcesResponse, node_request, node_response,
};
use prost::Message as _;
use tokio::sync::Mutex;

use crate::{
  overlay::{NodeRequestHandler, ResourceRequestHandler},
  sync::{ClusterSync, RPC_TIMEOUT, ResourceSync},
};

const ROUTE_HOPS: u8 = 8;

fn unavailable(error: impl std::fmt::Display) -> ClientError {
  ClientError::Status(Box::new(tonic::Status::unavailable(error.to_string())))
}

/// Factory of membership-plane clients routed through the overlay.
#[derive(Debug, Clone)]
pub struct OverlayPool {
  handle: LinkHandle,
  local: NodeId,
  cluster_id: ClusterId,
  enrollment: Option<Arc<Mutex<Enrollment>>>,
}

impl OverlayPool {
  pub fn new(handle: LinkHandle, local: NodeId, cluster_id: ClusterId) -> Self {
    Self {
      handle,
      local,
      cluster_id,
      enrollment: None,
    }
  }

  pub(crate) fn for_daemon(
    handle: LinkHandle, local: NodeId, cluster_id: ClusterId, enrollment: Arc<Mutex<Enrollment>>,
  ) -> Self {
    Self {
      handle,
      local,
      cluster_id,
      enrollment: Some(enrollment),
    }
  }

  /// Return a membership client for `peer`. Cheap: no I/O happens here, the
  /// overlay routes each request when it is sent.
  pub fn connect(&self, peer: NodeId) -> OverlayPeerClient {
    OverlayPeerClient {
      route: self.route(peer),
    }
  }

  pub(crate) fn connect_resource(&self, peer: NodeId) -> OverlayResourceClient {
    OverlayResourceClient {
      route: self.route(peer),
    }
  }

  fn route(&self, peer: NodeId) -> OverlayPeerRoute {
    OverlayPeerRoute {
      handle: self.handle.clone(),
      local: self.local,
      cluster_id: self.cluster_id,
      peer,
    }
  }

  pub const fn local_node_id(&self) -> NodeId {
    self.local
  }

  pub const fn cluster_id(&self) -> ClusterId {
    self.cluster_id
  }

  pub(crate) async fn authorized_peers(&self) -> Vec<String> {
    let Some(enrollment) = &self.enrollment else {
      return self
        .handle
        .snapshot()
        .connected_nodes
        .into_iter()
        .filter(|node_id| *node_id != self.local)
        .map(|node_id| node_id.to_string())
        .collect();
    };
    let registry = enrollment.lock().await.registry().clone();
    let mut peers: Vec<_> = registry
      .records()
      .into_iter()
      .map(|record| record.node_id())
      .filter(|node_id| {
        *node_id != self.local
          && matches!(registry.status(*node_id), AuthorizationStatus::Active(_))
      })
      .collect();
    peers.sort_unstable();
    peers.dedup();

    let snapshot = self.handle.snapshot();
    peers.sort_by_key(|node_id| {
      let rank = if snapshot.healthy_nodes.contains(node_id) {
        0
      } else if snapshot.connected_nodes.contains(node_id) {
        1
      } else {
        2
      };
      (rank, *node_id)
    });
    peers
      .into_iter()
      .map(|node_id| node_id.to_string())
      .collect()
  }
}

#[derive(Debug, Clone)]
struct OverlayPeerRoute {
  handle: LinkHandle,
  local: NodeId,
  cluster_id: ClusterId,
  peer: NodeId,
}

impl OverlayPeerRoute {
  async fn request(&self, protocol: ProtocolId, payload: Vec<u8>) -> Result<Vec<u8>, ClientError> {
    let header = EnvelopeHeader {
      version: PROTOCOL_VERSION,
      cluster_id: self.cluster_id,
      request_id: self.handle.next_request_id(),
      source: self.local,
      destination: self.peer,
      protocol,
      kind: MessageKind::Request,
      deadline_unix_ms: lycoris_core::now_ms() + RPC_TIMEOUT.as_millis() as i64,
      remaining_hops: ROUTE_HOPS,
    };
    let envelope = Envelope::new(header, payload)
      .map_err(|error| unavailable(format!("overlay request too large: {error}")))?;
    let response = self.handle.request(envelope).await.map_err(unavailable)?;
    Ok(response.payload().to_vec())
  }
}

/// Membership-plane client for one overlay peer.
#[derive(Debug, Clone)]
pub struct OverlayPeerClient {
  route: OverlayPeerRoute,
}

impl OverlayPeerClient {
  pub async fn probe(&mut self, seq: u64, target: &str) -> Result<ProbeResponse, ClientError> {
    match self
      .call(node_request::Kind::Probe(ProbeRequest {
        seq,
        target: target.to_string(),
      }))
      .await?
    {
      node_response::Kind::Probe(response) => Ok(response),
      other => Err(unavailable(format!("probe answered with {other:?}"))),
    }
  }

  pub async fn fetch_registers(
    &mut self, node_ids: Vec<String>,
  ) -> Result<Vec<ProtoNodeInfo>, ClientError> {
    match self
      .call(node_request::Kind::FetchRegisters(FetchRegistersRequest {
        node_ids,
      }))
      .await?
    {
      node_response::Kind::FetchRegisters(FetchRegistersResponse { registers }) => Ok(registers),
      other => Err(unavailable(format!(
        "fetch_registers answered with {other:?}"
      ))),
    }
  }

  pub async fn merkle_root(&mut self) -> Result<Vec<u8>, ClientError> {
    match self
      .call(node_request::Kind::MerkleRoot(MerkleRootRequest {}))
      .await?
    {
      node_response::Kind::MerkleRoot(MerkleRootResponse { root_hash }) => Ok(root_hash),
      other => Err(unavailable(format!("merkle_root answered with {other:?}"))),
    }
  }

  pub async fn merkle_nodes(
    &mut self, request: MerkleNodesRequest,
  ) -> Result<MerkleNodesResponse, ClientError> {
    match self.call(node_request::Kind::MerkleNodes(request)).await? {
      node_response::Kind::MerkleNodes(response) => Ok(response),
      other => Err(unavailable(format!("merkle_nodes answered with {other:?}"))),
    }
  }

  pub async fn push_registers(&mut self, registers: Vec<ProtoNodeInfo>) -> Result<(), ClientError> {
    match self
      .call(node_request::Kind::PushRegisters(PushRegistersRequest {
        registers,
      }))
      .await?
    {
      node_response::Kind::PushRegisters(PushRegistersResponse {}) => Ok(()),
      other => Err(unavailable(format!(
        "push_registers answered with {other:?}"
      ))),
    }
  }

  pub async fn state(&mut self, message: StateMessage) -> Result<StateResponse, ClientError> {
    match self.call(node_request::Kind::State(message)).await? {
      node_response::Kind::State(response) => Ok(response),
      other => Err(unavailable(format!("state answered with {other:?}"))),
    }
  }

  pub async fn sync_nodes(
    &mut self, nodes: Vec<ProtoNodeInfo>,
  ) -> Result<SyncNodesResponse, ClientError> {
    match self
      .call(node_request::Kind::SyncNodes(SyncNodesRequest { nodes }))
      .await?
    {
      node_response::Kind::SyncNodes(response) => Ok(response),
      other => Err(unavailable(format!("sync_nodes answered with {other:?}"))),
    }
  }

  pub async fn push_node(
    &mut self, info: ProtoNodeInfo, origin_node_id: String, sequence: u64,
  ) -> Result<(), ClientError> {
    match self
      .call(node_request::Kind::PushNode(PushNodeRequest {
        info: Some(info),
        origin_node_id,
        sequence,
      }))
      .await?
    {
      node_response::Kind::PushNode(PushNodeResponse { accepted: true }) => Ok(()),
      other => Err(unavailable(format!("push_node answered with {other:?}"))),
    }
  }

  async fn call(&mut self, kind: node_request::Kind) -> Result<node_response::Kind, ClientError> {
    let request = NodeRequest { kind: Some(kind) };
    let response = self
      .route
      .request(ProtocolId::Membership, request.encode_to_vec())
      .await?;
    let decoded = NodeResponse::decode(response.as_slice())
      .map_err(|error| unavailable(format!("invalid membership response: {error}")))?;
    decoded
      .kind
      .ok_or_else(|| unavailable("empty membership response"))
  }
}

#[derive(Debug, Clone)]
pub(crate) struct OverlayResourceClient {
  route: OverlayPeerRoute,
}

impl OverlayResourceClient {
  async fn sync_resources(
    &mut self, resources: Vec<Resource>,
  ) -> Result<Vec<Resource>, ClientError> {
    let request = SyncResourcesRequest { resources };
    let response = self
      .route
      .request(ProtocolId::Resource, request.encode_to_vec())
      .await?;
    Ok(
      SyncResourcesResponse::decode(response.as_slice())
        .map_err(|error| unavailable(format!("invalid resource response: {error}")))?
        .resources,
    )
  }
}

/// Resource transport. Production uses the overlay; the legacy branch keeps
/// the existing isolated unit fixtures without shipping a runtime fallback.
#[derive(Debug, Clone)]
pub(crate) enum ResourcePool {
  Overlay(OverlayPool),
  #[cfg(test)]
  Legacy(crate::transport::PeerPool),
}

impl From<OverlayPool> for ResourcePool {
  fn from(pool: OverlayPool) -> Self {
    Self::Overlay(pool)
  }
}

#[cfg(test)]
impl From<crate::transport::PeerPool> for ResourcePool {
  fn from(pool: crate::transport::PeerPool) -> Self {
    Self::Legacy(pool)
  }
}

impl ResourcePool {
  pub(crate) async fn connect(&self, peer: &str) -> Result<ResourcePeerClient, ClientError> {
    match self {
      Self::Overlay(pool) => {
        let node_id = peer
          .parse::<NodeId>()
          .map_err(|error| unavailable(format!("invalid overlay node id: {error}")))?;
        Ok(ResourcePeerClient::Overlay(pool.connect_resource(node_id)))
      }
      #[cfg(test)]
      Self::Legacy(pool) => Ok(ResourcePeerClient::Legacy(Box::new(
        pool.connect(peer).await?,
      ))),
    }
  }

  pub(crate) async fn candidates(
    &self, node: &lycoris_storage::NodeDomain, local: &str,
  ) -> Vec<String> {
    match self {
      Self::Overlay(pool) => {
        let _ = (node, local);
        pool.authorized_peers().await
      }
      #[cfg(test)]
      Self::Legacy(_) => crate::sync::peers::targets(node, local, lycoris_core::now_ms()),
    }
  }
}

#[derive(Debug)]
pub(crate) enum ResourcePeerClient {
  Overlay(OverlayResourceClient),
  #[cfg(test)]
  Legacy(Box<lycoris_client::PeerClient>),
}

impl ResourcePeerClient {
  pub(crate) async fn sync_resources(
    &mut self, resources: Vec<Resource>,
  ) -> Result<Vec<Resource>, ClientError> {
    match self {
      Self::Overlay(client) => client.sync_resources(resources).await,
      #[cfg(test)]
      Self::Legacy(client) => client.sync.sync_resources(resources).await,
    }
  }
}

/// Membership transport selected at compile time. Production daemons only
/// construct the overlay variant; the legacy variant exists solely for the
/// existing in-crate anti-entropy fixtures.
#[derive(Debug, Clone)]
pub(crate) enum MembershipPool {
  Overlay(OverlayPool),
  #[cfg(test)]
  Legacy(crate::transport::PeerPool),
}

impl From<OverlayPool> for MembershipPool {
  fn from(pool: OverlayPool) -> Self {
    Self::Overlay(pool)
  }
}

#[cfg(test)]
impl From<crate::transport::PeerPool> for MembershipPool {
  fn from(pool: crate::transport::PeerPool) -> Self {
    Self::Legacy(pool)
  }
}

impl MembershipPool {
  pub(crate) async fn connect(&self, peer: &str) -> Result<MembershipPeerClient, ClientError> {
    match self {
      Self::Overlay(pool) => {
        let node_id = peer
          .parse::<NodeId>()
          .map_err(|error| unavailable(format!("invalid overlay node id: {error}")))?;
        Ok(MembershipPeerClient::Overlay(pool.connect(node_id)))
      }
      #[cfg(test)]
      Self::Legacy(pool) => Ok(MembershipPeerClient::Legacy(Box::new(
        pool.connect(peer).await?,
      ))),
    }
  }

  pub(crate) async fn candidates(
    &self, node: &lycoris_storage::NodeDomain, local: &str, now_ms: i64,
  ) -> Vec<String> {
    match self {
      Self::Overlay(pool) => {
        let _ = (node, local, now_ms);
        pool.authorized_peers().await
      }
      #[cfg(test)]
      Self::Legacy(_) => crate::sync::peers::targets(node, local, now_ms),
    }
  }

  pub(crate) async fn retry_candidates(
    &self, node: &lycoris_storage::NodeDomain, local: &str,
  ) -> Vec<String> {
    match self {
      Self::Overlay(pool) => {
        let _ = (node, local);
        pool.authorized_peers().await
      }
      #[cfg(test)]
      Self::Legacy(_) => node
        .peers()
        .known_addresses()
        .unwrap_or_default()
        .into_iter()
        .filter(|peer| peer != local)
        .collect(),
    }
  }

  pub(crate) fn mark_seen(
    &self, node: &lycoris_storage::NodeDomain, peer: &str, now_ms: i64,
  ) -> Result<(), lycoris_storage::StorageError> {
    match self {
      Self::Overlay(_) => {
        let _ = (node, peer, now_ms);
        Ok(())
      }
      #[cfg(test)]
      Self::Legacy(_) => node.peers().mark_seen(peer, now_ms),
    }
  }

  pub(crate) fn mark_failed(
    &self, node: &lycoris_storage::NodeDomain, peer: &str,
  ) -> Result<(), lycoris_storage::StorageError> {
    match self {
      Self::Overlay(_) => {
        let _ = (node, peer);
        Ok(())
      }
      #[cfg(test)]
      Self::Legacy(_) => node.peers().mark_attempt(peer, false),
    }
  }

  pub(crate) fn promote(
    &self, node: &lycoris_storage::NodeDomain, peer: &str, local: &str,
  ) -> Result<(), lycoris_storage::StorageError> {
    match self {
      Self::Overlay(_) => {
        let _ = (node, peer, local);
        Ok(())
      }
      #[cfg(test)]
      Self::Legacy(_) => node.peers().set_primary(peer, local),
    }
  }

  pub(crate) fn is_primary(
    &self, node: &lycoris_storage::NodeDomain, peer: &str,
  ) -> Result<bool, lycoris_storage::StorageError> {
    match self {
      Self::Overlay(_) => {
        let _ = (node, peer);
        Ok(false)
      }
      #[cfg(test)]
      Self::Legacy(_) => Ok(node.peers().get_primary()?.as_deref() == Some(peer)),
    }
  }

  pub(crate) async fn remove(&self, peer: &str) {
    match self {
      Self::Overlay(_) => {
        let _ = peer;
      }
      #[cfg(test)]
      Self::Legacy(pool) => pool.remove(peer).await,
    }
  }
}

#[derive(Debug)]
pub(crate) enum MembershipPeerClient {
  Overlay(OverlayPeerClient),
  #[cfg(test)]
  Legacy(Box<lycoris_client::PeerClient>),
}

impl MembershipPeerClient {
  pub(crate) async fn probe(
    &mut self, seq: u64, target: &str,
  ) -> Result<ProbeResponse, ClientError> {
    match self {
      Self::Overlay(client) => client.probe(seq, target).await,
      #[cfg(test)]
      Self::Legacy(client) => client.membership.probe(seq, target).await,
    }
  }

  pub(crate) async fn fetch_registers(
    &mut self, node_ids: Vec<String>,
  ) -> Result<Vec<ProtoNodeInfo>, ClientError> {
    match self {
      Self::Overlay(client) => client.fetch_registers(node_ids).await,
      #[cfg(test)]
      Self::Legacy(client) => client.membership.fetch_registers(node_ids).await,
    }
  }

  pub(crate) async fn merkle_root(&mut self) -> Result<Vec<u8>, ClientError> {
    match self {
      Self::Overlay(client) => client.merkle_root().await,
      #[cfg(test)]
      Self::Legacy(client) => client.membership.merkle_root().await,
    }
  }

  pub(crate) async fn merkle_nodes(
    &mut self, request: MerkleNodesRequest,
  ) -> Result<MerkleNodesResponse, ClientError> {
    match self {
      Self::Overlay(client) => client.merkle_nodes(request).await,
      #[cfg(test)]
      Self::Legacy(client) => client.membership.merkle_nodes(request).await,
    }
  }

  pub(crate) async fn push_registers(
    &mut self, registers: Vec<ProtoNodeInfo>,
  ) -> Result<(), ClientError> {
    match self {
      Self::Overlay(client) => client.push_registers(registers).await,
      #[cfg(test)]
      Self::Legacy(client) => client.membership.push_registers(registers).await,
    }
  }

  pub(crate) async fn state(
    &mut self, message: StateMessage,
  ) -> Result<StateResponse, ClientError> {
    match self {
      Self::Overlay(client) => client.state(message).await,
      #[cfg(test)]
      Self::Legacy(client) => client.membership.state(message).await,
    }
  }

  pub(crate) async fn sync_nodes(
    &mut self, nodes: Vec<ProtoNodeInfo>,
  ) -> Result<SyncNodesResponse, ClientError> {
    match self {
      Self::Overlay(client) => client.sync_nodes(nodes).await,
      #[cfg(test)]
      Self::Legacy(client) => client.sync.sync_nodes(nodes).await,
    }
  }

  pub(crate) async fn push_node(
    &mut self, info: ProtoNodeInfo, origin_node_id: String, sequence: u64,
  ) -> Result<(), ClientError> {
    match self {
      Self::Overlay(client) => client.push_node(info, origin_node_id, sequence).await,
      #[cfg(test)]
      Self::Legacy(client) => {
        client
          .sync
          .push_node(info, origin_node_id, sequence)
          .await?;
        Ok(())
      }
    }
  }
}

/// Resource-plane request handler backed by `ResourceSync`'s merge boundary.
pub(crate) struct OverlayResourceRequestHandler {
  sync: ResourceSync,
}

impl OverlayResourceRequestHandler {
  pub(crate) fn new(sync: ResourceSync) -> Self {
    Self { sync }
  }
}

#[async_trait::async_trait]
impl ResourceRequestHandler for OverlayResourceRequestHandler {
  async fn handle(&self, request: SyncResourcesRequest) -> SyncResourcesResponse {
    SyncResourcesResponse {
      resources: self.sync.merge_and_list_shared(request.resources).await,
    }
  }
}

/// Membership-plane request handler backed by [`ClusterSync`]'s serve
/// methods: the overlay dispatcher routes every inbound [`NodeRequest`] here.
pub(crate) struct MembershipRequestHandler {
  sync: ClusterSync,
}

impl MembershipRequestHandler {
  pub(crate) fn new(sync: ClusterSync) -> Self {
    Self { sync }
  }
}

#[async_trait::async_trait]
impl NodeRequestHandler for MembershipRequestHandler {
  async fn handle(&self, request: NodeRequest) -> NodeResponse {
    let Some(request_kind) = request.kind else {
      return NodeResponse { kind: None };
    };
    let kind = match request_kind {
      node_request::Kind::Probe(request) => node_response::Kind::Probe(ProbeResponse {
        ack: self.sync.serve_probe(request.seq).await,
        seq: request.seq,
      }),
      node_request::Kind::FetchRegisters(request) => {
        node_response::Kind::FetchRegisters(FetchRegistersResponse {
          registers: self.sync.serve_fetch_registers(request.node_ids).await,
        })
      }
      node_request::Kind::MerkleRoot(_) => node_response::Kind::MerkleRoot(MerkleRootResponse {
        root_hash: self.sync.serve_merkle_root().await.to_vec(),
      }),
      node_request::Kind::MerkleNodes(request) => {
        node_response::Kind::MerkleNodes(MerkleNodesResponse {
          results: self.sync.serve_merkle_nodes(request.nodes).await,
        })
      }
      node_request::Kind::PushRegisters(request) => {
        self.sync.serve_push_registers(request.registers).await;
        node_response::Kind::PushRegisters(PushRegistersResponse {})
      }
      node_request::Kind::State(message) => {
        self.sync.serve_state_message(message).await;
        node_response::Kind::State(StateResponse { accepted: true })
      }
      node_request::Kind::SyncNodes(request) => node_response::Kind::SyncNodes(SyncNodesResponse {
        nodes: self.sync.serve_sync_nodes(request.nodes).await,
      }),
      node_request::Kind::PushNode(request) => {
        let accepted = match request.info {
          Some(info) => {
            self
              .sync
              .serve_push_node(info, request.origin_node_id, request.sequence)
              .await;
            true
          }
          None => false,
        };
        node_response::Kind::PushNode(PushNodeResponse { accepted })
      }
    };
    NodeResponse { kind: Some(kind) }
  }
}
