#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use std::{collections::HashMap, time::Duration};

use lycoris_proto::{
  CLUSTER_KEY_HEADER,
  node::{
    FetchRegistersRequest, GetResourceRequest, JoinRequest, LeaveRequest, ListResourcesRequest,
    NodeInfo as ProtoNodeInfo, ProbeRequest, ProbeResponse, PushNodeRequest, PushRegistersRequest,
    RegisterRequest, Resource as ProtoResource, ResourceKind as ProtoResourceKind,
    ResourceScope as ProtoResourceScope, SetPrimaryEndpointRequest, StateMessage, StateResponse,
    SyncNodesRequest, SyncNodesResponse, SyncResourcesRequest, cluster_client::ClusterClient,
    membership_client::MembershipClient, sync_client::SyncClient,
  },
};
use thiserror::Error;
use tonic::{Request, metadata::MetadataValue, transport::Channel};

/// Timeout applied to the connection handshake of every peer channel.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Maximum gRPC message size applied to every peer RPC, in both directions.
///
/// `sync_resources` carries the complete shared-resource set (memory entries
/// include full content plus embeddings) in a single message, so tonic's 4
/// MiB default makes every resource anti-entropy round fail once the set
/// outgrows it. 64 MiB is the temporary ceiling until the exchange is
/// paginated — headroom, not a target message size. Client stubs and daemon
/// servers share this single value so both sides of a connection agree.
pub const MAX_RPC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Build a connected channel with the shared client defaults: mutual TLS from
/// `tls_bundle` and a bounded connect handshake. Every handle constructor goes
/// through here so connection policy stays single-sourced.
async fn build_channel(
  address: &str, tls_bundle: &lycoris_tls::TlsBundle,
) -> Result<Channel, ClientError> {
  let endpoint = Channel::from_shared(address.to_string())?
    .tls_config(tls_bundle.client_config())?
    .connect_timeout(CONNECT_TIMEOUT);
  Ok(endpoint.connect().await?)
}

#[derive(Debug, Clone)]
pub struct ClusterClientHandle {
  inner: ClusterClient<Channel>,
  cluster_key: Option<String>,
}

impl ClusterClientHandle {
  pub async fn connect(
    address: &str, tls_bundle: &lycoris_tls::TlsBundle,
  ) -> Result<Self, ClientError> {
    Ok(Self::from_channel(
      build_channel(address, tls_bundle).await?,
    ))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      inner: ClusterClient::new(channel)
        .max_decoding_message_size(MAX_RPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_RPC_MESSAGE_BYTES),
      cluster_key: None,
    }
  }

  /// Attach a cluster key that will be sent as metadata on all `Cluster` RPCs.
  pub fn with_cluster_key(mut self, cluster_key: impl Into<String>) -> Self {
    self.cluster_key = Some(cluster_key.into());
    self
  }

  fn attach_cluster_key<T>(&self, mut request: Request<T>) -> Result<Request<T>, ClientError> {
    if let Some(key) = &self.cluster_key {
      let value =
        MetadataValue::try_from(key.as_str()).map_err(|_| ClientError::InvalidClusterKey)?;
      request.metadata_mut().insert(CLUSTER_KEY_HEADER, value);
    }
    Ok(request)
  }

  pub async fn register(&mut self, node: ProtoNodeInfo) -> Result<(), ClientError> {
    let request = Request::new(RegisterRequest { info: Some(node) });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.register(request).await?.into_inner();
    accepted("register", response.accepted, response.reason)
  }

  pub async fn set_primary_endpoint(&mut self, address: &str) -> Result<(), ClientError> {
    let request = Request::new(SetPrimaryEndpointRequest {
      address: address.to_string(),
    });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.set_primary_endpoint(request).await?.into_inner();
    accepted("set_primary_endpoint", response.accepted, response.reason)
  }

  pub async fn join(&mut self, node: ProtoNodeInfo) -> Result<(), ClientError> {
    let request = Request::new(JoinRequest { info: Some(node) });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.join(request).await?.into_inner();
    accepted("join", response.accepted, response.reason)
  }

  pub async fn leave(&mut self, node_id: &str) -> Result<(), ClientError> {
    let request = Request::new(LeaveRequest {
      node_id: node_id.to_string(),
    });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.leave(request).await?.into_inner();
    accepted("leave", response.accepted, response.reason)
  }

  pub async fn list_resources(
    &mut self, kind: ProtoResourceKind, selector: HashMap<String, String>,
    scope: ProtoResourceScope,
  ) -> Result<Vec<ProtoResource>, ClientError> {
    let request = Request::new(ListResourcesRequest {
      kind: kind as i32,
      selector,
      scope: scope as i32,
    });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.list_resources(request).await?;
    Ok(response.into_inner().resources)
  }

  pub async fn get_resource(
    &mut self, kind: ProtoResourceKind, id: &str,
  ) -> Result<Option<ProtoResource>, ClientError> {
    let request = Request::new(GetResourceRequest {
      kind: kind as i32,
      id: id.to_string(),
    });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.get_resource(request).await?;
    Ok(Some(response.into_inner()).filter(|r| r.metadata.is_some()))
  }
}

#[derive(Debug, Clone)]
pub struct SyncClientHandle {
  inner: SyncClient<Channel>,
}

impl SyncClientHandle {
  pub async fn connect(
    address: &str, tls_bundle: &lycoris_tls::TlsBundle,
  ) -> Result<Self, ClientError> {
    Ok(Self::from_channel(
      build_channel(address, tls_bundle).await?,
    ))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      inner: SyncClient::new(channel)
        .max_decoding_message_size(MAX_RPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_RPC_MESSAGE_BYTES),
    }
  }

  pub async fn sync_nodes(
    &mut self, nodes: Vec<ProtoNodeInfo>,
  ) -> Result<SyncNodesResponse, ClientError> {
    let request = Request::new(SyncNodesRequest { nodes });
    let response = self.inner.sync_nodes(request).await?;
    Ok(response.into_inner())
  }

  pub async fn sync_resources(
    &mut self, resources: Vec<ProtoResource>,
  ) -> Result<Vec<ProtoResource>, ClientError> {
    let request = Request::new(SyncResourcesRequest { resources });
    let response = self.inner.sync_resources(request).await?;
    Ok(response.into_inner().resources)
  }

  pub async fn push_node(
    &mut self, info: ProtoNodeInfo, origin_node_id: String, sequence: u64,
  ) -> Result<(), ClientError> {
    let request = Request::new(PushNodeRequest {
      info: Some(info),
      origin_node_id,
      sequence,
    });
    let response = self.inner.push_node(request).await?.into_inner();
    // The wire response carries no reason field, so a rejection has none.
    accepted("push_node", response.accepted, String::new())
  }
}

#[derive(Debug, Clone)]
pub struct MembershipClientHandle {
  inner: MembershipClient<Channel>,
}

impl MembershipClientHandle {
  pub async fn connect(
    address: &str, tls_bundle: &lycoris_tls::TlsBundle,
  ) -> Result<Self, ClientError> {
    Ok(Self::from_channel(
      build_channel(address, tls_bundle).await?,
    ))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      inner: MembershipClient::new(channel)
        .max_decoding_message_size(MAX_RPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_RPC_MESSAGE_BYTES),
    }
  }

  pub async fn probe(&mut self, seq: u64, target: &str) -> Result<ProbeResponse, ClientError> {
    let request = Request::new(ProbeRequest {
      seq,
      target: target.to_string(),
    });
    let response = self.inner.probe(request).await?;
    Ok(response.into_inner())
  }

  pub async fn fetch_registers(
    &mut self, node_ids: Vec<String>,
  ) -> Result<Vec<ProtoNodeInfo>, ClientError> {
    let request = Request::new(FetchRegistersRequest { node_ids });
    let response = self.inner.fetch_registers(request).await?;
    Ok(response.into_inner().registers)
  }

  pub async fn merkle_root(&mut self) -> Result<Vec<u8>, ClientError> {
    use lycoris_proto::node::MerkleRootRequest;
    let request = Request::new(MerkleRootRequest {});
    let response = self.inner.merkle_root(request).await?;
    Ok(response.into_inner().root_hash)
  }

  pub async fn merkle_nodes(
    &mut self, request: lycoris_proto::node::MerkleNodesRequest,
  ) -> Result<lycoris_proto::node::MerkleNodesResponse, ClientError> {
    let response = self.inner.merkle_nodes(request).await?;
    Ok(response.into_inner())
  }

  pub async fn push_registers(&mut self, registers: Vec<ProtoNodeInfo>) -> Result<(), ClientError> {
    let request = Request::new(PushRegistersRequest { registers });
    self.inner.push_registers(request).await?;
    Ok(())
  }

  pub async fn state(&mut self, message: StateMessage) -> Result<StateResponse, ClientError> {
    let request = Request::new(message);
    let response = self.inner.state(request).await?;
    Ok(response.into_inner())
  }
}

#[derive(Debug, Clone)]
pub struct PeerClient {
  pub cluster: ClusterClientHandle,
  pub sync: SyncClientHandle,
  pub membership: MembershipClientHandle,
}

impl PeerClient {
  pub async fn connect(
    address: &str, tls_bundle: &lycoris_tls::TlsBundle,
  ) -> Result<Self, ClientError> {
    Ok(Self::from_channel(
      build_channel(address, tls_bundle).await?,
    ))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      cluster: ClusterClientHandle::from_channel(channel.clone()),
      sync: SyncClientHandle::from_channel(channel.clone()),
      membership: MembershipClientHandle::from_channel(channel),
    }
  }
}

/// Translate an accepted/reason response pair into a `Result`, keeping the
/// server's rejection reason instead of dropping it.
fn accepted(operation: &str, accepted: bool, reason: String) -> Result<(), ClientError> {
  if accepted {
    Ok(())
  } else {
    Err(ClientError::Rejected {
      operation: operation.to_string(),
      reason,
    })
  }
}

#[derive(Debug, Error)]
pub enum ClientError {
  #[error("invalid peer address: {0}")]
  InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
  #[error("transport error: {0}")]
  Transport(#[from] tonic::transport::Error),
  #[error("rpc status: {0}")]
  Status(Box<tonic::Status>),
  #[error("timed out waiting for peer: {0}")]
  Timeout(&'static str),
  #[error("{operation} rejected by peer: {reason}")]
  Rejected { operation: String, reason: String },
  #[error("invalid cluster key header")]
  InvalidClusterKey,
}

impl From<tonic::Status> for ClientError {
  fn from(status: tonic::Status) -> Self {
    Self::Status(Box::new(status))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rejection_carries_server_reason() {
    let error = accepted("join", false, "cluster key mismatch".to_string()).unwrap_err();
    assert_eq!(
      error.to_string(),
      "join rejected by peer: cluster key mismatch"
    );
  }

  #[test]
  fn accepted_response_passes_through() {
    assert!(accepted("join", true, String::new()).is_ok());
  }
}
