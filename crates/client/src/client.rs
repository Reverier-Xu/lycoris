#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use std::{collections::HashMap, time::Duration};

use lycoris_proto::node::{
  DescribeResourceRequest, FetchRegistersRequest, GetOutDegreeRequest, GetOutDegreeResponse,
  GetResourceRequest, JoinRequest, LeaveRequest, ListResourcesRequest, NodeInfo as ProtoNodeInfo,
  ProbeRequest, ProbeResponse, PushNodeRequest, PushRegistersRequest, RegisterRequest,
  Resource as ProtoResource, ResourceKind as ProtoResourceKind, SetPrimaryEndpointRequest,
  StateMessage, StateResponse, SyncNodesRequest, SyncNodesResponse, SyncResourcesRequest,
  cluster_client::ClusterClient, membership_client::MembershipClient, sync_client::SyncClient,
};
use thiserror::Error;
use tonic::{Request, metadata::MetadataValue, transport::Channel};

const CLUSTER_KEY_HEADER: &str = "x-lycoris-cluster-key";

#[derive(Debug, Clone)]
pub struct ClusterClientHandle {
  inner: ClusterClient<Channel>,
  cluster_key: Option<String>,
}

impl ClusterClientHandle {
  pub async fn connect(
    address: &str, tls_bundle: &lycoris_tls::TlsBundle,
  ) -> Result<Self, ClientError> {
    Self::connect_with_tls(address, tls_bundle.client_config()).await
  }

  pub async fn connect_with_tls(
    address: &str, tls: tonic::transport::ClientTlsConfig,
  ) -> Result<Self, ClientError> {
    let endpoint = Channel::from_shared(address.to_string())?
      .tls_config(tls)?
      .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await?;
    Ok(Self::from_channel(channel))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      inner: ClusterClient::new(channel),
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

  pub async fn register(
    &mut self, node: ProtoNodeInfo, cluster_key: &str,
  ) -> Result<(), ClientError> {
    let request = Request::new(RegisterRequest {
      info: Some(node),
      cluster_key: cluster_key.to_string(),
    });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.register(request).await?;
    accepted(response.into_inner().accepted, "register")
  }

  pub async fn set_primary_endpoint(&mut self, address: &str) -> Result<(), ClientError> {
    let request = Request::new(SetPrimaryEndpointRequest {
      address: address.to_string(),
    });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.set_primary_endpoint(request).await?;
    accepted(response.into_inner().accepted, "set_primary_endpoint")
  }

  pub async fn get_out_degree(&mut self) -> Result<Option<GetOutDegreeResponse>, ClientError> {
    let request = Request::new(GetOutDegreeRequest {});
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.get_out_degree(request).await?;
    Ok(Some(response.into_inner()))
  }

  pub async fn join(&mut self, node: ProtoNodeInfo, cluster_key: &str) -> Result<(), ClientError> {
    let request = Request::new(JoinRequest {
      info: Some(node),
      cluster_key: cluster_key.to_string(),
    });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.join(request).await?;
    accepted(response.into_inner().accepted, "join")
  }

  pub async fn leave(&mut self, node_id: &str) -> Result<(), ClientError> {
    let request = Request::new(LeaveRequest {
      node_id: node_id.to_string(),
    });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.leave(request).await?;
    accepted(response.into_inner().accepted, "leave")
  }

  pub async fn list_resources(
    &mut self, kind: ProtoResourceKind, selector: HashMap<String, String>, scope: String,
  ) -> Result<Vec<ProtoResource>, ClientError> {
    let request = Request::new(ListResourcesRequest {
      kind: kind as i32,
      selector,
      scope,
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

  pub async fn describe_resource(
    &mut self, kind: ProtoResourceKind, id: &str,
  ) -> Result<Option<ProtoResource>, ClientError> {
    let request = Request::new(DescribeResourceRequest {
      kind: kind as i32,
      id: id.to_string(),
    });
    let request = self.attach_cluster_key(request)?;
    let response = self.inner.describe_resource(request).await?;
    Ok(Some(response.into_inner()).filter(|r| r.metadata.is_some()))
  }
}

#[derive(Debug, Clone)]
pub struct SyncClientHandle {
  inner: SyncClient<Channel>,
}

impl SyncClientHandle {
  pub async fn connect(
    address: &str, tls: tonic::transport::ClientTlsConfig,
  ) -> Result<Self, ClientError> {
    let endpoint = Channel::from_shared(address.to_string())?
      .tls_config(tls)?
      .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await?;
    Ok(Self::from_channel(channel))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      inner: SyncClient::new(channel),
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
    let response = self.inner.push_node(request).await?;
    accepted(response.into_inner().accepted, "push_node")
  }
}

#[derive(Debug, Clone)]
pub struct MembershipClientHandle {
  inner: MembershipClient<Channel>,
}

impl MembershipClientHandle {
  pub async fn connect(
    address: &str, tls: tonic::transport::ClientTlsConfig,
  ) -> Result<Self, ClientError> {
    let endpoint = Channel::from_shared(address.to_string())?
      .tls_config(tls)?
      .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await?;
    Ok(Self::from_channel(channel))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      inner: MembershipClient::new(channel),
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

  pub async fn push_registers(
    &mut self, registers: Vec<ProtoNodeInfo>,
  ) -> Result<Vec<ProtoNodeInfo>, ClientError> {
    let request = Request::new(PushRegistersRequest { registers });
    let response = self.inner.push_registers(request).await?;
    Ok(response.into_inner().registers)
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
    let endpoint = Channel::from_shared(address.to_string())?
      .tls_config(tls_bundle.client_config())?
      .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await?;
    Ok(Self::from_channel(channel))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      cluster: ClusterClientHandle::from_channel(channel.clone()),
      sync: SyncClientHandle::from_channel(channel.clone()),
      membership: MembershipClientHandle::from_channel(channel),
    }
  }
}

fn accepted(accepted: bool, operation: &str) -> Result<(), ClientError> {
  if accepted {
    Ok(())
  } else {
    Err(ClientError::Rejected(operation.to_string()))
  }
}

#[derive(Debug, Error)]
pub enum ClientError {
  #[error("invalid peer address: {0}")]
  InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
  #[error("transport error: {0}")]
  Transport(#[from] tonic::transport::Error),
  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
  #[error("tls error: {0}")]
  Tls(#[from] lycoris_tls::TlsError),
  #[error("rpc status: {0}")]
  Status(Box<tonic::Status>),
  #[error("{0} rejected by peer")]
  Rejected(String),
  #[error("invalid cluster key header")]
  InvalidClusterKey,
}

impl From<tonic::Status> for ClientError {
  fn from(status: tonic::Status) -> Self {
    Self::Status(Box::new(status))
  }
}
