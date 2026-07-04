#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use std::{collections::HashMap, sync::Arc, time::Duration};

use lycoris_config::{NodeInfo, time::now_ms};
use thiserror::Error;
use tokio::sync::Mutex;
use tonic::{
  Request,
  transport::{Channel, ClientTlsConfig},
};

pub mod proto {
  #![allow(clippy::result_large_err)]
  tonic::include_proto!("lycoris.daemon");
}

pub mod tls;

/// Install the rustls ring crypto provider as the process default.
/// This must be called before any TLS connection is established.
pub fn install_crypto_provider() -> Result<(), std::sync::Arc<rustls::crypto::CryptoProvider>> {
  rustls::crypto::ring::default_provider().install_default()
}

use proto::{
  FetchRegistersRequest, HeartbeatRequest, LeafHash, ListNodesRequest, ListNodesResponse,
  MerkleRootRequest, NodeInfo as ProtoNodeInfo, ProbeRequest, ProbeResponse, PushNodeRequest,
  PushRegistersRequest, RegisterRequest, SetPrimaryEndpointRequest, StateMessage, StateResponse,
  SyncNodesRequest, SyncNodesResponse, cluster_client::ClusterClient,
  membership_client::MembershipClient, sync_client::SyncClient,
};

impl NodeInfo for ProtoNodeInfo {
  fn id(&self) -> &str {
    &self.id
  }

  fn address(&self) -> &str {
    &self.address
  }

  fn labels(&self) -> HashMap<String, String> {
    self.labels.clone()
  }

  fn annotations(&self) -> HashMap<String, String> {
    self.annotations.clone()
  }
}

#[derive(Debug, Clone)]
pub struct ClusterRpcClient {
  inner: Arc<Mutex<ClusterClient<Channel>>>,
}

impl ClusterRpcClient {
  pub async fn connect(address: &str, tls: ClientTlsConfig) -> Result<Self, ClusterClientError> {
    let endpoint = Channel::from_shared(address.to_string())?
      .tls_config(tls)?
      .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await?;
    Ok(Self::from_channel(channel))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      inner: Arc::new(Mutex::new(ClusterClient::new(channel))),
    }
  }

  pub async fn register<T: NodeInfo>(&self, node: &T) -> Result<(), ClusterClientError> {
    let request = Request::new(RegisterRequest {
      info: Some(proto_from_node(node)),
    });
    let response = self.inner.lock().await.register(request).await?;
    if response.into_inner().accepted {
      Ok(())
    } else {
      Err(ClusterClientError::Rejected("register".to_string()))
    }
  }

  pub async fn heartbeat<T: NodeInfo>(&self, node: &T) -> Result<(), ClusterClientError> {
    let request = Request::new(HeartbeatRequest {
      node_id: node.id().to_string(),
      info: Some(proto_from_node(node)),
    });
    let response = self.inner.lock().await.heartbeat(request).await?;
    if response.into_inner().accepted {
      Ok(())
    } else {
      Err(ClusterClientError::Rejected("heartbeat".to_string()))
    }
  }

  pub async fn list_nodes(
    &self, selector: HashMap<String, String>,
  ) -> Result<ListNodesResponse, ClusterClientError> {
    let request = Request::new(ListNodesRequest { selector });
    let response = self.inner.lock().await.list_nodes(request).await?;
    Ok(response.into_inner())
  }

  pub async fn set_primary_endpoint(&self, address: &str) -> Result<(), ClusterClientError> {
    let request = Request::new(SetPrimaryEndpointRequest {
      address: address.to_string(),
    });
    let response = self
      .inner
      .lock()
      .await
      .set_primary_endpoint(request)
      .await?;
    if response.into_inner().accepted {
      Ok(())
    } else {
      Err(ClusterClientError::Rejected(
        "set_primary_endpoint".to_string(),
      ))
    }
  }
}

#[derive(Debug, Clone)]
pub struct SyncRpcClient {
  inner: Arc<Mutex<SyncClient<Channel>>>,
}

impl SyncRpcClient {
  pub async fn connect(address: &str, tls: ClientTlsConfig) -> Result<Self, ClusterClientError> {
    let endpoint = Channel::from_shared(address.to_string())?
      .tls_config(tls)?
      .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await?;
    Ok(Self::from_channel(channel))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      inner: Arc::new(Mutex::new(SyncClient::new(channel))),
    }
  }

  pub async fn sync_nodes(
    &self, nodes: Vec<ProtoNodeInfo>,
  ) -> Result<SyncNodesResponse, ClusterClientError> {
    let request = Request::new(SyncNodesRequest { nodes });
    let response = self.inner.lock().await.sync_nodes(request).await?;
    Ok(response.into_inner())
  }

  pub async fn push_node(
    &self, info: ProtoNodeInfo, origin_node_id: String, sequence: u64,
  ) -> Result<(), ClusterClientError> {
    let request = Request::new(PushNodeRequest {
      info: Some(info),
      origin_node_id,
      sequence,
    });
    let response = self.inner.lock().await.push_node(request).await?;
    if response.into_inner().accepted {
      Ok(())
    } else {
      Err(ClusterClientError::Rejected("push_node".to_string()))
    }
  }
}

#[derive(Debug, Clone)]
pub struct MembershipRpcClient {
  inner: Arc<Mutex<MembershipClient<Channel>>>,
}

impl MembershipRpcClient {
  pub async fn connect(address: &str, tls: ClientTlsConfig) -> Result<Self, ClusterClientError> {
    let endpoint = Channel::from_shared(address.to_string())?
      .tls_config(tls)?
      .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await?;
    Ok(Self::from_channel(channel))
  }

  pub fn from_channel(channel: Channel) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MembershipClient::new(channel))),
    }
  }

  pub async fn probe(&self, seq: u64, target: &str) -> Result<ProbeResponse, ClusterClientError> {
    let request = Request::new(ProbeRequest {
      seq,
      target: target.to_string(),
    });
    let response = self.inner.lock().await.probe(request).await?;
    Ok(response.into_inner())
  }

  pub async fn fetch_registers(
    &self, node_ids: Vec<String>,
  ) -> Result<Vec<ProtoNodeInfo>, ClusterClientError> {
    let request = Request::new(FetchRegistersRequest { node_ids });
    let response = self.inner.lock().await.fetch_registers(request).await?;
    Ok(response.into_inner().registers)
  }

  pub async fn merkle_root(&self) -> Result<(Vec<u8>, Vec<LeafHash>), ClusterClientError> {
    let request = Request::new(MerkleRootRequest {});
    let response = self.inner.lock().await.merkle_root(request).await?;
    let inner = response.into_inner();
    Ok((inner.root_hash, inner.leaf_hashes))
  }

  pub async fn push_registers(
    &self, registers: Vec<ProtoNodeInfo>,
  ) -> Result<Vec<ProtoNodeInfo>, ClusterClientError> {
    let request = Request::new(PushRegistersRequest { registers });
    let response = self.inner.lock().await.push_registers(request).await?;
    Ok(response.into_inner().registers)
  }

  pub async fn state(&self, message: StateMessage) -> Result<StateResponse, ClusterClientError> {
    let request = Request::new(message);
    let response = self.inner.lock().await.state(request).await?;
    Ok(response.into_inner())
  }
}

#[derive(Debug, Clone)]
pub struct PeerClient {
  pub cluster: ClusterRpcClient,
  pub sync: SyncRpcClient,
  pub membership: MembershipRpcClient,
}

impl PeerClient {
  pub async fn connect(address: &str, tls: ClientTlsConfig) -> Result<Self, ClusterClientError> {
    let endpoint = Channel::from_shared(address.to_string())?
      .tls_config(tls)?
      .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await?;
    Ok(Self {
      cluster: ClusterRpcClient::from_channel(channel.clone()),
      sync: SyncRpcClient::from_channel(channel.clone()),
      membership: MembershipRpcClient::from_channel(channel),
    })
  }
}

fn proto_from_node<T: NodeInfo>(node: &T) -> ProtoNodeInfo {
  ProtoNodeInfo {
    id: node.id().to_string(),
    address: node.address().to_string(),
    labels: node.labels(),
    annotations: node.annotations(),
    last_heartbeat_unix_ms: now_ms(),
    state: "active".to_string(),
    incarnation: 1,
    heartbeat: 0,
  }
}

#[derive(Debug, Error)]
pub enum ClusterClientError {
  #[error("invalid peer address: {0}")]
  InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
  #[error("transport error: {0}")]
  Transport(#[from] tonic::transport::Error),
  #[error("rpc status: {0}")]
  Status(Box<tonic::Status>),
  #[error("{0} rejected by peer")]
  Rejected(String),
  #[error("peer connection timed out")]
  Timeout,
}

impl From<tonic::Status> for ClusterClientError {
  fn from(status: tonic::Status) -> Self {
    Self::Status(Box::new(status))
  }
}
