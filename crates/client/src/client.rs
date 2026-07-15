#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use std::{collections::HashMap, time::Duration};

use lycoris_config::ClientConfig;
use lycoris_core::NodeInfo;
use lycoris_proto::node::{
  DescribeResourceRequest, FetchRegistersRequest, GetOutDegreeRequest, GetOutDegreeResponse,
  GetResourceRequest, JoinRequest, LeaveRequest, ListResourcesRequest, NodeInfo as ProtoNodeInfo,
  ProbeRequest, ProbeResponse, PushNodeRequest, PushRegistersRequest, RegisterRequest,
  Resource as ProtoResource, ResourceKind as ProtoResourceKind, SetPrimaryEndpointRequest,
  StateMessage, StateResponse, SyncNodesRequest, SyncNodesResponse, cluster_client::ClusterClient,
  membership_client::MembershipClient, sync_client::SyncClient,
};
use lycoris_tls::load_client_tls;
use thiserror::Error;
use tonic::{Request, transport::Channel};

/// Install the rustls ring crypto provider as the process default.
///
/// This must be called before any TLS connection is established.
pub fn install_crypto_provider() -> Result<(), std::sync::Arc<rustls::crypto::CryptoProvider>> {
  rustls::crypto::ring::default_provider().install_default()
}

#[derive(Debug, Clone)]
pub struct ClusterClientHandle {
  inner: ClusterClient<Channel>,
}

impl ClusterClientHandle {
  pub async fn connect(address: &str, client_config: &ClientConfig) -> Result<Self, ClientError> {
    let tls = load_client_tls(
      &client_config.cert,
      &client_config.key,
      &client_config.ca_cert,
    )?;
    Self::connect_with_tls(address, tls).await
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
    }
  }

  pub async fn register(&mut self, node: &dyn NodeInfo) -> Result<(), ClientError> {
    let request = Request::new(RegisterRequest {
      info: Some(proto_from_node(node)),
    });
    let response = self.inner.register(request).await?;
    accepted(response.into_inner().accepted, "register")
  }

  pub async fn set_primary_endpoint(&mut self, address: &str) -> Result<(), ClientError> {
    let request = Request::new(SetPrimaryEndpointRequest {
      address: address.to_string(),
    });
    let response = self.inner.set_primary_endpoint(request).await?;
    accepted(response.into_inner().accepted, "set_primary_endpoint")
  }

  pub async fn get_out_degree(&mut self) -> Result<Option<GetOutDegreeResponse>, ClientError> {
    let request = Request::new(GetOutDegreeRequest {});
    let response = self.inner.get_out_degree(request).await?;
    Ok(Some(response.into_inner()))
  }

  pub async fn join(&mut self, node: &dyn NodeInfo, cluster_key: &str) -> Result<(), ClientError> {
    let request = Request::new(JoinRequest {
      info: Some(proto_from_node(node)),
      cluster_key: cluster_key.to_string(),
    });
    let response = self.inner.join(request).await?;
    accepted(response.into_inner().accepted, "join")
  }

  pub async fn leave(&mut self, node_id: &str) -> Result<(), ClientError> {
    let request = Request::new(LeaveRequest {
      node_id: node_id.to_string(),
    });
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

  pub async fn merkle_root(
    &mut self,
  ) -> Result<(Vec<u8>, Vec<lycoris_proto::node::LeafHash>), ClientError> {
    use lycoris_proto::node::MerkleRootRequest;
    let request = Request::new(MerkleRootRequest {});
    let response = self.inner.merkle_root(request).await?;
    let inner = response.into_inner();
    Ok((inner.root_hash, inner.leaf_hashes))
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
      cluster: ClusterClientHandle::from_channel(channel.clone()),
      sync: SyncClientHandle::from_channel(channel.clone()),
      membership: MembershipClientHandle::from_channel(channel),
    }
  }
}

fn proto_from_node(node: &dyn NodeInfo) -> ProtoNodeInfo {
  use lycoris_core::time::now_ms;
  ProtoNodeInfo {
    id: node.id().to_string(),
    address: node.address().to_string(),
    labels: node.labels().clone(),
    annotations: node.annotations().clone(),
    last_heartbeat_unix_ms: now_ms(),
    state: "active".to_string(),
    incarnation: 1,
    heartbeat: 0,
    in_degree: Vec::new(),
    out_degree: Vec::new(),
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
  #[error("rpc status: {0}")]
  Status(Box<tonic::Status>),
  #[error("{0} rejected by peer")]
  Rejected(String),
  #[error("peer connection timed out")]
  Timeout,
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
  fn proto_from_node_sets_expected_defaults() {
    use std::collections::HashMap;

    struct SimpleNode {
      id: String,
      address: String,
      labels: HashMap<String, String>,
      annotations: HashMap<String, String>,
    }

    impl NodeInfo for SimpleNode {
      fn id(&self) -> &str {
        &self.id
      }
      fn address(&self) -> &str {
        &self.address
      }
      fn labels(&self) -> &HashMap<String, String> {
        &self.labels
      }
      fn annotations(&self) -> &HashMap<String, String> {
        &self.annotations
      }
    }

    let node = SimpleNode {
      id: "n1".to_string(),
      address: "https://127.0.0.1:5000".to_string(),
      labels: HashMap::new(),
      annotations: HashMap::new(),
    };

    let proto = proto_from_node(&node);
    assert_eq!(proto.id, "n1");
    assert_eq!(proto.state, "active");
  }
}
