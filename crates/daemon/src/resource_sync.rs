use std::time::Duration;

use lycoris_client::ClientError;
use tonic::{Request, Response, Status};

use crate::{rpc::resource::ResourceMapper, transport::PeerPool};

const RPC_TIMEOUT: Duration = Duration::from_secs(3);

/// Drives shared-resource anti-entropy between peers.
///
/// `ResourceSync` wraps the `ResourceMapper` and `PeerPool` so that the
/// membership component does not need to know how shared skills, rules,
/// workspaces, and memories are serialized or merged.
#[derive(Debug, Clone)]
pub struct ResourceSync {
  mapper: ResourceMapper,
  pool: PeerPool,
}

impl ResourceSync {
  pub fn new(mapper: ResourceMapper, pool: PeerPool) -> Self {
    Self { mapper, pool }
  }

  /// Push local shared resources to a peer and merge the remote shared set.
  pub async fn sync_with_peer(&self, peer: &str) -> Result<(), ClientError> {
    let mut client = self.pool.connect(peer).await?;

    let local_resources = match self.mapper.local_shared_resources().await {
      Ok(resources) => resources,
      Err(error) => {
        tracing::warn!(%peer, %error, "failed to read local shared resources");
        return Ok(());
      }
    };

    let remote_resources =
      match tokio::time::timeout(RPC_TIMEOUT, client.sync.sync_resources(local_resources)).await {
        Ok(Ok(resources)) => resources,
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "resource sync rpc failed");
          return Ok(());
        }
        Err(_) => {
          tracing::warn!(%peer, "resource sync rpc timed out");
          return Ok(());
        }
      };

    for resource in remote_resources {
      if let Err(error) = self.mapper.apply_resource(&resource).await {
        tracing::warn!(%peer, %error, "failed to apply remote resource");
      }
    }

    Ok(())
  }

  /// Handle an incoming `SyncResources` RPC: merge remote resources and return
  /// the local shared set.
  #[allow(clippy::result_large_err)]
  pub async fn handle_sync_resources(
    &self, request: Request<lycoris_proto::node::SyncResourcesRequest>,
  ) -> Result<Response<lycoris_proto::node::SyncResourcesResponse>, Status> {
    let remote_resources = request.into_inner().resources;
    for resource in &remote_resources {
      if let Err(error) = self.mapper.apply_resource(resource).await {
        tracing::warn!(%error, "failed to apply resource during sync");
      }
    }

    let local_resources = self
      .mapper
      .local_shared_resources()
      .await
      .unwrap_or_default();
    Ok(Response::new(lycoris_proto::node::SyncResourcesResponse {
      resources: local_resources,
    }))
  }
}
