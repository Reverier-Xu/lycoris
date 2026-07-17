//! Shared-resource anti-entropy between peers.
//!
//! `ResourceSync` wraps the `ResourceMapper` facade and `PeerPool` so that
//! the membership component does not need to know how shared skills, rules,
//! workspaces, and memories are serialized or merged.

use std::time::Duration;

use lycoris_client::ClientError;
use lycoris_core::now_ms;
use lycoris_proto::node::Resource;
use lycoris_storage::NodeDomain;
use tokio::time::{self, MissedTickBehavior, timeout};

use super::{RPC_TIMEOUT, peers::targets};
use crate::{resource::ResourceMapper, transport::PeerPool};

/// Drives shared-resource anti-entropy between peers.
#[derive(Debug, Clone)]
pub struct ResourceSync {
  mapper: ResourceMapper,
  node: NodeDomain,
  pool: PeerPool,
}

impl ResourceSync {
  pub fn new(mapper: ResourceMapper, node: NodeDomain, pool: PeerPool) -> Self {
    Self { mapper, node, pool }
  }

  /// Run resource anti-entropy as an independent periodic task (D5/I3).
  ///
  /// Resource sync used to be triggered from the membership anti-entropy path,
  /// which made its liveness depend on Merkle root churn: once the root stopped
  /// changing (e.g. after heartbeats were excluded from the tree hash, D3), the
  /// membership path short-circuits and resource sync would stall. This loop
  /// gives resource anti-entropy its own cadence, decoupled from membership.
  ///
  /// `local_address` is used to exclude the local node from peer selection.
  pub async fn run(&self, interval: Duration, local_address: String) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
      ticker.tick().await;
      self.sync_with_peers(&local_address).await;
    }
  }

  /// Run one anti-entropy round against every candidate peer.
  ///
  /// Resource merging is idempotent, so syncing with all peers is safe. Peer
  /// health bookkeeping (seen/attempt marks, channel eviction) is owned by the
  /// membership paths; this task only logs failures so the two sync planes
  /// stay orthogonal.
  async fn sync_with_peers(&self, local_address: &str) {
    for peer in targets(&self.node, local_address, now_ms()) {
      let _ = self.sync_with_peer(&peer).await;
    }
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
      match timeout(RPC_TIMEOUT, client.sync.sync_resources(local_resources)).await {
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

  /// Merge shared resources pushed by a peer and return the local shared set:
  /// the serving side of resource anti-entropy. Apply failures are logged and
  /// skipped so one corrupt record cannot stall the exchange.
  pub(crate) async fn merge_and_list_shared(
    &self, remote_resources: Vec<Resource>,
  ) -> Vec<Resource> {
    for resource in &remote_resources {
      if let Err(error) = self.mapper.apply_resource(resource).await {
        tracing::warn!(%error, "failed to apply resource during sync");
      }
    }

    match self.mapper.local_shared_resources().await {
      Ok(resources) => resources,
      Err(error) => {
        tracing::warn!(%error, "failed to list local shared resources");
        Vec::new()
      }
    }
  }
}
