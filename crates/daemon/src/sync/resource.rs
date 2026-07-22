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

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use lycoris_membership::SwimConfig;
  use lycoris_proto::node::{MemoryBody, ResourceMetadata, ResourceScope, resource::Body};
  use lycoris_storage::{DEFAULT_EMBEDDING_DIM, MemoryEntry, Storage};
  use tempfile::TempDir;

  use super::*;
  use crate::{
    membership::{MemberRegister, MembershipService},
    transport::PeerPool,
  };

  struct TestResourceSync {
    _data_dir: TempDir,
    _tls_dir: TempDir,
    mapper: ResourceMapper,
    sync: ResourceSync,
  }

  fn test_resource_sync() -> TestResourceSync {
    let data_dir = TempDir::new().unwrap();
    let storage = Storage::open(data_dir.path().join("lycoris.redb")).unwrap();
    let node = storage.node().clone();
    let service = Arc::new(MembershipService::new(
      "local",
      SwimConfig::default(),
      MemberRegister::new("local", "127.0.0.1:1", 1, 0),
    ));
    let mapper = ResourceMapper::new(storage, service);

    let (tls_dir, certs) = lycoris_testkit::certs::temp_test_certs(1);
    let tls =
      lycoris_tls::load_tls_bundle(&certs.nodes[0].cert, &certs.nodes[0].key, &certs.ca_cert)
        .unwrap();
    let sync = ResourceSync::new(mapper.clone(), node, PeerPool::new(&tls, None));

    TestResourceSync {
      _data_dir: data_dir,
      _tls_dir: tls_dir,
      mapper,
      sync,
    }
  }

  fn memory_resource(id: &str, content: &[u8], version: u64) -> Resource {
    Resource {
      metadata: Some(ResourceMetadata {
        id: id.to_string(),
        name: id.to_string(),
        kind: lycoris_proto::node::ResourceKind::Memory as i32,
        scope: ResourceScope::ClusterShared as i32,
        source_node_id: "peer".to_string(),
        updated_at_ms: version as i64,
        ..ResourceMetadata::default()
      }),
      body: Some(Body::Memory(MemoryBody {
        content: content.to_vec(),
        content_hash: MemoryEntry::compute_content_hash(content),
        embedding: vec![0.0; DEFAULT_EMBEDDING_DIM],
        version,
        ..MemoryBody::default()
      })),
    }
  }

  fn resource_ids(resources: &[Resource]) -> Vec<String> {
    let mut ids: Vec<_> = resources
      .iter()
      .filter_map(|resource| resource.metadata.as_ref())
      .map(|metadata| metadata.id.clone())
      .collect();
    ids.sort();
    ids
  }

  #[tokio::test]
  async fn merge_and_list_shared_returns_union_and_skips_invalid_resource() {
    let fixture = test_resource_sync();
    fixture
      .mapper
      .apply_resource(&memory_resource("local", b"local", 1))
      .await
      .unwrap();
    let valid = memory_resource("remote", b"remote", 1);
    let mut invalid = memory_resource("invalid", b"invalid", 1);
    let Some(Body::Memory(body)) = invalid.body.as_mut() else {
      panic!("expected memory body");
    };
    body.content_hash = "invalid-hash".to_string();

    let merged = fixture
      .sync
      .merge_and_list_shared(vec![valid, invalid])
      .await;

    assert_eq!(resource_ids(&merged), ["local", "remote"]);
  }

  #[tokio::test]
  async fn merge_and_list_shared_is_idempotent_and_preserves_winner() {
    let fixture = test_resource_sync();
    let winner = memory_resource("memory", b"winner", 2);
    let stale = memory_resource("memory", b"stale", 1);

    fixture
      .sync
      .merge_and_list_shared(vec![winner.clone(), winner, stale])
      .await;
    let merged = fixture.sync.merge_and_list_shared(Vec::new()).await;

    assert_eq!(resource_ids(&merged), ["memory"]);
    let Some(Body::Memory(body)) = merged[0].body.as_ref() else {
      panic!("expected memory body");
    };
    assert_eq!(body.version, 2);
    assert_eq!(body.content, b"winner");
  }
}
