//! Membership anti-entropy: the Merkle diff orchestration, the compatibility
//! full-sync path, and the serving side of the register-exchange RPCs.
//!
//! The diff algorithm itself lives in `lycoris_membership` (D8: the algorithm
//! is pure and transport-free); this module only drives the RPC round trips
//! and converts between wire types and membership types.

use std::collections::HashMap;

use lycoris_client::ClientError;
use lycoris_core::now_ms;
use lycoris_membership::{DiffResult, MERKLE_TREE_DEPTH, MerkleDiff, NodeRef, RemoteNode};
use lycoris_proto::node::NodeInfo as ProtoNodeInfo;
use tokio::time::timeout;

use super::{ClusterSync, RPC_TIMEOUT, peers::targets};
use crate::membership::convert::{proto_to_register, register_to_proto};

impl ClusterSync {
  pub(super) async fn sync_with_peers(&self) {
    let local_address = self.local_address().await.unwrap_or_default();
    let candidates = targets(&self.node, &local_address, now_ms());

    let Some((preferred, fallbacks)) = candidates.split_first() else {
      self.retry_seeds_when_isolated(&local_address).await;
      return;
    };

    // The policy's top choice gets a sequential attempt first: in the common
    // case (healthy primary) one RPC round suffices.
    if self.sync_attempt(preferred).await {
      self.stick_primary(preferred, &local_address);
      return;
    }

    let mut join_set = tokio::task::JoinSet::new();
    for peer in fallbacks {
      let sync = self.clone();
      let peer = peer.clone();
      join_set.spawn(async move {
        let reachable = sync.sync_attempt(&peer).await;
        (peer, reachable)
      });
    }

    let mut connected = false;
    while let Some(result) = join_set.join_next().await {
      match result {
        Ok((peer, true)) => {
          if !connected {
            self.stick_primary(&peer, &local_address);
          }
          connected = true;
        }
        Ok((_, false)) => {}
        Err(error) => {
          tracing::warn!(%error, "sync task panicked");
        }
      }
    }

    if !connected {
      self.retry_seeds_when_isolated(&local_address).await;
    }
  }

  /// Sync with one endpoint, with reachability bookkeeping: success is
  /// recorded by `sync_with_peer` (`mark_seen`); failure is recorded here
  /// (`mark_attempt`) so the selection policy backs off from recently-failed
  /// endpoints.
  ///
  /// There is deliberately no timeout around the whole exchange: every peer
  /// RPC inside `sync_with_peer` carries its own `RPC_TIMEOUT`, and an outer
  /// bound would fire before the inner ones, making the per-RPC fallback
  /// branches (e.g. Merkle timeout -> full sync) unreachable.
  async fn sync_attempt(&self, peer: &str) -> bool {
    match self.sync_with_peer(peer).await {
      Ok(()) => true,
      Err(error) => {
        tracing::warn!(%peer, %error, "peer sync failed");
        self.record_peer_failure(peer).await;
        false
      }
    }
  }

  /// Make `peer` the stored primary when it is not already. Failover must
  /// stick: otherwise every round would waste its first attempt on a dead
  /// primary once the backoff window lapses.
  fn stick_primary(&self, peer: &str, local_address: &str) {
    let current = self.node.peers().get_primary().unwrap_or(None);
    if current.as_deref() == Some(peer) {
      return;
    }
    if let Err(error) = self.node.peers().set_primary(peer, local_address) {
      tracing::warn!(%peer, %error, "failed to promote peer to primary");
    }
  }

  /// Minimum-connectivity guard: the vision requires every node to keep at
  /// least one connection. Fires when a sync round found no reachable peer —
  /// either selection returned nothing (every known endpoint is in failure
  /// backoff) or every attempt failed — and retries the complete known seed
  /// set once, stopping at the first endpoint that answers.
  ///
  /// Bounded to one pass per sync cycle, so a partitioned node polls its
  /// seeds at cycle cadence instead of storming them, and heals within one
  /// cycle of reconnection (I2). A node that knows no peers at all has
  /// nothing to retry — single-node clusters are legitimate and stay quiet.
  async fn retry_seeds_when_isolated(&self, local_address: &str) {
    let seeds: Vec<String> = self
      .node
      .peers()
      .known_addresses()
      .unwrap_or_default()
      .into_iter()
      .filter(|address| address != local_address)
      .collect();
    if seeds.is_empty() {
      return;
    }

    tracing::warn!(
      seeds = seeds.len(),
      "node isolated: no reachable peer, retrying known seeds"
    );
    for seed in seeds {
      if self.sync_attempt(&seed).await {
        self.stick_primary(&seed, local_address);
        return;
      }
    }
  }

  async fn sync_with_peer(&self, peer: &str) -> Result<(), ClientError> {
    let mut client = self.pool.connect(peer).await?;
    let local_address = self.local_address().await.unwrap_or_default();

    let remote_root = match timeout(RPC_TIMEOUT, client.membership.merkle_root()).await {
      Ok(Ok(root)) => root,
      Ok(Err(error)) => {
        tracing::warn!(%peer, %error, "merkle root failed, falling back to full sync");
        return self.full_sync_with_timeout(peer).await;
      }
      Err(_) => {
        tracing::warn!(%peer, "merkle root timed out, falling back to full sync");
        return self.full_sync_with_timeout(peer).await;
      }
    };

    let local_root = self.service.merkle_root().await;
    if remote_root == local_root.to_vec() {
      let now = now_ms();
      let _ = self.node.peers().mark_seen(peer, now);
      return Ok(());
    }

    let diff = self.merkle_diff_with_peer(&mut client, peer).await?;

    let fetched = if diff.need_from_remote.is_empty() {
      Vec::new()
    } else {
      match timeout(
        RPC_TIMEOUT,
        client.membership.fetch_registers(diff.need_from_remote),
      )
      .await
      {
        Ok(result) => result?,
        Err(_) => return Err(crate::peer_timeout("fetch registers")),
      }
    };

    let local_registers = self
      .service
      .fetch_registers(
        &diff
          .need_from_local
          .iter()
          .map(String::as_str)
          .collect::<Vec<_>>(),
      )
      .await;
    let local_registers: Vec<_> = local_registers.iter().map(register_to_proto).collect();

    if !local_registers.is_empty() {
      // Pushing is best-effort: the peer pulls the same registers on its next
      // diff round, so a failure only costs propagation delay.
      match timeout(
        RPC_TIMEOUT,
        client.membership.push_registers(local_registers),
      )
      .await
      {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%peer, %error, "failed to push local registers"),
        Err(_) => tracing::warn!(%peer, "pushing local registers timed out"),
      }
    }

    if !fetched.is_empty() {
      let (_, actions) = self
        .service
        .sync_nodes(fetched.iter().map(proto_to_register).collect())
        .await;
      self.spawn_dispatch(actions).await;
    }

    self.seed_known_peers(&local_address).await;

    let _ = self.node.peers().mark_seen(peer, now_ms());
    Ok(())
  }

  /// Run the two-round Merkle diff against `peer`.
  ///
  /// The diff logic itself lives in `lycoris_membership` (D8: the algorithm
  /// is pure and transport-free); this method only drives the RPC round
  /// trips and converts between wire types and membership types.
  async fn merkle_diff_with_peer(
    &self, client: &mut lycoris_client::PeerClient, peer: &str,
  ) -> Result<DiffResult, ClientError> {
    let tree = self.service.merkle_tree_snapshot().await;
    let mut diff = MerkleDiff::new(&tree);

    // Round one: compare the top of the tree down to SPLIT_DEPTH.
    let top = self
      .request_merkle_nodes(client, peer, proto_refs(MerkleDiff::top_refs()))
      .await?;
    let leaf_refs = diff.plan_leaf_refs(remote_nodes(top.results, peer));

    // Round two: fetch and diff the leaf buckets of the divergent subtrees.
    // No leaf refs means the diff already resolved during round one.
    let bottom = if leaf_refs.is_empty() {
      Vec::new()
    } else {
      let response = self
        .request_merkle_nodes(client, peer, proto_refs(leaf_refs))
        .await?;
      remote_nodes(response.results, peer)
    };

    Ok(diff.finish(bottom))
  }

  async fn request_merkle_nodes(
    &self, client: &mut lycoris_client::PeerClient, peer: &str,
    refs: Vec<lycoris_proto::node::MerkleNodeRef>,
  ) -> Result<lycoris_proto::node::MerkleNodesResponse, ClientError> {
    let request = lycoris_proto::node::MerkleNodesRequest { nodes: refs };
    match timeout(RPC_TIMEOUT, client.membership.merkle_nodes(request)).await {
      Ok(Ok(response)) => Ok(response),
      Ok(Err(error)) => {
        tracing::warn!(%peer, %error, "merkle nodes failed");
        Err(error)
      }
      Err(_) => {
        tracing::warn!(%peer, "merkle nodes timed out");
        Err(crate::peer_timeout("merkle nodes request"))
      }
    }
  }

  /// Full-sync fallback used when the peer cannot serve the Merkle protocol,
  /// bounded by the RPC timeout.
  async fn full_sync_with_timeout(&self, peer: &str) -> Result<(), ClientError> {
    timeout(RPC_TIMEOUT, self.full_sync_with_peer(peer))
      .await
      .map_err(|_| crate::peer_timeout("full sync"))
      .and_then(|result| result)
  }

  async fn full_sync_with_peer(&self, peer: &str) -> Result<(), ClientError> {
    let mut client = self.pool.connect(peer).await?;
    let snapshot = self.service.list_nodes(&HashMap::new()).await;
    let snapshot: Vec<_> = snapshot.iter().map(register_to_proto).collect();
    let response = client.sync.sync_nodes(snapshot).await?;
    let (_, actions) = self
      .service
      .sync_nodes(response.nodes.iter().map(proto_to_register).collect())
      .await;
    self.spawn_dispatch(actions).await;
    let local_address = self.local_address().await.unwrap_or_default();
    self.seed_known_peers(&local_address).await;

    let _ = self.node.peers().mark_seen(peer, now_ms());
    Ok(())
  }

  /// Seed every known member address (except the local one) into the peer
  /// bookkeeping, so endpoints learned through membership become sync
  /// candidates.
  async fn seed_known_peers(&self, local_address: &str) {
    for register in self.service.list_nodes(&HashMap::new()).await {
      if register.address() != local_address {
        let _ = self.node.peers().seed(register.address());
      }
    }
  }

  /// Serve an incoming `SyncNodes` exchange: seed the peer's endpoints, merge
  /// its registers, and return the local snapshot after the merge.
  pub async fn serve_sync_nodes(&self, nodes: Vec<ProtoNodeInfo>) -> Vec<ProtoNodeInfo> {
    let local_address = self.local_address().await.unwrap_or_default();
    for info in &nodes {
      if info.address != local_address {
        let _ = self.node.peers().seed(&info.address);
      }
    }
    let (snapshot, actions) = self
      .service
      .sync_nodes(nodes.iter().map(proto_to_register).collect())
      .await;
    self.spawn_dispatch(actions).await;
    snapshot.iter().map(register_to_proto).collect()
  }

  /// Serve the local Merkle root hash.
  pub async fn serve_merkle_root(&self) -> lycoris_membership::Hash {
    self.service.merkle_root().await
  }

  /// Answer a batch of Merkle node refs against the local tree.
  pub async fn serve_merkle_nodes(
    &self, refs: Vec<lycoris_proto::node::MerkleNodeRef>,
  ) -> Vec<lycoris_proto::node::MerkleNodeResult> {
    // Depths that do not fit a u8 are invalid refs; skip them like any other
    // out-of-range ref instead of truncating.
    let refs = refs
      .into_iter()
      .filter_map(|node| {
        Some(NodeRef {
          depth: u8::try_from(node.depth).ok()?,
          index: node.index,
        })
      })
      .collect();
    let results = self.service.merkle_nodes(refs).await;
    results
      .into_iter()
      .map(|node| lycoris_proto::node::MerkleNodeResult {
        node: Some(lycoris_proto::node::MerkleNodeRef {
          depth: u32::from(node.depth),
          index: node.index,
        }),
        hash: node.hash.to_vec(),
        entries: node
          .entries
          .unwrap_or_default()
          .into_iter()
          .map(|(node_id, hash)| lycoris_proto::node::MerkleLeafEntry {
            node_id,
            hash: hash.to_vec(),
          })
          .collect(),
      })
      .collect()
  }

  /// Serve the registers for the requested node ids.
  pub async fn serve_fetch_registers(&self, node_ids: Vec<String>) -> Vec<ProtoNodeInfo> {
    let node_ids: Vec<&str> = node_ids.iter().map(String::as_str).collect();
    let registers = self.service.fetch_registers(&node_ids).await;
    registers.iter().map(register_to_proto).collect()
  }

  /// Merge registers pushed by a peer.
  pub async fn serve_push_registers(&self, registers: Vec<ProtoNodeInfo>) {
    let (_, actions) = self
      .service
      .sync_nodes(registers.iter().map(proto_to_register).collect())
      .await;
    self.spawn_dispatch(actions).await;
  }
}

/// Convert membership node refs into the wire representation.
fn proto_refs(refs: Vec<NodeRef>) -> Vec<lycoris_proto::node::MerkleNodeRef> {
  refs
    .into_iter()
    .map(|node_ref| lycoris_proto::node::MerkleNodeRef {
      depth: u32::from(node_ref.depth),
      index: node_ref.index,
    })
    .collect()
}

/// Convert wire results into the membership diff representation.
///
/// Malformed results (missing ref, out-of-range depth, hash that is not 32
/// bytes) are logged and dropped: the diff treats missing nodes as empty
/// subtrees, so garbage degrades to over-exchange instead of divergence.
fn remote_nodes(
  results: Vec<lycoris_proto::node::MerkleNodeResult>, peer: &str,
) -> Vec<RemoteNode> {
  results
    .into_iter()
    .filter_map(|result| {
      let Some(node) = result.node else {
        tracing::warn!(%peer, "merkle node result without a ref, skipping");
        return None;
      };
      let Ok(depth) = u8::try_from(node.depth) else {
        tracing::warn!(%peer, depth = node.depth, "merkle node depth out of range, skipping");
        return None;
      };
      let Ok(hash) = <[u8; 32]>::try_from(result.hash.as_slice()) else {
        tracing::warn!(%peer, "merkle node hash is not 32 bytes, skipping");
        return None;
      };
      let entries = if depth == MERKLE_TREE_DEPTH {
        Some(
          result
            .entries
            .into_iter()
            .filter_map(|entry| match <[u8; 32]>::try_from(entry.hash.as_slice()) {
              Ok(hash) => Some((entry.node_id, hash)),
              Err(_) => {
                tracing::warn!(%peer, node_id = %entry.node_id, "merkle leaf entry hash is not 32 bytes, skipping");
                None
              }
            })
            .collect(),
        )
      } else {
        None
      };
      Some(RemoteNode {
        depth,
        index: node.index,
        hash,
        entries,
      })
    })
    .collect()
}
