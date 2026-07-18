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
      if let Err(error) = self.node.peers().mark_seen(peer, now) {
        tracing::warn!(%peer, %error, "failed to mark peer seen");
      }
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

    if let Err(error) = self.node.peers().mark_seen(peer, now_ms()) {
      tracing::warn!(%peer, %error, "failed to mark peer seen");
    }
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

    if let Err(error) = self.node.peers().mark_seen(peer, now_ms()) {
      tracing::warn!(%peer, %error, "failed to mark peer seen");
    }
    Ok(())
  }

  /// Seed every known member address (except the local one) into the peer
  /// bookkeeping, so endpoints learned through membership become sync
  /// candidates.
  async fn seed_known_peers(&self, local_address: &str) {
    for register in self.service.list_nodes(&HashMap::new()).await {
      if register.address() != local_address
        && let Err(error) = self.node.peers().seed(register.address())
      {
        tracing::warn!(address = %register.address(), %error, "failed to seed known peer");
      }
    }
  }

  /// Serve an incoming `SyncNodes` exchange: seed the peer's endpoints, merge
  /// its registers, and return the local snapshot after the merge.
  pub async fn serve_sync_nodes(&self, nodes: Vec<ProtoNodeInfo>) -> Vec<ProtoNodeInfo> {
    let local_address = self.local_address().await.unwrap_or_default();
    for info in &nodes {
      if info.address != local_address
        && let Err(error) = self.node.peers().seed(&info.address)
      {
        tracing::warn!(address = %info.address, %error, "failed to seed peer address");
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

#[cfg(test)]
mod tests {
  use std::{sync::Arc, time::Duration};

  use lycoris_membership::{MemberRegister, SwimConfig};
  use lycoris_proto::node::{
    FetchRegistersRequest, FetchRegistersResponse, MerkleLeafEntry, MerkleNodeRef,
    MerkleNodeResult, MerkleNodesRequest, MerkleNodesResponse, MerkleRootRequest,
    MerkleRootResponse, ProbeRequest, ProbeResponse, PushNodeRequest, PushNodeResponse,
    PushRegistersRequest, PushRegistersResponse, StateMessage, StateResponse, SyncNodesRequest,
    SyncNodesResponse, SyncResourcesRequest, SyncResourcesResponse,
    membership_server::{Membership, MembershipServer},
    sync_server::{Sync, SyncServer},
  };
  use lycoris_storage::{NodeDomain, Storage};
  use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
  use tempfile::TempDir;
  use tonic::{Request, Response, Status};

  use super::*;
  use crate::{
    membership::MembershipService, resource::ResourceMapper, sync::ResourceSync,
    transport::PeerPool,
  };

  fn wire_result(depth: u32, index: u64, hash_len: usize) -> MerkleNodeResult {
    MerkleNodeResult {
      node: Some(MerkleNodeRef { depth, index }),
      hash: vec![7; hash_len],
      entries: Vec::new(),
    }
  }

  #[test]
  fn remote_nodes_converts_well_formed_results() {
    let nodes = remote_nodes(vec![wire_result(3, 5, 32)], "peer");

    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].depth, 3);
    assert_eq!(nodes[0].index, 5);
    assert_eq!(nodes[0].hash, [7; 32]);
    assert_eq!(
      nodes[0].entries, None,
      "entries are only kept at leaf depth"
    );
  }

  #[test]
  fn remote_nodes_drops_result_without_ref() {
    let result = MerkleNodeResult {
      node: None,
      hash: vec![7; 32],
      entries: Vec::new(),
    };

    assert!(remote_nodes(vec![result], "peer").is_empty());
  }

  #[test]
  fn remote_nodes_drops_depth_that_does_not_fit_u8() {
    let nodes = remote_nodes(vec![wire_result(300, 0, 32)], "peer");

    assert!(nodes.is_empty());
  }

  #[test]
  fn remote_nodes_drops_non_32_byte_hash() {
    for hash_len in [0, 31, 33] {
      let nodes = remote_nodes(vec![wire_result(1, 0, hash_len)], "peer");
      assert!(nodes.is_empty(), "hash length {hash_len} must be dropped");
    }
  }

  #[test]
  fn remote_nodes_keeps_leaf_but_drops_malformed_entries() {
    let mut result = wire_result(u32::from(MERKLE_TREE_DEPTH), 42, 32);
    result.entries = vec![
      MerkleLeafEntry {
        node_id: "good".to_string(),
        hash: vec![9; 32],
      },
      MerkleLeafEntry {
        node_id: "bad".to_string(),
        hash: vec![9; 3],
      },
    ];

    let nodes = remote_nodes(vec![result], "peer");

    assert_eq!(nodes.len(), 1);
    let entries = nodes[0].entries.clone().unwrap_or_default();
    assert_eq!(entries, vec![("good".to_string(), [9; 32])]);
  }

  #[test]
  fn remote_nodes_ignores_entries_below_leaf_depth() {
    let mut result = wire_result(2, 0, 32);
    result.entries = vec![MerkleLeafEntry {
      node_id: "stray".to_string(),
      hash: vec![9; 32],
    }];

    let nodes = remote_nodes(vec![result], "peer");

    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].entries, None);
  }

  /// A peer that predates the Merkle protocol: Merkle RPCs fail or hang while
  /// the legacy full-sync exchange keeps working.
  #[derive(Debug, Clone)]
  struct LegacyPeer {
    hang_on_merkle: bool,
    snapshot: Vec<ProtoNodeInfo>,
  }

  #[tonic::async_trait]
  #[allow(clippy::result_large_err)]
  impl Membership for LegacyPeer {
    async fn merkle_root(
      &self, _request: Request<MerkleRootRequest>,
    ) -> Result<Response<MerkleRootResponse>, Status> {
      if self.hang_on_merkle {
        tokio::time::sleep(Duration::from_secs(60)).await;
      }
      Err(Status::unimplemented("merkle protocol not supported"))
    }

    async fn merkle_nodes(
      &self, _request: Request<MerkleNodesRequest>,
    ) -> Result<Response<MerkleNodesResponse>, Status> {
      Err(Status::unimplemented("merkle protocol not supported"))
    }

    async fn fetch_registers(
      &self, _request: Request<FetchRegistersRequest>,
    ) -> Result<Response<FetchRegistersResponse>, Status> {
      Err(Status::unimplemented("merkle protocol not supported"))
    }

    async fn push_registers(
      &self, _request: Request<PushRegistersRequest>,
    ) -> Result<Response<PushRegistersResponse>, Status> {
      Err(Status::unimplemented("merkle protocol not supported"))
    }

    async fn probe(
      &self, _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
      Err(Status::unimplemented("swim not supported"))
    }

    async fn state(
      &self, _request: Request<StateMessage>,
    ) -> Result<Response<StateResponse>, Status> {
      Err(Status::unimplemented("swim not supported"))
    }
  }

  #[tonic::async_trait]
  #[allow(clippy::result_large_err)]
  impl Sync for LegacyPeer {
    async fn sync_nodes(
      &self, _request: Request<SyncNodesRequest>,
    ) -> Result<Response<SyncNodesResponse>, Status> {
      Ok(Response::new(SyncNodesResponse {
        nodes: self.snapshot.clone(),
      }))
    }

    async fn push_node(
      &self, _request: Request<PushNodeRequest>,
    ) -> Result<Response<PushNodeResponse>, Status> {
      Err(Status::unimplemented("gossip not supported"))
    }

    async fn sync_resources(
      &self, _request: Request<SyncResourcesRequest>,
    ) -> Result<Response<SyncResourcesResponse>, Status> {
      Ok(Response::new(SyncResourcesResponse {
        resources: Vec::new(),
      }))
    }
  }

  /// Generate a test CA and one node identity; the returned bundle serves as
  /// both server and client credentials since both ends trust the same CA.
  fn test_tls(dir: &std::path::Path) -> lycoris_tls::TlsBundle {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec!["lycoris-test-ca".to_string()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_path = dir.join("ca.crt");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();

    let key = KeyPair::generate().unwrap();
    let params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    let cert = params.signed_by(&key, &ca_cert, &ca_key).unwrap();
    let cert_path = dir.join("node.crt");
    let key_path = dir.join("node.key");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();

    lycoris_tls::load_tls_bundle(&cert_path, &key_path, &ca_path).unwrap()
  }

  /// Serve `peer` over mTLS on an ephemeral loopback port, polling until the
  /// listener accepts connections instead of assuming a fixed startup delay.
  async fn serve_legacy_peer(
    peer: LegacyPeer, tls: &lycoris_tls::TlsBundle,
  ) -> (String, tokio::task::JoinHandle<()>) {
    // Reserve an ephemeral port, then release it for the tonic server; the
    // readiness probe below covers the small rebind race.
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
      .unwrap()
      .local_addr()
      .unwrap();
    let server_tls = tls.server_config();
    let handle = tokio::spawn(async move {
      let result = tonic::transport::Server::builder()
        .tls_config(server_tls)
        .unwrap()
        .add_service(SyncServer::new(peer.clone()))
        .add_service(MembershipServer::new(peer))
        .serve(addr)
        .await;
      if let Err(error) = result {
        eprintln!("legacy peer server failed: {error}");
      }
    });

    let address = format!("https://{addr}");
    let start = std::time::Instant::now();
    loop {
      match lycoris_client::PeerClient::connect(&address, tls).await {
        Ok(_) => return (address, handle),
        Err(error) if start.elapsed() >= Duration::from_secs(10) => {
          panic!("legacy peer never came up: {error}");
        }
        Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
      }
    }
  }

  /// A real `ClusterSync` wired against temporary storage, mirroring the
  /// assembly in `crate::runtime`.
  struct TestNode {
    _dir: TempDir,
    sync: ClusterSync,
    service: Arc<MembershipService>,
    node: NodeDomain,
  }

  fn test_node(tls: &lycoris_tls::TlsBundle) -> TestNode {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("test.redb")).unwrap();
    let node = storage.node().clone();
    let local = MemberRegister::new("local", "https://127.0.0.1:1", 1, 0);
    let service = Arc::new(MembershipService::new(
      "local",
      SwimConfig::default(),
      local,
    ));
    let mapper = ResourceMapper::new(storage.clone(), service.clone());
    let pool = PeerPool::new(tls);
    let resources = ResourceSync::new(mapper, node.clone(), pool.clone());
    let sync = ClusterSync::new(
      "local".to_string(),
      service.clone(),
      node.clone(),
      pool,
      resources,
    );
    TestNode {
      _dir: dir,
      sync,
      service,
      node,
    }
  }

  async fn run_full_sync_fallback(hang_on_merkle: bool) {
    let _ = lycoris_tls::install_crypto_provider();
    let tls_dir = TempDir::new().unwrap();
    let tls = test_tls(tls_dir.path());

    let legacy = LegacyPeer {
      hang_on_merkle,
      snapshot: vec![ProtoNodeInfo::new(
        "legacy-node",
        "https://127.0.0.1:9",
        HashMap::new(),
        HashMap::new(),
      )],
    };
    let (address, server) = serve_legacy_peer(legacy, &tls).await;

    let node = test_node(&tls);
    node.sync.sync_with_peer(&address).await.unwrap();

    // The full-sync fallback merges the legacy peer's snapshot...
    assert_eq!(
      node.service.member_address("legacy-node").await.as_deref(),
      Some("https://127.0.0.1:9")
    );
    // ...and the exchange counts as a successful contact for the peer
    // bookkeeping.
    let records = node.node.peers().records().unwrap();
    let record = records
      .iter()
      .find(|record| record.address == address)
      .unwrap();
    assert!(record.online);

    server.abort();
  }

  #[tokio::test]
  async fn merkle_unsupported_falls_back_to_full_sync() {
    run_full_sync_fallback(false).await;
  }

  #[tokio::test]
  async fn merkle_timeout_falls_back_to_full_sync() {
    run_full_sync_fallback(true).await;
  }
}
