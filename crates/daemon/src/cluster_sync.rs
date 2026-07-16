use std::{
  collections::{HashMap, HashSet, VecDeque},
  hash::Hash,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use lycoris_client::ClientError;
use lycoris_core::now_ms;
use lycoris_proto::node::{
  LeaveMessage as ProtoLeave, NodeInfo as ProtoNodeInfo, ProbeRequest, ProbeResponse,
  PushNodeRequest, PushNodeResponse, StateMessage, SuspectMessage as ProtoSuspect,
  SyncNodesRequest, SyncNodesResponse,
  membership_server::{Membership, MembershipServer},
  sync_server::{Sync, SyncServer},
};
use tokio::{
  sync::Mutex,
  time::{self, MissedTickBehavior, timeout},
};
use tonic::{Request, Response, Status};

use crate::{
  membership::{MembershipService, SwimAction, SwimMessage, register_to_proto},
  resource_sync::ResourceSync,
  transport::PeerPool,
};

/// A fixed-capacity deduplication set with FIFO eviction.
///
/// Used for gossip caches so that long-lived clusters do not grow memory
/// unbounded. Insert returns `true` when the key was newly added.
#[derive(Debug, Clone)]
struct DedupSet<T: Clone + Eq + Hash> {
  inner: HashSet<T>,
  order: VecDeque<T>,
  capacity: usize,
}

impl<T: Clone + Eq + Hash> DedupSet<T> {
  fn new(capacity: usize) -> Self {
    Self {
      inner: HashSet::with_capacity(capacity),
      order: VecDeque::with_capacity(capacity),
      capacity,
    }
  }

  fn insert(&mut self, key: T) -> bool {
    if !self.inner.insert(key.clone()) {
      return false;
    }
    self.order.push_back(key);
    if self.order.len() > self.capacity
      && let Some(oldest) = self.order.pop_front()
    {
      self.inner.remove(&oldest);
    }
    true
  }
}

/// Orchestrates peer-to-peer membership synchronization.
///
/// `ClusterSync` combines backward-compatible `Sync` RPCs, the new `Membership`
/// RPCs (Merkle anti-entropy, SWIM probes), and a background loop that drives
/// the SWIM failure detector. Peer channels and shared-resource sync have been
/// extracted into `PeerPool` and `ResourceSync` respectively.
#[derive(Debug, Clone)]
pub struct ClusterSync {
  local_node_id: String,
  service: Arc<MembershipService>,
  pool: PeerPool,
  resources: ResourceSync,
  seen_pushes: Arc<Mutex<DedupSet<(String, u64)>>>,
  seen_states: Arc<Mutex<DedupSet<(String, u64, u8)>>>,
  sequence: Arc<AtomicU64>,
}

const RPC_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_SEEN_PUSHES: usize = 10_000;
const MAX_SEEN_STATES: usize = 10_000;

impl ClusterSync {
  pub fn new(
    local_node_id: String, service: Arc<MembershipService>, pool: PeerPool, resources: ResourceSync,
  ) -> Self {
    Self {
      local_node_id,
      service,
      pool,
      resources,
      seen_pushes: Arc::new(Mutex::new(DedupSet::new(MAX_SEEN_PUSHES))),
      seen_states: Arc::new(Mutex::new(DedupSet::new(MAX_SEEN_STATES))),
      sequence: Arc::new(AtomicU64::new(1)),
    }
  }

  /// Start background anti-entropy sync and SWIM failure detection.
  pub async fn run(&self, interval: Duration) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
      ticker.tick().await;
      self.sync_with_peers().await;
    }
  }

  pub async fn run_swim(&self, interval: Duration) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
      ticker.tick().await;
      let actions = self.service.tick().await;
      let sync = self.clone();
      tokio::spawn(async move {
        sync.dispatch(actions).await;
      });
    }
  }

  /// Notify peers about a local registry change via push.
  pub async fn push_change(&self, info: ProtoNodeInfo) {
    let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
    let origin = self.local_node_id.clone();

    self
      .seen_pushes
      .lock()
      .await
      .insert((origin.clone(), sequence));

    self.broadcast_push(info, origin, sequence).await;
  }

  /// Dispatch a batch of SWIM actions produced by the membership service.
  pub async fn dispatch(&self, actions: Vec<SwimAction>) {
    for action in actions {
      match action {
        SwimAction::SendPing { target, seq } => {
          let _ = self.send_probe_to(&target, seq).await;
        }
        SwimAction::SendAck { .. } => {
          // Acks are returned inline in response to a Probe RPC.
        }
        SwimAction::Broadcast(SwimMessage::Alive { register }) => {
          let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
          let origin = self.local_node_id.clone();
          self
            .seen_pushes
            .lock()
            .await
            .insert((origin.clone(), sequence));
          self
            .broadcast_push(register_to_proto(&register), origin, sequence)
            .await;
        }
        SwimAction::Broadcast(SwimMessage::Suspect {
          node_id,
          incarnation,
        }) => {
          self
            .seen_states
            .lock()
            .await
            .insert((node_id.clone(), incarnation, 1));
          self
            .broadcast_state_message(StateMessage {
              payload: Some(lycoris_proto::node::state_message::Payload::Suspect(
                ProtoSuspect {
                  node_id,
                  incarnation,
                },
              )),
            })
            .await;
        }
        SwimAction::Broadcast(SwimMessage::Leave {
          node_id,
          incarnation,
        }) => {
          self
            .seen_states
            .lock()
            .await
            .insert((node_id.clone(), incarnation, 2));
          self
            .broadcast_state_message(StateMessage {
              payload: Some(lycoris_proto::node::state_message::Payload::Leave(
                ProtoLeave {
                  node_id,
                  incarnation,
                },
              )),
            })
            .await;
        }
        SwimAction::Broadcast(_) => {}
      }
    }
  }

  async fn send_probe_to(&self, target_id: &str, seq: u64) -> bool {
    let address = match self.resolve_address(target_id).await {
      Some(addr) => addr,
      None => return false,
    };

    let mut client = match self.pool.connect(&address).await {
      Ok(client) => client,
      Err(_) => {
        self.pool.remove(&address).await;
        return false;
      }
    };

    match timeout(RPC_TIMEOUT, client.membership.probe(seq, "")).await {
      Ok(Ok(response)) => {
        if response.ack {
          self
            .service
            .on_probe(target_id, SwimMessage::Ack { seq })
            .await;
        }
        response.ack
      }
      Ok(Err(error)) => {
        tracing::warn!(%target_id, %error, "probe failed");
        self.pool.remove(&address).await;
        false
      }
      Err(_) => {
        tracing::warn!(%target_id, "probe timed out");
        self.pool.remove(&address).await;
        false
      }
    }
  }

  async fn resolve_address(&self, node_id: &str) -> Option<String> {
    self.service.member_address(node_id).await
  }

  async fn sync_with_peers(&self) {
    let snapshot = self.service.list_nodes(&HashMap::new()).await;
    let local_address = self.local_address().await.unwrap_or_default();

    if let Some(primary) = self.pool.node().peers().get_primary().unwrap_or(None) {
      match timeout(RPC_TIMEOUT, self.sync_with_peer(&primary, snapshot.clone())).await {
        Ok(Ok(())) => return,
        Ok(Err(error)) => {
          tracing::warn!(%primary, %error, "primary endpoint unreachable, trying fallbacks");
          self.pool.remove(&primary).await;
        }
        Err(_) => {
          tracing::warn!(%primary, "primary endpoint timed out, trying fallbacks");
          self.pool.remove(&primary).await;
        }
      }
    }

    let fallbacks = self
      .pool
      .node()
      .peers()
      .fallback_addresses()
      .unwrap_or_default();
    let mut join_set = tokio::task::JoinSet::new();
    for peer in fallbacks {
      let sync = self.clone();
      let snapshot = snapshot.clone();
      join_set.spawn(async move {
        let result = timeout(RPC_TIMEOUT, sync.sync_with_peer(&peer, snapshot)).await;
        (peer, result)
      });
    }

    let mut promoted = false;
    while let Some(result) = join_set.join_next().await {
      match result {
        Ok((peer, Ok(Ok(())))) => {
          if !promoted && local_address != peer {
            if let Err(error) = self.pool.node().peers().set_primary(&peer) {
              tracing::warn!(%peer, %error, "failed to promote fallback to primary");
            }
            promoted = true;
          }
        }
        Ok((peer, Ok(Err(error)))) => {
          tracing::warn!(%peer, %error, "fallback peer sync failed");
          self.pool.remove(&peer).await;
        }
        Ok((peer, Err(_))) => {
          tracing::warn!(%peer, "fallback peer sync timed out");
          self.pool.remove(&peer).await;
        }
        Err(error) => {
          tracing::warn!(%error, "sync task panicked");
        }
      }
    }
  }

  async fn sync_with_peer(
    &self, peer: &str, _snapshot: Vec<ProtoNodeInfo>,
  ) -> Result<(), ClientError> {
    let mut client = self.pool.connect(peer).await?;
    let local_address = self.local_address().await.unwrap_or_default();

    let remote_root = match timeout(RPC_TIMEOUT, client.membership.merkle_root()).await {
      Ok(Ok(root)) => root,
      Ok(Err(error)) => {
        tracing::warn!(%peer, %error, "merkle root failed, falling back to full sync");
        return timeout(RPC_TIMEOUT, self.full_sync_with_peer(peer))
          .await
          .map_err(|_| {
            ClientError::Io(std::io::Error::new(
              std::io::ErrorKind::TimedOut,
              "peer connection timed out",
            ))
          })
          .and_then(|result| result);
      }
      Err(_) => {
        tracing::warn!(%peer, "merkle root timed out, falling back to full sync");
        return timeout(RPC_TIMEOUT, self.full_sync_with_peer(peer))
          .await
          .map_err(|_| {
            ClientError::Io(std::io::Error::new(
              std::io::ErrorKind::TimedOut,
              "peer connection timed out",
            ))
          })
          .and_then(|result| result);
      }
    };

    let local_root = self.service.merkle_root().await;
    if remote_root == local_root.to_vec() {
      let now = now_ms();
      let _ = self.pool.node().peers().mark_seen(peer, now);
      return Ok(());
    }

    let (need_from_remote, need_from_local) = self
      .merkle_diff_with_peer(&mut client, peer, remote_root)
      .await?;

    let fetched = if need_from_remote.is_empty() {
      Vec::new()
    } else {
      client.membership.fetch_registers(need_from_remote).await?
    };

    let local_registers = self
      .service
      .fetch_registers(
        &need_from_local
          .iter()
          .map(String::as_str)
          .collect::<Vec<_>>(),
      )
      .await;

    if !local_registers.is_empty()
      && let Err(error) = client.membership.push_registers(local_registers).await
    {
      tracing::warn!(%peer, %error, "failed to push local registers");
    }

    if !fetched.is_empty() {
      let _ = self.service.sync_nodes(fetched).await;
    }

    for info in self.service.list_nodes(&HashMap::new()).await {
      if info.address != local_address {
        let _ = self.pool.node().peers().seed(&info.address);
      }
    }

    let _ = self.pool.node().peers().mark_seen(peer, now_ms());
    let _ = self.resources.sync_with_peer(peer).await;
    Ok(())
  }

  async fn merkle_diff_with_peer(
    &self, client: &mut lycoris_client::PeerClient, peer: &str, _remote_root: Vec<u8>,
  ) -> Result<(Vec<String>, Vec<String>), ClientError> {
    use lycoris_membership::{MERKLE_TREE_DEPTH, hash_empty};

    const SPLIT_DEPTH: u8 = 8;

    let mut need_from_remote = Vec::new();
    let mut need_from_local = Vec::new();

    let tree = self.service.merkle_tree_snapshot().await;

    // First RPC: fetch the top half of the tree (down to SPLIT_DEPTH).
    let top_refs = subtree_refs(0, 0, SPLIT_DEPTH);
    let top_response = self.request_merkle_nodes(client, peer, top_refs).await?;
    let top_remote = build_remote_map(top_response);

    // Find bottom-half subtrees that differ.
    let mut bottom_refs = Vec::new();
    for index in 0..(1u64 << SPLIT_DEPTH) {
      let local_hash = tree
        .node_hash(SPLIT_DEPTH, index)
        .unwrap_or_else(|| tree.empty_subtree_hash(SPLIT_DEPTH).unwrap_or(hash_empty()));
      let remote_hash = top_remote
        .get(&(SPLIT_DEPTH, index))
        .unwrap_or_else(|| tree.empty_subtree_hash(SPLIT_DEPTH).unwrap_or(hash_empty()));

      if local_hash == remote_hash {
        continue;
      }

      // Optimization: if the remote side is empty under this subtree, all local
      // IDs need to be pushed.
      let remote_empty = tree
        .empty_subtree_hash(SPLIT_DEPTH)
        .map(|empty| remote_hash == empty)
        .unwrap_or(false);
      if remote_empty {
        need_from_local.extend(tree.collect_node_ids(SPLIT_DEPTH, index));
        continue;
      }

      bottom_refs.extend(subtree_refs(SPLIT_DEPTH, index, MERKLE_TREE_DEPTH));
    }

    // Second RPC: fetch all differing bottom subtrees.
    let bottom_response = if bottom_refs.is_empty() {
      lycoris_proto::node::MerkleNodesResponse {
        results: Vec::new(),
      }
    } else {
      self.request_merkle_nodes(client, peer, bottom_refs).await?
    };
    let bottom_remote = build_remote_map(bottom_response);

    // Diff leaves in the bottom subtrees.
    for index in 0..(1u64 << SPLIT_DEPTH) {
      let local_hash = tree
        .node_hash(SPLIT_DEPTH, index)
        .unwrap_or_else(|| tree.empty_subtree_hash(SPLIT_DEPTH).unwrap_or(hash_empty()));
      let remote_hash = top_remote
        .get(&(SPLIT_DEPTH, index))
        .unwrap_or_else(|| tree.empty_subtree_hash(SPLIT_DEPTH).unwrap_or(hash_empty()));

      if local_hash == remote_hash {
        continue;
      }

      let remote_empty = tree
        .empty_subtree_hash(SPLIT_DEPTH)
        .map(|empty| remote_hash == empty)
        .unwrap_or(false);
      if remote_empty {
        continue;
      }

      for leaf in 0..(1u64 << (MERKLE_TREE_DEPTH - SPLIT_DEPTH)) {
        let leaf_index = (index << (MERKLE_TREE_DEPTH - SPLIT_DEPTH)) | leaf;
        let local_leaf_hash = tree
          .node_hash(MERKLE_TREE_DEPTH, leaf_index)
          .unwrap_or_else(hash_empty);
        let remote_leaf_hash = bottom_remote
          .get(&(MERKLE_TREE_DEPTH, leaf_index))
          .unwrap_or_else(hash_empty);
        if local_leaf_hash == remote_leaf_hash {
          continue;
        }
        if let Some(entries) = bottom_remote.entries.get(&(MERKLE_TREE_DEPTH, leaf_index)) {
          self.diff_leaf_entries(
            &tree,
            leaf_index,
            entries,
            &mut need_from_remote,
            &mut need_from_local,
          );
        } else {
          // Remote leaf is empty; all local IDs need to be pushed.
          need_from_local.extend(
            tree
              .leaf_entries(leaf_index)
              .unwrap_or_default()
              .iter()
              .map(|(id, _)| id.clone()),
          );
        }
      }
    }

    need_from_remote.sort();
    need_from_remote.dedup();
    need_from_local.sort();
    need_from_local.dedup();

    Ok((need_from_remote, need_from_local))
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
        Err(ClientError::Io(std::io::Error::new(
          std::io::ErrorKind::TimedOut,
          "merkle nodes request timed out",
        )))
      }
    }
  }
}

fn subtree_refs(depth: u8, index: u64, max_depth: u8) -> Vec<lycoris_proto::node::MerkleNodeRef> {
  let mut refs = Vec::new();
  let mut stack = vec![(depth, index)];
  while let Some((d, i)) = stack.pop() {
    refs.push(lycoris_proto::node::MerkleNodeRef {
      depth: d as u32,
      index: i,
    });
    if d < max_depth {
      stack.push((d + 1, 2 * i + 1));
      stack.push((d + 1, 2 * i));
    }
  }
  refs
}

struct RemoteNodes {
  hashes: std::collections::HashMap<(u8, u64), lycoris_membership::Hash>,
  entries: std::collections::HashMap<(u8, u64), Vec<lycoris_proto::node::MerkleLeafEntry>>,
}

impl RemoteNodes {
  fn get(&self, key: &(u8, u64)) -> Option<lycoris_membership::Hash> {
    self.hashes.get(key).copied()
  }
}

fn build_remote_map(response: lycoris_proto::node::MerkleNodesResponse) -> RemoteNodes {
  use lycoris_membership::{MERKLE_TREE_DEPTH, hash_empty};

  let mut hashes = std::collections::HashMap::new();
  let mut entries = std::collections::HashMap::new();
  for result in response.results {
    let depth = result
      .node
      .as_ref()
      .map(|n| n.depth as u8)
      .unwrap_or(MERKLE_TREE_DEPTH);
    let index = result.node.as_ref().map(|n| n.index).unwrap_or(0);
    let hash = match result.hash.try_into() {
      Ok(hash) => hash,
      Err(_) => hash_empty(),
    };
    hashes.insert((depth, index), hash);
    if result.is_leaf || depth == MERKLE_TREE_DEPTH {
      entries.insert((depth, index), result.entries);
    }
  }
  RemoteNodes { hashes, entries }
}

impl ClusterSync {
  fn diff_leaf_entries(
    &self, tree: &lycoris_membership::MerkleTree, index: u64,
    remote_entries: &[lycoris_proto::node::MerkleLeafEntry], need_from_remote: &mut Vec<String>,
    need_from_local: &mut Vec<String>,
  ) {
    use lycoris_membership::hash_empty;

    let local_entries = tree.leaf_entries(index).unwrap_or_default();
    let mut remote_sorted: Vec<(String, lycoris_membership::Hash)> = remote_entries
      .iter()
      .map(|entry| {
        let hash = entry.hash.clone().try_into().unwrap_or(hash_empty());
        (entry.node_id.clone(), hash)
      })
      .collect();
    remote_sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut i = 0usize;
    let mut j = 0usize;

    while i < local_entries.len() && j < remote_sorted.len() {
      match local_entries[i].0.cmp(&remote_sorted[j].0) {
        std::cmp::Ordering::Less => {
          need_from_local.push(local_entries[i].0.clone());
          i += 1;
        }
        std::cmp::Ordering::Greater => {
          need_from_remote.push(remote_sorted[j].0.clone());
          j += 1;
        }
        std::cmp::Ordering::Equal => {
          if local_entries[i].1 != remote_sorted[j].1 {
            need_from_remote.push(local_entries[i].0.clone());
            need_from_local.push(local_entries[i].0.clone());
          }
          i += 1;
          j += 1;
        }
      }
    }

    while i < local_entries.len() {
      need_from_local.push(local_entries[i].0.clone());
      i += 1;
    }
    while j < remote_sorted.len() {
      need_from_remote.push(remote_sorted[j].0.clone());
      j += 1;
    }
  }

  async fn full_sync_with_peer(&self, peer: &str) -> Result<(), ClientError> {
    let mut client = self.pool.connect(peer).await?;
    let snapshot = self.service.list_nodes(&HashMap::new()).await;
    let response = client.sync.sync_nodes(snapshot).await?;
    let _ = self.service.sync_nodes(response.nodes).await;
    let local_address = self.local_address().await.unwrap_or_default();

    for info in self.service.list_nodes(&HashMap::new()).await {
      if info.address != local_address {
        let _ = self.pool.node().peers().seed(&info.address);
      }
    }

    let _ = self.pool.node().peers().mark_seen(peer, now_ms());
    let _ = self.resources.sync_with_peer(peer).await;
    Ok(())
  }

  async fn broadcast_push(&self, info: ProtoNodeInfo, origin: String, sequence: u64) {
    let local_address = self.local_address().await.unwrap_or_default();
    let targets = self.pool.targets(&local_address);
    for peer in targets {
      let info = info.clone();
      let origin = origin.clone();
      match timeout(
        RPC_TIMEOUT,
        self.push_to_peer(&peer, info, origin, sequence),
      )
      .await
      {
        Ok(Ok(())) => {
          let _ = self.pool.node().peers().mark_seen(&peer, now_ms());
        }
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "push to peer failed");
          let _ = self.pool.node().peers().mark_attempt(&peer, false);
          self.pool.remove(&peer).await;
        }
        Err(_) => {
          tracing::warn!(%peer, "push to peer timed out");
          let _ = self.pool.node().peers().mark_attempt(&peer, false);
          self.pool.remove(&peer).await;
        }
      }
    }
  }

  async fn push_to_peer(
    &self, peer: &str, info: ProtoNodeInfo, origin: String, sequence: u64,
  ) -> Result<(), ClientError> {
    let mut client = self.pool.connect(peer).await?;
    client.sync.push_node(info, origin, sequence).await?;
    Ok(())
  }

  async fn broadcast_state_message(&self, message: StateMessage) {
    let local_address = self.local_address().await.unwrap_or_default();
    let targets = self.pool.targets(&local_address);
    for peer in targets {
      let message = message.clone();
      match timeout(RPC_TIMEOUT, self.send_state_message_to_peer(&peer, message)).await {
        Ok(Ok(())) => {
          let _ = self.pool.node().peers().mark_seen(&peer, now_ms());
        }
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "state message to peer failed");
          let _ = self.pool.node().peers().mark_attempt(&peer, false);
          self.pool.remove(&peer).await;
        }
        Err(_) => {
          tracing::warn!(%peer, "state message to peer timed out");
          let _ = self.pool.node().peers().mark_attempt(&peer, false);
          self.pool.remove(&peer).await;
        }
      }
    }
  }

  async fn send_state_message_to_peer(
    &self, peer: &str, message: StateMessage,
  ) -> Result<(), ClientError> {
    let mut client = self.pool.connect(peer).await?;
    client.membership.state(message).await?;
    Ok(())
  }

  async fn local_address(&self) -> Option<String> {
    self.service.member_address(&self.local_node_id).await
  }

  /// Handle an incoming `SyncNodes` RPC: merge remote state and return our
  /// snapshot.
  #[allow(clippy::result_large_err)]
  pub async fn handle_sync_nodes(
    &self, request: Request<SyncNodesRequest>,
  ) -> Result<Response<SyncNodesResponse>, Status> {
    let nodes = request.into_inner().nodes;
    let local_address = self.local_address().await.unwrap_or_default();
    for info in &nodes {
      if info.address != local_address {
        let _ = self.pool.node().peers().seed(&info.address);
      }
    }
    let snapshot = self.service.sync_nodes(nodes).await;
    Ok(Response::new(SyncNodesResponse { nodes: snapshot }))
  }

  /// Handle an incoming `PushNode` RPC: deduplicate, merge, and forward.
  #[allow(clippy::result_large_err)]
  pub async fn handle_push_node(
    &self, request: Request<PushNodeRequest>,
  ) -> Result<Response<PushNodeResponse>, Status> {
    let push = request.into_inner();
    let info = push
      .info
      .ok_or_else(|| Status::invalid_argument("missing node info"))?;
    let key = (push.origin_node_id.clone(), push.sequence);

    let is_new = self.seen_pushes.lock().await.insert(key);
    if !is_new {
      return Ok(Response::new(PushNodeResponse { accepted: true }));
    }

    let local_address = self.local_address().await.unwrap_or_default();
    if info.address != local_address {
      let _ = self.pool.node().peers().seed(&info.address);
    }
    let actions = self.service.register(&info).await;

    let origin = push.origin_node_id;
    let sequence = push.sequence;
    let sync = self.clone();
    tokio::spawn(async move {
      sync.dispatch(actions).await;
      sync.broadcast_push(info, origin, sequence).await;
    });

    Ok(Response::new(PushNodeResponse { accepted: true }))
  }

  /// Handle an incoming SWIM probe.
  #[allow(clippy::result_large_err)]
  pub async fn handle_probe(
    &self, request: Request<ProbeRequest>,
  ) -> Result<Response<ProbeResponse>, Status> {
    let probe = request.into_inner();
    let from = self.local_node_id.clone();

    let ack = if probe.target.is_empty() {
      let actions = self
        .service
        .on_probe(&from, SwimMessage::Ping { seq: probe.seq })
        .await;
      let ack = actions
        .iter()
        .any(|action| matches!(action, SwimAction::SendAck { .. }));
      let sync = self.clone();
      tokio::spawn(async move {
        sync.dispatch_non_acks(actions).await;
      });
      ack
    } else if probe.target == self.local_node_id {
      true
    } else {
      self.send_probe_to(&probe.target, probe.seq).await
    };

    Ok(Response::new(ProbeResponse {
      ack,
      seq: probe.seq,
    }))
  }

  /// Handle an incoming state message.
  #[allow(clippy::result_large_err)]
  pub async fn handle_state_message(
    &self, request: Request<StateMessage>,
  ) -> Result<Response<lycoris_proto::node::StateResponse>, Status> {
    let from = self.local_node_id.clone();
    let message = request.into_inner();

    // Deduplicate gossiped Suspect/Leave/Alive state messages to prevent them
    // from cycling around the graph forever.
    let state_key = match &message.payload {
      Some(lycoris_proto::node::state_message::Payload::Alive(info)) => {
        Some((info.id.clone(), info.incarnation, 0u8))
      }
      Some(lycoris_proto::node::state_message::Payload::Suspect(suspect)) => {
        Some((suspect.node_id.clone(), suspect.incarnation, 1u8))
      }
      Some(lycoris_proto::node::state_message::Payload::Leave(leave)) => {
        Some((leave.node_id.clone(), leave.incarnation, 2u8))
      }
      None => None,
    };

    if let Some(key) = state_key {
      let already_seen = !self.seen_states.lock().await.insert(key);
      if already_seen {
        return Ok(Response::new(lycoris_proto::node::StateResponse {
          accepted: true,
        }));
      }
    }

    let actions = match message.payload {
      Some(lycoris_proto::node::state_message::Payload::Alive(info)) => {
        self.service.register(&info).await
      }
      Some(lycoris_proto::node::state_message::Payload::Suspect(suspect)) => {
        self
          .service
          .on_probe(
            &from,
            SwimMessage::Suspect {
              node_id: suspect.node_id,
              incarnation: suspect.incarnation,
            },
          )
          .await
      }
      Some(lycoris_proto::node::state_message::Payload::Leave(leave)) => {
        self
          .service
          .on_probe(
            &from,
            SwimMessage::Leave {
              node_id: leave.node_id,
              incarnation: leave.incarnation,
            },
          )
          .await
      }
      None => Vec::new(),
    };
    let sync = self.clone();
    tokio::spawn(async move {
      sync.dispatch(actions).await;
    });
    Ok(Response::new(lycoris_proto::node::StateResponse {
      accepted: true,
    }))
  }

  async fn dispatch_non_acks(&self, actions: Vec<SwimAction>) {
    let non_acks: Vec<SwimAction> = actions
      .into_iter()
      .filter(|action| !matches!(action, SwimAction::SendAck { .. }))
      .collect();
    self.dispatch(non_acks).await;
  }
}

pub type SyncServerHandle = SyncServer<ClusterSync>;
pub type MembershipServerHandle = MembershipServer<ClusterSync>;

impl ClusterSync {
  pub fn into_servers(self) -> (SyncServerHandle, MembershipServerHandle) {
    (SyncServer::new(self.clone()), MembershipServer::new(self))
  }
}

#[tonic::async_trait]
#[allow(clippy::result_large_err)]
impl Sync for ClusterSync {
  async fn sync_nodes(
    &self, request: Request<SyncNodesRequest>,
  ) -> Result<Response<SyncNodesResponse>, Status> {
    self.handle_sync_nodes(request).await
  }

  async fn push_node(
    &self, request: Request<PushNodeRequest>,
  ) -> Result<Response<PushNodeResponse>, Status> {
    self.handle_push_node(request).await
  }

  async fn sync_resources(
    &self, request: Request<lycoris_proto::node::SyncResourcesRequest>,
  ) -> Result<Response<lycoris_proto::node::SyncResourcesResponse>, Status> {
    self.resources.handle_sync_resources(request).await
  }
}

#[tonic::async_trait]
#[allow(clippy::result_large_err)]
impl Membership for ClusterSync {
  async fn merkle_root(
    &self, _request: Request<lycoris_proto::node::MerkleRootRequest>,
  ) -> Result<Response<lycoris_proto::node::MerkleRootResponse>, Status> {
    let root = self.service.merkle_root().await;
    Ok(Response::new(lycoris_proto::node::MerkleRootResponse {
      root_hash: root.to_vec(),
    }))
  }

  async fn merkle_nodes(
    &self, request: Request<lycoris_proto::node::MerkleNodesRequest>,
  ) -> Result<Response<lycoris_proto::node::MerkleNodesResponse>, Status> {
    let refs = request
      .into_inner()
      .nodes
      .into_iter()
      .map(|node| (node.depth as u8, node.index))
      .collect();
    let results = self.service.merkle_nodes(refs).await;
    Ok(Response::new(lycoris_proto::node::MerkleNodesResponse {
      results: results
        .into_iter()
        .map(
          |(depth, index, hash, is_leaf, entries)| lycoris_proto::node::MerkleNodeResult {
            node: Some(lycoris_proto::node::MerkleNodeRef {
              depth: depth as u32,
              index,
            }),
            hash: hash.to_vec(),
            is_leaf,
            entries: entries
              .into_iter()
              .map(|(node_id, hash)| lycoris_proto::node::MerkleLeafEntry {
                node_id,
                hash: hash.to_vec(),
              })
              .collect(),
          },
        )
        .collect(),
    }))
  }

  async fn fetch_registers(
    &self, request: Request<lycoris_proto::node::FetchRegistersRequest>,
  ) -> Result<Response<lycoris_proto::node::FetchRegistersResponse>, Status> {
    let inner = request.into_inner();
    let node_ids: Vec<&str> = inner.node_ids.iter().map(String::as_str).collect();
    let registers = self.service.fetch_registers(&node_ids).await;
    Ok(Response::new(lycoris_proto::node::FetchRegistersResponse {
      registers,
    }))
  }

  async fn push_registers(
    &self, request: Request<lycoris_proto::node::PushRegistersRequest>,
  ) -> Result<Response<lycoris_proto::node::FetchRegistersResponse>, Status> {
    let registers = request.into_inner().registers;
    let _ = self.service.sync_nodes(registers).await;
    Ok(Response::new(lycoris_proto::node::FetchRegistersResponse {
      registers: Vec::new(),
    }))
  }

  async fn probe(&self, request: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
    self.handle_probe(request).await
  }

  async fn state(
    &self, request: Request<StateMessage>,
  ) -> Result<Response<lycoris_proto::node::StateResponse>, Status> {
    self.handle_state_message(request).await
  }
}
