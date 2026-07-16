use std::collections::HashMap;

use lycoris_core::{matches_selector, now_ms};
use lycoris_membership::{
  Hash as MerkleHash, MemberRegister, MemberState, MerkleTree, Swim, SwimAction, SwimConfig,
  SwimMessage,
};
use lycoris_proto::node::NodeInfo as ProtoNodeInfo;
use tokio::sync::Mutex;

/// Bridge between the CRDT/SWIM membership layer and the rest of the daemon.
///
/// `MembershipService` owns the authoritative in-memory membership state. It
/// exposes synchronous-style methods for RPC handlers and returns
/// `SwimAction`s that the transport layer must dispatch over the network.
#[derive(Debug)]
pub struct MembershipService {
  local_node_id: String,
  state: Mutex<MembershipState>,
}

#[derive(Debug)]
struct MembershipState {
  swim: Swim,
}

impl MembershipService {
  /// Create a service seeded with the local node register.
  pub fn new(
    local_node_id: impl Into<String>, swim_config: SwimConfig, local: MemberRegister,
  ) -> Self {
    let local_node_id = local_node_id.into();
    let swim = Swim::new(local_node_id.clone(), swim_config, local);
    Self {
      local_node_id,
      state: Mutex::new(MembershipState { swim }),
    }
  }

  /// Return the local node id.
  pub fn local_node_id(&self) -> &str {
    &self.local_node_id
  }

  /// Register or update a node from an RPC request.
  pub async fn register(&self, info: &ProtoNodeInfo) -> Vec<SwimAction> {
    let now = now_ms();
    let mut state = self.state.lock().await;
    state.swim.on_message(
      &self.local_node_id,
      SwimMessage::Alive {
        register: proto_to_register(info, now),
      },
      now,
    )
  }

  /// Mark a node as leaving the cluster.
  pub async fn leave(&self, node_id: &str, now_ms: i64) -> Vec<SwimAction> {
    let mut state = self.state.lock().await;
    let incarnation = state
      .swim
      .membership()
      .get(node_id)
      .map(|register| register.incarnation())
      .unwrap_or(1);

    if node_id == self.local_node_id {
      state.swim.membership_mut().leave(node_id, now_ms);
    } else {
      state.swim.on_message(
        &self.local_node_id,
        SwimMessage::Leave {
          node_id: node_id.to_string(),
          incarnation,
        },
        now_ms,
      );
    }

    vec![SwimAction::Broadcast(SwimMessage::Leave {
      node_id: node_id.to_string(),
      incarnation,
    })]
  }

  /// Return alive nodes that match the optional label selector.
  pub async fn list_nodes(&self, selector: &HashMap<String, String>) -> Vec<ProtoNodeInfo> {
    let state = self.state.lock().await;
    state
      .swim
      .membership()
      .active()
      .into_iter()
      .filter(|register| matches_selector(register.labels(), selector))
      .map(register_to_proto)
      .collect()
  }

  /// Merge a full snapshot from a remote peer and return the local snapshot
  /// after merge. This is the compatibility full-sync path.
  pub async fn sync_nodes(&self, nodes: Vec<ProtoNodeInfo>) -> Vec<ProtoNodeInfo> {
    let now = now_ms();
    let mut state = self.state.lock().await;
    for info in nodes {
      state.swim.on_message(
        &self.local_node_id,
        SwimMessage::Alive {
          register: proto_to_register(&info, now),
        },
        now,
      );
    }
    state
      .swim
      .membership()
      .active()
      .into_iter()
      .map(register_to_proto)
      .collect()
  }

  /// Return the current Merkle root hash.
  pub async fn merkle_root(&self) -> MerkleHash {
    let state = self.state.lock().await;
    MerkleTree::from_membership(state.swim.membership()).root_hash()
  }

  /// Return a snapshot of the current Merkle tree.
  pub async fn merkle_tree_snapshot(&self) -> MerkleTree {
    let state = self.state.lock().await;
    MerkleTree::from_membership(state.swim.membership())
  }

  /// Return hashes and optional leaf contents for a batch of Merkle tree refs.
  ///
  /// Invalid refs (depth > `MERKLE_TREE_DEPTH` or index out of range) are
  /// skipped silently.
  pub async fn merkle_nodes(
    &self, refs: Vec<(u8, u64)>,
  ) -> Vec<(u8, u64, MerkleHash, bool, Vec<(String, MerkleHash)>)> {
    let state = self.state.lock().await;
    let tree = MerkleTree::from_membership(state.swim.membership());
    let empty = lycoris_membership::hash_empty();

    refs
      .into_iter()
      .filter_map(|(depth, index)| {
        if depth > lycoris_membership::MERKLE_TREE_DEPTH {
          return None;
        }
        let max_index = 1u64 << depth;
        if index >= max_index {
          return None;
        }
        let hash = tree.node_hash(depth, index).unwrap_or(empty);
        let is_leaf = depth == lycoris_membership::MERKLE_TREE_DEPTH;
        let entries = if is_leaf {
          tree.leaf_entries(index).unwrap_or_default().to_vec()
        } else {
          Vec::new()
        };
        Some((depth, index, hash, is_leaf, entries))
      })
      .collect()
  }

  /// Return registers for the requested node ids.
  pub async fn fetch_registers(&self, node_ids: &[&str]) -> Vec<ProtoNodeInfo> {
    let state = self.state.lock().await;
    node_ids
      .iter()
      .filter_map(|id| state.swim.membership().get(id))
      .map(register_to_proto)
      .collect()
  }

  /// Process an incoming SWIM probe and return any actions to dispatch.
  pub async fn on_probe(&self, from: &str, message: SwimMessage) -> Vec<SwimAction> {
    let now = now_ms();
    let mut state = self.state.lock().await;
    state.swim.on_message(from, message, now)
  }

  /// Drive the SWIM failure detector and return actions to dispatch.
  pub async fn tick(&self) -> Vec<SwimAction> {
    let now = now_ms();
    let mut state = self.state.lock().await;
    state.swim.tick(now)
  }

  /// Return the address of a known member, if any.
  pub async fn member_address(&self, node_id: &str) -> Option<String> {
    let state = self.state.lock().await;
    state
      .swim
      .membership()
      .get(node_id)
      .map(|register| register.address().to_string())
  }
}

fn proto_to_register(info: &ProtoNodeInfo, now_ms: i64) -> MemberRegister {
  MemberRegister::new(
    info.id.clone(),
    info.address.clone(),
    info.incarnation.max(1),
    info.heartbeat,
  )
  .with_state(parse_state(&info.state))
  .with_labels(info.labels.clone())
  .with_annotations(info.annotations.clone())
  .with_updated_at_ms(now_ms)
}

pub fn register_to_proto(register: &MemberRegister) -> ProtoNodeInfo {
  ProtoNodeInfo {
    id: register.node_id().to_string(),
    address: register.address().to_string(),
    labels: register.labels().clone(),
    annotations: register.annotations().clone(),
    last_heartbeat_unix_ms: register.updated_at_ms(),
    state: state_to_string(register.state()).to_string(),
    incarnation: register.incarnation(),
    heartbeat: register.heartbeat(),
    in_degree: Vec::new(),
    out_degree: Vec::new(),
  }
}

fn state_to_string(state: MemberState) -> &'static str {
  match state {
    MemberState::Active => "active",
    MemberState::Suspected => "suspected",
    MemberState::Leaving => "leaving",
    MemberState::Offline => "offline",
  }
}

fn parse_state(s: &str) -> MemberState {
  match s {
    "active" => MemberState::Active,
    "suspected" => MemberState::Suspected,
    "leaving" => MemberState::Leaving,
    "offline" => MemberState::Offline,
    _ => MemberState::Active,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn register(id: &str) -> MemberRegister {
    MemberRegister::new(id, "127.0.0.1:1", 1, 0).with_updated_at_ms(0)
  }

  fn proto(id: &str) -> ProtoNodeInfo {
    ProtoNodeInfo {
      id: id.to_string(),
      address: "127.0.0.1:1".to_string(),
      labels: HashMap::new(),
      annotations: HashMap::new(),
      last_heartbeat_unix_ms: 0,
      state: "active".to_string(),
      incarnation: 1,
      heartbeat: 0,
      in_degree: Vec::new(),
      out_degree: Vec::new(),
    }
  }

  #[tokio::test]
  async fn register_merges_node() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let _ = service.register(&proto("peer")).await;
    let nodes = service.list_nodes(&HashMap::new()).await;
    assert!(nodes.iter().any(|n| n.id == "peer"));
  }

  #[tokio::test]
  async fn list_nodes_filters_by_selector() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let mut info = proto("peer");
    info.labels.insert("zone".to_string(), "cn".to_string());
    let _ = service.register(&info).await;

    let mut selector = HashMap::new();
    selector.insert("zone".to_string(), "cn".to_string());
    let nodes = service.list_nodes(&selector).await;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id, "peer");
  }

  #[tokio::test]
  async fn sync_nodes_returns_snapshot() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let _ = service.register(&proto("a")).await;
    let snapshot = service.sync_nodes(vec![proto("b")]).await;
    assert!(snapshot.iter().any(|n| n.id == "a"));
    assert!(snapshot.iter().any(|n| n.id == "b"));
  }

  #[tokio::test]
  async fn merkle_root_is_stable() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let _ = service.register(&proto("a")).await;
    let root1 = service.merkle_root().await;
    let root2 = service.merkle_root().await;
    assert_eq!(root1, root2);
  }

  #[tokio::test]
  async fn merkle_nodes_returns_root_and_leaves() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let _ = service.register(&proto("a")).await;

    let root_result = service.merkle_nodes(vec![(0, 0)]).await;
    assert_eq!(root_result.len(), 1);
    assert!(!root_result[0].3);

    let leaf_count = 1u64 << lycoris_membership::MERKLE_TREE_DEPTH;
    let leaf_refs: Vec<(u8, u64)> = (0..leaf_count)
      .map(|i| (lycoris_membership::MERKLE_TREE_DEPTH, i))
      .collect();
    let leaf_results = service.merkle_nodes(leaf_refs).await;
    assert_eq!(leaf_results.len(), leaf_count as usize);
    assert!(leaf_results.iter().all(|r| r.3));
  }

  #[tokio::test]
  async fn merkle_nodes_skips_invalid_refs() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let results = service
      .merkle_nodes(vec![
        (lycoris_membership::MERKLE_TREE_DEPTH + 1, 0),
        (1, 10),
      ])
      .await;
    assert!(results.is_empty());
  }

  #[test]
  fn register_to_proto_preserves_all_states() {
    for (state, expected) in [
      (MemberState::Active, "active"),
      (MemberState::Suspected, "suspected"),
      (MemberState::Leaving, "leaving"),
      (MemberState::Offline, "offline"),
    ] {
      let mut register = register("node");
      register.set_state(state);
      let proto = register_to_proto(&register);
      assert_eq!(proto.state, expected);
      assert_eq!(proto.id, "node");
    }
  }
}
