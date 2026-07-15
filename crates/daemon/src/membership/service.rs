use std::collections::HashMap;

use lycoris_api::proto::NodeInfo as ProtoNodeInfo;
use lycoris_core::time::now_ms;
use tokio::sync::Mutex;

use crate::membership::{
  MemberRegister, MemberState, Membership, MerkleTree,
  merkle::Hash as MerkleHash,
  swim::{Swim, SwimAction, SwimConfig, SwimMessage},
};

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

  /// Record a heartbeat from an RPC request.
  pub async fn heartbeat(&self, info: &ProtoNodeInfo) -> Vec<SwimAction> {
    self.register(info).await
  }

  /// Mark a node as leaving the cluster.
  pub async fn leave(&self, node_id: &str, now_ms: i64) -> Vec<SwimAction> {
    let mut state = self.state.lock().await;
    let incarnation = state
      .swim
      .membership()
      .get(node_id)
      .map(|register| register.incarnation)
      .unwrap_or(1);

    if node_id == self.local_node_id {
      if let Some(register) = state.swim.membership_mut().get_mut(node_id) {
        register.leave(now_ms);
      }
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
      .filter(|register| matches_selector(&register.labels, selector))
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

  /// Return the current Merkle root hash and all leaf hashes.
  pub async fn merkle_root(&self) -> MerkleRoot {
    let state = self.state.lock().await;
    let tree = MerkleTree::from_membership(state.swim.membership());
    MerkleRoot {
      root_hash: tree.root_hash().to_vec(),
      leaf_hashes: tree
        .leaf_hashes()
        .into_iter()
        .map(|(id, hash)| (id, hash.to_vec()))
        .collect(),
    }
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
      .map(|register| register.address.clone())
  }

  /// Compute the symmetric Merkle diff against a remote leaf set.
  ///
  /// Returns the node ids missing or differing on each side:
  /// `(need_from_remote, need_from_local)`.
  pub async fn merkle_diff(
    &self, remote_leaves: &[(String, MerkleHash)],
  ) -> (Vec<String>, Vec<String>) {
    let mut local_leaves: Vec<(String, MerkleHash)> = {
      let state = self.state.lock().await;
      state
        .swim
        .membership()
        .all()
        .into_iter()
        .map(|register| (register.node_id.clone(), hash_register(register)))
        .collect()
    };
    local_leaves.sort_by(|a, b| a.0.cmp(&b.0));

    let mut remote_sorted = remote_leaves.to_vec();
    remote_sorted.sort_by(|a, b| a.0.cmp(&b.0));

    diff_leaf_sets(&local_leaves, &remote_sorted)
  }
}

/// Result of a Merkle root query.
#[derive(Debug, Clone)]
pub struct MerkleRoot {
  pub root_hash: Vec<u8>,
  pub leaf_hashes: Vec<(String, Vec<u8>)>,
}

fn matches_selector(labels: &HashMap<String, String>, selector: &HashMap<String, String>) -> bool {
  if selector.is_empty() {
    return true;
  }
  selector
    .iter()
    .all(|(key, value)| labels.get(key) == Some(value))
}

fn diff_leaf_sets(
  local: &[(String, MerkleHash)], remote: &[(String, MerkleHash)],
) -> (Vec<String>, Vec<String>) {
  let mut need_from_remote = Vec::new();
  let mut need_from_local = Vec::new();
  let mut i = 0usize;
  let mut j = 0usize;

  while i < local.len() && j < remote.len() {
    match local[i].0.cmp(&remote[j].0) {
      std::cmp::Ordering::Less => {
        need_from_local.push(local[i].0.clone());
        i += 1;
      }
      std::cmp::Ordering::Greater => {
        need_from_remote.push(remote[j].0.clone());
        j += 1;
      }
      std::cmp::Ordering::Equal => {
        if local[i].1 != remote[j].1 {
          need_from_remote.push(local[i].0.clone());
          need_from_local.push(local[i].0.clone());
        }
        i += 1;
        j += 1;
      }
    }
  }

  while i < local.len() {
    need_from_local.push(local[i].0.clone());
    i += 1;
  }
  while j < remote.len() {
    need_from_remote.push(remote[j].0.clone());
    j += 1;
  }

  (need_from_remote, need_from_local)
}

fn proto_to_register(info: &ProtoNodeInfo, now_ms: i64) -> MemberRegister {
  let mut register = MemberRegister::new(
    info.id.clone(),
    info.address.clone(),
    info.incarnation.max(1),
    info.heartbeat,
  );
  register.state = parse_state(&info.state);
  register.labels = info.labels.clone();
  register.annotations = info.annotations.clone();
  register.updated_at_ms = now_ms;
  register
}

pub fn register_to_proto(register: &MemberRegister) -> ProtoNodeInfo {
  ProtoNodeInfo {
    id: register.node_id.clone(),
    address: register.address.clone(),
    labels: register.labels.clone(),
    annotations: register.annotations.clone(),
    last_heartbeat_unix_ms: register.updated_at_ms,
    state: state_to_string(register.state).to_string(),
    incarnation: register.incarnation,
    heartbeat: register.heartbeat,
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

fn hash_register(register: &MemberRegister) -> MerkleHash {
  MerkleTree::from_membership(&{
    let mut m = Membership::new();
    m.merge_register(register);
    m
  })
  .root_hash()
}

#[cfg(test)]
mod tests {
  use super::*;

  fn register(id: &str) -> MemberRegister {
    let mut r = MemberRegister::new(id, "127.0.0.1:1", 1, 0);
    r.updated_at_ms = 0;
    r
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
  async fn heartbeat_behaves_like_register() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let _ = service.heartbeat(&proto("peer")).await;
    let nodes = service.list_nodes(&HashMap::new()).await;
    assert!(nodes.iter().any(|n| n.id == "peer"));
  }

  #[tokio::test]
  async fn merkle_diff_detects_missing_and_differing_nodes() {
    let local = register("local");
    let service = MembershipService::new("local", SwimConfig::default(), local.clone());
    let _ = service.register(&proto("a")).await;
    let _ = service.register(&proto("b")).await;

    let mut remote_only = proto("c");
    remote_only.address = "127.0.0.1:3".to_string();

    let mut remote_b = proto("b");
    remote_b.address = "127.0.0.1:9".to_string();

    let remote_leaves = vec![
      (
        "b".to_string(),
        hash_register(&proto_to_register(&remote_b, 0)),
      ),
      (
        "c".to_string(),
        hash_register(&proto_to_register(&remote_only, 0)),
      ),
    ];

    let (need_from_remote, need_from_local) = service.merkle_diff(&remote_leaves).await;
    assert!(need_from_remote.contains(&"b".to_string()));
    assert!(need_from_remote.contains(&"c".to_string()));
    assert!(need_from_local.contains(&"a".to_string()));
    assert!(need_from_local.contains(&"b".to_string()));
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
      register.state = state;
      let proto = register_to_proto(&register);
      assert_eq!(proto.state, expected);
      assert_eq!(proto.id, "node");
    }
  }
}
