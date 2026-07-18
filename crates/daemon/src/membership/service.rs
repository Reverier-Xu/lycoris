use std::collections::HashMap;

use lycoris_core::now_ms;
use lycoris_membership::{
  Hash as MerkleHash, MemberRegister, MerkleTree, NodeRef, RemoteNode, Swim, SwimAction,
  SwimConfig, SwimMessage, answer_refs,
};
use lycoris_storage::MetaStorage;
use tokio::sync::Mutex;

use crate::selector::matches_selector;

/// Meta-table key under which the local register's incarnation is persisted
/// (P5b restart monotonicity: a restarted node must resume its incarnation,
/// not rewind to 1 and lose the merge dominance it already earned).
pub const LOCAL_INCARNATION_KEY: &str = "local_incarnation";

/// Bridge between the CRDT/SWIM membership layer and the rest of the daemon.
///
/// `MembershipService` owns the authoritative in-memory membership state. Its
/// public API speaks only domain types (`MemberRegister` and friends, D8);
/// proto conversions live in [`crate::membership::convert`] at the call
/// boundaries. It exposes synchronous-style methods for RPC handlers and
/// returns `SwimAction`s that the transport layer must dispatch over the
/// network.
#[derive(Debug)]
pub struct MembershipService {
  local_node_id: String,
  state: Mutex<MembershipState>,
}

#[derive(Debug)]
struct MembershipState {
  swim: Swim,
  /// Cached Merkle tree keyed by the membership mutation version. The tree is
  /// expensive to build (2^17 nodes), so it is rebuilt only when a mutation
  /// advanced the version. Lives inside the same mutex as `swim`, so cache
  /// access never takes an additional lock.
  cached_tree: (u64, MerkleTree),
  /// Node meta table used to persist the local incarnation on change (P5b),
  /// plus the last value written, so the persist hook only writes when the
  /// incarnation actually advanced.
  meta: Option<MetaStorage>,
  persisted_incarnation: u64,
}

impl MembershipState {
  fn new(swim: Swim, local_node_id: &str) -> Self {
    let version = swim.membership().version();
    let tree = MerkleTree::from_membership(swim.membership());
    let persisted_incarnation = swim
      .membership()
      .get(local_node_id)
      .map(|register| register.incarnation())
      .unwrap_or(1);
    Self {
      swim,
      cached_tree: (version, tree),
      meta: None,
      persisted_incarnation,
    }
  }

  /// Return the cached Merkle tree, rebuilding it only when the membership
  /// version advanced since the last build.
  fn merkle_tree(&mut self) -> &MerkleTree {
    let version = self.swim.membership().version();
    if self.cached_tree.0 != version {
      self.cached_tree = (version, MerkleTree::from_membership(self.swim.membership()));
    }
    &self.cached_tree.1
  }

  /// Persist the local incarnation when a mutation advanced it (P5b restart
  /// monotonicity). A no-op when the value is unchanged, so callers run it
  /// after every mutation — including `tick` — without write amplification.
  fn persist_local_incarnation(&mut self, local_node_id: &str) {
    let Some(meta) = &self.meta else { return };
    let Some(local) = self.swim.membership().get(local_node_id) else {
      return;
    };
    let incarnation = local.incarnation();
    if incarnation == self.persisted_incarnation {
      return;
    }
    match meta.set(LOCAL_INCARNATION_KEY, &incarnation.to_string()) {
      Ok(()) => self.persisted_incarnation = incarnation,
      Err(error) => {
        tracing::warn!(%error, incarnation, "failed to persist local incarnation");
      }
    }
  }
}

impl MembershipService {
  /// Create a service seeded with the local node register.
  pub fn new(
    local_node_id: impl Into<String>, swim_config: SwimConfig, local: MemberRegister,
  ) -> Self {
    let local_node_id = local_node_id.into();
    let swim = Swim::new(local_node_id.clone(), swim_config, local);
    Self {
      state: Mutex::new(MembershipState::new(swim, &local_node_id)),
      local_node_id,
    }
  }

  /// Attach the node meta table used to persist the local incarnation on
  /// change (P5b). The runtime wires this up after loading the boot value.
  pub fn with_meta(mut self, meta: MetaStorage) -> Self {
    self.state.get_mut().meta = Some(meta);
    self
  }

  /// Return the local node id.
  pub fn local_node_id(&self) -> &str {
    &self.local_node_id
  }

  /// Register or update a node from an incoming register.
  ///
  /// `updated_at_ms` is deliberately overwritten with the local clock: merging
  /// a remote register is a local view update, and the field is republished to
  /// peers as `last_heartbeat_unix_ms`, so it must come from a clock we trust.
  /// It is excluded from both the merge order and the Merkle hash; the
  /// Suspected -> Offline timer runs on local observation times tracked inside
  /// the SWIM state machine, not on this field.
  pub async fn register(&self, register: MemberRegister) -> Vec<SwimAction> {
    let now = now_ms();
    let mut state = self.state.lock().await;
    let actions = state.swim.on_message(
      &self.local_node_id,
      SwimMessage::Alive {
        register: register.with_updated_at_ms(now),
      },
      now,
    );
    state.persist_local_incarnation(&self.local_node_id);
    actions
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
    state.persist_local_incarnation(&self.local_node_id);

    vec![SwimAction::Broadcast(SwimMessage::Leave {
      node_id: node_id.to_string(),
      incarnation,
    })]
  }

  /// Return alive nodes that match the optional label selector.
  pub async fn list_nodes(&self, selector: &HashMap<String, String>) -> Vec<MemberRegister> {
    let state = self.state.lock().await;
    state
      .swim
      .membership()
      .active()
      .into_iter()
      .filter(|register| matches_selector(register.labels(), selector))
      .cloned()
      .collect()
  }

  /// Merge a full snapshot from a remote peer and return the local snapshot
  /// after merge, plus any SWIM actions produced along the way (D4: loading a
  /// rumor about the local node may trigger a refutation broadcast that the
  /// caller must dispatch). `updated_at_ms` of every merged register is
  /// overwritten with the local clock; see [`Self::register`]. This is the
  /// compatibility full-sync path.
  pub async fn sync_nodes(
    &self, registers: Vec<MemberRegister>,
  ) -> (Vec<MemberRegister>, Vec<SwimAction>) {
    let now = now_ms();
    let mut state = self.state.lock().await;
    let mut actions = Vec::new();
    for register in registers {
      actions.extend(state.swim.on_message(
        &self.local_node_id,
        SwimMessage::Alive {
          register: register.with_updated_at_ms(now),
        },
        now,
      ));
    }
    let snapshot: Vec<_> = state
      .swim
      .membership()
      .active()
      .into_iter()
      .cloned()
      .collect();
    state.persist_local_incarnation(&self.local_node_id);
    (snapshot, actions)
  }

  /// Return the current Merkle root hash.
  pub async fn merkle_root(&self) -> MerkleHash {
    let mut state = self.state.lock().await;
    state.merkle_tree().root_hash()
  }

  /// Return a snapshot of the current Merkle tree.
  pub async fn merkle_tree_snapshot(&self) -> MerkleTree {
    let mut state = self.state.lock().await;
    state.merkle_tree().clone()
  }

  /// Answer a batch of Merkle node refs against the cached tree: the serving
  /// side of the anti-entropy diff protocol (`lycoris_membership::answer_refs`
  /// holds the single implementation; invalid refs are skipped silently).
  pub async fn merkle_nodes(&self, refs: Vec<NodeRef>) -> Vec<RemoteNode> {
    let mut state = self.state.lock().await;
    answer_refs(state.merkle_tree(), &refs)
  }

  /// Return registers for the requested node ids.
  pub async fn fetch_registers(&self, node_ids: &[&str]) -> Vec<MemberRegister> {
    let state = self.state.lock().await;
    node_ids
      .iter()
      .filter_map(|id| state.swim.membership().get(id))
      .cloned()
      .collect()
  }

  /// Process an incoming SWIM probe and return any actions to dispatch.
  pub async fn on_probe(&self, from: &str, message: SwimMessage) -> Vec<SwimAction> {
    let now = now_ms();
    let mut state = self.state.lock().await;
    let actions = state.swim.on_message(from, message, now);
    state.persist_local_incarnation(&self.local_node_id);
    actions
  }

  /// Drive the SWIM failure detector and return actions to dispatch.
  pub async fn tick(&self) -> Vec<SwimAction> {
    let now = now_ms();
    let mut state = self.state.lock().await;
    let actions = state.swim.tick(now);
    state.persist_local_incarnation(&self.local_node_id);
    actions
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

#[cfg(test)]
mod tests {
  use lycoris_membership::MemberState;

  use super::*;

  fn register(id: &str) -> MemberRegister {
    MemberRegister::new(id, "127.0.0.1:1", 1, 0).with_updated_at_ms(0)
  }

  #[tokio::test]
  async fn register_merges_node() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let _ = service.register(register("peer")).await;
    let nodes = service.list_nodes(&HashMap::new()).await;
    assert!(nodes.iter().any(|n| n.node_id() == "peer"));
  }

  #[tokio::test]
  async fn list_nodes_filters_by_selector() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let mut peer = register("peer");
    peer.set_labels(HashMap::from([("zone".to_string(), "cn".to_string())]));
    let _ = service.register(peer).await;

    let mut selector = HashMap::new();
    selector.insert("zone".to_string(), "cn".to_string());
    let nodes = service.list_nodes(&selector).await;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].node_id(), "peer");
  }

  #[tokio::test]
  async fn sync_nodes_returns_snapshot() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let _ = service.register(register("a")).await;
    let (snapshot, actions) = service.sync_nodes(vec![register("b")]).await;
    assert!(actions.is_empty());
    assert!(snapshot.iter().any(|n| n.node_id() == "a"));
    assert!(snapshot.iter().any(|n| n.node_id() == "b"));
  }

  #[tokio::test]
  async fn sync_nodes_refutes_offline_rumor_about_local() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));

    // Anti-entropy loads an Offline rumor about the local node (D4): the
    // service must refute it and hand the caller a broadcast to dispatch.
    let rumor = register("local").with_state(MemberState::Offline);
    let (_, actions) = service.sync_nodes(vec![rumor]).await;

    assert!(
      actions.iter().any(|a| matches!(
        a,
        SwimAction::Broadcast(SwimMessage::Alive { register })
          if register.node_id() == "local" && register.state() == MemberState::Active
      )),
      "expected a refutation broadcast, got {actions:?}"
    );

    let nodes = service.list_nodes(&HashMap::new()).await;
    let local = nodes.iter().find(|n| n.node_id() == "local").unwrap();
    assert_eq!(local.state(), MemberState::Active);
    assert_eq!(local.incarnation(), 2);
  }

  #[tokio::test]
  async fn merkle_root_ignores_local_heartbeat_bumps() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let root_before = service.merkle_root().await;

    // The SWIM tick bumps the local heartbeat every round (D2); the cached
    // tree must not rebuild and the root must not change (D3).
    let _ = service.tick().await;
    assert_eq!(service.merkle_root().await, root_before);

    // A real membership change rebuilds the cached tree.
    let _ = service.register(register("peer")).await;
    assert_ne!(service.merkle_root().await, root_before);
  }

  #[tokio::test]
  async fn merkle_root_is_stable() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let _ = service.register(register("a")).await;
    let root1 = service.merkle_root().await;
    let root2 = service.merkle_root().await;
    assert_eq!(root1, root2);
  }

  #[tokio::test]
  async fn merkle_nodes_returns_root_and_leaves() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let _ = service.register(register("a")).await;

    let root_result = service
      .merkle_nodes(vec![NodeRef { depth: 0, index: 0 }])
      .await;
    assert_eq!(root_result.len(), 1);
    assert!(root_result[0].entries.is_none());

    let leaf_count = 1u64 << lycoris_membership::MERKLE_TREE_DEPTH;
    let leaf_refs: Vec<NodeRef> = (0..leaf_count)
      .map(|index| NodeRef {
        depth: lycoris_membership::MERKLE_TREE_DEPTH,
        index,
      })
      .collect();
    let leaf_results = service.merkle_nodes(leaf_refs).await;
    assert_eq!(leaf_results.len(), leaf_count as usize);
    assert!(leaf_results.iter().all(|r| r.entries.is_some()));
  }

  #[tokio::test]
  async fn merkle_nodes_skips_invalid_refs() {
    let service = MembershipService::new("local", SwimConfig::default(), register("local"));
    let results = service
      .merkle_nodes(vec![
        NodeRef {
          depth: lycoris_membership::MERKLE_TREE_DEPTH + 1,
          index: 0,
        },
        NodeRef {
          depth: 1,
          index: 10,
        },
      ])
      .await;
    assert!(results.is_empty());
  }

  #[tokio::test]
  async fn refute_persists_local_incarnation_on_change_only() {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = lycoris_storage::Storage::open(dir.path().join("test.redb")).unwrap();
    let service = MembershipService::new("local", SwimConfig::default(), register("local"))
      .with_meta(storage.node().meta().clone());

    // Mutations that do not touch the local incarnation must not write.
    let _ = service.register(register("peer")).await;
    let _ = service.tick().await;
    assert_eq!(
      storage.node().meta().get(LOCAL_INCARNATION_KEY).unwrap(),
      None
    );

    // A suspect rumor about the local node forces a refute: incarnation 1->2
    // is persisted immediately.
    let actions = service
      .on_probe(
        "local",
        SwimMessage::Suspect {
          node_id: "local".to_string(),
          incarnation: 1,
        },
      )
      .await;
    assert!(
      actions
        .iter()
        .any(|action| matches!(action, SwimAction::Broadcast(SwimMessage::Alive { .. })))
    );
    assert_eq!(
      storage
        .node()
        .meta()
        .get(LOCAL_INCARNATION_KEY)
        .unwrap()
        .as_deref(),
      Some("2")
    );
  }

  #[tokio::test]
  async fn local_incarnation_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("test.redb");
    {
      let storage = lycoris_storage::Storage::open(&db_path).unwrap();
      let service = MembershipService::new("local", SwimConfig::default(), register("local"))
        .with_meta(storage.node().meta().clone());
      let _ = service
        .on_probe(
          "local",
          SwimMessage::Suspect {
            node_id: "local".to_string(),
            incarnation: 1,
          },
        )
        .await;
    }

    // Reopen the same database (simulated restart) and rebuild the local
    // register the way the runtime does: the incarnation must resume at 2,
    // not rewind to 1.
    let storage = lycoris_storage::Storage::open(&db_path).unwrap();
    let persisted = crate::persisted_counter(storage.node().meta(), LOCAL_INCARNATION_KEY);
    assert_eq!(persisted, Some(2));

    let register =
      MemberRegister::new("local", "127.0.0.1:1", persisted.unwrap_or(1), 0).with_updated_at_ms(0);
    let service = MembershipService::new("local", SwimConfig::default(), register);
    let nodes = service.list_nodes(&HashMap::new()).await;
    let local = nodes.iter().find(|node| node.node_id() == "local").unwrap();
    assert_eq!(local.incarnation(), 2);
  }
}
