use std::collections::HashMap;

/// Lifecycle state of a cluster member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberState {
  /// Node is healthy and participating.
  Active,
  /// Node has been unresponsive and is under suspicion.
  Suspected,
  /// Node is gracefully leaving the cluster.
  Leaving,
  /// Node is confirmed failed or has left.
  Offline,
}

impl MemberState {
  /// Return true if the node is still considered part of the active membership.
  pub fn is_active(self) -> bool {
    matches!(self, MemberState::Active | MemberState::Suspected)
  }

  /// Serialize the state as a stable byte value for hashing.
  pub fn as_u8(self) -> u8 {
    match self {
      MemberState::Active => 0,
      MemberState::Suspected => 1,
      MemberState::Leaving => 2,
      MemberState::Offline => 3,
    }
  }
}

/// A single node's membership register.
///
/// This is the unit of replication. Each node owns its own register and
/// monotonically increases `incarnation` on restart and `heartbeat` on every
/// update. The pair `(incarnation, heartbeat)` gives a total order for the
/// same `node_id`, which makes merge deterministic and CRDT-safe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRegister {
  pub node_id: String,
  pub address: String,
  pub state: MemberState,
  pub incarnation: u64,
  pub heartbeat: u64,
  pub labels: HashMap<String, String>,
  pub annotations: HashMap<String, String>,
  pub updated_at_ms: i64,
}

impl MemberRegister {
  /// Create a new active register for a node.
  pub fn new(
    node_id: impl Into<String>, address: impl Into<String>, incarnation: u64, heartbeat: u64,
  ) -> Self {
    Self {
      node_id: node_id.into(),
      address: address.into(),
      state: MemberState::Active,
      incarnation,
      heartbeat,
      labels: HashMap::new(),
      annotations: HashMap::new(),
      updated_at_ms: 0,
    }
  }

  /// Return true if `self` dominates `other` according to CRDT ordering.
  ///
  /// Ordering: higher incarnation wins; if equal, higher heartbeat wins;
  /// if equal, later timestamp wins. This is a total preorder per node.
  pub fn dominates(&self, other: &Self) -> bool {
    if self.incarnation != other.incarnation {
      return self.incarnation > other.incarnation;
    }
    if self.heartbeat != other.heartbeat {
      return self.heartbeat > other.heartbeat;
    }
    self.updated_at_ms >= other.updated_at_ms
  }

  /// Merge another register into this one, keeping the dominant state.
  ///
  /// If the other register dominates, all fields are replaced. If they are
  /// equivalent in ordering, fields are merged defensively (union of maps).
  pub fn merge(&mut self, other: &Self) {
    if other.dominates(self) {
      *self = other.clone();
      return;
    }

    if self.dominates(other) {
      // Keep self, but still merge label/annotation sets defensively so a
      // stale duplicate does not lose metadata.
      self.labels.extend(other.labels.clone());
      self.annotations.extend(other.annotations.clone());
      return;
    }

    // Same ordering: merge all maps and keep the more recent timestamp.
    self.labels.extend(other.labels.clone());
    self.annotations.extend(other.annotations.clone());
    self.updated_at_ms = self.updated_at_ms.max(other.updated_at_ms);
  }

  /// Bump the heartbeat and update the timestamp.
  pub fn heartbeat(&mut self, now_ms: i64) {
    self.heartbeat = self.heartbeat.saturating_add(1);
    self.state = MemberState::Active;
    self.updated_at_ms = now_ms;
  }

  /// Mark the node as suspected.
  pub fn suspect(&mut self, now_ms: i64) {
    // Only suspect currently active nodes; never downgrade a higher-state
    // register (e.g., Offline) via a suspect call.
    if matches!(self.state, MemberState::Active) {
      self.state = MemberState::Suspected;
      self.updated_at_ms = now_ms;
    }
  }

  /// Mark the node as offline.
  pub fn fail(&mut self, now_ms: i64) {
    self.state = MemberState::Offline;
    self.updated_at_ms = now_ms;
  }

  /// Mark the node as leaving.
  pub fn leave(&mut self, now_ms: i64) {
    self.state = MemberState::Leaving;
    self.updated_at_ms = now_ms;
  }

  /// Rejoin with a higher incarnation.
  pub fn rejoin(&mut self, address: impl Into<String>, now_ms: i64) {
    self.incarnation = self.incarnation.saturating_add(1);
    self.heartbeat = 0;
    self.address = address.into();
    self.state = MemberState::Active;
    self.updated_at_ms = now_ms;
  }
}

/// The cluster membership CRDT.
///
/// `Membership` is a map from node id to `MemberRegister`. Merging two
/// memberships merges each register independently, which guarantees
/// convergence without coordination.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Membership {
  members: HashMap<String, MemberRegister>,
}

impl Membership {
  /// Create an empty membership.
  pub fn new() -> Self {
    Self {
      members: HashMap::new(),
    }
  }

  /// Insert or merge a single register.
  pub fn merge_register(&mut self, register: &MemberRegister) {
    self
      .members
      .entry(register.node_id.clone())
      .and_modify(|existing| existing.merge(register))
      .or_insert_with(|| register.clone());
  }

  /// Merge another membership into this one.
  pub fn merge(&mut self, other: &Self) {
    for register in other.members.values() {
      self.merge_register(register);
    }
  }

  /// Get a member by id.
  pub fn get(&self, node_id: &str) -> Option<&MemberRegister> {
    self.members.get(node_id)
  }

  /// Get a mutable member by id.
  pub fn get_mut(&mut self, node_id: &str) -> Option<&mut MemberRegister> {
    self.members.get_mut(node_id)
  }

  /// Return all members.
  pub fn all(&self) -> Vec<&MemberRegister> {
    self.members.values().collect()
  }

  /// Return members considered active (Active or Suspected).
  pub fn active(&self) -> Vec<&MemberRegister> {
    self
      .members
      .values()
      .filter(|m| m.state.is_active())
      .collect()
  }

  /// Return true if the membership contains no members.
  pub fn is_empty(&self) -> bool {
    self.members.is_empty()
  }

  /// Return the number of members.
  pub fn len(&self) -> usize {
    self.members.len()
  }

  /// Return an iterator over member ids.
  pub fn node_ids(&self) -> impl Iterator<Item = &String> {
    self.members.keys()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn register(id: &str, incarnation: u64, heartbeat: u64, state: MemberState) -> MemberRegister {
    let mut r = MemberRegister::new(id, "127.0.0.1:1", incarnation, heartbeat);
    r.state = state;
    r.updated_at_ms = heartbeat.try_into().unwrap_or(i64::MAX);
    r
  }

  #[test]
  fn higher_incarnation_wins() {
    let old = register("a", 1, 100, MemberState::Offline);
    let new = register("a", 2, 0, MemberState::Active);

    let mut membership = Membership::new();
    membership.merge_register(&old);
    membership.merge_register(&new);

    assert_eq!(membership.get("a").unwrap().state, MemberState::Active);
    assert_eq!(membership.get("a").unwrap().incarnation, 2);
  }

  #[test]
  fn higher_heartbeat_wins_same_incarnation() {
    let old = register("a", 1, 10, MemberState::Active);
    let new_addr = {
      let mut r = register("a", 1, 20, MemberState::Active);
      r.address = "127.0.0.1:2".to_string();
      r
    };

    let mut membership = Membership::new();
    membership.merge_register(&old);
    membership.merge_register(&new_addr);

    assert_eq!(membership.get("a").unwrap().address, "127.0.0.1:2");
  }

  #[test]
  fn offline_can_be_revived_by_higher_incarnation() {
    let failed = register("a", 1, 100, MemberState::Offline);
    let revived = register("a", 2, 0, MemberState::Active);

    let mut membership = Membership::new();
    membership.merge_register(&failed);
    membership.merge_register(&revived);

    assert_eq!(membership.get("a").unwrap().state, MemberState::Active);
  }

  #[test]
  fn merge_is_idempotent() {
    let a = register("a", 1, 10, MemberState::Active);
    let b = register("b", 1, 5, MemberState::Active);

    let mut m1 = Membership::new();
    m1.merge_register(&a);
    m1.merge_register(&b);

    let m2 = m1.clone();
    m1.merge(&m2);

    assert_eq!(m1, m2);
  }

  #[test]
  fn merge_is_commutative() {
    let a = register("a", 1, 10, MemberState::Active);
    let b = register("b", 1, 5, MemberState::Active);

    let mut m1 = Membership::new();
    m1.merge_register(&a);
    m1.merge_register(&b);

    let mut m2 = Membership::new();
    m2.merge_register(&b);
    m2.merge_register(&a);

    assert_eq!(m1, m2);
  }

  #[test]
  fn merge_is_associative() {
    let a = register("a", 1, 10, MemberState::Active);
    let b = register("b", 1, 5, MemberState::Active);
    let c = register("c", 1, 7, MemberState::Active);

    let mut left = Membership::new();
    left.merge_register(&a);
    left.merge_register(&b);

    let mut right = Membership::new();
    right.merge_register(&c);

    let mut assoc1 = left.clone();
    assoc1.merge(&right);

    let mut mid = Membership::new();
    mid.merge_register(&b);
    mid.merge_register(&c);

    let mut assoc2 = Membership::new();
    assoc2.merge_register(&a);
    assoc2.merge(&mid);

    assert_eq!(assoc1, assoc2);
  }

  #[test]
  fn partition_merge_keeps_latest_per_node() {
    let mut left = Membership::new();
    left.merge_register(&register("a", 1, 10, MemberState::Active));
    left.merge_register(&register("b", 1, 5, MemberState::Offline));

    let mut right = Membership::new();
    right.merge_register(&register("a", 1, 12, MemberState::Active));
    right.merge_register(&register("b", 1, 6, MemberState::Active));

    left.merge(&right);

    assert_eq!(left.get("a").unwrap().heartbeat, 12);
    assert_eq!(left.get("b").unwrap().state, MemberState::Active);
    assert_eq!(left.get("b").unwrap().heartbeat, 6);
  }

  #[test]
  fn heartbeat_bumps_and_reactivates() {
    let mut r = register("a", 1, 10, MemberState::Suspected);
    r.heartbeat(1000);

    assert_eq!(r.heartbeat, 11);
    assert_eq!(r.state, MemberState::Active);
    assert_eq!(r.updated_at_ms, 1000);
  }

  #[test]
  fn suspect_only_downgrades_active() {
    let mut offline = register("a", 1, 10, MemberState::Offline);
    offline.suspect(1000);
    assert_eq!(offline.state, MemberState::Offline);

    let mut active = register("a", 1, 10, MemberState::Active);
    active.suspect(1000);
    assert_eq!(active.state, MemberState::Suspected);
  }

  #[test]
  fn rejoin_increments_incarnation() {
    let mut r = register("a", 1, 100, MemberState::Offline);
    r.rejoin("127.0.0.1:9", 2000);

    assert_eq!(r.incarnation, 2);
    assert_eq!(r.heartbeat, 0);
    assert_eq!(r.state, MemberState::Active);
    assert_eq!(r.address, "127.0.0.1:9");
  }

  #[test]
  fn active_filter_excludes_offline() {
    let mut m = Membership::new();
    m.merge_register(&register("a", 1, 10, MemberState::Active));
    m.merge_register(&register("b", 1, 5, MemberState::Suspected));
    m.merge_register(&register("c", 1, 3, MemberState::Offline));

    let active: Vec<_> = m.active().into_iter().map(|r| r.node_id.clone()).collect();
    assert_eq!(active.len(), 2);
    assert!(active.contains(&"a".to_string()));
    assert!(active.contains(&"b".to_string()));
  }
}
