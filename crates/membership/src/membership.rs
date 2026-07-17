//! Cluster membership CRDT.

use std::collections::HashMap;

use crate::register::{MemberRegister, MemberState};

/// The cluster membership CRDT.
///
/// `Membership` is a map from node id to `MemberRegister`. Merging two
/// memberships merges each register independently, which guarantees
/// convergence without coordination.
///
/// The `version` counter is bumped by every mutation that can change the
/// Merkle tree contents (register writes/merges and state transitions). It
/// lets callers cache expensive derived structures — such as the Merkle tree —
/// and rebuild them only when the membership actually changed. `heartbeat`
/// bumps do not advance the version while the state stays unchanged, because
/// the heartbeat counter is excluded from the tree hash (see
/// `merkle::hash_register`); a heartbeat that flips `Suspected` back to
/// `Active` does advance it.
#[derive(Debug, Clone, Default)]
pub struct Membership {
  members: HashMap<String, MemberRegister>,
  version: u64,
}

/// Equality compares only the replicated state. The `version` counter is a
/// local cache-invalidation signal, not part of the CRDT value.
impl PartialEq for Membership {
  fn eq(&self, other: &Self) -> bool {
    self.members == other.members
  }
}

impl Eq for Membership {}

impl Membership {
  /// Create an empty membership.
  pub fn new() -> Self {
    Self {
      members: HashMap::new(),
      version: 0,
    }
  }

  /// Insert or merge a single register.
  pub fn merge_register(&mut self, register: &MemberRegister) {
    self
      .members
      .entry(register.node_id().to_string())
      .and_modify(|existing| existing.merge(register))
      .or_insert_with(|| register.clone());
    // Conservatively bump on every write/merge; detecting a no-op merge would
    // cost a full register comparison and merges on the anti-entropy path are
    // rare compared to reads.
    self.version += 1;
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

  /// Return all members.
  pub fn all(&self) -> Vec<&MemberRegister> {
    self.members.values().collect()
  }

  /// Return members considered active (Active or Suspected).
  pub fn active(&self) -> Vec<&MemberRegister> {
    self
      .members
      .values()
      .filter(|m| m.state().is_active())
      .collect()
  }

  /// Return the mutation version counter (see the type-level docs).
  pub fn version(&self) -> u64 {
    self.version
  }

  /// Bump the heartbeat for `node_id` if it exists.
  pub fn heartbeat(&mut self, node_id: &str, now_ms: i64) -> bool {
    if let Some(register) = self.members.get_mut(node_id) {
      let state_before = register.state();
      register.bump_heartbeat(now_ms);
      // The heartbeat counter is not part of the Merkle hash, so a pure
      // heartbeat bump does not change the tree; only an accompanying state
      // flip (Suspected -> Active) does.
      if register.state() != state_before {
        self.version += 1;
      }
      return true;
    }
    false
  }

  /// Mark `node_id` as suspected if it exists and is currently active.
  ///
  /// Returns the new incarnation when the state changed.
  pub fn suspect(&mut self, node_id: &str, now_ms: i64) -> Option<u64> {
    let register = self.members.get_mut(node_id)?;
    let previous = register.state();
    register.suspect(now_ms);
    if previous == MemberState::Active && register.state() == MemberState::Suspected {
      self.version += 1;
      Some(register.incarnation())
    } else {
      None
    }
  }

  /// Mark `node_id` as offline if it exists.
  pub fn offline(&mut self, node_id: &str, now_ms: i64) -> bool {
    if let Some(register) = self.members.get_mut(node_id) {
      register.offline(now_ms);
      self.version += 1;
      return true;
    }
    false
  }

  /// Mark `node_id` as leaving if it exists.
  pub fn leave(&mut self, node_id: &str, now_ms: i64) -> bool {
    if let Some(register) = self.members.get_mut(node_id) {
      register.leave(now_ms);
      self.version += 1;
      return true;
    }
    false
  }

  /// Refute a suspect rumor for `node_id` if it exists.
  pub fn refute(&mut self, node_id: &str, now_ms: i64) -> bool {
    if let Some(register) = self.members.get_mut(node_id) {
      register.refute(now_ms);
      self.version += 1;
      return true;
    }
    false
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn register(id: &str, incarnation: u64, heartbeat: u64, state: MemberState) -> MemberRegister {
    MemberRegister::new(id, "127.0.0.1:1", incarnation, heartbeat)
      .with_state(state)
      .with_updated_at_ms(heartbeat.try_into().unwrap_or(i64::MAX))
  }

  #[test]
  fn higher_incarnation_wins() {
    let old = register("a", 1, 100, MemberState::Offline);
    let new = register("a", 2, 0, MemberState::Active);

    let mut membership = Membership::new();
    membership.merge_register(&old);
    membership.merge_register(&new);

    assert_eq!(membership.get("a").unwrap().state(), MemberState::Active);
    assert_eq!(membership.get("a").unwrap().incarnation(), 2);
  }

  #[test]
  fn higher_heartbeat_wins_same_incarnation() {
    let old = register("a", 1, 10, MemberState::Active);
    let new_addr = MemberRegister::new("a", "127.0.0.1:1", 1, 20)
      .with_state(MemberState::Active)
      .with_address("127.0.0.1:2")
      .with_updated_at_ms(20);

    let mut membership = Membership::new();
    membership.merge_register(&old);
    membership.merge_register(&new_addr);

    assert_eq!(membership.get("a").unwrap().address(), "127.0.0.1:2");
  }

  #[test]
  fn offline_can_be_revived_by_higher_incarnation() {
    let failed = register("a", 1, 100, MemberState::Offline);
    let revived = register("a", 2, 0, MemberState::Active);

    let mut membership = Membership::new();
    membership.merge_register(&failed);
    membership.merge_register(&revived);

    assert_eq!(membership.get("a").unwrap().state(), MemberState::Active);
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
    // D1: at the same incarnation the Offline rumor outranks Active, so a
    // revival must arrive with a higher incarnation (I2).
    right.merge_register(&register("b", 2, 6, MemberState::Active));

    left.merge(&right);

    assert_eq!(left.get("a").unwrap().heartbeat(), 12);
    assert_eq!(left.get("b").unwrap().state(), MemberState::Active);
    assert_eq!(left.get("b").unwrap().incarnation(), 2);
    assert_eq!(left.get("b").unwrap().heartbeat(), 6);
  }

  #[test]
  fn offline_outranks_active_at_same_incarnation() {
    let mut left = Membership::new();
    left.merge_register(&register("b", 1, 5, MemberState::Offline));

    let mut right = Membership::new();
    right.merge_register(&register("b", 1, 6, MemberState::Active));

    left.merge(&right);
    assert_eq!(left.get("b").unwrap().state(), MemberState::Offline);

    // Merge direction must not matter.
    right.merge(&left);
    assert_eq!(right.get("b").unwrap().state(), MemberState::Offline);
  }

  #[test]
  fn equal_key_different_state_merge_is_commutative() {
    // Same (incarnation, heartbeat), different states: the state rank decides
    // regardless of merge direction (D1/I1).
    for (lower, higher) in [
      (MemberState::Active, MemberState::Suspected),
      (MemberState::Suspected, MemberState::Offline),
      (MemberState::Active, MemberState::Offline),
    ] {
      let a = register("x", 1, 10, lower);
      let b = register("x", 1, 10, higher);

      let mut ab = Membership::new();
      ab.merge_register(&a);
      ab.merge_register(&b);

      let mut ba = Membership::new();
      ba.merge_register(&b);
      ba.merge_register(&a);

      assert_eq!(ab, ba, "merge not commutative for {lower:?} vs {higher:?}");
      assert_eq!(ab.get("x").unwrap().state(), higher);
    }
  }

  #[test]
  fn equal_key_label_conflict_resolves_deterministically() {
    // Identical order keys but conflicting labels: the lexicographically
    // larger map wins, so the outcome is direction-independent (D1/I1).
    let mut labels_a = HashMap::new();
    labels_a.insert("zone".to_string(), "a".to_string());
    let mut labels_b = HashMap::new();
    labels_b.insert("zone".to_string(), "b".to_string());

    let a = register("x", 1, 10, MemberState::Active).with_labels(labels_a);
    let b = register("x", 1, 10, MemberState::Active).with_labels(labels_b);

    let mut ab = Membership::new();
    ab.merge_register(&a);
    ab.merge_register(&b);

    let mut ba = Membership::new();
    ba.merge_register(&b);
    ba.merge_register(&a);

    assert_eq!(ab, ba);
    assert_eq!(
      ab.get("x").unwrap().labels().get("zone"),
      Some(&"b".to_string())
    );
  }

  #[test]
  fn version_tracks_hash_relevant_mutations() {
    let mut m = Membership::new();
    let v0 = m.version();

    m.merge_register(&register("a", 1, 1, MemberState::Active));
    let v1 = m.version();
    assert!(v1 > v0, "register merge must bump the version");

    // A pure heartbeat bump is invisible to the Merkle tree (D3), so the
    // version — and any cached tree — stays put.
    assert!(m.heartbeat("a", 2_000));
    assert_eq!(m.version(), v1);

    // A state transition changes the tree and must bump the version.
    assert!(m.suspect("a", 3_000).is_some());
    assert!(m.version() > v1);
  }

  #[test]
  fn heartbeat_bumps_and_reactivates() {
    let mut r = register("a", 1, 10, MemberState::Suspected);
    r.bump_heartbeat(1000);

    assert_eq!(r.heartbeat(), 11);
    assert_eq!(r.state(), MemberState::Active);
    assert_eq!(r.updated_at_ms(), 1000);
  }

  #[test]
  fn suspect_only_downgrades_active() {
    let mut offline = register("a", 1, 10, MemberState::Offline);
    offline.suspect(1000);
    assert_eq!(offline.state(), MemberState::Offline);

    let mut active = register("a", 1, 10, MemberState::Active);
    active.suspect(1000);
    assert_eq!(active.state(), MemberState::Suspected);
  }

  #[test]
  fn rejoin_increments_incarnation() {
    let mut r = register("a", 1, 100, MemberState::Offline);
    r.rejoin("127.0.0.1:9", 2000);

    assert_eq!(r.incarnation(), 2);
    assert_eq!(r.heartbeat(), 0);
    assert_eq!(r.state(), MemberState::Active);
    assert_eq!(r.address(), "127.0.0.1:9");
  }

  #[test]
  fn active_filter_excludes_offline() {
    let mut m = Membership::new();
    m.merge_register(&register("a", 1, 10, MemberState::Active));
    m.merge_register(&register("b", 1, 5, MemberState::Suspected));
    m.merge_register(&register("c", 1, 3, MemberState::Offline));

    let active: Vec<_> = m
      .active()
      .into_iter()
      .map(|r| r.node_id().to_string())
      .collect();
    assert_eq!(active.len(), 2);
    assert!(active.contains(&"a".to_string()));
    assert!(active.contains(&"b".to_string()));
  }

  #[test]
  fn heartbeat_does_not_revive_offline() {
    let mut r = register("a", 1, 10, MemberState::Offline);
    r.bump_heartbeat(1000);

    assert_eq!(r.heartbeat(), 11);
    assert_eq!(r.state(), MemberState::Offline);
    assert_eq!(r.updated_at_ms(), 1000);
  }

  #[test]
  fn offline_dominates_suspect_within_same_incarnation() {
    let suspected = register("a", 1, 100, MemberState::Suspected);
    let mut offline = register("a", 1, 1, MemberState::Active);
    offline.offline(1000);

    let mut membership = Membership::new();
    membership.merge_register(&suspected);
    membership.merge_register(&offline);

    // The Offline rank (D1), not a heartbeat trick, is what decides the merge:
    // the winning register keeps its own low heartbeat.
    let merged = membership.get("a").unwrap();
    assert_eq!(merged.state(), MemberState::Offline);
    assert_eq!(merged.heartbeat(), 2);
  }
}
