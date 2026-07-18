//! Cluster membership CRDT types shared across crates.
//!
//! These types intentionally avoid any transport or persistence dependencies
//! so that `lycoris-membership` remains a small, testable library.

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

  /// Rank of the state in the CRDT merge order:
  /// `Active < Suspected < Leaving < Offline`.
  ///
  /// Within one incarnation a more severe rumor always wins, so an `Offline`
  /// rumor cannot be overwritten by a concurrent `Suspected`/`Active` register
  /// with a higher heartbeat; only a higher incarnation (e.g. the node
  /// refuting the rumor about itself) revives it. The numeric values match
  /// `as_u8`, but the two serve different purposes: `rank` drives merge
  /// ordering, `as_u8` is the hash serialization.
  pub fn rank(self) -> u8 {
    self.as_u8()
  }
}

/// A single node's membership register.
///
/// This is the unit of replication. Each node owns its own register and
/// monotonically increases `incarnation` on restart and `heartbeat` on every
/// update. The triple `(incarnation, state_rank, heartbeat)` gives a total
/// order for the same `node_id` (D1), which makes merge deterministic and
/// CRDT-safe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRegister {
  node_id: String,
  address: String,
  state: MemberState,
  incarnation: u64,
  heartbeat: u64,
  labels: HashMap<String, String>,
  annotations: HashMap<String, String>,
  updated_at_ms: i64,
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

  /// Return the node id.
  pub fn node_id(&self) -> &str {
    &self.node_id
  }

  /// Return the node's cluster address.
  pub fn address(&self) -> &str {
    &self.address
  }

  /// Return the current lifecycle state.
  pub fn state(&self) -> MemberState {
    self.state
  }

  /// Return the current incarnation.
  pub fn incarnation(&self) -> u64 {
    self.incarnation
  }

  /// Return the current heartbeat counter.
  pub fn heartbeat(&self) -> u64 {
    self.heartbeat
  }

  /// Return the label map.
  pub fn labels(&self) -> &HashMap<String, String> {
    &self.labels
  }

  /// Return the annotation map.
  pub fn annotations(&self) -> &HashMap<String, String> {
    &self.annotations
  }

  /// Return the last update timestamp.
  pub fn updated_at_ms(&self) -> i64 {
    self.updated_at_ms
  }

  /// Set the lifecycle state.
  pub fn set_state(&mut self, state: MemberState) {
    self.state = state;
  }

  /// Set the incarnation.
  pub fn set_incarnation(&mut self, incarnation: u64) {
    self.incarnation = incarnation;
  }

  /// Replace the label map.
  pub fn set_labels(&mut self, labels: HashMap<String, String>) {
    self.labels = labels;
  }

  /// Replace the annotation map.
  pub fn set_annotations(&mut self, annotations: HashMap<String, String>) {
    self.annotations = annotations;
  }

  /// Set the last update timestamp.
  pub fn set_updated_at_ms(&mut self, updated_at_ms: i64) {
    self.updated_at_ms = updated_at_ms;
  }

  /// Builder: set the lifecycle state.
  pub fn with_state(mut self, state: MemberState) -> Self {
    self.state = state;
    self
  }

  /// Builder: set the cluster address.
  pub fn with_address(mut self, address: impl Into<String>) -> Self {
    self.address = address.into();
    self
  }

  /// Builder: replace the label map.
  pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
    self.labels = labels;
    self
  }

  /// Builder: replace the annotation map.
  pub fn with_annotations(mut self, annotations: HashMap<String, String>) -> Self {
    self.annotations = annotations;
    self
  }

  /// Builder: set the last update timestamp.
  pub fn with_updated_at_ms(mut self, updated_at_ms: i64) -> Self {
    self.updated_at_ms = updated_at_ms;
    self
  }

  /// Return the total-order key used for CRDT merges (D1):
  /// `(incarnation, state_rank, heartbeat)`, lexicographic, larger wins.
  ///
  /// `state_rank` orders `Active < Suspected < Leaving < Offline`, so within
  /// one incarnation a more severe rumor always wins and a node revives only
  /// by refuting with a higher incarnation. `heartbeat` is only the final
  /// tiebreak between registers with identical incarnation and state.
  fn order_key(&self) -> (u64, u8, u64) {
    (self.incarnation, self.state.rank(), self.heartbeat)
  }

  /// Return true if `self` dominates `other` according to CRDT ordering.
  ///
  /// `updated_at_ms` is intentionally not part of the order because wall
  /// clocks are unreliable across partitions.
  pub fn dominates(&self, other: &Self) -> bool {
    self.order_key() > other.order_key()
  }

  /// Merge another register into this one, keeping the dominant state.
  ///
  /// If one register clearly dominates, its full state wins so that stale
  /// duplicates cannot revert newer labels or annotations. When the two
  /// registers are equivalent under the total order, conflicting metadata is
  /// resolved field-by-field with deterministic rules (see below), which
  /// makes merge commutative, associative, and idempotent (I1).
  pub fn merge(&mut self, other: &Self) {
    if other.dominates(self) {
      *self = other.clone();
      return;
    }

    if self.dominates(other) {
      // The dominant register already wins; do not merge stale metadata.
      return;
    }

    // Equal order keys (same incarnation, state, and heartbeat): resolve each
    // field independently with an order-independent "larger value wins" rule.
    // For maps this compares the key-sorted entry sequences lexicographically
    // and takes the larger map as a whole, so the outcome never depends on
    // merge direction.
    if other.address > self.address {
      self.address.clone_from(&other.address);
    }
    if canonical_map_cmp(&other.labels, &self.labels) == std::cmp::Ordering::Greater {
      self.labels.clone_from(&other.labels);
    }
    if canonical_map_cmp(&other.annotations, &self.annotations) == std::cmp::Ordering::Greater {
      self.annotations.clone_from(&other.annotations);
    }
    self.updated_at_ms = self.updated_at_ms.max(other.updated_at_ms);
  }

  /// Bump the heartbeat and update the timestamp.
  ///
  /// Only `Active` or `Suspected` nodes are moved back to `Active`; terminal
  /// states (`Leaving`, `Offline`) are not resurrected by a heartbeat alone.
  pub fn bump_heartbeat(&mut self, now_ms: i64) {
    self.heartbeat = self.heartbeat.saturating_add(1);
    if matches!(self.state, MemberState::Active | MemberState::Suspected) {
      self.state = MemberState::Active;
    }
    self.updated_at_ms = now_ms;
  }

  /// Mark the node as suspected.
  pub fn suspect(&mut self, now_ms: i64) {
    // Only suspect currently active nodes; never downgrade a higher-state
    // register (e.g., Offline) via a suspect call.
    if matches!(self.state, MemberState::Active) {
      self.state = MemberState::Suspected;
      self.heartbeat = self.heartbeat.saturating_add(1);
      self.updated_at_ms = now_ms;
    }
  }

  /// Mark the node as leaving.
  pub fn leave(&mut self, now_ms: i64) {
    self.state = MemberState::Leaving;
    self.heartbeat = self.heartbeat.saturating_add(1);
    self.updated_at_ms = now_ms;
  }

  /// Mark the node as confirmed failed or departed.
  ///
  /// The `Offline` state's higher rank in the merge order (D1) is what makes
  /// this register dominate any concurrent `Suspected`/`Active` register with
  /// the same incarnation; no heartbeat inflation is needed.
  pub fn offline(&mut self, now_ms: i64) {
    self.state = MemberState::Offline;
    self.heartbeat = self.heartbeat.saturating_add(1);
    self.updated_at_ms = now_ms;
  }

  /// Rejoin the cluster with the next incarnation.
  ///
  /// The single restart path: bumping the persisted incarnation makes the
  /// fresh `Active` register dominate — and thereby refute — any suspect or
  /// offline rumor the cluster still holds about this node from before the
  /// restart, without waiting for a refutation round trip.
  pub fn rejoin(&mut self, address: impl Into<String>, now_ms: i64) {
    self.incarnation = self.incarnation.saturating_add(1);
    self.heartbeat = 0;
    self.address = address.into();
    self.state = MemberState::Active;
    self.updated_at_ms = now_ms;
  }

  /// Refute a suspect rumor about ourselves by bumping incarnation.
  ///
  /// This produces a strictly dominant register that overrides the
  /// suspicion and can be gossiped back as an `Alive` message.
  pub fn refute(&mut self, now_ms: i64) {
    self.incarnation = self.incarnation.saturating_add(1);
    self.heartbeat = self.heartbeat.saturating_add(1);
    self.state = MemberState::Active;
    self.updated_at_ms = now_ms;
  }
}

/// Compare two label/annotation maps deterministically: entries are sorted by
/// key and the resulting sequences are compared lexicographically. Used by
/// `MemberRegister::merge` to resolve conflicts between registers with equal
/// order keys without depending on merge direction.
fn canonical_map_cmp(
  a: &HashMap<String, String>, b: &HashMap<String, String>,
) -> std::cmp::Ordering {
  let mut a_sorted: Vec<(&String, &String)> = a.iter().collect();
  a_sorted.sort();
  let mut b_sorted: Vec<(&String, &String)> = b.iter().collect();
  b_sorted.sort();
  a_sorted.cmp(&b_sorted)
}
