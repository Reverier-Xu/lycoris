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
}

const OFFLINE_HEARTBEAT_BUMP: u64 = 1_000_000;

/// A single node's membership register.
///
/// This is the unit of replication. Each node owns its own register and
/// monotonically increases `incarnation` on restart and `heartbeat` on every
/// update. The pair `(incarnation, heartbeat)` gives a total order for the
/// same `node_id`, which makes merge deterministic and CRDT-safe.
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

  /// Set the cluster address.
  pub fn set_address(&mut self, address: impl Into<String>) {
    self.address = address.into();
  }

  /// Set the incarnation.
  pub fn set_incarnation(&mut self, incarnation: u64) {
    self.incarnation = incarnation;
  }

  /// Set the heartbeat counter.
  pub fn set_heartbeat(&mut self, heartbeat: u64) {
    self.heartbeat = heartbeat;
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

  /// Builder: set the incarnation.
  pub fn with_incarnation(mut self, incarnation: u64) -> Self {
    self.incarnation = incarnation;
    self
  }

  /// Builder: set the heartbeat counter.
  pub fn with_heartbeat(mut self, heartbeat: u64) -> Self {
    self.heartbeat = heartbeat;
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

  /// Return true if `self` dominates `other` according to CRDT ordering.
  ///
  /// Ordering: higher incarnation wins; if equal, higher heartbeat wins. This
  /// is a total preorder per node. `updated_at_ms` is intentionally not used
  /// as a tiebreaker because wall clocks are unreliable across partitions.
  pub fn dominates(&self, other: &Self) -> bool {
    if self.incarnation != other.incarnation {
      return self.incarnation > other.incarnation;
    }
    self.heartbeat > other.heartbeat
  }

  /// Merge another register into this one, keeping the dominant state.
  ///
  /// If one register clearly dominates, its full state wins so that stale
  /// duplicates cannot revert newer labels or annotations. Only when the two
  /// registers are equivalent in ordering are the maps merged defensively.
  pub fn merge(&mut self, other: &Self) {
    if other.dominates(self) {
      *self = other.clone();
      return;
    }

    if self.dominates(other) {
      // The dominant register already wins; do not merge stale metadata.
      return;
    }

    // Same ordering: merge all maps and keep the more recent timestamp.
    self.labels.extend(other.labels.clone());
    self.annotations.extend(other.annotations.clone());
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
  /// The heartbeat is bumped by a large constant so that, within the same
  /// incarnation, an `Offline` register dominates any concurrent `Suspected`
  /// register and cannot be accidentally downgraded back to `Suspected`.
  pub fn offline(&mut self, now_ms: i64) {
    self.state = MemberState::Offline;
    self.heartbeat = self.heartbeat.saturating_add(OFFLINE_HEARTBEAT_BUMP);
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
