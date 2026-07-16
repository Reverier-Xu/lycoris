use std::collections::HashMap;

use crate::{
  membership::Membership,
  register::{MemberRegister, MemberState},
};

/// Configuration for the SWIM failure detector.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SwimConfig {
  /// Milliseconds between periodic probe rounds.
  pub ping_interval_ms: u64,
  /// Milliseconds to wait for a direct ack before declaring the probe failed.
  pub ping_timeout_ms: u64,
  /// Consecutive probe failures required before marking a peer suspected.
  pub failure_threshold: u32,
  /// Milliseconds a peer may stay suspected before being marked offline.
  pub suspect_timeout_ms: u64,
}

impl Default for SwimConfig {
  fn default() -> Self {
    Self {
      ping_interval_ms: 1_000,
      ping_timeout_ms: 3_000,
      failure_threshold: 3,
      suspect_timeout_ms: 30_000,
    }
  }
}

/// A SWIM protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwimMessage {
  /// Direct liveness probe.
  Ping { seq: u64 },
  /// Positive response to a `Ping`.
  Ack { seq: u64 },
  /// Disseminate that a node is suspected.
  Suspect { node_id: String, incarnation: u64 },
  /// Disseminate that a node is alive.
  Alive { register: MemberRegister },
  /// Disseminate that a node is leaving or has left.
  Leave { node_id: String, incarnation: u64 },
}

/// An action the SWIM state machine wants the runtime to perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwimAction {
  /// Send a direct ping to `target`.
  SendPing { target: String, seq: u64 },
  /// Send an ack back to `to`.
  SendAck { to: String, seq: u64 },
  /// Broadcast a state message to all active members except ourselves.
  Broadcast(SwimMessage),
}

/// State for a direct probe that has not yet been acked.
#[derive(Debug, Clone)]
struct PendingPing {
  target: String,
  sent_at: u64,
}

/// A minimal SWIM membership layer backed by the CRDT membership set.
///
/// Indirect probes and Phi accrual are intentionally omitted: the current
/// topology is a sparse graph where each node maintains direct connections to
/// one or more peers, so simple direct probes with a consecutive-failure
/// threshold are sufficient and easier to reason about.
///
/// `Swim` is transport agnostic. It consumes events and emits `SwimAction`s
/// that the caller must dispatch over the network.
#[derive(Debug, Clone)]
pub struct Swim {
  local_node_id: String,
  config: SwimConfig,
  membership: Membership,
  consecutive_failures: HashMap<String, u32>,
  sequence: u64,
  pending_pings: HashMap<u64, PendingPing>,
  last_probe_ms: HashMap<String, u64>,
}

impl Swim {
  /// Create a new SWIM instance seeded with the local node.
  pub fn new(local_node_id: impl Into<String>, config: SwimConfig, local: MemberRegister) -> Self {
    let local_node_id = local_node_id.into();
    let mut membership = Membership::new();
    membership.merge_register(&local);

    Self {
      local_node_id,
      config,
      membership,
      consecutive_failures: HashMap::new(),
      sequence: 0,
      pending_pings: HashMap::new(),
      last_probe_ms: HashMap::new(),
    }
  }

  /// Read-only access to the underlying membership CRDT.
  pub fn membership(&self) -> &Membership {
    &self.membership
  }

  /// Mutable access to the underlying membership CRDT.
  pub fn membership_mut(&mut self) -> &mut Membership {
    &mut self.membership
  }

  /// Record a heartbeat for `node_id` at `now_ms`.
  ///
  /// This updates the CRDT register (if the node is already known).
  pub fn heartbeat(&mut self, node_id: &str, now_ms: i64) {
    self.membership.heartbeat(node_id, now_ms);
  }

  /// Process an incoming SWIM message from `from`.
  pub fn on_message(&mut self, from: &str, message: SwimMessage, now_ms: i64) -> Vec<SwimAction> {
    match message {
      SwimMessage::Ping { seq } => {
        vec![SwimAction::SendAck {
          to: from.to_string(),
          seq,
        }]
      }
      SwimMessage::Ack { seq } => self.handle_ack(seq, now_ms),
      SwimMessage::Suspect {
        node_id,
        incarnation,
      } => self.handle_suspect_state(&node_id, incarnation, now_ms),
      SwimMessage::Alive { register } => {
        self.handle_alive_state(&register, now_ms);
        Vec::new()
      }
      SwimMessage::Leave {
        node_id,
        incarnation,
      } => {
        self.handle_leave_state(&node_id, incarnation, now_ms);
        Vec::new()
      }
    }
  }

  /// Advance timers and possibly issue new probes.
  ///
  /// Callers should drive this method from a periodic tick loop.
  pub fn tick(&mut self, now_ms: i64) -> Vec<SwimAction> {
    let now_u64 = u64::try_from(now_ms).unwrap_or(0);
    let mut actions = self.check_timeouts(now_u64, now_ms);
    actions.extend(self.check_suspect_timeouts(now_ms));
    actions.extend(self.generate_probes(now_u64, now_ms));
    actions
  }

  fn generate_probes(&mut self, now_u64: u64, now_ms: i64) -> Vec<SwimAction> {
    let interval = self.config.ping_interval_ms;
    let active: Vec<String> = self
      .membership
      .active()
      .into_iter()
      .filter(|r| r.node_id() != self.local_node_id)
      .map(|r| r.node_id().to_string())
      .collect();

    let mut candidate: Option<String> = None;
    let mut oldest_gap: u64 = 0;
    for target in active {
      if self
        .pending_pings
        .values()
        .any(|pending| pending.target == target)
      {
        continue;
      }
      let last = self.last_probe_ms.get(&target).copied().unwrap_or(0);
      let gap = now_u64.saturating_sub(last);
      if gap >= interval && gap > oldest_gap {
        oldest_gap = gap;
        candidate = Some(target);
      }
    }

    if let Some(target) = candidate {
      self.send_ping_to(&target, now_ms).into_iter().collect()
    } else {
      Vec::new()
    }
  }

  fn handle_ack(&mut self, seq: u64, now_ms: i64) -> Vec<SwimAction> {
    let Some(pending) = self.pending_pings.remove(&seq) else {
      return Vec::new();
    };

    let target = pending.target;
    self.consecutive_failures.remove(&target);
    self.heartbeat(&target, now_ms);

    let mut actions = Vec::new();
    if let Some(register) = self.membership.get(&target)
      && register.state() == MemberState::Suspected
    {
      actions.push(SwimAction::Broadcast(SwimMessage::Alive {
        register: register.clone(),
      }));
    }
    actions
  }

  fn handle_suspect_state(
    &mut self, node_id: &str, incarnation: u64, now_ms: i64,
  ) -> Vec<SwimAction> {
    if node_id == self.local_node_id {
      self.membership.refute(node_id, now_ms);
      if let Some(register) = self.membership.get(node_id) {
        return vec![SwimAction::Broadcast(SwimMessage::Alive {
          register: register.clone(),
        })];
      }
      return Vec::new();
    }

    if let Some(existing) = self.membership.get(node_id) {
      if existing.incarnation() > incarnation {
        return Vec::new();
      }
      let mut register = existing.clone();
      register.set_incarnation(incarnation);
      register.suspect(now_ms);
      self.membership.merge_register(&register);
    }
    Vec::new()
  }

  fn handle_alive_state(&mut self, register: &MemberRegister, now_ms: i64) {
    if register.node_id() == self.local_node_id {
      return;
    }
    self.membership.merge_register(register);
    let _ = now_ms;
  }

  fn handle_leave_state(&mut self, node_id: &str, incarnation: u64, now_ms: i64) {
    if node_id == self.local_node_id {
      return;
    }
    let Some(existing) = self.membership.get(node_id) else {
      return;
    };

    if existing.incarnation() > incarnation {
      return;
    }

    // A Leave rumor for the same incarnation must not revive a node that has
    // already been confirmed Offline.
    if existing.incarnation() == incarnation && existing.state() == MemberState::Offline {
      return;
    }

    let mut register = existing.clone();
    register.set_incarnation(incarnation);
    register.leave(now_ms);
    self.membership.merge_register(&register);
  }

  fn check_timeouts(&mut self, now_u64: u64, now_ms: i64) -> Vec<SwimAction> {
    let mut actions = Vec::new();
    let timed_out: Vec<u64> = self
      .pending_pings
      .iter()
      .filter(|(_, pending)| now_u64 - pending.sent_at >= self.config.ping_timeout_ms)
      .map(|(seq, _)| *seq)
      .collect();

    for seq in timed_out {
      if let Some(pending) = self.pending_pings.remove(&seq) {
        actions.extend(self.mark_suspected(&pending.target, now_ms));
      }
    }

    actions
  }

  fn check_suspect_timeouts(&mut self, now_ms: i64) -> Vec<SwimAction> {
    let timeout = i64::try_from(self.config.suspect_timeout_ms).unwrap_or(i64::MAX);
    let expired: Vec<String> = self
      .membership
      .active()
      .into_iter()
      .filter(|register| {
        register.node_id() != self.local_node_id
          && register.state() == MemberState::Suspected
          && now_ms.saturating_sub(register.updated_at_ms()) >= timeout
      })
      .map(|register| register.node_id().to_string())
      .collect();

    for node_id in expired {
      self.membership.offline(&node_id, now_ms);
    }

    Vec::new()
  }

  fn mark_suspected(&mut self, target: &str, now_ms: i64) -> Vec<SwimAction> {
    let mut actions = Vec::new();
    let failures = self
      .consecutive_failures
      .entry(target.to_string())
      .and_modify(|count| *count += 1)
      .or_insert(1);

    if *failures < self.config.failure_threshold {
      return actions;
    }

    self.consecutive_failures.remove(target);

    if let Some(incarnation) = self.membership.suspect(target, now_ms) {
      actions.push(SwimAction::Broadcast(SwimMessage::Suspect {
        node_id: target.to_string(),
        incarnation,
      }));
    }
    actions
  }

  fn send_ping_to(&mut self, target: &str, now_ms: i64) -> Option<SwimAction> {
    let now_u64 = u64::try_from(now_ms).unwrap_or(0);
    self.sequence = self.sequence.wrapping_add(1);
    let seq = self.sequence;
    self.pending_pings.insert(
      seq,
      PendingPing {
        target: target.to_string(),
        sent_at: now_u64,
      },
    );
    self.last_probe_ms.insert(target.to_string(), now_u64);
    Some(SwimAction::SendPing {
      target: target.to_string(),
      seq,
    })
  }

  pub fn local_node_id(&self) -> &str {
    &self.local_node_id
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn local_register(id: &str) -> MemberRegister {
    MemberRegister::new(id, "127.0.0.1:1", 1, 0).with_updated_at_ms(0)
  }

  fn peer_register(id: &str) -> MemberRegister {
    MemberRegister::new(id, "127.0.0.1:2", 1, 0).with_updated_at_ms(0)
  }

  #[test]
  fn ping_generates_send_action() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));
    swim.membership.merge_register(&peer_register("peer"));

    let actions = swim.tick(1_000);
    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], SwimAction::SendPing { ref target, .. } if target == "peer"));
  }

  #[test]
  fn ack_records_heartbeat_and_clears_pending() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));
    swim.membership.merge_register(&peer_register("peer"));

    let actions = swim.tick(1_000);
    let seq = match actions[0] {
      SwimAction::SendPing { seq, .. } => seq,
      _ => panic!("expected ping"),
    };

    let ack_actions = swim.on_message("peer", SwimMessage::Ack { seq }, 1_100);
    assert!(ack_actions.is_empty());
    assert!(swim.pending_pings.is_empty());
    assert!(swim.membership.get("peer").unwrap().heartbeat() > 0);
  }

  #[test]
  fn timeout_marks_suspected_and_broadcasts() {
    let config = SwimConfig {
      ping_interval_ms: 1_000,
      ping_timeout_ms: 2_000,
      failure_threshold: 1,
      suspect_timeout_ms: 60_000,
    };
    let mut swim = Swim::new("local", config, local_register("local"));
    swim.membership.merge_register(&peer_register("peer"));

    let _ = swim.tick(1_000);
    let actions = swim.tick(3_100);

    let has_suspect = actions.iter().any(|a| {
      matches!(
        a,
        SwimAction::Broadcast(SwimMessage::Suspect { node_id, .. }) if node_id == "peer"
      )
    });
    assert!(has_suspect);
    assert_eq!(
      swim.membership.get("peer").unwrap().state(),
      MemberState::Suspected
    );
  }

  #[test]
  fn consecutive_failures_required_by_default() {
    let config = SwimConfig {
      ping_interval_ms: 1_000,
      ping_timeout_ms: 2_000,
      failure_threshold: 3,
      suspect_timeout_ms: 60_000,
    };
    let mut swim = Swim::new("local", config, local_register("local"));
    swim.membership.merge_register(&peer_register("peer"));

    let _ = swim.tick(1_000);
    let actions = swim.tick(3_100);
    assert!(
      !actions
        .iter()
        .any(|a| matches!(a, SwimAction::Broadcast(SwimMessage::Suspect { .. })))
    );
    assert_eq!(
      swim.membership.get("peer").unwrap().state(),
      MemberState::Active
    );

    let actions = swim.tick(5_200);
    assert!(
      !actions
        .iter()
        .any(|a| matches!(a, SwimAction::Broadcast(SwimMessage::Suspect { .. })))
    );

    let actions = swim.tick(7_300);
    assert!(
      actions
        .iter()
        .any(|a| matches!(a, SwimAction::Broadcast(SwimMessage::Suspect { .. })))
    );
    assert_eq!(
      swim.membership.get("peer").unwrap().state(),
      MemberState::Suspected
    );
  }

  #[test]
  fn suspect_state_merges_state() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));
    swim.membership.merge_register(&peer_register("peer"));

    swim.on_message(
      "other",
      SwimMessage::Suspect {
        node_id: "peer".to_string(),
        incarnation: 1,
      },
      1_000,
    );

    assert_eq!(
      swim.membership.get("peer").unwrap().state(),
      MemberState::Suspected
    );
  }

  #[test]
  fn suspect_for_local_refutes_with_higher_incarnation() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));
    swim.membership.merge_register(&peer_register("peer"));

    let actions = swim.on_message(
      "peer",
      SwimMessage::Suspect {
        node_id: "local".to_string(),
        incarnation: 1,
      },
      1_000,
    );

    let local = swim.membership.get("local").unwrap();
    assert_eq!(local.state(), MemberState::Active);
    assert!(local.incarnation() > 1);
    assert!(
      actions.iter().any(|a| matches!(
        a,
        SwimAction::Broadcast(SwimMessage::Alive { register }) if register.node_id() == "local"
      )),
      "expected an Alive broadcast for the local node"
    );
  }

  #[test]
  fn alive_state_merges_register() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));
    let mut register = peer_register("peer");
    register.bump_heartbeat(2_000);

    swim.on_message("peer", SwimMessage::Alive { register }, 2_000);
    assert_eq!(swim.membership.get("peer").unwrap().heartbeat(), 1);
  }

  #[test]
  fn ping_to_local_returns_ack() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));
    let actions = swim.on_message("peer", SwimMessage::Ping { seq: 7 }, 1_000);
    assert_eq!(
      actions,
      vec![SwimAction::SendAck {
        to: "peer".to_string(),
        seq: 7
      }]
    );
  }
}
