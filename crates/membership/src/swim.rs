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
  /// Milliseconds a peer may stay suspected before being marked offline,
  /// counted on the local clock from the first local observation of the
  /// suspected state.
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
  /// Local timestamp at which each currently-suspected member was first
  /// observed in the Suspected state. The Suspected -> Offline timeout reads
  /// only this map, so the verdict depends on the local clock alone.
  suspected_since_ms: HashMap<String, i64>,
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
      suspected_since_ms: HashMap::new(),
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

  /// Process an incoming SWIM message from `from`.
  pub fn on_message(&mut self, from: &str, message: SwimMessage, now_ms: i64) -> Vec<SwimAction> {
    match message {
      SwimMessage::Ping { seq } => {
        vec![SwimAction::SendAck {
          to: from.to_string(),
          seq,
        }]
      }
      SwimMessage::Ack { seq } => {
        self.handle_ack(seq);
        Vec::new()
      }
      SwimMessage::Suspect {
        node_id,
        incarnation,
      } => self.handle_suspect_state(&node_id, incarnation, now_ms),
      SwimMessage::Alive { register } => self.handle_alive_state(&register, now_ms),
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
    // Heartbeat ownership (D2): only the node itself bumps its own heartbeat,
    // once per probe round. The counter is the final tiebreak of the merge
    // order and is excluded from the Merkle hash (D3), so this bump neither
    // triggers anti-entropy exchanges nor needs to be gossiped on its own.
    self.membership.heartbeat(&self.local_node_id, now_ms);
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

  /// Record a successful probe.
  ///
  /// An ack only updates local probe bookkeeping: it clears the pending entry
  /// and resets the consecutive-failure counter. It never bumps the peer's
  /// heartbeat — heartbeat ownership stays with the node itself (D2).
  ///
  /// Suspicion clearing follows the faithful SWIM+Suspicion model: a
  /// `Suspected` view is only cleared by an `Alive` register with a higher
  /// incarnation (i.e. the suspected node refuting the rumor itself), never
  /// directly by an ack. An ack-driven clear cannot converge under the D1
  /// merge order anyway: `Active` ranks below `Suspected` at the same
  /// incarnation, so any peer still holding the suspicion rumor would win the
  /// next merge and reinstate it.
  fn handle_ack(&mut self, seq: u64) {
    if let Some(pending) = self.pending_pings.remove(&seq) {
      self.consecutive_failures.remove(&pending.target);
    }
  }

  fn handle_suspect_state(
    &mut self, node_id: &str, incarnation: u64, now_ms: i64,
  ) -> Vec<SwimAction> {
    if node_id == self.local_node_id {
      // Refute only rumors that are not older than our own incarnation: an
      // older rumor is an echo of a state we already refuted, and answering
      // it again would needlessly inflate the incarnation counter. A node
      // that declared `Leaving` never refutes — leaving is its own choice.
      let dominated = self
        .membership
        .get(node_id)
        .map(|local| local.state() != MemberState::Leaving && incarnation >= local.incarnation())
        .unwrap_or(false);
      if !dominated {
        return Vec::new();
      }
      return self.refute_and_broadcast(now_ms);
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

  fn handle_alive_state(&mut self, register: &MemberRegister, now_ms: i64) -> Vec<SwimAction> {
    if register.node_id() == self.local_node_id {
      return self.refute_if_dominated(register, now_ms);
    }
    self.membership.merge_register(register);
    Vec::new()
  }

  /// Handle a rumor about the local node carried by an `Alive` message (the
  /// register's state may be `Suspected`, `Leaving`, or `Offline`; gossiped
  /// `Alive` messages are the propagation channel for all state changes).
  ///
  /// A non-`Active` rumor whose order key is not below the local register's
  /// key is refuted by bumping the incarnation and broadcasting the fresh
  /// `Active` register (D4). `Active` rumors need no refutation, and a local
  /// node that declared `Leaving` never refutes — leaving is its own choice.
  fn refute_if_dominated(&mut self, rumor: &MemberRegister, now_ms: i64) -> Vec<SwimAction> {
    if rumor.state() == MemberState::Active {
      return Vec::new();
    }
    let Some(local) = self.membership.get(&self.local_node_id) else {
      return Vec::new();
    };
    if local.state() == MemberState::Leaving || local.dominates(rumor) {
      return Vec::new();
    }
    self.refute_and_broadcast(now_ms)
  }

  /// Bump the local incarnation to override a rumor about ourselves and
  /// broadcast the resulting `Active` register.
  fn refute_and_broadcast(&mut self, now_ms: i64) -> Vec<SwimAction> {
    self.membership.refute(&self.local_node_id, now_ms);
    match self.membership.get(&self.local_node_id) {
      Some(register) => vec![SwimAction::Broadcast(SwimMessage::Alive {
        register: register.clone(),
      })],
      None => Vec::new(),
    }
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

    // No special-casing for an existing `Offline` register: in the D1 merge
    // order `Offline` outranks `Leaving` at the same incarnation, so a Leave
    // rumor cannot revive an Offline node — the merge below is a no-op then.
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
      .filter(|(_, pending)| now_u64.saturating_sub(pending.sent_at) >= self.config.ping_timeout_ms)
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
    let suspected: Vec<String> = self
      .membership
      .active()
      .into_iter()
      .filter(|register| {
        register.node_id() != self.local_node_id && register.state() == MemberState::Suspected
      })
      .map(|register| register.node_id().to_string())
      .collect();

    // The Suspected -> Offline timer runs purely on the local clock: it starts
    // when this node first observes a member in the Suspected state, and a
    // member that recovers and is suspected again later gets a fresh timer.
    // The register's `updated_at_ms` must not drive this verdict: merging
    // resolves equal-order registers field-wise with "largest value wins", so
    // a fast remote clock could push the timestamp into the future and
    // postpone the Offline transition indefinitely.
    self
      .suspected_since_ms
      .retain(|node_id, _| suspected.contains(node_id));

    let mut expired = Vec::new();
    for node_id in suspected {
      let since = *self
        .suspected_since_ms
        .entry(node_id.clone())
        .or_insert(now_ms);
      if now_ms.saturating_sub(since) >= timeout {
        expired.push(node_id);
      }
    }

    let mut actions = Vec::new();
    for node_id in expired {
      // The Suspected -> Offline transition is disseminated (D4) so the whole
      // cluster converges on the failure verdict instead of rediscovering it
      // node by node. Gossiped `Alive` messages carry full registers, so the
      // `Offline` state rides the same broadcast path as `Active` updates.
      if self.membership.offline(&node_id, now_ms)
        && let Some(register) = self.membership.get(&node_id)
      {
        actions.push(SwimAction::Broadcast(SwimMessage::Alive {
          register: register.clone(),
        }));
      }
    }
    actions
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
  fn ack_clears_pending_and_failures_without_bumping_peer_heartbeat() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));
    swim.membership.merge_register(&peer_register("peer"));

    let actions = swim.tick(1_000);
    let seq = match actions[0] {
      SwimAction::SendPing { seq, .. } => seq,
      _ => panic!("expected ping"),
    };

    let heartbeat_before = swim.membership.get("peer").unwrap().heartbeat();
    let ack_actions = swim.on_message("peer", SwimMessage::Ack { seq }, 1_100);
    assert!(ack_actions.is_empty());
    assert!(swim.pending_pings.is_empty());
    // D2: an ack never bumps the peer's heartbeat; only the peer itself may.
    assert_eq!(
      swim.membership.get("peer").unwrap().heartbeat(),
      heartbeat_before
    );
  }

  #[test]
  fn local_node_bumps_own_heartbeat_each_tick() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));

    let _ = swim.tick(1_000);
    assert_eq!(swim.membership.get("local").unwrap().heartbeat(), 1);
    let _ = swim.tick(2_000);
    assert_eq!(swim.membership.get("local").unwrap().heartbeat(), 2);
    // Owner bumps keep the node Active but do not resurrect terminal states.
    assert_eq!(
      swim.membership.get("local").unwrap().state(),
      MemberState::Active
    );
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

  #[test]
  fn suspect_timer_uses_local_observation_time_not_remote_clock() {
    let config = SwimConfig {
      ping_interval_ms: 1_000,
      ping_timeout_ms: 2_000,
      failure_threshold: 1,
      suspect_timeout_ms: 5_000,
    };
    let mut swim = Swim::new("local", config, local_register("local"));

    // A suspect register stamped by a fast remote clock: its timestamp lies
    // far in the local future. The Offline verdict must still advance on the
    // local clock, starting when the rumor is first observed locally.
    let mut rumor = peer_register("peer");
    rumor.suspect(1_000_000);
    let _ = swim.on_message("other", SwimMessage::Alive { register: rumor }, 1_000);
    assert_eq!(
      swim.membership.get("peer").unwrap().state(),
      MemberState::Suspected
    );

    // This tick records the first local observation (t=1_000).
    let _ = swim.tick(1_000);

    // Before the local suspect timeout elapses the peer stays suspected.
    let _ = swim.tick(5_999);
    assert_eq!(
      swim.membership.get("peer").unwrap().state(),
      MemberState::Suspected
    );

    // Once the local timeout has elapsed the peer goes Offline even though
    // the register timestamp is still in the future.
    let actions = swim.tick(6_000);
    assert_eq!(
      swim.membership.get("peer").unwrap().state(),
      MemberState::Offline
    );
    assert!(
      actions.iter().any(|a| matches!(
        a,
        SwimAction::Broadcast(SwimMessage::Alive { register })
          if register.node_id() == "peer" && register.state() == MemberState::Offline
      )),
      "expected an Offline register broadcast, got {actions:?}"
    );
  }

  #[test]
  fn suspected_to_offline_transition_broadcasts_register() {
    let config = SwimConfig {
      ping_interval_ms: 1_000,
      ping_timeout_ms: 2_000,
      failure_threshold: 1,
      suspect_timeout_ms: 5_000,
    };
    let mut swim = Swim::new("local", config, local_register("local"));
    swim.membership.merge_register(&peer_register("peer"));

    // Fail one probe so the peer becomes suspected at t=3_100.
    let _ = swim.tick(1_000);
    let _ = swim.tick(3_100);
    assert_eq!(
      swim.membership.get("peer").unwrap().state(),
      MemberState::Suspected
    );

    // After the suspect timeout expires the peer goes Offline and the new
    // register is gossiped (D4).
    let actions = swim.tick(9_000);
    assert_eq!(
      swim.membership.get("peer").unwrap().state(),
      MemberState::Offline
    );
    assert!(
      actions.iter().any(|a| matches!(
        a,
        SwimAction::Broadcast(SwimMessage::Alive { register })
          if register.node_id() == "peer" && register.state() == MemberState::Offline
      )),
      "expected an Offline register broadcast, got {actions:?}"
    );
  }

  #[test]
  fn offline_rumor_about_local_triggers_refute() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));

    let mut rumor = local_register("local");
    rumor.offline(1_000);
    let actions = swim.on_message("peer", SwimMessage::Alive { register: rumor }, 2_000);

    let local = swim.membership.get("local").unwrap();
    assert_eq!(local.state(), MemberState::Active);
    assert_eq!(local.incarnation(), 2);
    assert!(
      actions.iter().any(|a| matches!(
        a,
        SwimAction::Broadcast(SwimMessage::Alive { register })
          if register.node_id() == "local" && register.state() == MemberState::Active
      )),
      "expected an Active refutation broadcast, got {actions:?}"
    );
  }

  #[test]
  fn stale_rumor_about_local_does_not_inflate_incarnation() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));

    // Refute once: local incarnation becomes 2.
    let _ = swim.on_message(
      "peer",
      SwimMessage::Suspect {
        node_id: "local".to_string(),
        incarnation: 1,
      },
      1_000,
    );
    assert_eq!(swim.membership.get("local").unwrap().incarnation(), 2);

    // An older suspect rumor (incarnation 1 < 2) must be ignored.
    let actions = swim.on_message(
      "peer",
      SwimMessage::Suspect {
        node_id: "local".to_string(),
        incarnation: 1,
      },
      2_000,
    );
    assert!(actions.is_empty());
    assert_eq!(swim.membership.get("local").unwrap().incarnation(), 2);
  }

  #[test]
  fn partition_heals_after_reconnect() {
    // Partition A declares X offline; X lives on in partition B, bumping its
    // own heartbeat. After the partitions reconnect, X refutes the Offline
    // rumor and every view converges back to Active (I2).
    let mut swim_a = Swim::new("a", SwimConfig::default(), local_register("a"));
    let mut offline_x = peer_register("x");
    offline_x.offline(1_000);
    swim_a.membership.merge_register(&offline_x);
    assert_eq!(
      swim_a.membership.get("x").unwrap().state(),
      MemberState::Offline
    );

    let mut swim_x = Swim::new("x", SwimConfig::default(), local_register("x"));
    let _ = swim_x.tick(1_500);

    // X receives the Offline rumor about itself (anti-entropy or gossip) and
    // refutes it with a higher incarnation.
    let refute_actions = swim_x.on_message(
      "a",
      SwimMessage::Alive {
        register: offline_x,
      },
      2_000,
    );
    let refuted = swim_x.membership.get("x").unwrap().clone();
    assert_eq!(refuted.state(), MemberState::Active);
    assert_eq!(refuted.incarnation(), 2);
    assert!(
      refute_actions.iter().any(|a| matches!(
        a,
        SwimAction::Broadcast(SwimMessage::Alive { register })
          if register.node_id() == "x" && register.incarnation() == 2
      )),
      "expected a refutation broadcast, got {refute_actions:?}"
    );

    // A merges the refutation; X is Active everywhere with incarnation 2.
    let _ = swim_a.on_message(
      "x",
      SwimMessage::Alive {
        register: refuted.clone(),
      },
      2_500,
    );
    let x_in_a = swim_a.membership.get("x").unwrap();
    assert_eq!(x_in_a.state(), MemberState::Active);
    assert_eq!(x_in_a.incarnation(), 2);
    assert_eq!(x_in_a, &refuted);
  }
}
