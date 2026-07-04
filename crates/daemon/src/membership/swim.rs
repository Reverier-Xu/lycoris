use std::collections::HashMap;

use crate::membership::{
  crdt::{MemberRegister, MemberState, Membership},
  detector::PhiAccrualDetector,
};

/// Configuration for the SWIM failure detector.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SwimConfig {
  /// Milliseconds between periodic probe rounds.
  pub ping_interval_ms: u64,
  /// Milliseconds to wait for a direct ack before escalating.
  pub ping_timeout_ms: u64,
  /// Number of random members to ask for an indirect probe.
  pub indirect_probe_count: usize,
  /// Milliseconds to keep a node suspected before declaring it failed.
  pub suspicion_timeout_ms: u64,
  /// Phi value at which a peer is considered suspected.
  pub phi_threshold: f64,
  /// Window size for the per-peer Phi detector.
  pub phi_window_size: usize,
}

impl Default for SwimConfig {
  fn default() -> Self {
    Self {
      ping_interval_ms: 1_000,
      ping_timeout_ms: 5_000,
      indirect_probe_count: 3,
      suspicion_timeout_ms: 10_000,
      phi_threshold: 8.0,
      phi_window_size: 100,
    }
  }
}

/// A SWIM protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwimMessage {
  /// Direct liveness probe.
  Ping { seq: u64 },
  /// Request that `target` be probed indirectly.
  PingReq { target: String, seq: u64 },
  /// Positive response to a `Ping` or `PingReq`.
  Ack { seq: u64 },
  /// Negative response: the target could not be reached.
  Nack { seq: u64 },
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
  /// Ask `proxy` to probe `target` on our behalf.
  SendPingReq {
    proxy: String,
    target: String,
    seq: u64,
  },
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
  indirect_sent: bool,
}

/// State for an indirect probe that is waiting for proxy responses.
#[derive(Debug, Clone)]
struct PendingIndirect {
  target: String,
  proxies: Vec<String>,
  sent_at: u64,
}

/// A SWIM membership layer backed by the CRDT membership set and Phi accrual
/// failure detectors.
///
/// `Swim` is intentionally transport agnostic. It consumes events and emits
/// `SwimAction`s that the caller must dispatch over the network.
#[derive(Debug, Clone)]
pub struct Swim {
  local_node_id: String,
  config: SwimConfig,
  membership: Membership,
  detectors: HashMap<String, PhiAccrualDetector>,
  sequence: u64,
  pending_pings: HashMap<u64, PendingPing>,
  pending_indirect: HashMap<u64, PendingIndirect>,
  last_probe_ms: HashMap<String, u64>,
  next_probe_at_ms: u64,
}

impl Swim {
  /// Create a new SWIM instance seeded with the local node.
  pub fn new(local_node_id: impl Into<String>, config: SwimConfig, local: MemberRegister) -> Self {
    let local_node_id = local_node_id.into();
    let mut membership = Membership::new();
    membership.merge_register(&local);

    let mut detectors = HashMap::new();
    detectors.insert(
      local_node_id.clone(),
      PhiAccrualDetector::with_window(config.phi_window_size),
    );

    Self {
      local_node_id,
      config,
      membership,
      detectors,
      sequence: 0,
      pending_pings: HashMap::new(),
      pending_indirect: HashMap::new(),
      last_probe_ms: HashMap::new(),
      next_probe_at_ms: 0,
    }
  }

  /// Read-only access to the underlying membership CRDT.
  pub fn membership(&self) -> &Membership {
    &self.membership
  }

  /// Return the current phi value for `node_id` at `now_ms`.
  pub fn phi(&self, node_id: &str, now_ms: u64) -> f64 {
    self
      .detectors
      .get(node_id)
      .map(|d| d.phi(now_ms))
      .unwrap_or(0.0)
  }

  /// Record a heartbeat for `node_id` at `now_ms`.
  ///
  /// This updates both the failure detector and the CRDT register (if the node
  /// is already known).
  pub fn heartbeat(&mut self, node_id: &str, now_ms: i64) {
    let now_u64 = u64::try_from(now_ms).unwrap_or(0);
    self
      .detectors
      .entry(node_id.to_string())
      .or_insert_with(|| PhiAccrualDetector::with_window(self.config.phi_window_size))
      .heartbeat(now_u64);

    if let Some(register) = self.membership.get_mut(node_id) {
      register.heartbeat(now_ms);
    }
  }

  /// Process an incoming SWIM message from `from`.
  pub fn on_message(&mut self, from: &str, message: SwimMessage, now_ms: i64) -> Vec<SwimAction> {
    let now_u64 = u64::try_from(now_ms).unwrap_or(0);
    match message {
      SwimMessage::Ping { seq } => {
        vec![SwimAction::SendAck {
          to: from.to_string(),
          seq,
        }]
      }
      SwimMessage::PingReq { target, seq } => self.handle_ping_req(from, &target, seq, now_ms),
      SwimMessage::Ack { seq } => self.handle_ack(seq, now_ms),
      SwimMessage::Nack { seq } => self.handle_nack(seq, now_u64),
      SwimMessage::Suspect {
        node_id,
        incarnation,
      } => {
        self.handle_suspect_state(&node_id, incarnation, now_ms);
        Vec::new()
      }
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

  /// Advance timers and possibly issue a new probe.
  ///
  /// Callers should drive this method from a periodic tick loop.
  pub fn tick(&mut self, now_ms: i64) -> Vec<SwimAction> {
    let now_u64 = u64::try_from(now_ms).unwrap_or(0);
    let mut actions = self.check_timeouts(now_u64, now_ms);
    actions.extend(self.maybe_send_probe(now_u64));
    actions
  }

  fn handle_ping_req(
    &mut self, from: &str, target: &str, seq: u64, now_ms: i64,
  ) -> Vec<SwimAction> {
    if target == self.local_node_id {
      return vec![SwimAction::SendAck {
        to: from.to_string(),
        seq,
      }];
    }

    if self.membership.get(target).is_some() {
      self
        .send_ping_to(target, now_ms)
        .map(|action| vec![action])
        .unwrap_or_default()
    } else {
      vec![SwimAction::SendAck {
        to: from.to_string(),
        seq,
      }]
    }
  }

  fn handle_ack(&mut self, seq: u64, now_ms: i64) -> Vec<SwimAction> {
    let target = if let Some(pending) = self.pending_pings.remove(&seq) {
      Some(pending.target)
    } else if let Some(pending) = self.pending_indirect.remove(&seq) {
      Some(pending.target)
    } else {
      None
    };

    let Some(target) = target else {
      return Vec::new();
    };

    self.heartbeat(&target, now_ms);

    let mut actions = Vec::new();
    if let Some(register) = self.membership.get(&target)
      && register.state == MemberState::Suspected
    {
      actions.push(SwimAction::Broadcast(SwimMessage::Alive {
        register: register.clone(),
      }));
    }
    actions
  }

  fn handle_nack(&mut self, seq: u64, now_u64: u64) -> Vec<SwimAction> {
    let local_id = self.local_node_id().to_string();
    if let Some(pending) = self.pending_indirect.get_mut(&seq) {
      pending.proxies.retain(|proxy| proxy != &local_id);
    }

    if let Some(pending) = self.pending_indirect.get(&seq)
      && pending.proxies.is_empty()
    {
      let target = pending.target.clone();
      let now_ms = i64::try_from(now_u64).unwrap_or(0);
      return self.mark_suspected(&target, now_ms);
    }
    Vec::new()
  }

  fn handle_suspect_state(&mut self, node_id: &str, incarnation: u64, now_ms: i64) {
    if node_id == self.local_node_id {
      return;
    }

    if let Some(existing) = self.membership.get(node_id) {
      if existing.incarnation > incarnation {
        return;
      }
      let mut register = existing.clone();
      register.incarnation = incarnation;
      register.state = MemberState::Suspected;
      register.updated_at_ms = now_ms;
      self.membership.merge_register(&register);
    }
  }

  fn handle_alive_state(&mut self, register: &MemberRegister, now_ms: i64) {
    if register.node_id == self.local_node_id {
      return;
    }
    self.membership.merge_register(register);
    let now_u64 = u64::try_from(now_ms).unwrap_or(0);
    self
      .detectors
      .entry(register.node_id.clone())
      .or_insert_with(|| PhiAccrualDetector::with_window(self.config.phi_window_size))
      .heartbeat(now_u64);
  }

  fn handle_leave_state(&mut self, node_id: &str, incarnation: u64, now_ms: i64) {
    if node_id == self.local_node_id {
      return;
    }
    if let Some(existing) = self.membership.get(node_id) {
      if existing.incarnation > incarnation {
        return;
      }
      let mut register = existing.clone();
      register.incarnation = incarnation;
      register.state = MemberState::Leaving;
      register.updated_at_ms = now_ms;
      self.membership.merge_register(&register);
    }
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
      let escalate = self
        .pending_pings
        .get(&seq)
        .is_some_and(|pending| !pending.indirect_sent);

      if escalate {
        let target = self
          .pending_pings
          .get(&seq)
          .map(|pending| pending.target.clone())
          .unwrap_or_default();
        let proxies = self.pick_indirect_proxies(&target, self.config.indirect_probe_count);

        if proxies.is_empty() {
          if let Some(pending) = self.pending_pings.remove(&seq) {
            actions.extend(self.mark_suspected(&pending.target, now_ms));
          }
        } else {
          if let Some(pending) = self.pending_pings.get_mut(&seq) {
            pending.indirect_sent = true;
          }
          self.pending_indirect.insert(
            seq,
            PendingIndirect {
              target: target.clone(),
              proxies: proxies.clone(),
              sent_at: now_u64,
            },
          );
          for proxy in proxies {
            actions.push(SwimAction::SendPingReq {
              proxy,
              target: target.clone(),
              seq,
            });
          }
        }
        continue;
      }

      if let Some(pending) = self.pending_pings.remove(&seq) {
        actions.extend(self.mark_suspected(&pending.target, now_ms));
      }
    }

    let indirect_timed_out: Vec<u64> = self
      .pending_indirect
      .iter()
      .filter(|(_, pending)| now_u64 - pending.sent_at >= self.config.ping_timeout_ms)
      .map(|(seq, _)| *seq)
      .collect();

    for seq in indirect_timed_out {
      self.pending_pings.remove(&seq);
      if let Some(pending) = self.pending_indirect.remove(&seq) {
        actions.extend(self.mark_suspected(&pending.target, now_ms));
      }
    }

    actions
  }

  fn mark_suspected(&mut self, target: &str, now_ms: i64) -> Vec<SwimAction> {
    let mut actions = Vec::new();
    if let Some(register) = self.membership.get_mut(target)
      && register.state == MemberState::Active
    {
      register.suspect(now_ms);
      actions.push(SwimAction::Broadcast(SwimMessage::Suspect {
        node_id: target.to_string(),
        incarnation: register.incarnation,
      }));
    }
    actions
  }

  fn maybe_send_probe(&mut self, now_u64: u64) -> Option<SwimAction> {
    if now_u64 < self.next_probe_at_ms {
      return None;
    }

    if !self.pending_pings.is_empty() {
      return None;
    }

    let target = self.pick_probe_target()?;
    self.next_probe_at_ms = now_u64 + self.config.ping_interval_ms;
    self.send_ping_to(&target, i64::try_from(now_u64).unwrap_or(0))
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
        indirect_sent: false,
      },
    );
    self.last_probe_ms.insert(target.to_string(), now_u64);
    Some(SwimAction::SendPing {
      target: target.to_string(),
      seq,
    })
  }

  fn pick_probe_target(&self) -> Option<String> {
    let active: Vec<&MemberRegister> = self
      .membership
      .active()
      .into_iter()
      .filter(|r| r.node_id != self.local_node_id)
      .collect();

    if active.is_empty() {
      return None;
    }

    active
      .iter()
      .min_by_key(|r| self.last_probe_ms.get(&r.node_id).copied().unwrap_or(0))
      .map(|r| r.node_id.clone())
  }

  fn pick_indirect_proxies(&self, target: &str, count: usize) -> Vec<String> {
    let mut proxies: Vec<String> = self
      .membership
      .active()
      .into_iter()
      .filter(|r| r.node_id != self.local_node_id && r.node_id != target)
      .map(|r| r.node_id.clone())
      .collect();

    proxies.sort();
    proxies.truncate(count);
    proxies
  }

  pub fn local_node_id(&self) -> &str {
    &self.local_node_id
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn local_register(id: &str) -> MemberRegister {
    let mut r = MemberRegister::new(id, "127.0.0.1:1", 1, 0);
    r.updated_at_ms = 0;
    r
  }

  fn peer_register(id: &str) -> MemberRegister {
    let mut r = MemberRegister::new(id, "127.0.0.1:2", 1, 0);
    r.updated_at_ms = 0;
    r
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
    assert!(swim.membership.get("peer").unwrap().heartbeat > 0);
  }

  #[test]
  fn timeout_marks_suspected_and_broadcasts() {
    let config = SwimConfig {
      ping_interval_ms: 1_000,
      ping_timeout_ms: 5_000,
      ..SwimConfig::default()
    };
    let mut swim = Swim::new("local", config, local_register("local"));
    swim.membership.merge_register(&peer_register("peer"));

    let _ = swim.tick(1_000);
    let actions = swim.tick(6_100);

    let has_suspect = actions.iter().any(|a| {
      matches!(
        a,
        SwimAction::Broadcast(SwimMessage::Suspect { node_id, .. }) if node_id == "peer"
      )
    });
    assert!(has_suspect);
    assert_eq!(
      swim.membership.get("peer").unwrap().state,
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
      swim.membership.get("peer").unwrap().state,
      MemberState::Suspected
    );
  }

  #[test]
  fn alive_state_merges_register() {
    let mut swim = Swim::new("local", SwimConfig::default(), local_register("local"));
    let mut register = peer_register("peer");
    register.heartbeat(2_000);

    swim.on_message("peer", SwimMessage::Alive { register }, 2_000);
    assert_eq!(swim.membership.get("peer").unwrap().heartbeat, 1);
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
