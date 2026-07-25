use std::{
  collections::BTreeMap,
  time::{Duration, Instant},
};

use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};

use crate::NodeId;

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum AddressSource {
  Configured,
  Mdns,
}

#[derive(Debug, Clone)]
struct PeerAddress {
  source: AddressSource,
  expires_at: Option<Instant>,
  last_success: Option<Instant>,
  last_failure: Option<Instant>,
}

impl PeerAddress {
  fn new(source: AddressSource, now: Instant, ttl: Duration) -> Self {
    Self {
      source,
      expires_at: expires_at(source, now, ttl),
      last_success: None,
      last_failure: None,
    }
  }

  fn refresh(&mut self, source: AddressSource, now: Instant, ttl: Duration) {
    self.source = self.source.min(source);
    self.expires_at = expires_at(self.source, now, ttl);
  }
}

#[derive(Debug)]
struct PeerDirectoryEntry {
  peer_id: PeerId,
  addresses: BTreeMap<Multiaddr, PeerAddress>,
  attempts: u32,
  next_attempt_at: Option<Instant>,
  paused: bool,
}

impl PeerDirectoryEntry {
  fn new(peer_id: PeerId) -> Self {
    Self {
      peer_id,
      addresses: BTreeMap::new(),
      attempts: 0,
      next_attempt_at: None,
      paused: false,
    }
  }

  fn candidate(&self, now: Instant) -> Option<Multiaddr> {
    if self.paused || self.next_attempt_at.is_some_and(|deadline| deadline > now) {
      return None;
    }
    self
      .addresses
      .iter()
      .min_by_key(|(address, record)| address_rank(address, record.source))
      .map(|(address, _)| address.clone())
  }

  fn note_success(&mut self, address: &Multiaddr, now: Instant) {
    self.attempts = 0;
    self.next_attempt_at = None;
    if let Some(record) = self.addresses.get_mut(address) {
      record.last_success = Some(now);
    }
  }

  fn note_failure(
    &mut self, address: Option<&Multiaddr>, now: Instant, min_delay: Duration, max_delay: Duration,
  ) {
    let factor = 1_u32.checked_shl(self.attempts.min(20)).unwrap_or(u32::MAX);
    let delay = min_delay.saturating_mul(factor).min(max_delay);
    self.attempts = self.attempts.saturating_add(1);
    self.next_attempt_at = Some(now + delay);
    if let Some(record) = address.and_then(|address| self.addresses.get_mut(address)) {
      record.last_failure = Some(now);
    }
  }

  fn note_connection_closed(&mut self, now: Instant, min_delay: Duration) {
    if !self.paused && self.next_attempt_at.is_none() {
      self.next_attempt_at = Some(now + min_delay);
    }
  }
}

#[derive(Debug, Default)]
pub(crate) struct PeerDirectory {
  entries: BTreeMap<NodeId, PeerDirectoryEntry>,
}

impl PeerDirectory {
  pub(crate) fn record(
    &mut self, node_id: NodeId, peer_id: PeerId, address: Multiaddr, source: AddressSource,
    now: Instant, ttl: Duration,
  ) {
    let entry = self
      .entries
      .entry(node_id)
      .or_insert_with(|| PeerDirectoryEntry::new(peer_id));
    debug_assert_eq!(entry.peer_id, peer_id);
    entry
      .addresses
      .entry(address)
      .and_modify(|record| record.refresh(source, now, ttl))
      .or_insert_with(|| PeerAddress::new(source, now, ttl));
  }

  pub(crate) fn remove(&mut self, peer_id: PeerId, address: &Multiaddr) {
    for entry in self.entries.values_mut() {
      if entry.peer_id == peer_id {
        entry.addresses.remove(address);
      }
    }
  }

  pub(crate) fn expire(&mut self, now: Instant) {
    for entry in self.entries.values_mut() {
      entry
        .addresses
        .retain(|_, record| record.expires_at.is_none_or(|deadline| deadline > now));
    }
  }

  pub(crate) fn resume(&mut self, node_id: NodeId) {
    if let Some(entry) = self.entries.get_mut(&node_id) {
      entry.paused = false;
      entry.next_attempt_at = None;
    }
  }

  pub(crate) fn pause(&mut self, node_id: NodeId) {
    if let Some(entry) = self.entries.get_mut(&node_id) {
      entry.paused = true;
    }
  }

  pub(crate) fn retain(&mut self, mut keep: impl FnMut(NodeId) -> bool) {
    self.entries.retain(|node_id, _| keep(*node_id));
  }

  pub(crate) fn candidates(&self, now: Instant) -> Vec<(NodeId, PeerId, Multiaddr)> {
    self
      .entries
      .iter()
      .filter_map(|(node_id, entry)| {
        entry
          .candidate(now)
          .map(|address| (*node_id, entry.peer_id, address))
      })
      .collect()
  }

  #[cfg(test)]
  pub(crate) fn candidate(&self, node_id: NodeId, now: Instant) -> Option<(PeerId, Multiaddr)> {
    self
      .entries
      .get(&node_id)
      .and_then(|entry| entry.candidate(now).map(|address| (entry.peer_id, address)))
  }

  pub(crate) fn note_success(&mut self, node_id: NodeId, address: &Multiaddr, now: Instant) {
    if let Some(entry) = self.entries.get_mut(&node_id) {
      entry.note_success(address, now);
    }
  }

  pub(crate) fn note_failure(
    &mut self, node_id: NodeId, address: Option<&Multiaddr>, now: Instant, min_delay: Duration,
    max_delay: Duration,
  ) {
    if let Some(entry) = self.entries.get_mut(&node_id) {
      entry.note_failure(address, now, min_delay, max_delay);
    }
  }

  pub(crate) fn note_connection_closed(
    &mut self, node_id: NodeId, now: Instant, min_delay: Duration,
  ) {
    if let Some(entry) = self.entries.get_mut(&node_id) {
      entry.note_connection_closed(now, min_delay);
    }
  }
}

fn expires_at(source: AddressSource, now: Instant, ttl: Duration) -> Option<Instant> {
  match source {
    AddressSource::Configured => None,
    AddressSource::Mdns => Some(now + ttl),
  }
}

fn address_rank(address: &Multiaddr, source: AddressSource) -> (u8, u8, Vec<u8>) {
  let quic = address
    .iter()
    .any(|protocol| matches!(protocol, Protocol::QuicV1));
  let tcp = address
    .iter()
    .any(|protocol| matches!(protocol, Protocol::Tcp(_)));
  let transport = match (quic, tcp) {
    (true, _) => 0,
    (false, true) => 1,
    (false, false) => 2,
  };
  (transport, source as u8, address.to_vec())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn address(text: &str) -> Multiaddr {
    match text.parse() {
      Ok(address) => address,
      Err(error) => panic!("invalid test address {text}: {error}"),
    }
  }

  #[test]
  fn configured_addresses_outlive_and_outrank_mdns_at_the_same_transport() {
    let node_id = NodeId::from_bytes([7; 32]);
    let peer_id = PeerId::random();
    let now = Instant::now();
    let mut directory = PeerDirectory::default();
    directory.record(
      node_id,
      peer_id,
      address("/ip4/192.0.2.1/tcp/4001"),
      AddressSource::Mdns,
      now,
      Duration::from_secs(1),
    );
    directory.record(
      node_id,
      peer_id,
      address("/ip4/192.0.2.2/tcp/4001"),
      AddressSource::Configured,
      now,
      Duration::from_secs(1),
    );

    assert_eq!(
      directory.candidate(node_id, now + Duration::from_secs(2)),
      Some((peer_id, address("/ip4/192.0.2.2/tcp/4001")))
    );
  }

  #[test]
  fn mdns_addresses_expire_and_backoff_defers_retries() {
    let node_id = NodeId::from_bytes([9; 32]);
    let peer_id = PeerId::random();
    let now = Instant::now();
    let discovered = address("/ip4/192.0.2.3/udp/4001/quic-v1");
    let mut directory = PeerDirectory::default();
    directory.record(
      node_id,
      peer_id,
      discovered.clone(),
      AddressSource::Mdns,
      now,
      Duration::from_millis(100),
    );
    assert_eq!(
      directory.candidate(node_id, now),
      Some((peer_id, discovered.clone()))
    );

    directory.note_failure(
      node_id,
      Some(&discovered),
      now,
      Duration::from_millis(50),
      Duration::from_millis(80),
    );
    assert_eq!(
      directory.candidate(node_id, now + Duration::from_millis(40)),
      None
    );
    assert_eq!(
      directory.candidate(node_id, now + Duration::from_millis(60)),
      Some((peer_id, discovered))
    );

    directory.expire(now + Duration::from_millis(150));
    assert_eq!(
      directory.candidate(node_id, now + Duration::from_millis(200)),
      None
    );
  }

  #[test]
  fn paused_entries_do_not_resume_until_an_explicit_dial() {
    let node_id = NodeId::from_bytes([11; 32]);
    let peer_id = PeerId::random();
    let now = Instant::now();
    let mut directory = PeerDirectory::default();
    directory.record(
      node_id,
      peer_id,
      address("/ip4/192.0.2.4/tcp/4001"),
      AddressSource::Configured,
      now,
      Duration::from_secs(1),
    );
    directory.pause(node_id);
    assert_eq!(directory.candidate(node_id, now), None);

    directory.resume(node_id);
    assert!(directory.candidate(node_id, now).is_some());
  }
}
