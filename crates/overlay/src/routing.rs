use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Envelope, FrameError, NodeId, NodeIdentity, RequestId, identity::decode_public_key};

/// Maximum edges one node may advertise in a single link-state record.
pub const MAX_LINK_STATE_EDGES: usize = 256;
/// Maximum body bytes forwarded through the sparse overlay.
pub const MAX_ROUTE_BODY_BYTES: usize = 1024 * 1024;

const DEFAULT_SEEN_CAPACITY: usize = 1024;
const DEFAULT_MAX_INFLIGHT: usize = 256;

#[derive(Debug, Error)]
pub enum RoutingError {
  #[error("link state advertises too many edges")]
  TooManyEdges,
  #[error("link state key is not the authorized key for the node")]
  UnauthorizedKey,
  #[error("link state signature is invalid")]
  InvalidSignature,
  #[error(transparent)]
  Identity(#[from] crate::IdentityError),
  #[error(transparent)]
  Encoding(#[from] postcard::Error),
  #[error(transparent)]
  Frame(#[from] FrameError),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct LinkStateBody {
  node_id: NodeId,
  public_key: Vec<u8>,
  edges: Vec<NodeId>,
  sequence: u64,
}

/// A signed advertisement of one node's confirmed logical edges.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkStateRecord {
  body: LinkStateBody,
  signature: Vec<u8>,
}

impl LinkStateRecord {
  pub fn sign(
    identity: &NodeIdentity, mut edges: Vec<NodeId>, sequence: u64,
  ) -> Result<Self, RoutingError> {
    edges.retain(|edge| *edge != identity.node_id());
    edges.sort();
    edges.dedup();
    if edges.len() > MAX_LINK_STATE_EDGES {
      return Err(RoutingError::TooManyEdges);
    }
    let body = LinkStateBody {
      node_id: identity.node_id(),
      public_key: identity.public_key_bytes(),
      edges,
      sequence,
    };
    let signature = identity.sign(&postcard::to_stdvec(&body)?)?;
    Ok(Self { body, signature })
  }

  pub const fn node_id(&self) -> NodeId {
    self.body.node_id
  }

  pub const fn sequence(&self) -> u64 {
    self.body.sequence
  }

  pub fn edges(&self) -> &[NodeId] {
    &self.body.edges
  }

  /// Verify that the record carries the currently authorized key for the
  /// node and a valid signature over the advertised body.
  pub fn verify_with(&self, authorized_key: &[u8]) -> Result<(), RoutingError> {
    if self.body.edges.len() > MAX_LINK_STATE_EDGES {
      return Err(RoutingError::TooManyEdges);
    }
    if self.body.public_key != authorized_key {
      return Err(RoutingError::UnauthorizedKey);
    }
    let key = decode_public_key(&self.body.public_key)?;
    let bytes = postcard::to_stdvec(&self.body)?;
    if !key.verify(&bytes, &self.signature) {
      return Err(RoutingError::InvalidSignature);
    }
    Ok(())
  }
}

/// The highest-sequence verified link-state record known for each node.
#[derive(Debug, Default)]
pub struct LinkStateDb {
  records: BTreeMap<NodeId, LinkStateRecord>,
}

impl LinkStateDb {
  /// Insert a verified record, returning `true` when it advanced the
  /// database. Older or equal sequences are ignored.
  pub fn insert(
    &mut self, record: LinkStateRecord, authorized_key: &[u8],
  ) -> Result<bool, RoutingError> {
    record.verify_with(authorized_key)?;
    let stale = self
      .records
      .get(&record.node_id())
      .is_some_and(|current| record.sequence() <= current.sequence());
    if stale {
      return Ok(false);
    }
    self.records.insert(record.node_id(), record);
    Ok(true)
  }

  pub fn record(&self, node_id: NodeId) -> Option<&LinkStateRecord> {
    self.records.get(&node_id)
  }

  pub fn records(&self) -> impl Iterator<Item = &LinkStateRecord> {
    self.records.values()
  }

  /// Deterministic breadth-first next hop from `local` to `destination`.
  /// Returns `destination` itself when it is a direct neighbor, `None` when
  /// no path exists.
  pub fn next_hop(&self, local: NodeId, destination: NodeId) -> Option<NodeId> {
    if local == destination {
      return Some(destination);
    }
    let mut visited = BTreeSet::from([local]);
    let mut parent: BTreeMap<NodeId, NodeId> = BTreeMap::new();
    let mut queue = VecDeque::from([local]);
    while let Some(node) = queue.pop_front() {
      let Some(record) = self.records.get(&node) else {
        continue;
      };
      for edge in record.edges() {
        if !visited.insert(*edge) {
          continue;
        }
        parent.insert(*edge, node);
        if *edge == destination {
          let mut hop = destination;
          while let Some(previous) = parent.get(&hop).copied() {
            if previous == local {
              return Some(hop);
            }
            hop = previous;
          }
          return None;
        }
        queue.push_back(*edge);
      }
    }
    None
  }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DropReason {
  Expired,
  Duplicate,
  HopLimitExhausted,
  Backpressure,
  PayloadTooLarge,
  NoRoute,
}

#[derive(Debug)]
pub enum RouteDecision {
  Deliver(Envelope),
  Forward {
    next_hop: NodeId,
    envelope: Envelope,
  },
  Drop(DropReason),
}

/// Bounded forwarding engine for sparse-graph routed envelopes.
#[derive(Debug)]
pub struct Router {
  local: NodeId,
  links: LinkStateDb,
  seen: VecDeque<RequestId>,
  seen_set: BTreeSet<RequestId>,
  seen_capacity: usize,
  inflight: usize,
  max_inflight: usize,
}

impl Router {
  pub fn new(local: NodeId) -> Self {
    Self {
      local,
      links: LinkStateDb::default(),
      seen: VecDeque::new(),
      seen_set: BTreeSet::new(),
      seen_capacity: DEFAULT_SEEN_CAPACITY,
      inflight: 0,
      max_inflight: DEFAULT_MAX_INFLIGHT,
    }
  }

  pub const fn with_bounds(mut self, seen_capacity: usize, max_inflight: usize) -> Self {
    self.seen_capacity = seen_capacity;
    self.max_inflight = max_inflight;
    self
  }

  pub const fn links(&self) -> &LinkStateDb {
    &self.links
  }

  pub fn links_mut(&mut self) -> &mut LinkStateDb {
    &mut self.links
  }

  pub const fn inflight(&self) -> usize {
    self.inflight
  }

  /// Release one in-flight forward slot after the downstream write settled.
  pub fn complete_forward(&mut self) {
    self.inflight = self.inflight.saturating_sub(1);
  }

  /// Decide what to do with an inbound routed envelope.
  pub fn handle(&mut self, envelope: Envelope, now_unix_ms: i64) -> RouteDecision {
    let header = envelope.header().clone();
    if header.deadline_unix_ms <= now_unix_ms {
      return RouteDecision::Drop(DropReason::Expired);
    }
    if !self.mark_seen(header.request_id) {
      return RouteDecision::Drop(DropReason::Duplicate);
    }
    if header.destination == self.local {
      return RouteDecision::Deliver(envelope);
    }
    if header.remaining_hops == 0 {
      return RouteDecision::Drop(DropReason::HopLimitExhausted);
    }
    if envelope.payload().len() > MAX_ROUTE_BODY_BYTES {
      return RouteDecision::Drop(DropReason::PayloadTooLarge);
    }
    if self.inflight >= self.max_inflight {
      return RouteDecision::Drop(DropReason::Backpressure);
    }
    let Some(next_hop) = self.links.next_hop(self.local, header.destination) else {
      return RouteDecision::Drop(DropReason::NoRoute);
    };
    let mut forwarded = header;
    forwarded.remaining_hops -= 1;
    let envelope = match Envelope::new(forwarded, envelope.into_payload()) {
      Ok(envelope) => envelope,
      Err(_) => return RouteDecision::Drop(DropReason::PayloadTooLarge),
    };
    self.inflight += 1;
    RouteDecision::Forward { next_hop, envelope }
  }

  fn mark_seen(&mut self, request_id: RequestId) -> bool {
    if !self.seen_set.insert(request_id) {
      return false;
    }
    self.seen.push_back(request_id);
    while self.seen.len() > self.seen_capacity {
      if let Some(oldest) = self.seen.pop_front() {
        self.seen_set.remove(&oldest);
      }
    }
    true
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    AuthorizationRecord, AuthorizationRegistry,
    protocol::{MessageKind, ProtocolId},
  };

  fn must<T, E: std::fmt::Debug>(result: Result<T, E>) -> T {
    match result {
      Ok(value) => value,
      Err(error) => panic!("unexpected failure: {error:?}"),
    }
  }

  fn member() -> (NodeIdentity, AuthorizationRecord) {
    let identity = NodeIdentity::generate();
    let (_, record) = must(AuthorizationRecord::genesis(&identity));
    (identity, record)
  }

  fn authorized_key(record: &AuthorizationRecord) -> Vec<u8> {
    record.public_key().to_vec()
  }

  fn chain(nodes: &[NodeIdentity], sequence: u64) -> Vec<(AuthorizationRecord, LinkStateRecord)> {
    let mut signed = Vec::new();
    for (index, identity) in nodes.iter().enumerate() {
      let mut edges = Vec::new();
      if index > 0 {
        edges.push(nodes[index - 1].node_id());
      }
      if index + 1 < nodes.len() {
        edges.push(nodes[index + 1].node_id());
      }
      let (_, genesis) = must(AuthorizationRecord::genesis(identity));
      let link_state = must(LinkStateRecord::sign(identity, edges, sequence));
      signed.push((genesis, link_state));
    }
    signed
  }

  fn envelope(source: NodeId, destination: NodeId, hops: u8, deadline: i64) -> Envelope {
    let header = crate::EnvelopeHeader {
      version: crate::PROTOCOL_VERSION,
      cluster_id: crate::ClusterId::from_genesis(source),
      request_id: RequestId::from_bytes([9; RequestId::BYTE_LENGTH]),
      source,
      destination,
      protocol: ProtocolId::Route,
      kind: MessageKind::Request,
      deadline_unix_ms: deadline,
      remaining_hops: hops,
    };
    must(Envelope::new(header, b"payload".to_vec()))
  }

  #[test]
  fn link_state_verifies_only_with_the_authorized_key() {
    let (identity, record) = member();
    let link_state = must(LinkStateRecord::sign(&identity, Vec::new(), 7));
    must(link_state.verify_with(&authorized_key(&record)));

    let (other, _) = member();
    let forged = must(LinkStateRecord::sign(&other, Vec::new(), 7));
    assert!(matches!(
      forged.verify_with(&authorized_key(&record)),
      Err(RoutingError::UnauthorizedKey)
    ));

    let mut tampered = link_state;
    tampered.body.sequence = 8;
    assert!(matches!(
      tampered.verify_with(&authorized_key(&record)),
      Err(RoutingError::InvalidSignature)
    ));
  }

  #[test]
  fn link_state_db_keeps_the_highest_sequence() {
    let (identity, record) = member();
    let key = authorized_key(&record);
    let mut db = LinkStateDb::default();
    assert!(must(db.insert(
      must(LinkStateRecord::sign(&identity, Vec::new(), 1)),
      &key,
    )));
    assert!(!must(db.insert(
      must(LinkStateRecord::sign(&identity, Vec::new(), 1)),
      &key,
    )));
    assert!(must(db.insert(
      must(LinkStateRecord::sign(&identity, Vec::new(), 2)),
      &key,
    )));
    let Some(stored) = db.record(identity.node_id()) else {
      panic!("record must be stored");
    };
    assert_eq!(stored.sequence(), 2);
  }

  #[test]
  fn next_hop_follows_the_shortest_path_deterministically() {
    let identities: Vec<NodeIdentity> = (0..4).map(|_| NodeIdentity::generate()).collect();
    let signed = chain(&identities, 1);
    let mut db = LinkStateDb::default();
    for (authorization, link_state) in &signed {
      assert!(must(
        db.insert(link_state.clone(), &authorized_key(authorization))
      ));
    }
    let first = identities[0].node_id();
    let second = identities[1].node_id();
    let last = identities[3].node_id();

    assert_eq!(db.next_hop(first, last), Some(second));
    assert_eq!(db.next_hop(first, first), Some(first));
    let (stranger, _) = member();
    assert_eq!(db.next_hop(first, stranger.node_id()), None);
  }

  fn with_request_id(envelope: Envelope, byte: u8) -> Envelope {
    let mut header = envelope.header().clone();
    header.request_id = RequestId::from_bytes([byte; RequestId::BYTE_LENGTH]);
    must(Envelope::new(header, envelope.into_payload()))
  }

  #[test]
  fn router_delivers_locally_and_suppresses_replays() {
    let (local_identity, _) = member();
    let (source_identity, _) = member();
    let mut router = Router::new(local_identity.node_id());
    let request = envelope(source_identity.node_id(), local_identity.node_id(), 4, 10);

    assert!(matches!(
      router.handle(request.clone(), 1),
      RouteDecision::Deliver(delivered) if delivered == request
    ));
    assert!(matches!(
      router.handle(request, 1),
      RouteDecision::Drop(DropReason::Duplicate)
    ));
  }

  #[test]
  fn router_forwards_with_decremented_hops() {
    let identities: Vec<NodeIdentity> = (0..3).map(|_| NodeIdentity::generate()).collect();
    let signed = chain(&identities, 1);
    let middle = identities[1].node_id();
    let mut router = Router::new(middle);
    for (authorization, link_state) in &signed {
      assert!(must(
        router
          .links_mut()
          .insert(link_state.clone(), &authorized_key(authorization))
      ));
    }
    let request = envelope(identities[0].node_id(), identities[2].node_id(), 4, 10);

    let RouteDecision::Forward { next_hop, envelope } = router.handle(request, 1) else {
      panic!("request must be forwarded");
    };
    assert_eq!(next_hop, identities[2].node_id());
    assert_eq!(envelope.header().remaining_hops, 3);
    assert_eq!(router.inflight(), 1);
    router.complete_forward();
    assert_eq!(router.inflight(), 0);
  }

  #[test]
  fn router_enforces_deadline_hop_limit_and_backpressure() {
    let identities: Vec<NodeIdentity> = (0..3).map(|_| NodeIdentity::generate()).collect();
    let signed = chain(&identities, 1);
    let middle = identities[1].node_id();
    let mut router = Router::new(middle).with_bounds(8, 1);
    for (authorization, link_state) in &signed {
      assert!(must(
        router
          .links_mut()
          .insert(link_state.clone(), &authorized_key(authorization))
      ));
    }
    let source = identities[0].node_id();
    let destination = identities[2].node_id();

    let expired = with_request_id(envelope(source, destination, 4, 1), 1);
    assert!(matches!(
      router.handle(expired, 1),
      RouteDecision::Drop(DropReason::Expired)
    ));
    let exhausted = with_request_id(envelope(source, destination, 0, 10), 2);
    assert!(matches!(
      router.handle(exhausted, 1),
      RouteDecision::Drop(DropReason::HopLimitExhausted)
    ));
    let forwarded = with_request_id(envelope(source, destination, 4, 10), 3);
    assert!(matches!(
      router.handle(forwarded, 1),
      RouteDecision::Forward { .. }
    ));
    let second = with_request_id(envelope(source, destination, 4, 10), 4);
    assert!(matches!(
      router.handle(second, 1),
      RouteDecision::Drop(DropReason::Backpressure)
    ));
  }

  #[test]
  fn router_drops_oversized_bodies_and_missing_routes() {
    let (local_identity, _) = member();
    let (source_identity, _) = member();
    let mut router = Router::new(local_identity.node_id());
    let (other_identity, _) = member();
    let header = crate::EnvelopeHeader {
      version: crate::PROTOCOL_VERSION,
      cluster_id: crate::ClusterId::from_genesis(source_identity.node_id()),
      request_id: RequestId::from_bytes([5; RequestId::BYTE_LENGTH]),
      source: source_identity.node_id(),
      destination: other_identity.node_id(),
      protocol: ProtocolId::Route,
      kind: MessageKind::Request,
      deadline_unix_ms: 10,
      remaining_hops: 4,
    };
    let oversized = must(Envelope::new(
      header.clone(),
      vec![0; MAX_ROUTE_BODY_BYTES + 1],
    ));
    assert!(matches!(
      router.handle(oversized, 1),
      RouteDecision::Drop(DropReason::PayloadTooLarge)
    ));
    let small = with_request_id(must(Envelope::new(header, b"body".to_vec())), 6);
    assert!(matches!(
      router.handle(small, 1),
      RouteDecision::Drop(DropReason::NoRoute)
    ));
  }

  #[test]
  fn router_dedup_cache_evicts_the_oldest_request() {
    let (local_identity, _) = member();
    let (source_identity, _) = member();
    let mut router = Router::new(local_identity.node_id()).with_bounds(2, 4);
    for byte in 0..3_u8 {
      let mut request = envelope(source_identity.node_id(), local_identity.node_id(), 4, 10);
      let mut header = request.header().clone();
      header.request_id = RequestId::from_bytes([byte; RequestId::BYTE_LENGTH]);
      request = must(Envelope::new(header, request.into_payload()));
      assert!(matches!(
        router.handle(request, 1),
        RouteDecision::Deliver(_)
      ));
    }
    let mut replay = envelope(source_identity.node_id(), local_identity.node_id(), 4, 10);
    let mut header = replay.header().clone();
    header.request_id = RequestId::from_bytes([0; RequestId::BYTE_LENGTH]);
    replay = must(Envelope::new(header, replay.into_payload()));
    assert!(matches!(
      router.handle(replay, 1),
      RouteDecision::Deliver(_)
    ));
  }

  #[test]
  fn authorized_registry_supplies_the_current_key_for_link_state() {
    let (identity, record) = member();
    let registry = must(AuthorizationRegistry::from_records(
      record.cluster_id(),
      [record.clone()],
    ));
    let link_state = must(LinkStateRecord::sign(&identity, Vec::new(), 3));
    let Some(active) = registry.active_record_for_node(identity.node_id()) else {
      panic!("genesis record must be active");
    };
    let mut db = LinkStateDb::default();
    assert!(must(db.insert(link_state, active.public_key())));
  }
}
