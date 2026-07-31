use std::{
  collections::{BTreeMap, BTreeSet},
  sync::Arc,
  time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::StreamExt;
use libp2p::{
  Multiaddr, PeerId, Swarm, SwarmBuilder,
  core::ConnectedPoint,
  dcutr, mdns,
  multiaddr::Protocol,
  noise, ping, relay, request_response,
  swarm::{
    ConnectionId, NetworkBehaviour, SwarmEvent, behaviour::toggle::Toggle, dial_opts::DialOpts,
  },
  tcp, yamux,
};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::{
  sync::{mpsc, oneshot, watch},
  task::JoinHandle,
};

use super::{
  LinkCommand, LinkConfig, LinkError, LinkHandle, LinkSnapshot,
  directory::{AddressSource, PeerDirectory},
  handle::{InboundEnvelope, InboundToken, REQUEST_BOOT_ID_BYTES, RequestSequence},
  messaging::{EnvelopeCodec, OVERLAY_STREAM_PROTOCOL},
};
use crate::{
  AuthorizationRegistry, Envelope, EnvelopeHeader, LinkStateRecord, MessageKind, NodeId,
  NodeIdentity, PROTOCOL_VERSION, ProtocolId, RequestId, RouteDecision, Router,
  authorization::AuthorizationError,
};

const MAX_PENDING_INBOUND: usize = 256;
const MAX_INBOUND_LIFETIME_MS: i64 = 10_000;

pub struct LinkRuntime {
  handle: LinkHandle,
  task: Option<JoinHandle<()>>,
}

impl LinkRuntime {
  pub fn start(
    identity: &NodeIdentity, config: LinkConfig, authorization: AuthorizationRegistry,
  ) -> Result<Self, LinkError> {
    Self::start_inner(
      identity,
      config,
      authorization,
      random_request_boot_id()?,
      0,
    )
  }

  #[cfg(test)]
  pub(crate) fn start_with_request_sequence(
    identity: &NodeIdentity, config: LinkConfig, authorization: AuthorizationRegistry,
    boot_id: [u8; REQUEST_BOOT_ID_BYTES], next_sequence: u64,
  ) -> Result<Self, LinkError> {
    Self::start_inner(identity, config, authorization, boot_id, next_sequence)
  }

  fn start_inner(
    identity: &NodeIdentity, config: LinkConfig, authorization: AuthorizationRegistry,
    boot_id: [u8; REQUEST_BOOT_ID_BYTES], next_sequence: u64,
  ) -> Result<Self, LinkError> {
    let mut swarm = build_swarm(identity, &config)?;
    for address in config.listen_addresses() {
      swarm
        .listen_on(address.clone())
        .map_err(|error| LinkError::Transport(error.to_string()))?;
    }

    let local_peer_id = identity.peer_id();
    let state = LinkState::new(identity.node_id(), local_peer_id, authorization);
    let (snapshot_tx, snapshots) = watch::channel(state.snapshot());
    let (commands, command_rx) = mpsc::channel(config.command_capacity());
    let (inbound_tx, inbound_rx) = mpsc::channel(config.command_capacity());
    let request_ids = Arc::new(RequestSequence::new(boot_id, next_sequence));
    let actor = LinkActor {
      swarm,
      command_rx,
      snapshot_tx,
      state,
      config: config.clone(),
      maintenance: tokio::time::interval(config.reconnect_interval()),
      identity: identity.clone(),
      router: Router::new(identity.node_id()),
      inbound_tx,
      pending_outbound: BTreeMap::new(),
      pending_inbound: BTreeMap::new(),
      inbound_requests: BTreeMap::new(),
      pending_forwards: BTreeMap::new(),
      broadcast_requests: BTreeSet::new(),
      next_inbound_token: 0,
      request_ids: request_ids.clone(),
      last_link_state_sequence: 0,
    };
    let task = tokio::spawn(actor.run());
    Ok(Self {
      handle: LinkHandle::new(commands, snapshots, inbound_rx, request_ids),
      task: Some(task),
    })
  }

  pub fn handle(&self) -> LinkHandle {
    self.handle.clone()
  }

  pub async fn shutdown(mut self) -> Result<(), LinkError> {
    self.handle.shutdown().await?;
    let task = self.task.take().ok_or(LinkError::ActorStopped)?;
    task.await?;
    Ok(())
  }
}

impl Drop for LinkRuntime {
  fn drop(&mut self) {
    if let Some(task) = self.task.take() {
      task.abort();
    }
  }
}

#[derive(NetworkBehaviour)]
struct LinkBehaviour {
  ping: ping::Behaviour,
  mdns: Toggle<mdns::tokio::Behaviour>,
  relay_client: relay::client::Behaviour,
  relay_server: relay::Behaviour,
  dcutr: dcutr::Behaviour,
  messaging: request_response::Behaviour<EnvelopeCodec>,
}

struct PendingRequest {
  expected: ResponseExpectation,
  reply: oneshot::Sender<Result<Envelope, LinkError>>,
}

struct PendingInbound {
  request_id: request_response::InboundRequestId,
  peer_id: PeerId,
  deadline_unix_ms: i64,
  channel: request_response::ResponseChannel<Envelope>,
}

struct ForwardContext {
  channel: request_response::ResponseChannel<Envelope>,
  expected: ResponseExpectation,
}

#[derive(Clone)]
struct ResponseExpectation {
  expected_peer: PeerId,
  cluster_id: crate::ClusterId,
  request_id: RequestId,
  source: NodeId,
  destination: NodeId,
  protocol: ProtocolId,
  deadline_unix_ms: i64,
}

impl ResponseExpectation {
  fn from_request(request: &Envelope, expected_peer: PeerId) -> Self {
    let header = request.header();
    Self {
      expected_peer,
      cluster_id: header.cluster_id,
      request_id: header.request_id,
      source: header.source,
      destination: header.destination,
      protocol: header.protocol,
      deadline_unix_ms: header.deadline_unix_ms,
    }
  }

  fn validate(&self, response: &Envelope, responder: PeerId) -> Result<(), LinkError> {
    let header = response.header();
    let sentinel = NodeId::from_bytes([0; NodeId::BYTE_LENGTH]);
    let source_matches = self.destination == sentinel || header.source == self.destination;
    if responder != self.expected_peer
      || header.version != PROTOCOL_VERSION
      || header.cluster_id != self.cluster_id
      || header.request_id != self.request_id
      || header.destination != self.source
      || header.protocol != self.protocol
      || header.kind != MessageKind::Response
      || !source_matches
    {
      return Err(LinkError::InvalidResponse);
    }
    Ok(())
  }
}

struct LinkActor {
  swarm: Swarm<LinkBehaviour>,
  command_rx: mpsc::Receiver<LinkCommand>,
  snapshot_tx: watch::Sender<LinkSnapshot>,
  state: LinkState,
  config: LinkConfig,
  maintenance: tokio::time::Interval,
  identity: NodeIdentity,
  router: Router,
  inbound_tx: mpsc::Sender<InboundEnvelope>,
  pending_outbound: BTreeMap<request_response::OutboundRequestId, PendingRequest>,
  pending_inbound: BTreeMap<u64, PendingInbound>,
  inbound_requests: BTreeMap<request_response::InboundRequestId, u64>,
  pending_forwards: BTreeMap<request_response::OutboundRequestId, ForwardContext>,
  broadcast_requests: BTreeSet<request_response::OutboundRequestId>,
  next_inbound_token: u64,
  request_ids: Arc<RequestSequence>,
  last_link_state_sequence: u64,
}

impl LinkActor {
  async fn run(mut self) {
    loop {
      let deadline_wait = self
        .next_pending_deadline()
        .map(|deadline| Duration::from_millis(deadline.saturating_sub(now_unix_ms()).max(0) as u64))
        .unwrap_or(Duration::from_secs(3_600));
      tokio::select! {
        command = self.command_rx.recv() => {
          let Some(command) = command else {
            break;
          };
          if self.handle_command(command) {
            break;
          }
        }
        event = self.swarm.select_next_some() => self.handle_event(event),
        _ = self.maintenance.tick() => self.maintain(),
        _ = tokio::time::sleep(deadline_wait) => self.expire_pending_requests(now_unix_ms()),
      }
    }
  }

  fn handle_command(&mut self, command: LinkCommand) -> bool {
    match command {
      LinkCommand::Request { envelope, reply } => {
        self.send_routed_request(envelope, reply);
        false
      }
      LinkCommand::RequestAdmission {
        sponsor,
        envelope,
        reply,
      } => {
        self.send_admission_request(sponsor, envelope, reply);
        false
      }
      LinkCommand::Respond {
        token,
        envelope,
        reply,
      } => {
        let result = self.respond(token, envelope);
        let _ = reply.send(result);
        false
      }
      LinkCommand::Dial {
        node_id,
        address,
        reply,
      } => {
        let result = self.dial(node_id, address);
        let _ = reply.send(result);
        false
      }
      LinkCommand::DialAdmission { address, reply } => {
        let result = self.dial_admission(address);
        let _ = reply.send(result);
        false
      }
      LinkCommand::ListenViaRelay {
        node_id,
        address,
        reply,
      } => {
        let result = self.listen_via_relay(node_id, address);
        let _ = reply.send(result);
        false
      }
      LinkCommand::Disconnect { node_id, reply } => {
        self.state.directory.pause(node_id);
        let result = self
          .state
          .active_peer_for_node(node_id)
          .ok_or(LinkError::UnauthorizedNode(node_id))
          .and_then(|peer_id| {
            self
              .swarm
              .disconnect_peer_id(peer_id)
              .map_err(|()| LinkError::NotConnected(node_id))
          });
        let _ = reply.send(result);
        false
      }
      LinkCommand::CommitAuthorization {
        registry,
        check_cluster,
        persist,
        reply,
      } => {
        let result = self
          .validate_authorization(&registry, check_cluster)
          .and_then(|()| persist().map_err(LinkError::AuthorizationCommit))
          .map(|()| self.install_authorization(registry));
        let _ = reply.send(result);
        false
      }
      LinkCommand::Shutdown { reply } => {
        let _ = reply.send(());
        true
      }
    }
  }

  fn dial_admission(&mut self, address: Multiaddr) -> Result<PeerId, LinkError> {
    let peer_id = terminal_peer_id(&address);
    let Some(peer_id) = peer_id else {
      return Err(LinkError::Transport(
        "admission dial requires a /p2p/ peer id component".to_string(),
      ));
    };
    let options = DialOpts::peer_id(peer_id).addresses(vec![address]).build();
    self
      .swarm
      .dial(options)
      .map(|()| peer_id)
      .map_err(|error| LinkError::Transport(error.to_string()))
  }

  fn listen_via_relay(&mut self, node_id: NodeId, address: Multiaddr) -> Result<(), LinkError> {
    let relay_peer = self
      .state
      .active_peer_for_node(node_id)
      .ok_or(LinkError::UnauthorizedNode(node_id))?;
    let relayed_address = address
      .clone()
      .with(Protocol::P2p(relay_peer))
      .with(Protocol::P2pCircuit);
    self
      .state
      .relay_reservations
      .insert(node_id, relayed_address.clone());
    self.reserve_via(node_id, relayed_address)
  }

  fn reserve_via(&mut self, node_id: NodeId, relayed_address: Multiaddr) -> Result<(), LinkError> {
    if self.state.has_listener_with_prefix(&relayed_address) {
      return Ok(());
    }
    self
      .swarm
      .listen_on(relayed_address)
      .map(|_| ())
      .map_err(|error| {
        self.state.relay_reservations.remove(&node_id);
        LinkError::Transport(error.to_string())
      })
  }

  fn dial(&mut self, node_id: NodeId, address: Multiaddr) -> Result<(), LinkError> {
    let peer_id = self
      .state
      .active_peer_for_node(node_id)
      .ok_or(LinkError::UnauthorizedNode(node_id))?;
    let now = Instant::now();
    self.state.directory.record(
      node_id,
      peer_id,
      address.clone(),
      AddressSource::Configured,
      now,
      self.config.discovered_address_ttl(),
    );
    self.state.directory.resume(node_id);
    self.dial_address(node_id, peer_id, address, now)
  }

  fn dial_address(
    &mut self, node_id: NodeId, peer_id: PeerId, address: Multiaddr, now: Instant,
  ) -> Result<(), LinkError> {
    let options = DialOpts::peer_id(peer_id)
      .addresses(vec![address.clone()])
      .build();
    self.swarm.dial(options).map_err(|error| {
      self.state.directory.note_failure(
        node_id,
        Some(&address),
        now,
        self.config.reconnect_min_delay(),
        self.config.reconnect_max_delay(),
      );
      LinkError::Transport(error.to_string())
    })
  }

  fn maintain(&mut self) {
    let now = Instant::now();
    self.expire_pending_requests(now_unix_ms());
    self.state.directory.expire(now);
    for (node_id, peer_id, address) in self.state.directory.candidates(now) {
      if self.state.node_is_connected(node_id) || self.state.pending_for_node(node_id) {
        continue;
      }
      if let Err(error) = self.dial_address(node_id, peer_id, address, now) {
        tracing::debug!(%node_id, %error, "overlay reconnect attempt failed");
      }
    }
    for (node_id, relayed_address) in self.state.relay_reservations.clone() {
      if self.state.has_listener_with_prefix(&relayed_address) {
        continue;
      }
      if let Err(error) = self.reserve_via(node_id, relayed_address) {
        tracing::warn!(%node_id, %error, "overlay relay reservation listener failed");
      }
    }
  }

  fn next_pending_deadline(&self) -> Option<i64> {
    self
      .pending_outbound
      .values()
      .map(|pending| pending.expected.deadline_unix_ms)
      .chain(
        self
          .pending_forwards
          .values()
          .map(|forward| forward.expected.deadline_unix_ms),
      )
      .chain(
        self
          .pending_inbound
          .values()
          .map(|pending| pending.deadline_unix_ms),
      )
      .min()
  }

  fn expire_pending_requests(&mut self, now_unix_ms: i64) {
    let expired_outbound: Vec<_> = self
      .pending_outbound
      .iter()
      .filter_map(|(request_id, pending)| {
        (pending.expected.deadline_unix_ms <= now_unix_ms).then_some(*request_id)
      })
      .collect();
    let expired_forwards: Vec<_> = self
      .pending_forwards
      .iter()
      .filter_map(|(request_id, forward)| {
        (forward.expected.deadline_unix_ms <= now_unix_ms).then_some(*request_id)
      })
      .collect();
    let expired_inbound: Vec<_> = self
      .pending_inbound
      .iter()
      .filter_map(|(token, pending)| (pending.deadline_unix_ms <= now_unix_ms).then_some(*token))
      .collect();
    let mut failed_peers = BTreeSet::new();
    for request_id in expired_outbound {
      if let Some(pending) = self.pending_outbound.remove(&request_id) {
        failed_peers.insert(pending.expected.expected_peer);
        let _ = pending.reply.send(Err(LinkError::Timeout));
      }
    }
    for request_id in expired_forwards {
      if let Some(forward) = self.pending_forwards.remove(&request_id) {
        failed_peers.insert(forward.expected.expected_peer);
        self.router.complete_forward();
        drop(forward.channel);
      }
    }
    for token in expired_inbound {
      self.remove_pending_inbound(token);
    }
    for peer_id in failed_peers {
      self.close_timed_out_peer(peer_id);
    }
  }

  fn close_timed_out_peer(&mut self, peer_id: PeerId) {
    let connections: Vec<_> = self
      .state
      .connections
      .iter()
      .filter_map(|(connection_id, connection)| {
        (connection.peer_id == peer_id).then_some(*connection_id)
      })
      .collect();
    let connection = if connections.len() == 1 {
      connections.first().copied()
    } else {
      connections
        .into_iter()
        .find(|connection_id| !self.state.healthy_connections.contains(connection_id))
    };
    if let Some(connection_id) = connection {
      self.swarm.close_connection(connection_id);
    }
  }

  fn remove_pending_inbound(&mut self, token: u64) {
    if let Some(pending) = self.pending_inbound.remove(&token) {
      self.inbound_requests.remove(&pending.request_id);
    }
  }

  fn remove_pending_inbound_request(&mut self, request_id: request_response::InboundRequestId) {
    if let Some(token) = self.inbound_requests.remove(&request_id) {
      self.pending_inbound.remove(&token);
    }
  }

  fn remove_pending_inbound_peer(&mut self, peer_id: PeerId) {
    let tokens: Vec<_> = self
      .pending_inbound
      .iter()
      .filter_map(|(token, pending)| (pending.peer_id == peer_id).then_some(*token))
      .collect();
    for token in tokens {
      self.remove_pending_inbound(token);
    }
  }

  fn handle_mdns(&mut self, event: mdns::Event) {
    let now = Instant::now();
    match event {
      mdns::Event::Discovered(discovered) => {
        for (peer_id, address) in discovered {
          self.state.record_discovered_address(
            peer_id,
            address,
            now,
            self.config.discovered_address_ttl(),
          );
        }
      }
      mdns::Event::Expired(expired) => {
        for (peer_id, address) in expired {
          self.state.expire_discovered_address(peer_id, &address);
        }
      }
    }
  }

  fn send_routed_request(
    &mut self, envelope: Envelope, reply: oneshot::Sender<Result<Envelope, LinkError>>,
  ) {
    if envelope.header().protocol == ProtocolId::Admission {
      let _ = reply.send(Err(LinkError::InvalidAdmissionRequest));
      return;
    }
    match self.route_outbound(&envelope) {
      Ok(peer_id) => self.send_peer_request(peer_id, envelope, reply),
      Err(error) => {
        let _ = reply.send(Err(error));
      }
    }
  }

  fn send_admission_request(
    &mut self, sponsor: PeerId, envelope: Envelope,
    reply: oneshot::Sender<Result<Envelope, LinkError>>,
  ) {
    if envelope.header().protocol != ProtocolId::Admission {
      let _ = reply.send(Err(LinkError::InvalidAdmissionRequest));
      return;
    }
    let quarantined = self
      .state
      .quarantined
      .values()
      .any(|connection| connection.peer_id == sponsor);
    let authorized = self.state.authorization.node_for_peer(&sponsor).is_some();
    if !quarantined && !authorized {
      let _ = reply.send(Err(LinkError::AdmissionPeerUnavailable(sponsor)));
      return;
    }
    self.send_peer_request(sponsor, envelope, reply);
  }

  fn send_peer_request(
    &mut self, peer_id: PeerId, envelope: Envelope,
    reply: oneshot::Sender<Result<Envelope, LinkError>>,
  ) {
    if envelope.header().deadline_unix_ms <= now_unix_ms() {
      let _ = reply.send(Err(LinkError::Timeout));
      return;
    }
    let expected = ResponseExpectation::from_request(&envelope, peer_id);
    let request_id = self
      .swarm
      .behaviour_mut()
      .messaging
      .send_request(&peer_id, envelope);
    self
      .pending_outbound
      .insert(request_id, PendingRequest { expected, reply });
  }

  fn route_outbound(&self, envelope: &Envelope) -> Result<PeerId, LinkError> {
    let header = envelope.header();
    let destination = header.destination;
    if header.version != PROTOCOL_VERSION || destination == self.state.node_id {
      return Err(LinkError::NoRoute(destination));
    }
    if header.cluster_id != self.state.authorization.cluster_id() {
      return Err(LinkError::NoRoute(destination));
    }
    let next_hop = if self.state.node_is_connected(destination) {
      destination
    } else {
      self
        .router
        .links()
        .next_hop(self.state.node_id, destination)
        .ok_or(LinkError::NoRoute(destination))?
    };
    self
      .state
      .active_peer_for_node(next_hop)
      .ok_or(LinkError::NoRoute(next_hop))
  }

  fn respond(&mut self, token: InboundToken, envelope: Envelope) -> Result<(), LinkError> {
    let pending = self
      .pending_inbound
      .remove(&token.0)
      .ok_or(LinkError::UnknownInbound)?;
    self.inbound_requests.remove(&pending.request_id);
    if pending.deadline_unix_ms <= now_unix_ms() {
      return Err(LinkError::Timeout);
    }
    self
      .swarm
      .behaviour_mut()
      .messaging
      .send_response(pending.channel, envelope)
      .map_err(|_| LinkError::Transport("failed to flush the overlay response".to_string()))
  }

  fn handle_messaging(&mut self, event: request_response::Event<Envelope, Envelope>) {
    match event {
      request_response::Event::Message { peer, message, .. } => match message {
        request_response::Message::Request {
          request_id,
          request,
          channel,
        } => self.handle_inbound_request(peer, request_id, request, channel),
        request_response::Message::Response {
          request_id,
          response,
        } => {
          if let Some(pending) = self.pending_outbound.remove(&request_id) {
            let result = if pending.expected.deadline_unix_ms <= now_unix_ms() {
              self.close_timed_out_peer(peer);
              Err(LinkError::Timeout)
            } else {
              pending
                .expected
                .validate(&response, peer)
                .map(|()| response)
            };
            let _ = pending.reply.send(result);
          } else if let Some(forward) = self.pending_forwards.remove(&request_id) {
            self.router.complete_forward();
            if forward.expected.deadline_unix_ms > now_unix_ms()
              && forward.expected.validate(&response, peer).is_ok()
            {
              let _ = self
                .swarm
                .behaviour_mut()
                .messaging
                .send_response(forward.channel, response);
            } else {
              self.close_timed_out_peer(peer);
            }
          } else if !self.broadcast_requests.remove(&request_id) {
            tracing::debug!(?request_id, "ignoring orphan overlay response");
          }
        }
      },
      request_response::Event::OutboundFailure {
        request_id, error, ..
      } => {
        if let Some(pending) = self.pending_outbound.remove(&request_id) {
          let _ = pending
            .reply
            .send(Err(LinkError::Transport(error.to_string())));
        } else if let Some(forward) = self.pending_forwards.remove(&request_id) {
          self.router.complete_forward();
          drop(forward.channel);
        } else {
          self.broadcast_requests.remove(&request_id);
        }
      }
      request_response::Event::InboundFailure {
        request_id, error, ..
      } => {
        self.remove_pending_inbound_request(request_id);
        tracing::debug!(%error, "overlay inbound request failed");
      }
      request_response::Event::ResponseSent { .. } => {}
    }
  }

  fn handle_inbound_request(
    &mut self, peer_id: PeerId, request_id: request_response::InboundRequestId, envelope: Envelope,
    channel: request_response::ResponseChannel<Envelope>,
  ) {
    let header = envelope.header();
    if header.version != PROTOCOL_VERSION {
      tracing::debug!(%peer_id, "dropping overlay message with an unsupported version");
      return;
    }
    if self.state.authorization.node_for_peer(&peer_id).is_none() {
      let quarantined = self
        .state
        .quarantined
        .values()
        .any(|connection| connection.peer_id == peer_id);
      if quarantined && envelope.header().protocol == ProtocolId::Admission {
        self.deliver(peer_id, request_id, envelope, channel);
      } else {
        tracing::debug!(%peer_id, "dropping overlay message from an unauthorized peer");
      }
      return;
    }
    if header.protocol == ProtocolId::Admission {
      self.deliver(peer_id, request_id, envelope, channel);
      return;
    }
    if header.cluster_id != self.state.authorization.cluster_id() {
      tracing::debug!(%peer_id, "dropping overlay message from a foreign overlay");
      return;
    }
    if header.protocol == ProtocolId::Route && header.kind == MessageKind::Event {
      self.accept_link_state(&envelope);
      self.reply_empty(channel, &envelope);
      return;
    }
    match self.router.handle(envelope, now_unix_ms()) {
      RouteDecision::Deliver(envelope) => {
        self.deliver(peer_id, request_id, envelope, channel);
      }
      RouteDecision::Forward { next_hop, envelope } => {
        let Some(next_peer) = self.state.active_peer_for_node(next_hop) else {
          self.router.complete_forward();
          return;
        };
        let expected = ResponseExpectation::from_request(&envelope, next_peer);
        let request_id = self
          .swarm
          .behaviour_mut()
          .messaging
          .send_request(&next_peer, envelope);
        self
          .pending_forwards
          .insert(request_id, ForwardContext { channel, expected });
      }
      RouteDecision::Drop(reason) => {
        tracing::debug!(?reason, "dropping routed overlay envelope");
      }
    }
  }

  fn deliver(
    &mut self, peer_id: PeerId, request_id: request_response::InboundRequestId, envelope: Envelope,
    channel: request_response::ResponseChannel<Envelope>,
  ) {
    let now = now_unix_ms();
    if envelope.header().deadline_unix_ms <= now {
      tracing::debug!(%peer_id, "dropping an expired overlay request");
      return;
    }
    if self.pending_inbound.len() >= MAX_PENDING_INBOUND {
      tracing::warn!(%peer_id, "overlay pending inbound limit reached; dropping request");
      return;
    }
    let deadline_unix_ms = envelope
      .header()
      .deadline_unix_ms
      .min(now.saturating_add(MAX_INBOUND_LIFETIME_MS));
    let token = self.next_inbound_token;
    self.next_inbound_token += 1;
    if self
      .inbound_tx
      .try_send(InboundEnvelope {
        token: InboundToken(token),
        sender: peer_id,
        envelope,
      })
      .is_ok()
    {
      self.pending_inbound.insert(
        token,
        PendingInbound {
          request_id,
          peer_id,
          deadline_unix_ms,
          channel,
        },
      );
      self.inbound_requests.insert(request_id, token);
    } else {
      tracing::warn!("overlay inbound queue is full; dropping a delivered envelope");
    }
  }

  fn accept_link_state(&mut self, envelope: &Envelope) {
    let record = match postcard::from_bytes::<LinkStateRecord>(envelope.payload()) {
      Ok(record) => record,
      Err(error) => {
        tracing::debug!(%error, "ignoring a malformed link-state record");
        return;
      }
    };
    let Some(authorized_key) = self
      .state
      .authorization
      .active_record_for_node(record.node_id())
      .map(|record| record.public_key().to_vec())
    else {
      tracing::debug!(node_id = %record.node_id(), "ignoring link state from an unknown node");
      return;
    };
    match self.router.links_mut().insert(record, &authorized_key) {
      Ok(true) => tracing::debug!("accepted a newer link-state record"),
      Ok(false) => {}
      Err(error) => tracing::debug!(%error, "rejected a link-state record"),
    }
  }

  fn reply_empty(
    &mut self, channel: request_response::ResponseChannel<Envelope>, request: &Envelope,
  ) {
    if let Some(response) = self.response_envelope(request, Vec::new()) {
      let _ = self
        .swarm
        .behaviour_mut()
        .messaging
        .send_response(channel, response);
    }
  }

  fn response_envelope(&self, request: &Envelope, payload: Vec<u8>) -> Option<Envelope> {
    let request_header = request.header();
    let header = EnvelopeHeader {
      version: PROTOCOL_VERSION,
      cluster_id: self.state.authorization.cluster_id(),
      request_id: request_header.request_id,
      source: self.state.node_id,
      destination: request_header.source,
      protocol: request_header.protocol,
      kind: MessageKind::Response,
      deadline_unix_ms: request_header.deadline_unix_ms,
      remaining_hops: request_header.remaining_hops,
    };
    Envelope::new(header, payload).ok()
  }

  fn advertise_link_state(&mut self) {
    let now = now_unix_ms().max(0) as u64;
    let sequence = now.max(self.last_link_state_sequence + 1);
    self.last_link_state_sequence = sequence;
    let edges: Vec<NodeId> = self
      .state
      .connections
      .values()
      .map(|connection| connection.node_id)
      .collect();
    let record = match LinkStateRecord::sign(&self.identity, edges, sequence) {
      Ok(record) => record,
      Err(error) => {
        tracing::warn!(%error, "failed to sign the local link-state record");
        return;
      }
    };
    if let Err(error) = self
      .router
      .links_mut()
      .insert(record.clone(), &self.identity.public_key_bytes())
    {
      tracing::warn!(%error, "failed to track the local link-state record");
    }
    let payload = match postcard::to_stdvec(&record) {
      Ok(payload) => payload,
      Err(error) => {
        tracing::warn!(%error, "failed to encode the local link-state record");
        return;
      }
    };
    let peers: BTreeSet<(PeerId, NodeId)> = self
      .state
      .connections
      .values()
      .map(|connection| (connection.peer_id, connection.node_id))
      .collect();
    for (peer_id, node_id) in peers {
      let request_id = match self.next_request_id() {
        Ok(request_id) => request_id,
        Err(error) => {
          tracing::error!(%error, "stopping link-state publication");
          return;
        }
      };
      let header = EnvelopeHeader {
        version: PROTOCOL_VERSION,
        cluster_id: self.state.authorization.cluster_id(),
        request_id,
        source: self.state.node_id,
        destination: node_id,
        protocol: ProtocolId::Route,
        kind: MessageKind::Event,
        deadline_unix_ms: now_unix_ms() + 10_000,
        remaining_hops: 0,
      };
      let Ok(envelope) = Envelope::new(header, payload.clone()) else {
        return;
      };
      let request_id = self
        .swarm
        .behaviour_mut()
        .messaging
        .send_request(&peer_id, envelope);
      self.broadcast_requests.insert(request_id);
    }
  }

  fn next_request_id(&self) -> Result<RequestId, LinkError> {
    self.request_ids.allocate(self.state.node_id)
  }

  fn validate_authorization(
    &self, registry: &AuthorizationRegistry, check_cluster: bool,
  ) -> Result<(), LinkError> {
    if check_cluster && registry.cluster_id() != self.state.authorization.cluster_id() {
      return Err(
        AuthorizationError::ForeignCluster {
          expected: self.state.authorization.cluster_id(),
          actual: registry.cluster_id(),
        }
        .into(),
      );
    }
    Ok(())
  }

  fn install_authorization(&mut self, registry: AuthorizationRegistry) {
    self.state.authorization = registry;
    let authorization = self.state.authorization.clone();
    self
      .state
      .directory
      .retain(|node_id| authorization.active_peer_for_node(node_id).is_some());
    self
      .state
      .relay_reservations
      .retain(|node_id, _| authorization.active_peer_for_node(*node_id).is_some());
    self.router = Router::new(self.state.node_id);
    let promoted: Vec<ConnectionId> = self
      .state
      .quarantined
      .iter()
      .filter_map(|(connection_id, connection)| {
        self
          .state
          .authorization
          .node_for_peer(&connection.peer_id)
          .map(|_| *connection_id)
      })
      .collect();
    for connection_id in promoted {
      if let Some(quarantined) = self.state.quarantined.remove(&connection_id)
        && let Some(node_id) = self.state.authorization.node_for_peer(&quarantined.peer_id)
      {
        self.state.connection_established(
          quarantined.peer_id,
          node_id,
          connection_id,
          quarantined.endpoint,
        );
      }
    }
    let stale_connections: Vec<_> = self
      .state
      .connections
      .iter()
      .filter_map(|(connection_id, connection)| {
        let authorized = self
          .state
          .authorization
          .node_for_peer(&connection.peer_id)
          .is_some_and(|node_id| node_id == connection.node_id);
        (!authorized).then_some(*connection_id)
      })
      .collect();
    for connection_id in stale_connections {
      self.swarm.close_connection(connection_id);
    }
    self.snapshot_tx.send_replace(self.state.snapshot());
    self.advertise_link_state();
  }

  fn handle_event(&mut self, event: SwarmEvent<LinkBehaviourEvent>) {
    let edges_before = self.state.edge_set();
    let changed = match event {
      SwarmEvent::NewListenAddr { address, .. } => {
        self.swarm.add_external_address(address.clone());
        self.state.listeners.insert(address)
      }
      SwarmEvent::ExpiredListenAddr { address, .. } => self.state.listeners.remove(&address),
      SwarmEvent::ListenerClosed {
        addresses, reason, ..
      } => {
        if let Err(error) = reason {
          tracing::warn!(%error, "overlay listener closed");
        }
        let mut changed = false;
        for address in addresses {
          changed = self.state.listeners.remove(&address) || changed;
        }
        changed
      }
      SwarmEvent::ConnectionEstablished {
        peer_id,
        connection_id,
        endpoint,
        ..
      } => self.connection_established(peer_id, connection_id, endpoint),
      SwarmEvent::ConnectionClosed {
        peer_id,
        connection_id,
        ..
      } => {
        let node_id = self
          .state
          .connections
          .get(&connection_id)
          .map(|connection| connection.node_id);
        self.state.pending_dials.remove(&connection_id);
        let quarantined_closed = self.state.quarantined.remove(&connection_id).is_some();
        let changed = self.state.connection_closed(peer_id, connection_id) || quarantined_closed;
        let peer_still_connected = self.state.peer_is_connected(peer_id)
          || self
            .state
            .quarantined
            .values()
            .any(|connection| connection.peer_id == peer_id);
        if !peer_still_connected {
          self.remove_pending_inbound_peer(peer_id);
        }
        if let Some(node_id) = node_id
          && !self.state.node_is_connected(node_id)
        {
          self.state.directory.note_connection_closed(
            node_id,
            Instant::now(),
            self.config.reconnect_min_delay(),
          );
        }
        changed
      }
      SwarmEvent::Dialing {
        peer_id,
        connection_id,
      } => {
        if let Some(node_id) =
          peer_id.and_then(|peer_id| self.state.authorization.node_for_peer(&peer_id))
        {
          self.state.pending_dials.insert(connection_id, node_id);
        }
        false
      }
      SwarmEvent::Behaviour(LinkBehaviourEvent::Ping(event)) => match event.result {
        Ok(_) => self
          .state
          .set_connection_health(event.peer, event.connection, true),
        Err(error) => {
          tracing::debug!(peer_id = %event.peer, %error, "overlay ping failed");
          let changed = self
            .state
            .set_connection_health(event.peer, event.connection, false);
          self.swarm.close_connection(event.connection);
          changed
        }
      },
      SwarmEvent::Behaviour(LinkBehaviourEvent::Mdns(event)) => {
        self.handle_mdns(event);
        self.maintain();
        false
      }
      SwarmEvent::Behaviour(LinkBehaviourEvent::RelayClient(event)) => {
        tracing::debug!(?event, "overlay relay client event");
        false
      }
      SwarmEvent::Behaviour(LinkBehaviourEvent::RelayServer(event)) => {
        tracing::debug!(?event, "overlay relay server event");
        false
      }
      SwarmEvent::Behaviour(LinkBehaviourEvent::Dcutr(event)) => {
        match event.result {
          Ok(connection_id) => tracing::debug!(
            peer_id = %event.remote_peer_id,
            ?connection_id,
            "overlay hole punch upgraded a relayed connection"
          ),
          Err(error) => tracing::debug!(
            peer_id = %event.remote_peer_id,
            %error,
            "overlay hole punch failed"
          ),
        }
        false
      }
      SwarmEvent::Behaviour(LinkBehaviourEvent::Messaging(event)) => {
        self.handle_messaging(event);
        false
      }
      SwarmEvent::OutgoingConnectionError {
        connection_id,
        peer_id,
        error,
      } => {
        tracing::debug!(?peer_id, %error, "overlay dial failed");
        if let Some(node_id) = self.state.pending_dials.remove(&connection_id) {
          self.state.directory.note_failure(
            node_id,
            None,
            Instant::now(),
            self.config.reconnect_min_delay(),
            self.config.reconnect_max_delay(),
          );
        }
        false
      }
      _ => false,
    };
    if changed {
      self.snapshot_tx.send_replace(self.state.snapshot());
    }
    if self.state.edge_set() != edges_before {
      self.advertise_link_state();
    }
  }

  fn connection_established(
    &mut self, peer_id: PeerId, connection_id: ConnectionId, endpoint: ConnectedPoint,
  ) -> bool {
    self.state.pending_dials.remove(&connection_id);
    let Some(node_id) = self.state.authorization.node_for_peer(&peer_id) else {
      if self.state.quarantined.len() >= MAX_QUARANTINED_CONNECTIONS {
        tracing::debug!(%peer_id, "rejecting a quarantined overlay connection beyond the cap");
        self.swarm.close_connection(connection_id);
        return false;
      }
      tracing::debug!(%peer_id, "quarantining an unauthorized overlay connection");
      self
        .state
        .quarantined
        .insert(connection_id, QuarantinedConnection { peer_id, endpoint });
      return true;
    };
    let remote_address = endpoint.get_remote_address().clone();
    let changed = self
      .state
      .connection_established(peer_id, node_id, connection_id, endpoint);
    self
      .state
      .directory
      .note_success(node_id, &remote_address, Instant::now());
    self.arbitrate_connections(peer_id, node_id);
    changed
  }

  fn arbitrate_connections(&mut self, peer_id: PeerId, node_id: NodeId) {
    let mut candidates: Vec<_> = self
      .state
      .connections
      .iter()
      .filter(|(_, connection)| connection.node_id == node_id)
      .map(|(connection_id, connection)| {
        (
          *connection_id,
          connection_preference(self.state.local_peer_id, peer_id, &connection.endpoint),
        )
      })
      .collect();
    if candidates.len() <= 1 {
      return;
    }
    candidates.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
    let winner = candidates[0].0;
    for (connection_id, _) in candidates.into_iter().skip(1) {
      self.swarm.close_connection(connection_id);
    }
    tracing::debug!(%peer_id, ?winner, "closed duplicate overlay connections");
  }
}

#[derive(Debug, Clone)]
struct ConnectionInfo {
  peer_id: PeerId,
  node_id: NodeId,
  endpoint: ConnectedPoint,
}

/// Maximum simultaneously quarantined connections (unauthenticated peers
/// that may only speak the bounded admission protocol).
const MAX_QUARANTINED_CONNECTIONS: usize = 32;

#[derive(Debug, Clone)]
struct QuarantinedConnection {
  peer_id: PeerId,
  endpoint: ConnectedPoint,
}

struct LinkState {
  node_id: NodeId,
  local_peer_id: PeerId,
  authorization: AuthorizationRegistry,
  listeners: BTreeSet<Multiaddr>,
  connections: BTreeMap<ConnectionId, ConnectionInfo>,
  healthy_connections: BTreeSet<ConnectionId>,
  quarantined: BTreeMap<ConnectionId, QuarantinedConnection>,
  directory: PeerDirectory,
  pending_dials: BTreeMap<ConnectionId, NodeId>,
  relay_reservations: BTreeMap<NodeId, Multiaddr>,
}

impl LinkState {
  fn new(node_id: NodeId, local_peer_id: PeerId, authorization: AuthorizationRegistry) -> Self {
    Self {
      node_id,
      local_peer_id,
      authorization,
      listeners: BTreeSet::new(),
      connections: BTreeMap::new(),
      healthy_connections: BTreeSet::new(),
      quarantined: BTreeMap::new(),
      directory: PeerDirectory::default(),
      pending_dials: BTreeMap::new(),
      relay_reservations: BTreeMap::new(),
    }
  }

  fn active_peer_for_node(&self, node_id: NodeId) -> Option<PeerId> {
    self.authorization.active_peer_for_node(node_id)
  }

  fn record_discovered_address(
    &mut self, peer_id: PeerId, address: Multiaddr, now: Instant, ttl: Duration,
  ) {
    if let Some(node_id) = self.authorization.node_for_peer(&peer_id) {
      self
        .directory
        .record(node_id, peer_id, address, AddressSource::Mdns, now, ttl);
    }
  }

  fn expire_discovered_address(&mut self, peer_id: PeerId, address: &Multiaddr) {
    self.directory.remove(peer_id, address);
  }

  fn has_listener_with_prefix(&self, prefix: &Multiaddr) -> bool {
    self.listeners.iter().any(|address| {
      let mut protocols = address.iter();
      prefix
        .iter()
        .all(|expected| protocols.next() == Some(expected))
    })
  }

  fn edge_set(&self) -> BTreeSet<NodeId> {
    self
      .connections
      .values()
      .map(|connection| connection.node_id)
      .collect()
  }

  fn node_is_connected(&self, node_id: NodeId) -> bool {
    self
      .connections
      .values()
      .any(|connection| connection.node_id == node_id)
  }

  fn peer_is_connected(&self, peer_id: PeerId) -> bool {
    self
      .connections
      .values()
      .any(|connection| connection.peer_id == peer_id)
  }

  fn pending_for_node(&self, node_id: NodeId) -> bool {
    self
      .pending_dials
      .values()
      .any(|pending| *pending == node_id)
  }

  fn connection_established(
    &mut self, peer_id: PeerId, node_id: NodeId, connection_id: ConnectionId,
    endpoint: ConnectedPoint,
  ) -> bool {
    self.connections.insert(
      connection_id,
      ConnectionInfo {
        peer_id,
        node_id,
        endpoint,
      },
    );
    true
  }

  fn connection_closed(&mut self, peer_id: PeerId, connection_id: ConnectionId) -> bool {
    let Some(connection) = self.connections.get(&connection_id) else {
      self.healthy_connections.remove(&connection_id);
      return false;
    };
    debug_assert_eq!(connection.peer_id, peer_id);
    let _node_id = connection.node_id;
    self.healthy_connections.remove(&connection_id);
    self.connections.remove(&connection_id);
    true
  }

  fn set_connection_health(
    &mut self, peer_id: PeerId, connection_id: ConnectionId, healthy: bool,
  ) -> bool {
    let Some(connection) = self.connections.get(&connection_id) else {
      self.healthy_connections.remove(&connection_id);
      return false;
    };
    if connection.peer_id != peer_id {
      return false;
    }
    let node_id = connection.node_id;
    let was_healthy = self.node_is_healthy(node_id);
    if healthy {
      self.healthy_connections.insert(connection_id);
    } else {
      self.healthy_connections.remove(&connection_id);
    }
    was_healthy != self.node_is_healthy(node_id)
  }

  fn node_is_healthy(&self, node_id: NodeId) -> bool {
    self.healthy_connections.iter().any(|connection_id| {
      self
        .connections
        .get(connection_id)
        .is_some_and(|connection| connection.node_id == node_id)
    })
  }

  fn snapshot(&self) -> LinkSnapshot {
    let connected_nodes: BTreeSet<_> = self
      .connections
      .values()
      .map(|connection| connection.node_id)
      .collect();
    let healthy_nodes = connected_nodes
      .iter()
      .copied()
      .filter(|node_id| self.node_is_healthy(*node_id))
      .collect();
    LinkSnapshot {
      node_id: self.node_id,
      listen_addresses: self.listeners.iter().cloned().collect(),
      connected_nodes: connected_nodes.into_iter().collect(),
      healthy_nodes,
      connection_count: self.connections.len(),
      quarantined_peers: self
        .quarantined
        .values()
        .map(|connection| connection.peer_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect(),
      quarantined_count: self.quarantined.len(),
    }
  }
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ConnectionPreference {
  transport: u8,
  initiator: u8,
  remote_address: Vec<u8>,
}

fn connection_preference(
  local_peer_id: PeerId, peer_id: PeerId, endpoint: &ConnectedPoint,
) -> ConnectionPreference {
  const DIRECT_QUIC: u8 = 0;
  const DIRECT_TCP: u8 = 1;
  const RELAYED_QUIC: u8 = 2;
  const RELAYED_TCP: u8 = 3;
  const OTHER_TRANSPORT: u8 = 4;

  let transport_address = match endpoint {
    ConnectedPoint::Dialer { address, .. }
    | ConnectedPoint::Listener {
      local_addr: address,
      ..
    } => address,
  };
  let quic = transport_address
    .iter()
    .any(|protocol| matches!(protocol, Protocol::QuicV1));
  let tcp = transport_address
    .iter()
    .any(|protocol| matches!(protocol, Protocol::Tcp(_)));
  let transport = match (endpoint.is_relayed(), quic, tcp) {
    (false, true, _) => DIRECT_QUIC,
    (false, _, true) => DIRECT_TCP,
    (true, true, _) => RELAYED_QUIC,
    (true, _, true) => RELAYED_TCP,
    _ => OTHER_TRANSPORT,
  };
  let local_is_canonical_initiator = local_peer_id.to_bytes() < peer_id.to_bytes();
  let preferred_role = endpoint.is_dialer() == local_is_canonical_initiator;
  ConnectionPreference {
    transport,
    initiator: u8::from(!preferred_role),
    remote_address: endpoint.get_remote_address().to_vec(),
  }
}

fn now_unix_ms() -> i64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|duration| duration.as_millis() as i64)
    .unwrap_or(0)
}

fn terminal_peer_id(address: &Multiaddr) -> Option<PeerId> {
  address
    .iter()
    .filter_map(|protocol| match protocol {
      Protocol::P2p(peer_id) => Some(peer_id),
      _ => None,
    })
    .last()
}

fn random_request_boot_id() -> Result<[u8; REQUEST_BOOT_ID_BYTES], LinkError> {
  let mut bytes = [0_u8; REQUEST_BOOT_ID_BYTES];
  SystemRandom::new()
    .fill(&mut bytes)
    .map_err(|_| LinkError::RandomGeneration)?;
  Ok(bytes)
}

fn build_swarm(
  identity: &NodeIdentity, config: &LinkConfig,
) -> Result<Swarm<LinkBehaviour>, LinkError> {
  SwarmBuilder::with_existing_identity(identity.keypair().clone())
    .with_tokio()
    .with_tcp(
      tcp::Config::default().nodelay(true),
      noise::Config::new,
      yamux::Config::default,
    )
    .map_err(|error| LinkError::Transport(error.to_string()))?
    .with_quic()
    .with_dns()
    .map_err(|error| LinkError::Transport(error.to_string()))?
    .with_relay_client(noise::Config::new, yamux::Config::default)
    .map_err(|error| LinkError::Transport(error.to_string()))?
    .with_behaviour(|_, relay_client| {
      let discovery_config = mdns::Config {
        ttl: config.discovered_address_ttl(),
        query_interval: config.mdns_query_interval(),
        enable_ipv6: false,
      };
      let discovery = if config.lan_discovery() {
        match mdns::tokio::Behaviour::new(discovery_config, identity.peer_id()) {
          Ok(behaviour) => Some(behaviour),
          Err(error) => {
            tracing::warn!(%error, "overlay lan discovery is unavailable");
            None
          }
        }
      } else {
        None
      };
      LinkBehaviour {
        ping: ping::Behaviour::new(
          ping::Config::new()
            .with_interval(config.ping_interval())
            .with_timeout(config.ping_timeout()),
        ),
        mdns: discovery.into(),
        relay_client,
        relay_server: relay::Behaviour::new(identity.peer_id(), relay::Config::default()),
        dcutr: dcutr::Behaviour::new(identity.peer_id()),
        messaging: request_response::Behaviour::new(
          [(
            OVERLAY_STREAM_PROTOCOL,
            request_response::ProtocolSupport::Full,
          )],
          request_response::Config::default(),
        ),
      }
    })
    .map_err(|error| LinkError::Transport(error.to_string()))
    .map(|builder| {
      builder
        .with_swarm_config(|swarm| swarm.with_idle_connection_timeout(config.idle_timeout()))
        .build()
    })
}

#[cfg(test)]
mod tests {
  use libp2p::{
    PeerId,
    core::{ConnectedPoint, Endpoint, transport::PortUse},
    swarm::ConnectionId,
  };

  use super::*;
  use crate::{AuthorizationRecord, NodeId};

  fn must<T, E: std::fmt::Debug>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|error| panic!("unexpected failure: {error:?}"))
  }

  fn state() -> LinkState {
    let identity = NodeIdentity::generate();
    let (cluster_id, record) = must(AuthorizationRecord::genesis(&identity));
    let registry = must(AuthorizationRegistry::from_records(cluster_id, [record]));
    LinkState::new(identity.node_id(), identity.peer_id(), registry)
  }

  fn endpoint(address: &str) -> ConnectedPoint {
    ConnectedPoint::Dialer {
      address: must(address.parse()),
      role_override: Endpoint::Dialer,
      port_use: PortUse::New,
    }
  }

  #[test]
  fn mdns_events_update_only_authorized_directory_entries() {
    let sponsor = NodeIdentity::generate();
    let discovered = NodeIdentity::generate();
    let (cluster_id, genesis) = must(AuthorizationRecord::genesis(&sponsor));
    let admission = must(AuthorizationRecord::admit(
      cluster_id,
      &discovered.public_identity(),
      &genesis,
      &genesis,
      &sponsor,
    ));
    let registry = must(AuthorizationRegistry::from_records(
      cluster_id,
      [genesis, admission],
    ));
    let mut state = LinkState::new(sponsor.node_id(), sponsor.peer_id(), registry);
    let now = Instant::now();
    let ttl = Duration::from_secs(30);
    let address: Multiaddr = must("/ip4/192.0.2.10/udp/4001/quic-v1".parse());

    state.record_discovered_address(discovered.peer_id(), address.clone(), now, ttl);
    assert_eq!(
      state.directory.candidate(discovered.node_id(), now),
      Some((discovered.peer_id(), address.clone()))
    );

    let unknown = NodeIdentity::generate();
    state.record_discovered_address(unknown.peer_id(), address.clone(), now, ttl);
    assert_eq!(state.directory.candidate(unknown.node_id(), now), None);

    state.expire_discovered_address(discovered.peer_id(), &address);
    assert_eq!(state.directory.candidate(discovered.node_id(), now), None);
  }

  #[test]
  fn healthy_node_state_requires_only_one_healthy_connection() {
    let mut state = state();
    let peer = PeerId::random();
    let node = NodeId::from_bytes([1; NodeId::BYTE_LENGTH]);
    let first = ConnectionId::new_unchecked(1);
    let second = ConnectionId::new_unchecked(2);
    state.connection_established(peer, node, first, endpoint("/ip4/127.0.0.1/tcp/4001"));
    state.connection_established(
      peer,
      node,
      second,
      endpoint("/ip4/127.0.0.1/udp/4001/quic-v1"),
    );

    assert!(state.set_connection_health(peer, first, true));
    assert!(!state.set_connection_health(peer, second, true));
    assert!(!state.set_connection_health(peer, first, false));
    assert!(state.snapshot().healthy_nodes.contains(&node));

    assert!(state.connection_closed(peer, second));
    assert!(state.snapshot().connected_nodes.contains(&node));
    assert!(!state.snapshot().healthy_nodes.contains(&node));
    assert!(state.connection_closed(peer, first));
    assert!(!state.snapshot().connected_nodes.contains(&node));
  }

  #[test]
  fn response_expectation_rejects_mismatched_metadata() {
    let source = NodeId::from_bytes([1; NodeId::BYTE_LENGTH]);
    let destination = NodeId::from_bytes([2; NodeId::BYTE_LENGTH]);
    let cluster_id = crate::ClusterId::from_bytes([3; crate::ClusterId::BYTE_LENGTH]);
    let request_id = RequestId::from_bytes([4; RequestId::BYTE_LENGTH]);
    let request = must(Envelope::new(
      EnvelopeHeader {
        version: PROTOCOL_VERSION,
        cluster_id,
        request_id,
        source,
        destination,
        protocol: ProtocolId::Membership,
        kind: MessageKind::Request,
        deadline_unix_ms: 1_000,
        remaining_hops: 4,
      },
      Vec::new(),
    ));
    let expected_peer = PeerId::random();
    let expected = ResponseExpectation::from_request(&request, expected_peer);
    let response_header = EnvelopeHeader {
      version: PROTOCOL_VERSION,
      cluster_id,
      request_id,
      source: destination,
      destination: source,
      protocol: ProtocolId::Membership,
      kind: MessageKind::Response,
      deadline_unix_ms: 1_000,
      remaining_hops: 3,
    };
    assert!(
      expected
        .validate(
          &must(Envelope::new(response_header.clone(), Vec::new())),
          expected_peer,
        )
        .is_ok()
    );

    let valid_response = must(Envelope::new(response_header.clone(), Vec::new()));
    assert!(matches!(
      expected.validate(&valid_response, PeerId::random()),
      Err(LinkError::InvalidResponse)
    ));

    let invalid_headers = [
      EnvelopeHeader {
        version: PROTOCOL_VERSION + 1,
        ..response_header.clone()
      },
      EnvelopeHeader {
        cluster_id: crate::ClusterId::from_bytes([5; crate::ClusterId::BYTE_LENGTH]),
        ..response_header.clone()
      },
      EnvelopeHeader {
        request_id: RequestId::from_bytes([6; RequestId::BYTE_LENGTH]),
        ..response_header.clone()
      },
      EnvelopeHeader {
        source,
        ..response_header.clone()
      },
      EnvelopeHeader {
        destination,
        ..response_header.clone()
      },
      EnvelopeHeader {
        protocol: ProtocolId::Resource,
        ..response_header.clone()
      },
      EnvelopeHeader {
        kind: MessageKind::Request,
        ..response_header
      },
    ];
    for header in invalid_headers {
      let response = must(Envelope::new(header, Vec::new()));
      assert!(matches!(
        expected.validate(&response, expected_peer),
        Err(LinkError::InvalidResponse)
      ));
    }
  }

  #[test]
  fn admission_response_allows_a_real_source_for_the_sentinel_destination() {
    let source = NodeId::from_bytes([1; NodeId::BYTE_LENGTH]);
    let sponsor = NodeId::from_bytes([2; NodeId::BYTE_LENGTH]);
    let cluster_id = crate::ClusterId::from_bytes([0; crate::ClusterId::BYTE_LENGTH]);
    let request_id = RequestId::from_bytes([3; RequestId::BYTE_LENGTH]);
    let request = must(Envelope::new(
      EnvelopeHeader {
        version: PROTOCOL_VERSION,
        cluster_id,
        request_id,
        source,
        destination: NodeId::from_bytes([0; NodeId::BYTE_LENGTH]),
        protocol: ProtocolId::Admission,
        kind: MessageKind::Request,
        deadline_unix_ms: 1_000,
        remaining_hops: 0,
      },
      Vec::new(),
    ));
    let response = must(Envelope::new(
      EnvelopeHeader {
        version: PROTOCOL_VERSION,
        cluster_id,
        request_id,
        source: sponsor,
        destination: source,
        protocol: ProtocolId::Admission,
        kind: MessageKind::Response,
        deadline_unix_ms: 1_000,
        remaining_hops: 0,
      },
      Vec::new(),
    ));

    let expected_peer = PeerId::random();
    assert!(
      ResponseExpectation::from_request(&request, expected_peer)
        .validate(&response, expected_peer)
        .is_ok()
    );
  }

  #[test]
  fn admission_dial_uses_the_terminal_peer_after_a_relay() {
    let relay = PeerId::random();
    let sponsor = PeerId::random();
    let address =
      must(format!("/ip4/127.0.0.1/tcp/4001/p2p/{relay}/p2p-circuit/p2p/{sponsor}").parse());

    assert_eq!(terminal_peer_id(&address), Some(sponsor));
  }

  #[test]
  fn duplicate_preference_prefers_quic_then_the_canonical_initiator() {
    let local = PeerId::random();
    let remote = PeerId::random();
    let quic = connection_preference(local, remote, &endpoint("/ip4/127.0.0.1/udp/4001/quic-v1"));
    let tcp = connection_preference(local, remote, &endpoint("/ip4/127.0.0.1/tcp/4001"));

    assert!(quic < tcp);

    let listener = ConnectedPoint::Listener {
      local_addr: must("/ip4/127.0.0.1/udp/4001/quic-v1".parse()),
      send_back_addr: must("/ip4/127.0.0.1/udp/5000/quic-v1".parse()),
    };
    let canonical = connection_preference(local, remote, &listener);
    let non_canonical = connection_preference(remote, local, &listener);
    assert_ne!(canonical.initiator, non_canonical.initiator);
  }
}
