use std::{
  collections::{BTreeMap, BTreeSet},
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::{Instant, SystemTime, UNIX_EPOCH},
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
use tokio::{
  sync::{mpsc, oneshot, watch},
  task::JoinHandle,
};

use super::{
  LinkCommand, LinkConfig, LinkError, LinkHandle, LinkSnapshot,
  directory::{AddressSource, PeerDirectory},
  handle::{InboundEnvelope, InboundToken},
  messaging::{EnvelopeCodec, OVERLAY_STREAM_PROTOCOL},
};
use crate::{
  AuthorizationRegistry, Envelope, EnvelopeHeader, LinkStateRecord, MessageKind, NodeId,
  NodeIdentity, PROTOCOL_VERSION, ProtocolId, RequestId, RouteDecision, Router,
  authorization::AuthorizationError,
};

pub struct LinkRuntime {
  handle: LinkHandle,
  task: Option<JoinHandle<()>>,
}

impl LinkRuntime {
  pub fn start(
    identity: &NodeIdentity, config: LinkConfig, authorization: AuthorizationRegistry,
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
    let request_nonce = Arc::new(AtomicU64::new(0));
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
      pending_forwards: BTreeMap::new(),
      broadcast_requests: BTreeSet::new(),
      next_inbound_token: 0,
      request_nonce: request_nonce.clone(),
      last_link_state_sequence: 0,
    };
    let task = tokio::spawn(actor.run());
    Ok(Self {
      handle: LinkHandle::new(commands, snapshots, inbound_rx, request_nonce),
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

struct ForwardContext {
  channel: request_response::ResponseChannel<Envelope>,
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
  pending_outbound:
    BTreeMap<request_response::OutboundRequestId, oneshot::Sender<Result<Envelope, LinkError>>>,
  pending_inbound: BTreeMap<u64, request_response::ResponseChannel<Envelope>>,
  pending_forwards: BTreeMap<request_response::OutboundRequestId, ForwardContext>,
  broadcast_requests: BTreeSet<request_response::OutboundRequestId>,
  next_inbound_token: u64,
  request_nonce: Arc<AtomicU64>,
  last_link_state_sequence: u64,
}

impl LinkActor {
  async fn run(mut self) {
    loop {
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
      }
    }
  }

  fn handle_command(&mut self, command: LinkCommand) -> bool {
    match command {
      LinkCommand::Request { envelope, reply } => {
        self.send_routed_request(envelope, reply);
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
    let peer_id = address.iter().find_map(|protocol| match protocol {
      Protocol::P2p(peer_id) => Some(peer_id),
      _ => None,
    });
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

  fn handle_mdns(&mut self, event: mdns::Event) {
    let now = Instant::now();
    match event {
      mdns::Event::Discovered(discovered) => {
        for (peer_id, address) in discovered {
          if let Some(node_id) = self.state.authorization.node_for_peer(&peer_id) {
            self.state.directory.record(
              node_id,
              peer_id,
              address,
              AddressSource::Mdns,
              now,
              self.config.discovered_address_ttl(),
            );
          }
        }
      }
      mdns::Event::Expired(expired) => {
        for (peer_id, address) in expired {
          self.state.directory.remove(peer_id, &address);
        }
      }
    }
  }

  fn send_routed_request(
    &mut self, envelope: Envelope, reply: oneshot::Sender<Result<Envelope, LinkError>>,
  ) {
    match self.route_outbound(&envelope) {
      Ok(peer_id) => {
        let request_id = self
          .swarm
          .behaviour_mut()
          .messaging
          .send_request(&peer_id, envelope);
        self.pending_outbound.insert(request_id, reply);
      }
      Err(error) => {
        let _ = reply.send(Err(error));
      }
    }
  }

  fn route_outbound(&self, envelope: &Envelope) -> Result<PeerId, LinkError> {
    let header = envelope.header();
    let destination = header.destination;
    if header.version != PROTOCOL_VERSION || destination == self.state.node_id {
      return Err(LinkError::NoRoute(destination));
    }
    if header.protocol == ProtocolId::Admission
      && self
        .state
        .authorization
        .active_peer_for_node(destination)
        .is_none()
    {
      let mut quarantined = self
        .state
        .quarantined
        .values()
        .map(|connection| connection.peer_id);
      let Some(peer_id) = quarantined.next() else {
        return Err(LinkError::NoRoute(destination));
      };
      if quarantined.next().is_some() {
        return Err(LinkError::NoRoute(destination));
      }
      return Ok(peer_id);
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
    let channel = self
      .pending_inbound
      .remove(&token.0)
      .ok_or(LinkError::UnknownInbound)?;
    self
      .swarm
      .behaviour_mut()
      .messaging
      .send_response(channel, envelope)
      .map_err(|_| LinkError::Transport("failed to flush the overlay response".to_string()))
  }

  fn handle_messaging(&mut self, event: request_response::Event<Envelope, Envelope>) {
    match event {
      request_response::Event::Message { peer, message, .. } => match message {
        request_response::Message::Request {
          request, channel, ..
        } => self.handle_inbound_request(peer, request, channel),
        request_response::Message::Response {
          request_id,
          response,
        } => {
          if let Some(reply) = self.pending_outbound.remove(&request_id) {
            let _ = reply.send(Ok(response));
          } else if let Some(forward) = self.pending_forwards.remove(&request_id) {
            self.router.complete_forward();
            let _ = self
              .swarm
              .behaviour_mut()
              .messaging
              .send_response(forward.channel, response);
          } else if !self.broadcast_requests.remove(&request_id) {
            tracing::debug!(?request_id, "ignoring orphan overlay response");
          }
        }
      },
      request_response::Event::OutboundFailure {
        request_id, error, ..
      } => {
        if let Some(reply) = self.pending_outbound.remove(&request_id) {
          let _ = reply.send(Err(LinkError::Transport(error.to_string())));
        } else if let Some(forward) = self.pending_forwards.remove(&request_id) {
          self.router.complete_forward();
          drop(forward.channel);
        } else {
          self.broadcast_requests.remove(&request_id);
        }
      }
      request_response::Event::InboundFailure { error, .. } => {
        tracing::debug!(%error, "overlay inbound request failed");
      }
      request_response::Event::ResponseSent { .. } => {}
    }
  }

  fn handle_inbound_request(
    &mut self, peer_id: PeerId, envelope: Envelope,
    channel: request_response::ResponseChannel<Envelope>,
  ) {
    if self.state.authorization.node_for_peer(&peer_id).is_none() {
      let quarantined = self
        .state
        .quarantined
        .values()
        .any(|connection| connection.peer_id == peer_id);
      if quarantined && envelope.header().protocol == ProtocolId::Admission {
        self.deliver(peer_id, envelope, channel);
      } else {
        tracing::debug!(%peer_id, "dropping overlay message from an unauthorized peer");
      }
      return;
    }
    let header = envelope.header();
    if header.version != PROTOCOL_VERSION
      || header.cluster_id != self.state.authorization.cluster_id()
    {
      tracing::debug!(%peer_id, "dropping overlay message from a foreign overlay");
      return;
    }
    if header.protocol == ProtocolId::Route && header.kind == MessageKind::Event {
      self.accept_link_state(&envelope);
      self.reply_empty(channel, &envelope);
      return;
    }
    if header.protocol == ProtocolId::Admission {
      self.deliver(peer_id, envelope, channel);
      return;
    }
    match self.router.handle(envelope, now_unix_ms()) {
      RouteDecision::Deliver(envelope) => self.deliver(peer_id, envelope, channel),
      RouteDecision::Forward { next_hop, envelope } => {
        let Some(next_peer) = self.state.active_peer_for_node(next_hop) else {
          self.router.complete_forward();
          return;
        };
        let request_id = self
          .swarm
          .behaviour_mut()
          .messaging
          .send_request(&next_peer, envelope);
        self
          .pending_forwards
          .insert(request_id, ForwardContext { channel });
      }
      RouteDecision::Drop(reason) => {
        tracing::debug!(?reason, "dropping routed overlay envelope");
      }
    }
  }

  fn deliver(
    &mut self, peer_id: PeerId, envelope: Envelope,
    channel: request_response::ResponseChannel<Envelope>,
  ) {
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
      self.pending_inbound.insert(token, channel);
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
      let header = EnvelopeHeader {
        version: PROTOCOL_VERSION,
        cluster_id: self.state.authorization.cluster_id(),
        request_id: self.next_request_id(),
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

  fn next_request_id(&self) -> RequestId {
    let nonce = self.request_nonce.fetch_add(1, Ordering::Relaxed) + 1;
    RequestId::derive(self.state.node_id, nonce)
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
          self
            .state
            .set_connection_health(event.peer, event.connection, false)
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
