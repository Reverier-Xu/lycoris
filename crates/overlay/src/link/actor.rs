use std::{
  collections::{BTreeMap, BTreeSet},
  time::Instant,
};

use futures_util::StreamExt;
use libp2p::{
  Multiaddr, PeerId, Swarm, SwarmBuilder,
  core::ConnectedPoint,
  mdns,
  multiaddr::Protocol,
  noise, ping,
  swarm::{
    ConnectionId, NetworkBehaviour, SwarmEvent, behaviour::toggle::Toggle, dial_opts::DialOpts,
  },
  tcp, yamux,
};
use tokio::{
  sync::{mpsc, watch},
  task::JoinHandle,
};

use super::{
  LinkCommand, LinkConfig, LinkError, LinkHandle, LinkSnapshot,
  directory::{AddressSource, PeerDirectory},
};
use crate::{AuthorizationRegistry, NodeId, NodeIdentity, authorization::AuthorizationError};

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
    let actor = LinkActor {
      swarm,
      command_rx,
      snapshot_tx,
      state,
      config: config.clone(),
      maintenance: tokio::time::interval(config.reconnect_interval()),
    };
    let task = tokio::spawn(actor.run());
    Ok(Self {
      handle: LinkHandle::new(commands, snapshots),
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
}

struct LinkActor {
  swarm: Swarm<LinkBehaviour>,
  command_rx: mpsc::Receiver<LinkCommand>,
  snapshot_tx: watch::Sender<LinkSnapshot>,
  state: LinkState,
  config: LinkConfig,
  maintenance: tokio::time::Interval,
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
      LinkCommand::Dial {
        node_id,
        address,
        reply,
      } => {
        let result = self.dial(node_id, address);
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
      LinkCommand::SetAuthorization { registry, reply } => {
        let result = self.set_authorization(registry);
        let _ = reply.send(result);
        false
      }
      LinkCommand::Shutdown { reply } => {
        let _ = reply.send(());
        true
      }
    }
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

  fn set_authorization(&mut self, registry: AuthorizationRegistry) -> Result<(), LinkError> {
    if registry.cluster_id() != self.state.authorization.cluster_id() {
      return Err(
        AuthorizationError::ForeignCluster {
          expected: self.state.authorization.cluster_id(),
          actual: registry.cluster_id(),
        }
        .into(),
      );
    }
    self.state.authorization = registry;
    let authorization = self.state.authorization.clone();
    self
      .state
      .directory
      .retain(|node_id| authorization.active_peer_for_node(node_id).is_some());
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
    Ok(())
  }

  fn handle_event(&mut self, event: SwarmEvent<LinkBehaviourEvent>) {
    let changed = match event {
      SwarmEvent::NewListenAddr { address, .. } => self.state.listeners.insert(address),
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
        let changed = self.state.connection_closed(peer_id, connection_id);
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
  }

  fn connection_established(
    &mut self, peer_id: PeerId, connection_id: ConnectionId, endpoint: ConnectedPoint,
  ) -> bool {
    self.state.pending_dials.remove(&connection_id);
    let Some(node_id) = self.state.authorization.node_for_peer(&peer_id) else {
      tracing::debug!(%peer_id, "closing unauthorized overlay connection");
      self.swarm.close_connection(connection_id);
      return false;
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

struct LinkState {
  node_id: NodeId,
  local_peer_id: PeerId,
  authorization: AuthorizationRegistry,
  listeners: BTreeSet<Multiaddr>,
  connections: BTreeMap<ConnectionId, ConnectionInfo>,
  healthy_connections: BTreeSet<ConnectionId>,
  directory: PeerDirectory,
  pending_dials: BTreeMap<ConnectionId, NodeId>,
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
      directory: PeerDirectory::default(),
      pending_dials: BTreeMap::new(),
    }
  }

  fn active_peer_for_node(&self, node_id: NodeId) -> Option<PeerId> {
    self.authorization.active_peer_for_node(node_id)
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
    .with_behaviour(|_| {
      let discovery_config = mdns::Config {
        ttl: config.discovered_address_ttl(),
        query_interval: config.mdns_query_interval(),
        enable_ipv6: false,
      };
      let discovery = match mdns::tokio::Behaviour::new(discovery_config, identity.peer_id()) {
        Ok(behaviour) => Some(behaviour),
        Err(error) => {
          tracing::warn!(%error, "overlay lan discovery is unavailable");
          None
        }
      };
      LinkBehaviour {
        ping: ping::Behaviour::new(
          ping::Config::new()
            .with_interval(config.ping_interval())
            .with_timeout(config.ping_timeout()),
        ),
        mdns: discovery.into(),
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

  fn state() -> LinkState {
    let identity = NodeIdentity::generate();
    let (cluster_id, record) = AuthorizationRecord::genesis(&identity).unwrap();
    let registry = AuthorizationRegistry::from_records(cluster_id, [record]).unwrap();
    LinkState::new(identity.node_id(), identity.peer_id(), registry)
  }

  fn endpoint(address: &str) -> ConnectedPoint {
    ConnectedPoint::Dialer {
      address: address.parse().unwrap(),
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
      local_addr: "/ip4/127.0.0.1/udp/4001/quic-v1".parse().unwrap(),
      send_back_addr: "/ip4/127.0.0.1/udp/5000/quic-v1".parse().unwrap(),
    };
    let canonical = connection_preference(local, remote, &listener);
    let non_canonical = connection_preference(remote, local, &listener);
    assert_ne!(canonical.initiator, non_canonical.initiator);
  }
}
