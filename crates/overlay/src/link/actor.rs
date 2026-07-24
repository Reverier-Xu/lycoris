use std::collections::BTreeSet;

use futures_util::StreamExt;
use libp2p::{
  Multiaddr, PeerId, Swarm, SwarmBuilder, noise, ping,
  swarm::{ConnectionId, SwarmEvent, dial_opts::DialOpts},
  tcp, yamux,
};
use tokio::{
  sync::{mpsc, watch},
  task::JoinHandle,
};

use super::{LinkCommand, LinkConfig, LinkError, LinkHandle, LinkSnapshot};
use crate::NodeIdentity;

pub struct LinkRuntime {
  handle: LinkHandle,
  task: Option<JoinHandle<()>>,
}

impl LinkRuntime {
  pub fn start(identity: &NodeIdentity, config: LinkConfig) -> Result<Self, LinkError> {
    let mut swarm = build_swarm(identity, &config)?;
    for address in config.listen_addresses() {
      swarm
        .listen_on(address.clone())
        .map_err(|error| LinkError::Transport(error.to_string()))?;
    }

    let local_peer_id = identity.peer_id();
    let state = LinkState::new(identity.node_id(), local_peer_id);
    let (snapshot_tx, snapshots) = watch::channel(state.snapshot());
    let (commands, command_rx) = mpsc::channel(config.command_capacity());
    let actor = LinkActor {
      swarm,
      command_rx,
      snapshot_tx,
      state,
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

struct LinkActor {
  swarm: Swarm<ping::Behaviour>,
  command_rx: mpsc::Receiver<LinkCommand>,
  snapshot_tx: watch::Sender<LinkSnapshot>,
  state: LinkState,
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
      }
    }
  }

  fn handle_command(&mut self, command: LinkCommand) -> bool {
    match command {
      LinkCommand::Dial {
        peer_id,
        address,
        reply,
      } => {
        let options = DialOpts::peer_id(peer_id).addresses(vec![address]).build();
        let result = self
          .swarm
          .dial(options)
          .map_err(|error| LinkError::Transport(error.to_string()));
        let _ = reply.send(result);
        false
      }
      LinkCommand::Disconnect { peer_id, reply } => {
        let result = self
          .swarm
          .disconnect_peer_id(peer_id)
          .map_err(|()| LinkError::NotConnected(peer_id));
        let _ = reply.send(result);
        false
      }
      LinkCommand::Shutdown { reply } => {
        let _ = reply.send(());
        true
      }
    }
  }

  fn handle_event(&mut self, event: SwarmEvent<ping::Event>) {
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
      SwarmEvent::ConnectionEstablished { peer_id, .. } => self.state.connected.insert(peer_id),
      SwarmEvent::ConnectionClosed {
        peer_id,
        connection_id,
        num_established,
        ..
      } => self
        .state
        .connection_closed(peer_id, connection_id, num_established),
      SwarmEvent::Behaviour(event) => match event.result {
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
      SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
        tracing::debug!(?peer_id, %error, "overlay dial failed");
        false
      }
      _ => false,
    };
    if changed {
      self.snapshot_tx.send_replace(self.state.snapshot());
    }
  }
}

struct LinkState {
  node_id: crate::NodeId,
  local_peer_id: PeerId,
  listeners: BTreeSet<Multiaddr>,
  connected: BTreeSet<PeerId>,
  healthy_connections: BTreeSet<(PeerId, ConnectionId)>,
}

impl LinkState {
  fn new(node_id: crate::NodeId, local_peer_id: PeerId) -> Self {
    Self {
      node_id,
      local_peer_id,
      listeners: BTreeSet::new(),
      connected: BTreeSet::new(),
      healthy_connections: BTreeSet::new(),
    }
  }

  fn connection_closed(
    &mut self, peer_id: PeerId, connection_id: ConnectionId, remaining_connections: u32,
  ) -> bool {
    let health_changed = self.set_connection_health(peer_id, connection_id, false);
    let connection_changed = (remaining_connections == 0).then(|| self.connected.remove(&peer_id));
    health_changed || connection_changed.unwrap_or(false)
  }

  fn set_connection_health(
    &mut self, peer_id: PeerId, connection_id: ConnectionId, healthy: bool,
  ) -> bool {
    let was_healthy = self.peer_is_healthy(peer_id);
    let connection = (peer_id, connection_id);
    if healthy {
      self.healthy_connections.insert(connection);
    } else {
      self.healthy_connections.remove(&connection);
    }
    was_healthy != self.peer_is_healthy(peer_id)
  }

  fn peer_is_healthy(&self, peer_id: PeerId) -> bool {
    self
      .healthy_connections
      .iter()
      .any(|(healthy_peer, _)| *healthy_peer == peer_id)
  }

  fn snapshot(&self) -> LinkSnapshot {
    let healthy_peers = self
      .connected
      .iter()
      .copied()
      .filter(|peer_id| self.peer_is_healthy(*peer_id))
      .collect();
    LinkSnapshot {
      node_id: self.node_id,
      local_peer_id: self.local_peer_id,
      listen_addresses: self.listeners.iter().cloned().collect(),
      connected_peers: self.connected.iter().copied().collect(),
      healthy_peers,
    }
  }
}

fn build_swarm(
  identity: &NodeIdentity, config: &LinkConfig,
) -> Result<Swarm<ping::Behaviour>, LinkError> {
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
      ping::Behaviour::new(
        ping::Config::new()
          .with_interval(config.ping_interval())
          .with_timeout(config.ping_timeout()),
      )
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
  use libp2p::{PeerId, swarm::ConnectionId};

  use super::*;
  use crate::NodeId;

  #[test]
  fn healthy_peer_state_requires_only_one_healthy_connection() {
    let mut state = LinkState::new(
      NodeId::from_bytes([0; NodeId::BYTE_LENGTH]),
      PeerId::random(),
    );
    let peer = PeerId::random();
    let first = ConnectionId::new_unchecked(1);
    let second = ConnectionId::new_unchecked(2);
    state.connected.insert(peer);

    assert!(state.set_connection_health(peer, first, true));
    assert!(!state.set_connection_health(peer, second, true));
    assert!(!state.set_connection_health(peer, first, false));
    assert!(state.snapshot().healthy_peers.contains(&peer));

    assert!(state.connection_closed(peer, second, 1));
    assert!(state.connected.contains(&peer));
    assert!(!state.snapshot().healthy_peers.contains(&peer));
    assert!(state.connection_closed(peer, first, 0));
    assert!(!state.connected.contains(&peer));
  }
}
