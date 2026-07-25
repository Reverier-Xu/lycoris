use std::{sync::Arc, time::Duration};

use libp2p::Multiaddr;
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::{AuthorizationRegistry, Envelope, NodeId, authorization::AuthorizationError};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LinkSnapshot {
  pub node_id: NodeId,
  pub listen_addresses: Vec<Multiaddr>,
  pub connected_nodes: Vec<NodeId>,
  pub healthy_nodes: Vec<NodeId>,
  pub connection_count: usize,
}

/// Token correlating an inbound routed envelope with its response channel.
#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct InboundToken(pub u64);

/// An envelope delivered to the local node that awaits a response.
#[derive(Debug)]
pub struct InboundEnvelope {
  pub token: InboundToken,
  pub envelope: Envelope,
}

#[derive(Debug, Clone)]
pub struct LinkHandle {
  commands: mpsc::Sender<LinkCommand>,
  snapshots: watch::Receiver<LinkSnapshot>,
  inbound: Arc<Mutex<mpsc::Receiver<InboundEnvelope>>>,
}

impl LinkHandle {
  pub fn snapshot(&self) -> LinkSnapshot {
    self.snapshots.borrow().clone()
  }

  pub fn subscribe(&self) -> watch::Receiver<LinkSnapshot> {
    self.snapshots.clone()
  }

  /// Queue a dial attempt. Use [`Self::wait_connected`] to observe success.
  pub async fn dial(&self, node_id: NodeId, address: Multiaddr) -> Result<(), LinkError> {
    let (reply, response) = oneshot::channel();
    self
      .send(LinkCommand::Dial {
        node_id,
        address,
        reply,
      })
      .await?;
    response.await.map_err(|_| LinkError::ActorStopped)?
  }

  /// Reserve a relay slot and listen through the relay for inbound circuits.
  pub async fn listen_via_relay(
    &self, relay_node: NodeId, relay_address: Multiaddr,
  ) -> Result<(), LinkError> {
    let (reply, response) = oneshot::channel();
    self
      .send(LinkCommand::ListenViaRelay {
        node_id: relay_node,
        address: relay_address,
        reply,
      })
      .await?;
    response.await.map_err(|_| LinkError::ActorStopped)?
  }

  /// Close the node's established connections; pending dials are unaffected.
  pub async fn disconnect(&self, node_id: NodeId) -> Result<(), LinkError> {
    let (reply, response) = oneshot::channel();
    self
      .send(LinkCommand::Disconnect { node_id, reply })
      .await?;
    response.await.map_err(|_| LinkError::ActorStopped)?
  }

  /// Send a routed request and await the correlated response envelope.
  pub async fn request(&self, envelope: Envelope) -> Result<Envelope, LinkError> {
    let (reply, response) = oneshot::channel();
    self.send(LinkCommand::Request { envelope, reply }).await?;
    response.await.map_err(|_| LinkError::ActorStopped)?
  }

  /// Respond to an envelope previously delivered via [`Self::next_inbound`].
  pub async fn respond(&self, token: InboundToken, envelope: Envelope) -> Result<(), LinkError> {
    let (reply, response) = oneshot::channel();
    self
      .send(LinkCommand::Respond {
        token,
        envelope,
        reply,
      })
      .await?;
    response.await.map_err(|_| LinkError::ActorStopped)?
  }

  /// Receive the next envelope delivered to this node.
  pub async fn next_inbound(&self) -> Option<InboundEnvelope> {
    self.inbound.lock().await.recv().await
  }

  pub async fn set_authorization(&self, registry: AuthorizationRegistry) -> Result<(), LinkError> {
    let (reply, response) = oneshot::channel();
    self
      .send(LinkCommand::SetAuthorization { registry, reply })
      .await?;
    response.await.map_err(|_| LinkError::ActorStopped)?
  }

  pub async fn wait_connected(&self, node_id: NodeId, timeout: Duration) -> Result<(), LinkError> {
    self
      .wait_for(timeout, |snapshot| {
        snapshot.connected_nodes.contains(&node_id)
      })
      .await
  }

  pub async fn wait_healthy(&self, node_id: NodeId, timeout: Duration) -> Result<(), LinkError> {
    self
      .wait_for(timeout, |snapshot| {
        snapshot.healthy_nodes.contains(&node_id)
      })
      .await
  }

  pub(crate) async fn shutdown(&self) -> Result<(), LinkError> {
    let (reply, response) = oneshot::channel();
    self.send(LinkCommand::Shutdown { reply }).await?;
    response.await.map_err(|_| LinkError::ActorStopped)
  }

  pub(crate) fn new(
    commands: mpsc::Sender<LinkCommand>, snapshots: watch::Receiver<LinkSnapshot>,
    inbound: mpsc::Receiver<InboundEnvelope>,
  ) -> Self {
    Self {
      commands,
      snapshots,
      inbound: Arc::new(Mutex::new(inbound)),
    }
  }

  async fn send(&self, command: LinkCommand) -> Result<(), LinkError> {
    self
      .commands
      .send(command)
      .await
      .map_err(|_| LinkError::ActorStopped)
  }

  async fn wait_for(
    &self, timeout: Duration, predicate: impl Fn(&LinkSnapshot) -> bool,
  ) -> Result<(), LinkError> {
    let mut snapshots = self.snapshots.clone();
    let wait = async {
      loop {
        if predicate(&snapshots.borrow()) {
          return Ok(());
        }
        snapshots
          .changed()
          .await
          .map_err(|_| LinkError::ActorStopped)?;
      }
    };
    tokio::time::timeout(timeout, wait)
      .await
      .map_err(|_| LinkError::Timeout)?
  }
}

#[derive(Debug)]
pub(crate) enum LinkCommand {
  Request {
    envelope: Envelope,
    reply: oneshot::Sender<Result<Envelope, LinkError>>,
  },
  Respond {
    token: InboundToken,
    envelope: Envelope,
    reply: oneshot::Sender<Result<(), LinkError>>,
  },
  Dial {
    node_id: NodeId,
    address: Multiaddr,
    reply: oneshot::Sender<Result<(), LinkError>>,
  },
  ListenViaRelay {
    node_id: NodeId,
    address: Multiaddr,
    reply: oneshot::Sender<Result<(), LinkError>>,
  },
  Disconnect {
    node_id: NodeId,
    reply: oneshot::Sender<Result<(), LinkError>>,
  },
  SetAuthorization {
    registry: AuthorizationRegistry,
    reply: oneshot::Sender<Result<(), LinkError>>,
  },
  Shutdown {
    reply: oneshot::Sender<()>,
  },
}

#[derive(Debug, Error)]
pub enum LinkError {
  #[error("link transport failed: {0}")]
  Transport(String),
  #[error("link actor is not running")]
  ActorStopped,
  #[error("node {0} is not authorized")]
  UnauthorizedNode(NodeId),
  #[error("node {0} is not connected")]
  NotConnected(NodeId),
  #[error("no overlay route to node {0}")]
  NoRoute(NodeId),
  #[error("inbound envelope token is unknown")]
  UnknownInbound,
  #[error("link state did not reach the requested condition before the deadline")]
  Timeout,
  #[error(transparent)]
  Authorization(#[from] AuthorizationError),
  #[error("link actor task failed: {0}")]
  Task(#[from] tokio::task::JoinError),
}
