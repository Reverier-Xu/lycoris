use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use libp2p::Multiaddr;
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::{
  AuthorizationRegistry, Envelope, NodeId, RequestId, authorization::AuthorizationError,
};

pub(super) const REQUEST_BOOT_ID_BYTES: usize = 16;

#[derive(Debug)]
pub(super) struct RequestSequence {
  boot_id: [u8; REQUEST_BOOT_ID_BYTES],
  next: AtomicU64,
}

impl RequestSequence {
  pub(super) const fn new(boot_id: [u8; REQUEST_BOOT_ID_BYTES], next: u64) -> Self {
    Self {
      boot_id,
      next: AtomicU64::new(next),
    }
  }

  pub(super) fn allocate(&self, node_id: NodeId) -> Result<RequestId, LinkError> {
    let sequence = self
      .next
      .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        current.checked_add(1)
      })
      .map_err(|_| LinkError::RequestSequenceExhausted)?;
    Ok(RequestId::derive(node_id, &self.boot_id, sequence))
  }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LinkSnapshot {
  pub node_id: NodeId,
  pub listen_addresses: Vec<Multiaddr>,
  pub connected_nodes: Vec<NodeId>,
  pub healthy_nodes: Vec<NodeId>,
  pub connection_count: usize,
  /// Unauthorized peers currently confined to the admission protocol.
  pub quarantined_peers: Vec<libp2p::PeerId>,
  pub quarantined_count: usize,
}

/// Token correlating an inbound routed envelope with its response channel.
#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct InboundToken(pub u64);

/// An envelope delivered to the local node that awaits a response.
#[derive(Debug)]
pub struct InboundEnvelope {
  pub token: InboundToken,
  /// Transport peer that handed the envelope to this node. For admission
  /// traffic this is the authenticated peer the enrollment proof binds to.
  pub sender: libp2p::PeerId,
  pub envelope: Envelope,
}

#[derive(Debug, Clone)]
pub struct LinkHandle {
  commands: mpsc::Sender<LinkCommand>,
  snapshots: watch::Receiver<LinkSnapshot>,
  inbound: Arc<Mutex<mpsc::Receiver<InboundEnvelope>>>,
  request_ids: Arc<RequestSequence>,
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

  /// Dial a bootstrap address for admission without requiring prior
  /// authorization. The address must embed the sponsor's `/p2p/` peer id;
  /// the resulting connection stays quarantined until enrollment completes.
  pub async fn dial_admission(&self, address: Multiaddr) -> Result<crate::PeerId, LinkError> {
    let (reply, response) = oneshot::channel();
    self
      .send(LinkCommand::DialAdmission { address, reply })
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

  /// Send admission traffic only to the explicitly dialed sponsor peer.
  pub async fn request_admission(
    &self, sponsor: libp2p::PeerId, envelope: Envelope,
  ) -> Result<Envelope, LinkError> {
    let (reply, response) = oneshot::channel();
    self
      .send(LinkCommand::RequestAdmission {
        sponsor,
        envelope,
        reply,
      })
      .await?;
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

  /// Validate, durably persist, and install a same-cluster authorization
  /// checkpoint in one actor turn. No swarm event can observe a registry
  /// between the persistence and installation steps.
  pub async fn commit_authorization<F, E>(
    &self, registry: AuthorizationRegistry, persist: F,
  ) -> Result<(), LinkError>
  where
    F: FnOnce() -> Result<(), E> + Send + 'static,
    E: std::fmt::Display, {
    self
      .commit_authorization_inner(registry, true, persist)
      .await
  }

  /// Commit a foreign checkpoint when a solo node adopts its sponsor's
  /// cluster during initial enrollment.
  pub async fn commit_adopt_authorization<F, E>(
    &self, registry: AuthorizationRegistry, persist: F,
  ) -> Result<(), LinkError>
  where
    F: FnOnce() -> Result<(), E> + Send + 'static,
    E: std::fmt::Display, {
    self
      .commit_authorization_inner(registry, false, persist)
      .await
  }

  async fn commit_authorization_inner<F, E>(
    &self, registry: AuthorizationRegistry, check_cluster: bool, persist: F,
  ) -> Result<(), LinkError>
  where
    F: FnOnce() -> Result<(), E> + Send + 'static,
    E: std::fmt::Display, {
    let (reply, response) = oneshot::channel();
    let persist = Box::new(move || persist().map_err(|error| error.to_string()));
    self
      .send(LinkCommand::CommitAuthorization {
        registry,
        check_cluster,
        persist,
        reply,
      })
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

  pub async fn wait_quarantined(
    &self, peer_id: libp2p::PeerId, timeout: Duration,
  ) -> Result<(), LinkError> {
    self
      .wait_for(timeout, |snapshot| {
        snapshot.quarantined_peers.contains(&peer_id)
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

  /// Allocate the next request id from the runtime-wide sequence shared by
  /// link-state broadcasts and every upper protocol.
  pub fn next_request_id(&self) -> Result<RequestId, LinkError> {
    self.request_ids.allocate(self.snapshot().node_id)
  }

  pub(crate) async fn shutdown(&self) -> Result<(), LinkError> {
    let (reply, response) = oneshot::channel();
    self.send(LinkCommand::Shutdown { reply }).await?;
    response.await.map_err(|_| LinkError::ActorStopped)
  }

  pub(super) fn new(
    commands: mpsc::Sender<LinkCommand>, snapshots: watch::Receiver<LinkSnapshot>,
    inbound: mpsc::Receiver<InboundEnvelope>, request_ids: Arc<RequestSequence>,
  ) -> Self {
    Self {
      commands,
      snapshots,
      inbound: Arc::new(Mutex::new(inbound)),
      request_ids,
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

pub(crate) type AuthorizationCommit = Box<dyn FnOnce() -> Result<(), String> + Send>;

pub(crate) enum LinkCommand {
  Request {
    envelope: Envelope,
    reply: oneshot::Sender<Result<Envelope, LinkError>>,
  },
  RequestAdmission {
    sponsor: libp2p::PeerId,
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
  DialAdmission {
    address: Multiaddr,
    reply: oneshot::Sender<Result<libp2p::PeerId, LinkError>>,
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
  CommitAuthorization {
    registry: AuthorizationRegistry,
    check_cluster: bool,
    persist: AuthorizationCommit,
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
  #[error("direct admission request used a non-admission protocol")]
  InvalidAdmissionRequest,
  #[error("admission peer {0} is not quarantined or authorized")]
  AdmissionPeerUnavailable(libp2p::PeerId),
  #[error("inbound envelope token is unknown")]
  UnknownInbound,
  #[error("link state did not reach the requested condition before the deadline")]
  Timeout,
  #[error("overlay response metadata does not match its request")]
  InvalidResponse,
  #[error("failed to generate a random request boot namespace")]
  RandomGeneration,
  #[error("the per-boot request sequence is exhausted")]
  RequestSequenceExhausted,
  #[error("authorization checkpoint commit failed: {0}")]
  AuthorizationCommit(String),
  #[error(transparent)]
  Authorization(#[from] AuthorizationError),
  #[error("link actor task failed: {0}")]
  Task(#[from] tokio::task::JoinError),
}
