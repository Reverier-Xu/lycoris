use std::time::Duration;

use libp2p::{Multiaddr, PeerId};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

use crate::NodeId;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LinkSnapshot {
  pub node_id: NodeId,
  pub local_peer_id: PeerId,
  pub listen_addresses: Vec<Multiaddr>,
  pub connected_peers: Vec<PeerId>,
  pub healthy_peers: Vec<PeerId>,
}

#[derive(Debug, Clone)]
pub struct LinkHandle {
  commands: mpsc::Sender<LinkCommand>,
  snapshots: watch::Receiver<LinkSnapshot>,
}

impl LinkHandle {
  pub fn snapshot(&self) -> LinkSnapshot {
    self.snapshots.borrow().clone()
  }

  pub fn subscribe(&self) -> watch::Receiver<LinkSnapshot> {
    self.snapshots.clone()
  }

  /// Queue a dial attempt. Use [`Self::wait_connected`] to observe success.
  pub async fn dial(&self, peer_id: PeerId, address: Multiaddr) -> Result<(), LinkError> {
    let (reply, response) = oneshot::channel();
    self
      .send(LinkCommand::Dial {
        peer_id,
        address,
        reply,
      })
      .await?;
    response.await.map_err(|_| LinkError::ActorStopped)?
  }

  /// Close the peer's established connections; pending dials are unaffected.
  pub async fn disconnect(&self, peer_id: PeerId) -> Result<(), LinkError> {
    let (reply, response) = oneshot::channel();
    self
      .send(LinkCommand::Disconnect { peer_id, reply })
      .await?;
    response.await.map_err(|_| LinkError::ActorStopped)?
  }

  pub async fn wait_connected(&self, peer_id: PeerId, timeout: Duration) -> Result<(), LinkError> {
    self
      .wait_for(timeout, |snapshot| {
        snapshot.connected_peers.contains(&peer_id)
      })
      .await
  }

  pub async fn wait_healthy(&self, peer_id: PeerId, timeout: Duration) -> Result<(), LinkError> {
    self
      .wait_for(timeout, |snapshot| {
        snapshot.healthy_peers.contains(&peer_id)
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
  ) -> Self {
    Self {
      commands,
      snapshots,
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
  Dial {
    peer_id: PeerId,
    address: Multiaddr,
    reply: oneshot::Sender<Result<(), LinkError>>,
  },
  Disconnect {
    peer_id: PeerId,
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
  #[error("peer {0} is not connected")]
  NotConnected(PeerId),
  #[error("link state did not reach the requested condition before the deadline")]
  Timeout,
  #[error("link actor task failed: {0}")]
  Task(#[from] tokio::task::JoinError),
}
