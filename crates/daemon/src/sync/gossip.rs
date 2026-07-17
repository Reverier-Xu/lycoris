//! Gossip fan-out: push broadcasts, state-message broadcasts, and the
//! deduplication caches that keep long-lived clusters from cycling messages
//! forever.

use std::{
  collections::{HashSet, VecDeque},
  future::Future,
  hash::Hash,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use lycoris_client::ClientError;
use lycoris_core::now_ms;
use lycoris_proto::node::{NodeInfo as ProtoNodeInfo, StateMessage};
use lycoris_storage::MetaStorage;
use tokio::time::timeout;

use super::{ClusterSync, RPC_TIMEOUT, peers::targets};
use crate::membership::convert::proto_to_register;

pub(super) const MAX_SEEN_PUSHES: usize = 10_000;
pub(super) const MAX_SEEN_STATES: usize = 10_000;

/// Meta-table key for the persisted gossip sequence counter.
const GOSSIP_SEQUENCE_KEY: &str = "gossip_sequence";

/// Gossip sequence counter persisted in the node meta table.
///
/// Peers deduplicate pushes on `(origin, sequence)`: if the counter restarted
/// at 1 after a process restart, peers would silently drop every new push as
/// already-seen. Each value is persisted *before* it is handed out, so a
/// crash can only waste a sequence number, never reuse one. Gossip is
/// low-frequency, so one meta write per allocation is acceptable.
#[derive(Debug, Clone)]
pub(super) struct PersistedSequence {
  next: Arc<AtomicU64>,
  meta: MetaStorage,
}

impl PersistedSequence {
  /// Resume from the last persisted value; a fresh node starts at 1.
  pub(super) fn load(meta: MetaStorage) -> Self {
    let next =
      crate::persisted_counter(&meta, GOSSIP_SEQUENCE_KEY).map_or(1, |last| last.saturating_add(1));
    Self {
      next: Arc::new(AtomicU64::new(next)),
      meta,
    }
  }

  /// Allocate the next sequence number, persisting it first.
  pub(super) fn next(&self) -> u64 {
    let sequence = self.next.fetch_add(1, Ordering::SeqCst);
    if let Err(error) = self.meta.set(GOSSIP_SEQUENCE_KEY, &sequence.to_string()) {
      tracing::warn!(%error, "failed to persist gossip sequence");
    }
    sequence
  }
}

/// A fixed-capacity deduplication set with FIFO eviction.
///
/// Used for gossip caches so that long-lived clusters do not grow memory
/// unbounded. Insert returns `true` when the key was newly added.
#[derive(Debug, Clone)]
pub(super) struct DedupSet<T: Clone + Eq + Hash> {
  inner: HashSet<T>,
  order: VecDeque<T>,
  capacity: usize,
}

impl<T: Clone + Eq + Hash> DedupSet<T> {
  pub(super) fn new(capacity: usize) -> Self {
    Self {
      inner: HashSet::with_capacity(capacity),
      order: VecDeque::with_capacity(capacity),
      capacity,
    }
  }

  pub(super) fn insert(&mut self, key: T) -> bool {
    if !self.inner.insert(key.clone()) {
      return false;
    }
    self.order.push_back(key);
    if self.order.len() > self.capacity
      && let Some(oldest) = self.order.pop_front()
    {
      self.inner.remove(&oldest);
    }
    true
  }
}

impl ClusterSync {
  /// Notify peers about a local registry change via push.
  pub async fn push_change(&self, info: ProtoNodeInfo) {
    let sequence = self.sequence.next();
    let origin = self.local_node_id.clone();

    self
      .seen_pushes
      .lock()
      .await
      .insert((origin.clone(), sequence));

    self.broadcast_push(info, origin, sequence).await;
  }

  pub(super) async fn broadcast_push(&self, info: ProtoNodeInfo, origin: String, sequence: u64) {
    let local_address = self.local_address().await.unwrap_or_default();
    let targets = targets(&self.node, &local_address, now_ms());
    for peer in targets {
      self
        .send_with_bookkeeping(
          &peer,
          "push",
          self.push_to_peer(&peer, info.clone(), origin.clone(), sequence),
        )
        .await;
    }
  }

  /// Send one message to `peer` under the RPC timeout, recording reachability:
  /// success marks the peer seen; failure and timeout share the same
  /// bookkeeping (failed attempt mark + channel eviction).
  async fn send_with_bookkeeping(
    &self, peer: &str, what: &'static str, send: impl Future<Output = Result<(), ClientError>>,
  ) {
    match timeout(RPC_TIMEOUT, send).await {
      Ok(Ok(())) => {
        let _ = self.node.peers().mark_seen(peer, now_ms());
      }
      Ok(Err(error)) => {
        tracing::warn!(%peer, %error, "{what} to peer failed");
        self.record_peer_failure(peer).await;
      }
      Err(_) => {
        tracing::warn!(%peer, "{what} to peer timed out");
        self.record_peer_failure(peer).await;
      }
    }
  }

  async fn push_to_peer(
    &self, peer: &str, info: ProtoNodeInfo, origin: String, sequence: u64,
  ) -> Result<(), ClientError> {
    let mut client = self.pool.connect(peer).await?;
    client.sync.push_node(info, origin, sequence).await?;
    Ok(())
  }

  pub(super) async fn broadcast_state_message(&self, message: StateMessage) {
    let local_address = self.local_address().await.unwrap_or_default();
    let targets = targets(&self.node, &local_address, now_ms());
    for peer in targets {
      self
        .send_with_bookkeeping(
          &peer,
          "state message",
          self.send_state_message_to_peer(&peer, message.clone()),
        )
        .await;
    }
  }

  async fn send_state_message_to_peer(
    &self, peer: &str, message: StateMessage,
  ) -> Result<(), ClientError> {
    let mut client = self.pool.connect(peer).await?;
    client.membership.state(message).await?;
    Ok(())
  }

  /// Handle an incoming node push: deduplicate on `(origin, sequence)`, merge,
  /// and forward to our own targets in the background.
  pub async fn serve_push_node(&self, info: ProtoNodeInfo, origin: String, sequence: u64) {
    let is_new = self
      .seen_pushes
      .lock()
      .await
      .insert((origin.clone(), sequence));
    if !is_new {
      return;
    }

    let local_address = self.local_address().await.unwrap_or_default();
    if info.address != local_address {
      let _ = self.node.peers().seed(&info.address);
    }
    let actions = self.service.register(proto_to_register(&info)).await;

    let sync = self.clone();
    self
      .spawn_task(async move {
        sync.dispatch(actions).await;
        sync.broadcast_push(info, origin, sequence).await;
      })
      .await;
  }
}

#[cfg(test)]
mod tests {
  use lycoris_storage::Storage;
  use tempfile::TempDir;

  use super::*;

  #[test]
  fn sequence_does_not_rewind_across_restarts() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.redb");
    {
      let storage = Storage::open(&db_path).unwrap();
      let sequence = PersistedSequence::load(storage.node().meta().clone());
      assert_eq!(sequence.next(), 1);
      assert_eq!(sequence.next(), 2);
    }

    // Reopening the same database simulates a process restart: the counter
    // must resume past the last issued value instead of rewinding to 1,
    // otherwise peers would dedup-drop every new push.
    {
      let storage = Storage::open(&db_path).unwrap();
      let sequence = PersistedSequence::load(storage.node().meta().clone());
      assert_eq!(sequence.next(), 3);
    }
  }
}
