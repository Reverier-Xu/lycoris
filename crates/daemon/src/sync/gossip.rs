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
    // Fan out concurrently: a sequential loop costs up to N x RPC_TIMEOUT per
    // round, which stalls the dispatching task and lets work pile up while a
    // partition makes targets time out one by one.
    let mut fanout = tokio::task::JoinSet::new();
    for peer in targets {
      let sync = self.clone();
      let info = info.clone();
      let origin = origin.clone();
      fanout.spawn(async move {
        sync
          .send_with_bookkeeping(
            &peer,
            "push",
            sync.push_to_peer(&peer, info, origin, sequence),
          )
          .await;
      });
    }
    while let Some(result) = fanout.join_next().await {
      if let Err(error) = result {
        tracing::warn!(%error, "gossip fan-out task failed");
      }
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
        if let Err(error) = self.node.peers().mark_seen(peer, now_ms()) {
          tracing::warn!(%peer, %error, "failed to mark peer seen");
        }
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
    // Concurrent fan-out, same as `broadcast_push`.
    let mut fanout = tokio::task::JoinSet::new();
    for peer in targets {
      let sync = self.clone();
      let message = message.clone();
      fanout.spawn(async move {
        sync
          .send_with_bookkeeping(
            &peer,
            "state message",
            sync.send_state_message_to_peer(&peer, message),
          )
          .await;
      });
    }
    while let Some(result) = fanout.join_next().await {
      if let Err(error) = result {
        tracing::warn!(%error, "gossip fan-out task failed");
      }
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
  ///
  /// The rpc layer answers every well-formed push with `accepted: true`: the
  /// flag only means the request was received — deduplication and the merge
  /// decision happen here, so a duplicate or stale push is silently dropped
  /// after the ack.
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
    if info.address != local_address
      && let Err(error) = self.node.peers().seed(&info.address)
    {
      tracing::warn!(address = %info.address, %error, "failed to seed peer address");
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

  #[test]
  fn dedup_set_insert_reports_new_vs_duplicate() {
    let mut set = DedupSet::new(2);

    assert!(set.insert("a"), "first insert of a key must report new");
    assert!(!set.insert("a"), "re-insert must report duplicate");
    assert!(set.insert("b"));
    assert!(set.inner.contains("a") && set.inner.contains("b"));
  }

  #[test]
  fn dedup_set_holds_exactly_capacity_entries() {
    let mut set = DedupSet::new(2);

    // At exactly capacity nothing is evicted yet.
    set.insert("a");
    set.insert("b");
    assert_eq!(set.inner.len(), 2);
    assert_eq!(set.order.len(), 2);
  }

  #[test]
  fn dedup_set_evicts_oldest_first_over_capacity() {
    let mut set = DedupSet::new(2);
    set.insert("a");
    set.insert("b");

    // The third insert evicts "a", the oldest; "b" survives.
    set.insert("c");
    assert!(!set.inner.contains("a"));
    assert!(set.inner.contains("b") && set.inner.contains("c"));
    assert_eq!(set.inner.len(), 2);

    // An evicted key is fully forgotten: inserting it again reports new.
    assert!(set.insert("a"));
  }

  #[test]
  fn dedup_set_duplicate_insert_keeps_fifo_position() {
    let mut set = DedupSet::new(2);
    set.insert("a");
    set.insert("b");

    // Re-inserting the oldest key must not refresh its position: the set is a
    // strict FIFO, so "a" stays the next eviction victim.
    assert!(!set.insert("a"));
    set.insert("c");

    assert!(
      !set.inner.contains("a"),
      "duplicate insert must not protect \"a\" from eviction"
    );
    assert!(set.inner.contains("b") && set.inner.contains("c"));
  }
}
