//! Peer-to-peer synchronization: SWIM dispatch, gossip, and anti-entropy.
//!
//! [`ClusterSync`] orchestrates the cluster protocols: the background loops
//! (membership anti-entropy and the SWIM failure detector), SWIM action
//! dispatch ([`swim`]), gossip fan-out with deduplication ([`gossip`]), and
//! Merkle anti-entropy plus the compatibility full-sync path
//! ([`antientropy`]). Membership and resource requests use routed overlay
//! adapters. Endpoint ranking in [`peers`] remains for extension forwarding
//! until its cutover and for test-only legacy transport fixtures.

mod antientropy;
mod gossip;
#[cfg(test)]
pub(crate) mod peers;
mod resource;
mod swim;

use std::{future::Future, sync::Arc, time::Duration};

use lycoris_core::now_ms;
use lycoris_storage::NodeDomain;
pub(crate) use resource::ResourceSync;
use tokio::{
  sync::Mutex,
  task::JoinSet,
  time::{self, MissedTickBehavior},
};

use self::gossip::{DedupSet, MAX_SEEN_PUSHES, MAX_SEEN_STATES, PersistedSequence};
use crate::{membership::MembershipService, overlay_transport::MembershipPool};

/// Timeout applied to each individual peer RPC call driven by this module
/// tree. Exchange flows (a Merkle anti-entropy round, a gossip send) wrap
/// every call separately and never the exchange as a whole, so per-call
/// fallback branches stay reachable.
pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(3);

/// Orchestrates peer-to-peer membership synchronization.
///
/// `ClusterSync` owns the background loops and inbound overlay business logic.
/// Membership and resource requests use overlay-backed pools in production;
/// the storage node domain remains the source for CRDT persistence.
#[derive(Debug, Clone)]
pub struct ClusterSync {
  local_node_id: String,
  service: Arc<MembershipService>,
  node: NodeDomain,
  pool: MembershipPool,
  seen_pushes: Arc<Mutex<DedupSet<(String, u64)>>>,
  seen_states: Arc<Mutex<DedupSet<(String, u64, u8)>>>,
  sequence: PersistedSequence,
  /// Registry of short-lived background tasks (gossip forwarding, SWIM action
  /// dispatch); aborted as a whole on shutdown via [`Self::abort_tasks`].
  tasks: Arc<Mutex<JoinSet<()>>>,
}

impl ClusterSync {
  pub(crate) fn new(
    local_node_id: String, service: Arc<MembershipService>, node: NodeDomain,
    pool: impl Into<MembershipPool>,
  ) -> Self {
    let sequence = PersistedSequence::load(node.meta().clone());
    Self {
      local_node_id,
      service,
      node,
      pool: pool.into(),
      seen_pushes: Arc::new(Mutex::new(DedupSet::new(MAX_SEEN_PUSHES))),
      seen_states: Arc::new(Mutex::new(DedupSet::new(MAX_SEEN_STATES))),
      sequence,
      tasks: Arc::new(Mutex::new(JoinSet::new())),
    }
  }

  /// Allocate a push dedup key before broadcasting the corresponding gossip.
  pub(super) async fn allocate_push(&self) -> (String, u64) {
    let sequence = self.sequence.next();
    let origin = self.local_node_id.clone();
    self
      .seen_pushes
      .lock()
      .await
      .insert((origin.clone(), sequence));
    (origin, sequence)
  }

  /// Record a successful membership exchange.
  pub(super) fn record_sync_success(&self, peer: &str) {
    if let Err(error) = self.pool.mark_seen(&self.node, peer, now_ms()) {
      tracing::warn!(%peer, %error, "failed to mark peer seen");
    }
  }

  /// Seed non-local member addresses into the legacy peer bookkeeping used by
  /// resources and extension routing. Membership itself routes by `NodeId`.
  pub(super) async fn seed_member_nodes(&self) {
    let local_address = self.local_address().await.unwrap_or_default();
    for register in self
      .service
      .list_nodes(&std::collections::HashMap::new())
      .await
    {
      if register.address() != local_address
        && let Err(error) = self.node.peers().seed(register.address())
      {
        tracing::warn!(address = %register.address(), %error, "failed to seed known peer");
      }
    }
  }

  /// Start background anti-entropy sync and SWIM failure detection.
  pub async fn run(&self, interval: Duration) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
      ticker.tick().await;
      self.sync_with_peers().await;
    }
  }

  pub async fn run_swim(&self, interval: Duration) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
      ticker.tick().await;
      let actions = self.service.tick().await;
      let sync = self.clone();
      self
        .spawn_task(async move {
          sync.dispatch(actions).await;
        })
        .await;
    }
  }

  pub(super) async fn local_address(&self) -> Option<String> {
    self.service.member_address(&self.local_node_id).await
  }

  /// Spawn short-lived background work (gossip forwarding, action dispatch)
  /// into the shared task registry. Tracked tasks are aborted on shutdown by
  /// [`Self::abort_tasks`], so fire-and-forget work never outlives the
  /// daemon's managed lifetime. Finished tasks are reaped on each spawn, which
  /// keeps the registry bounded by the number of tasks actually in flight.
  pub(crate) async fn spawn_task(&self, task: impl Future<Output = ()> + Send + 'static) {
    let mut tasks = self.tasks.lock().await;
    while let Some(result) = tasks.try_join_next() {
      if let Err(error) = result {
        tracing::warn!(%error, "background sync task failed");
      }
    }
    tasks.spawn(task);
  }

  /// Abort every tracked background task; the runtime calls this alongside
  /// its own shutdown of the periodic loops.
  pub async fn abort_tasks(&self) {
    self.tasks.lock().await.abort_all();
  }

  /// Record a failed membership contact. Overlay health is owned by the link
  /// actor; the test-only legacy adapter keeps exercising persisted endpoint
  /// backoff.
  pub(super) async fn record_peer_failure(&self, peer: &str) {
    if let Err(error) = self.pool.mark_failed(&self.node, peer) {
      tracing::warn!(%peer, %error, "failed to record failed peer attempt");
    }
    self.pool.remove(peer).await;
  }
}
