//! Peer-to-peer synchronization: SWIM dispatch, gossip, and anti-entropy.
//!
//! [`ClusterSync`] orchestrates the cluster protocols: the background loops
//! (membership anti-entropy and the SWIM failure detector), SWIM action
//! dispatch ([`swim`]), gossip fan-out with deduplication ([`gossip`]), and
//! Merkle anti-entropy plus the compatibility full-sync path
//! ([`antientropy`]). The tonic service wiring lives in `crate::rpc::cluster`;
//! resource anti-entropy is the sibling [`ResourceSync`] task
//! ([`resource`]). Peer channels come from `crate::transport::PeerPool`,
//! peer endpoint bookkeeping reads the storage node domain held here
//! directly, and every sync plane picks endpoints through the single
//! selection policy in [`peers`] (D9).

mod antientropy;
mod gossip;
mod peers;
mod resource;
mod swim;

use std::{future::Future, sync::Arc, time::Duration};

use lycoris_storage::NodeDomain;
pub(crate) use resource::ResourceSync;
use tokio::{
  sync::Mutex,
  task::JoinSet,
  time::{self, MissedTickBehavior},
};

use self::gossip::{DedupSet, MAX_SEEN_PUSHES, MAX_SEEN_STATES, PersistedSequence};
use crate::{membership::MembershipService, transport::PeerPool};

/// Timeout applied to each individual peer RPC call driven by this module
/// tree. Exchange flows (a Merkle anti-entropy round, a gossip send) wrap
/// every call separately and never the exchange as a whole, so per-call
/// fallback branches stay reachable.
const RPC_TIMEOUT: Duration = Duration::from_secs(3);

/// Orchestrates peer-to-peer membership synchronization.
///
/// `ClusterSync` owns the background loops and the inbound business logic
/// behind the `Sync`/`Membership` RPCs (served in `crate::rpc::cluster`).
/// Peer channels and shared-resource sync live in `PeerPool` and
/// `ResourceSync` respectively; peer endpoint bookkeeping goes through the
/// storage node domain held in `node`.
#[derive(Debug, Clone)]
pub struct ClusterSync {
  local_node_id: String,
  service: Arc<MembershipService>,
  node: NodeDomain,
  pool: PeerPool,
  resources: ResourceSync,
  seen_pushes: Arc<Mutex<DedupSet<(String, u64)>>>,
  seen_states: Arc<Mutex<DedupSet<(String, u64, u8)>>>,
  sequence: PersistedSequence,
  /// Registry of short-lived background tasks (gossip forwarding, SWIM action
  /// dispatch); aborted as a whole on shutdown via [`Self::abort_tasks`].
  tasks: Arc<Mutex<JoinSet<()>>>,
}

impl ClusterSync {
  pub fn new(
    local_node_id: String, service: Arc<MembershipService>, node: NodeDomain, pool: PeerPool,
    resources: ResourceSync,
  ) -> Self {
    let sequence = PersistedSequence::load(node.meta().clone());
    Self {
      local_node_id,
      service,
      node,
      pool,
      resources,
      seen_pushes: Arc::new(Mutex::new(DedupSet::new(MAX_SEEN_PUSHES))),
      seen_states: Arc::new(Mutex::new(DedupSet::new(MAX_SEEN_STATES))),
      sequence,
      tasks: Arc::new(Mutex::new(JoinSet::new())),
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

  /// Access the resource anti-entropy task (used by the rpc boundary to serve
  /// incoming `SyncResources` requests).
  pub(crate) fn resources(&self) -> &ResourceSync {
    &self.resources
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

  /// Record a failed contact with `peer`: mark the attempt failed so the
  /// selection policy backs off ([`peers`]), and evict the cached channel.
  pub(super) async fn record_peer_failure(&self, peer: &str) {
    if let Err(error) = self.node.peers().mark_attempt(peer, false) {
      tracing::warn!(%peer, %error, "failed to record failed peer attempt");
    }
    self.pool.remove(peer).await;
  }
}
