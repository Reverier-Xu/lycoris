use std::{
  collections::HashSet,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use lycoris_api::{
  ClusterClientError, PeerClient,
  proto::{
    NodeInfo as ProtoNodeInfo, PushNodeRequest, PushNodeResponse, SyncNodesRequest,
    SyncNodesResponse,
    sync_server::{Sync, SyncServer},
  },
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::ClientTlsConfig};

use crate::{node::registry::NodeRegistry, storage::Storage, tls::TlsBundle};

/// Orchestrates peer-to-peer registry synchronization.
#[derive(Debug, Clone)]
pub struct Gossip {
  local_node_id: String,
  registry: NodeRegistry,
  storage: Storage,
  tls: ClientTlsConfig,
  clients: Arc<Mutex<std::collections::HashMap<String, PeerClient>>>,
  seen_pushes: Arc<Mutex<HashSet<(String, u64)>>>,
  sequence: Arc<AtomicU64>,
}

impl Gossip {
  pub fn new(
    local_node_id: String, registry: NodeRegistry, storage: Storage, tls_bundle: &TlsBundle,
  ) -> Self {
    let tls = ClientTlsConfig::new()
      .identity(tls_bundle.identity.clone())
      .ca_certificate(tls_bundle.ca.clone());

    Self {
      local_node_id,
      registry,
      storage,
      tls,
      clients: Arc::new(Mutex::new(std::collections::HashMap::new())),
      seen_pushes: Arc::new(Mutex::new(HashSet::new())),
      sequence: Arc::new(AtomicU64::new(1)),
    }
  }

  /// Start background anti-entropy sync with the primary endpoint and
  /// fallbacks. Runs both on a timer and whenever local storage changes.
  pub async fn run(&self, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    let notify = self.storage.change_notify();
    loop {
      tokio::select! {
        _ = ticker.tick() => {}
        _ = notify.notified() => {}
      }
      self.sync_with_peers().await;
    }
  }

  /// Notify peers about a local registry change via push.
  pub async fn push_change(&self, info: ProtoNodeInfo) {
    let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
    let origin = self.local_node_id.clone();

    // Record locally so we don't re-forward our own push if it comes back.
    self
      .seen_pushes
      .lock()
      .await
      .insert((origin.clone(), sequence));

    self.broadcast_push(info, origin, sequence).await;
  }

  async fn sync_with_peers(&self) {
    let snapshot = self.registry.snapshot();

    if let Some(primary) = self.storage.get_primary().unwrap_or(None) {
      match self.sync_with_peer(&primary, snapshot.clone()).await {
        Ok(()) => return,
        Err(error) => {
          tracing::warn!(%primary, %error, "primary endpoint unreachable, trying fallbacks");
        }
      }
    }

    let fallbacks = self.storage.fallback_peers().unwrap_or_default();
    for peer in fallbacks {
      if self.sync_with_peer(&peer, snapshot.clone()).await.is_ok() {
        if let Err(error) = self.storage.set_primary(&peer) {
          tracing::warn!(%peer, %error, "failed to promote fallback to primary");
        }
        return;
      }
    }
  }

  async fn sync_with_peer(
    &self, peer: &str, snapshot: Vec<ProtoNodeInfo>,
  ) -> Result<(), ClusterClientError> {
    let client = self.connect_peer(peer).await?;
    let response = client.sync.sync_nodes(snapshot).await?;
    self.registry.merge(response.nodes);

    for info in self.registry.snapshot() {
      let _ = self.storage.seed_peer(&info.address);
    }

    let now = now_ms();
    if let Err(error) = self.storage.mark_peer_seen(peer, now) {
      tracing::warn!(%peer, %error, "failed to record peer seen state");
    }
    Ok(())
  }

  async fn broadcast_push(&self, info: ProtoNodeInfo, origin: String, sequence: u64) {
    let targets = self.current_targets().await;
    for peer in targets {
      let info = info.clone();
      let origin = origin.clone();
      match self.connect_peer(&peer).await {
        Ok(client) => {
          if let Err(error) = client.sync.push_node(info, origin, sequence).await {
            tracing::warn!(%peer, %error, "push to peer failed");
            let _ = self.storage.mark_peer_attempt(&peer, false);
          } else {
            let _ = self.storage.mark_peer_seen(&peer, now_ms());
          }
        }
        Err(error) => {
          tracing::warn!(%peer, %error, "failed to connect to peer for push");
          let _ = self.storage.mark_peer_attempt(&peer, false);
        }
      }
    }
  }

  async fn current_targets(&self) -> Vec<String> {
    let mut targets = Vec::new();
    if let Ok(Some(primary)) = self.storage.get_primary() {
      targets.push(primary);
    }
    if let Ok(fallbacks) = self.storage.fallback_peers() {
      for peer in fallbacks {
        if !targets.contains(&peer) {
          targets.push(peer);
        }
      }
    }
    targets
  }

  async fn connect_peer(&self, address: &str) -> Result<PeerClient, ClusterClientError> {
    let mut clients = self.clients.lock().await;
    if let Some(client) = clients.get(address) {
      return Ok(client.clone());
    }
    let client = PeerClient::connect(address, self.tls.clone()).await?;
    clients.insert(address.to_string(), client.clone());
    Ok(client)
  }

  /// Handle an incoming `SyncNodes` RPC: merge remote state and return our
  /// snapshot.
  #[allow(clippy::result_large_err)]
  pub async fn handle_sync_nodes(
    &self, request: Request<SyncNodesRequest>,
  ) -> Result<Response<SyncNodesResponse>, Status> {
    let nodes = request.into_inner().nodes;
    for info in &nodes {
      let _ = self.storage.seed_peer(&info.address);
    }
    self.registry.merge(nodes);
    Ok(Response::new(SyncNodesResponse {
      nodes: self.registry.snapshot(),
    }))
  }

  /// Handle an incoming `PushNode` RPC: deduplicate, merge, and forward.
  #[allow(clippy::result_large_err)]
  pub async fn handle_push_node(
    &self, request: Request<PushNodeRequest>,
  ) -> Result<Response<PushNodeResponse>, Status> {
    let push = request.into_inner();
    let info = push
      .info
      .ok_or_else(|| Status::invalid_argument("missing node info"))?;
    let key = (push.origin_node_id.clone(), push.sequence);

    let already_seen = self.seen_pushes.lock().await.insert(key);
    if !already_seen {
      return Ok(Response::new(PushNodeResponse { accepted: true }));
    }

    let _ = self.storage.seed_peer(&info.address);
    self.registry.merge(vec![info.clone()]);

    // Forward to all peers (deduplication will prevent loops).
    self
      .broadcast_push(info, push.origin_node_id, push.sequence)
      .await;

    Ok(Response::new(PushNodeResponse { accepted: true }))
  }
}

pub type SyncServerHandle = SyncServer<Gossip>;

impl Gossip {
  pub fn into_sync_server(self) -> SyncServerHandle {
    SyncServer::new(self)
  }
}

#[tonic::async_trait]
#[allow(clippy::result_large_err)]
impl Sync for Gossip {
  async fn sync_nodes(
    &self, request: Request<SyncNodesRequest>,
  ) -> Result<Response<SyncNodesResponse>, Status> {
    self.handle_sync_nodes(request).await
  }

  async fn push_node(
    &self, request: Request<PushNodeRequest>,
  ) -> Result<Response<PushNodeResponse>, Status> {
    self.handle_push_node(request).await
  }
}

fn now_ms() -> i64 {
  use std::time::{SystemTime, UNIX_EPOCH};
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(0))
    .unwrap_or(0)
}
