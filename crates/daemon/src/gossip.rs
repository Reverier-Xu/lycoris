use std::{
  collections::{HashMap, HashSet},
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use lycoris_api::{
  ClusterClientError, PeerClient,
  proto::{
    GossipMessage, LeaveMessage as ProtoLeave, NodeInfo as ProtoNodeInfo, ProbeRequest,
    ProbeResponse, PushNodeRequest, PushNodeResponse, SuspectMessage as ProtoSuspect,
    SyncNodesRequest, SyncNodesResponse,
    membership_server::{Membership, MembershipServer},
    sync_server::{Sync, SyncServer},
  },
};
use lycoris_config::time::now_ms;
use lycoris_storage::ClusterStorage;
use tokio::{sync::Mutex, time::timeout};
use tonic::{Request, Response, Status, transport::ClientTlsConfig};

use crate::{
  membership::{MembershipService, SwimAction, SwimMessage, merkle::Hash as MerkleHash},
  tls::TlsBundle,
};

/// Orchestrates peer-to-peer membership synchronization.
///
/// `Gossip` combines backward-compatible `Sync` RPCs, the new `Membership`
/// RPCs (Merkle anti-entropy, SWIM probes), and a background loop that drives
/// the SWIM failure detector.
#[derive(Debug, Clone)]
pub struct Gossip {
  local_node_id: String,
  service: Arc<MembershipService>,
  storage: ClusterStorage,
  tls: ClientTlsConfig,
  clients: Arc<Mutex<HashMap<String, PeerClient>>>,
  seen_pushes: Arc<Mutex<HashSet<(String, u64)>>>,
  sequence: Arc<AtomicU64>,
}

const RPC_TIMEOUT: Duration = Duration::from_secs(5);

impl Gossip {
  pub fn new(
    local_node_id: String, service: Arc<MembershipService>, storage: ClusterStorage,
    tls_bundle: &TlsBundle,
  ) -> Self {
    let tls = ClientTlsConfig::new()
      .identity(tls_bundle.identity.clone())
      .ca_certificate(tls_bundle.ca.clone());

    Self {
      local_node_id,
      service,
      storage,
      tls,
      clients: Arc::new(Mutex::new(HashMap::new())),
      seen_pushes: Arc::new(Mutex::new(HashSet::new())),
      sequence: Arc::new(AtomicU64::new(1)),
    }
  }

  /// Start background anti-entropy sync and SWIM failure detection.
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

  pub async fn run_swim(&self, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    loop {
      ticker.tick().await;
      let actions = self.service.tick().await;
      self.dispatch(actions).await;
    }
  }

  /// Notify peers about a local registry change via push.
  pub async fn push_change(&self, info: ProtoNodeInfo) {
    let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
    let origin = self.local_node_id.clone();

    self
      .seen_pushes
      .lock()
      .await
      .insert((origin.clone(), sequence));

    self.broadcast_push(info, origin, sequence).await;
  }

  /// Dispatch a batch of SWIM actions produced by the membership service.
  pub async fn dispatch(&self, actions: Vec<SwimAction>) {
    for action in actions {
      match action {
        SwimAction::SendPing { target, seq } => {
          let _ = self.send_probe_to(&target, seq).await;
        }
        SwimAction::SendPingReq { proxy, target, seq } => {
          self.send_ping_req(&proxy, &target, seq).await;
        }
        SwimAction::SendAck { .. } => {
          // Acks are returned inline in response to a Probe RPC.
        }
        SwimAction::Broadcast(SwimMessage::Alive { register }) => {
          let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
          let origin = self.local_node_id.clone();
          self
            .seen_pushes
            .lock()
            .await
            .insert((origin.clone(), sequence));
          self
            .broadcast_push(register_to_proto(&register), origin, sequence)
            .await;
        }
        SwimAction::Broadcast(SwimMessage::Suspect {
          node_id,
          incarnation,
        }) => {
          self
            .broadcast_gossip(GossipMessage {
              payload: Some(lycoris_api::proto::gossip_message::Payload::Suspect(
                ProtoSuspect {
                  node_id,
                  incarnation,
                },
              )),
            })
            .await;
        }
        SwimAction::Broadcast(SwimMessage::Leave {
          node_id,
          incarnation,
        }) => {
          self
            .broadcast_gossip(GossipMessage {
              payload: Some(lycoris_api::proto::gossip_message::Payload::Leave(
                ProtoLeave {
                  node_id,
                  incarnation,
                },
              )),
            })
            .await;
        }
        SwimAction::Broadcast(_) => {}
      }
    }
  }

  async fn send_probe_to(&self, target_id: &str, seq: u64) -> bool {
    let address = match self.resolve_address(target_id).await {
      Some(addr) => addr,
      None => return false,
    };

    match self.connect_peer(&address).await {
      Ok(client) => match client.membership.probe(seq, "").await {
        Ok(response) => {
          if response.ack {
            self
              .service
              .on_probe(target_id, SwimMessage::Ack { seq })
              .await;
          }
          response.ack
        }
        Err(error) => {
          tracing::warn!(%target_id, %error, "probe failed");
          false
        }
      },
      Err(error) => {
        tracing::warn!(%target_id, %error, "failed to connect for probe");
        false
      }
    }
  }

  async fn send_ping_req(&self, proxy: &str, target: &str, seq: u64) {
    let address = match self.resolve_address(proxy).await {
      Some(addr) => addr,
      None => return,
    };

    match self.connect_peer(&address).await {
      Ok(client) => {
        if let Err(error) = client.membership.probe(seq, target).await {
          tracing::warn!(%proxy, %target, %error, "indirect probe failed");
        }
      }
      Err(error) => {
        tracing::warn!(%proxy, %target, %error, "failed to connect for indirect probe");
      }
    }
  }

  async fn resolve_address(&self, node_id: &str) -> Option<String> {
    self.service.member_address(node_id).await
  }

  async fn sync_with_peers(&self) {
    let snapshot = self.service.list_nodes(&HashMap::new()).await;
    let mut primary_set = false;

    if let Some(primary) = self.storage.get_primary().unwrap_or(None) {
      match timeout(RPC_TIMEOUT, self.sync_with_peer(&primary, snapshot.clone())).await {
        Ok(Ok(())) => {
          primary_set = true;
        }
        Ok(Err(error)) => {
          tracing::warn!(%primary, %error, "primary endpoint unreachable, trying fallbacks");
          self.remove_client(&primary).await;
        }
        Err(_) => {
          tracing::warn!(%primary, "primary endpoint timed out, trying fallbacks");
          self.remove_client(&primary).await;
        }
      }
    }

    let fallbacks = self.storage.fallback_peers().unwrap_or_default();
    for peer in fallbacks {
      let result = timeout(RPC_TIMEOUT, self.sync_with_peer(&peer, snapshot.clone())).await;
      match result {
        Ok(Ok(())) => {
          if !primary_set {
            if let Err(error) = self.storage.set_primary(&peer) {
              tracing::warn!(%peer, %error, "failed to promote fallback to primary");
            }
            primary_set = true;
          }
        }
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "fallback peer sync failed");
          self.remove_client(&peer).await;
        }
        Err(_) => {
          tracing::warn!(%peer, "fallback peer sync timed out");
          self.remove_client(&peer).await;
        }
      }
    }
  }

  async fn sync_with_peer(
    &self, peer: &str, _snapshot: Vec<ProtoNodeInfo>,
  ) -> Result<(), ClusterClientError> {
    let client = self.connect_peer(peer).await?;

    let (remote_root, remote_leaves) =
      match timeout(RPC_TIMEOUT, client.membership.merkle_root()).await {
        Ok(Ok(root)) => root,
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "merkle root failed, falling back to full sync");
          return timeout(RPC_TIMEOUT, self.full_sync_with_peer(peer))
            .await
            .map_err(|_| ClusterClientError::Timeout)
            .and_then(|result| result);
        }
        Err(_) => {
          tracing::warn!(%peer, "merkle root timed out, falling back to full sync");
          return timeout(RPC_TIMEOUT, self.full_sync_with_peer(peer))
            .await
            .map_err(|_| ClusterClientError::Timeout)
            .and_then(|result| result);
        }
      };

    let local_root = self.service.merkle_root().await;
    if remote_root == local_root.root_hash {
      let now = now_ms();
      let _ = self.storage.mark_peer_seen(peer, now);
      return Ok(());
    }

    let remote_leaves_parsed: Vec<(String, MerkleHash)> = remote_leaves
      .into_iter()
      .filter_map(|leaf| {
        let hash = leaf.hash.try_into().ok()?;
        Some((leaf.node_id, hash))
      })
      .collect();

    let local_leaves_parsed: Vec<(String, MerkleHash)> = local_root
      .leaf_hashes
      .into_iter()
      .filter_map(|(id, hash)| {
        let hash = hash.try_into().ok()?;
        Some((id, hash))
      })
      .collect();

    let (need_from_remote, need_from_local) =
      merkle_diff(&local_leaves_parsed, &remote_leaves_parsed);

    let fetched = if need_from_remote.is_empty() {
      Vec::new()
    } else {
      client.membership.fetch_registers(need_from_remote).await?
    };

    let local_registers = self
      .service
      .fetch_registers(
        &need_from_local
          .iter()
          .map(String::as_str)
          .collect::<Vec<_>>(),
      )
      .await;

    if !local_registers.is_empty()
      && let Err(error) = client.membership.push_registers(local_registers).await
    {
      tracing::warn!(%peer, %error, "failed to push local registers");
    }

    if !fetched.is_empty() {
      let _ = self.service.sync_nodes(fetched).await;
    }

    for info in self.service.list_nodes(&HashMap::new()).await {
      let _ = self.storage.seed_peer(&info.address);
    }

    let now = now_ms();
    if let Err(error) = self.storage.mark_peer_seen(peer, now) {
      tracing::warn!(%peer, %error, "failed to record peer seen state");
    }
    Ok(())
  }

  async fn full_sync_with_peer(&self, peer: &str) -> Result<(), ClusterClientError> {
    let client = self.connect_peer(peer).await?;
    let snapshot = self.service.list_nodes(&HashMap::new()).await;
    let response = client.sync.sync_nodes(snapshot).await?;
    let _ = self.service.sync_nodes(response.nodes).await;

    for info in self.service.list_nodes(&HashMap::new()).await {
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
      match timeout(
        RPC_TIMEOUT,
        self.push_to_peer(&peer, info, origin, sequence),
      )
      .await
      {
        Ok(Ok(())) => {
          let _ = self.storage.mark_peer_seen(&peer, now_ms());
        }
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "push to peer failed");
          let _ = self.storage.mark_peer_attempt(&peer, false);
          self.remove_client(&peer).await;
        }
        Err(_) => {
          tracing::warn!(%peer, "push to peer timed out");
          let _ = self.storage.mark_peer_attempt(&peer, false);
          self.remove_client(&peer).await;
        }
      }
    }
  }

  async fn push_to_peer(
    &self, peer: &str, info: ProtoNodeInfo, origin: String, sequence: u64,
  ) -> Result<(), ClusterClientError> {
    let client = self.connect_peer(peer).await?;
    client.sync.push_node(info, origin, sequence).await?;
    Ok(())
  }

  async fn broadcast_gossip(&self, message: GossipMessage) {
    let targets = self.current_targets().await;
    for peer in targets {
      let message = message.clone();
      match timeout(RPC_TIMEOUT, self.gossip_to_peer(&peer, message)).await {
        Ok(Ok(())) => {
          let _ = self.storage.mark_peer_seen(&peer, now_ms());
        }
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "gossip to peer failed");
          let _ = self.storage.mark_peer_attempt(&peer, false);
          self.remove_client(&peer).await;
        }
        Err(_) => {
          tracing::warn!(%peer, "gossip to peer timed out");
          let _ = self.storage.mark_peer_attempt(&peer, false);
          self.remove_client(&peer).await;
        }
      }
    }
  }

  async fn gossip_to_peer(
    &self, peer: &str, message: GossipMessage,
  ) -> Result<(), ClusterClientError> {
    let client = self.connect_peer(peer).await?;
    client.membership.gossip(message).await?;
    Ok(())
  }

  async fn current_targets(&self) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut targets = Vec::new();
    if let Ok(Some(primary)) = self.storage.get_primary() {
      seen.insert(primary.clone());
      targets.push(primary);
    }
    if let Ok(fallbacks) = self.storage.fallback_peers() {
      for peer in fallbacks {
        if seen.insert(peer.clone()) {
          targets.push(peer);
        }
      }
    }
    targets
  }

  async fn connect_peer(&self, address: &str) -> Result<PeerClient, ClusterClientError> {
    {
      let clients = self.clients.lock().await;
      if let Some(client) = clients.get(address) {
        return Ok(client.clone());
      }
    }

    let connect = PeerClient::connect(address, self.tls.clone());
    let client = match timeout(Duration::from_secs(3), connect).await {
      Ok(Ok(client)) => client,
      Ok(Err(error)) => {
        tracing::warn!(%address, %error, "peer connect failed");
        return Err(error);
      }
      Err(_) => {
        tracing::warn!(%address, "peer connect timed out");
        return Err(ClusterClientError::Timeout);
      }
    };

    let mut clients = self.clients.lock().await;
    clients.insert(address.to_string(), client.clone());
    Ok(client)
  }

  async fn remove_client(&self, address: &str) {
    let mut clients = self.clients.lock().await;
    clients.remove(address);
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
    let snapshot = self.service.sync_nodes(nodes).await;
    Ok(Response::new(SyncNodesResponse { nodes: snapshot }))
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
    let actions = self.service.register(&info).await;
    self.dispatch(actions).await;

    self
      .broadcast_push(info, push.origin_node_id, push.sequence)
      .await;

    Ok(Response::new(PushNodeResponse { accepted: true }))
  }

  /// Handle an incoming SWIM probe.
  #[allow(clippy::result_large_err)]
  pub async fn handle_probe(
    &self, request: Request<ProbeRequest>,
  ) -> Result<Response<ProbeResponse>, Status> {
    let probe = request.into_inner();
    let from = self.local_node_id.clone();

    let ack = if probe.target.is_empty() {
      let actions = self
        .service
        .on_probe(&from, SwimMessage::Ping { seq: probe.seq })
        .await;
      self.dispatch_non_acks(actions.clone()).await;
      actions
        .iter()
        .any(|action| matches!(action, SwimAction::SendAck { .. }))
    } else if probe.target == self.local_node_id {
      true
    } else {
      self.send_probe_to(&probe.target, probe.seq).await
    };

    Ok(Response::new(ProbeResponse {
      ack,
      seq: probe.seq,
    }))
  }

  /// Handle an incoming gossip message.
  #[allow(clippy::result_large_err)]
  pub async fn handle_gossip(
    &self, request: Request<GossipMessage>,
  ) -> Result<Response<lycoris_api::proto::GossipResponse>, Status> {
    let from = self.local_node_id.clone();
    let message = request.into_inner();
    let actions = match message.payload {
      Some(lycoris_api::proto::gossip_message::Payload::Alive(info)) => {
        self.service.register(&info).await
      }
      Some(lycoris_api::proto::gossip_message::Payload::Suspect(suspect)) => {
        self
          .service
          .on_probe(
            &from,
            SwimMessage::Suspect {
              node_id: suspect.node_id,
              incarnation: suspect.incarnation,
            },
          )
          .await
      }
      Some(lycoris_api::proto::gossip_message::Payload::Leave(leave)) => {
        self
          .service
          .on_probe(
            &from,
            SwimMessage::Leave {
              node_id: leave.node_id,
              incarnation: leave.incarnation,
            },
          )
          .await
      }
      None => Vec::new(),
    };
    self.dispatch(actions).await;
    Ok(Response::new(lycoris_api::proto::GossipResponse {
      accepted: true,
    }))
  }

  async fn dispatch_non_acks(&self, actions: Vec<SwimAction>) {
    let non_acks: Vec<SwimAction> = actions
      .into_iter()
      .filter(|action| !matches!(action, SwimAction::SendAck { .. }))
      .collect();
    self.dispatch(non_acks).await;
  }
}

pub type SyncServerHandle = SyncServer<Gossip>;
pub type MembershipServerHandle = MembershipServer<Gossip>;

impl Gossip {
  pub fn into_sync_server(self) -> SyncServerHandle {
    SyncServer::new(self.clone())
  }

  pub fn into_membership_server(self) -> MembershipServerHandle {
    MembershipServer::new(self)
  }

  pub fn into_servers(self) -> (SyncServerHandle, MembershipServerHandle) {
    (SyncServer::new(self.clone()), MembershipServer::new(self))
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

#[tonic::async_trait]
#[allow(clippy::result_large_err)]
impl Membership for Gossip {
  async fn merkle_root(
    &self, _request: Request<lycoris_api::proto::MerkleRootRequest>,
  ) -> Result<Response<lycoris_api::proto::MerkleRootResponse>, Status> {
    let root = self.service.merkle_root().await;
    Ok(Response::new(lycoris_api::proto::MerkleRootResponse {
      root_hash: root.root_hash,
      leaf_hashes: root
        .leaf_hashes
        .into_iter()
        .map(|(node_id, hash)| lycoris_api::proto::LeafHash { node_id, hash })
        .collect(),
    }))
  }

  async fn fetch_registers(
    &self, request: Request<lycoris_api::proto::FetchRegistersRequest>,
  ) -> Result<Response<lycoris_api::proto::FetchRegistersResponse>, Status> {
    let inner = request.into_inner();
    let node_ids: Vec<&str> = inner.node_ids.iter().map(String::as_str).collect();
    let registers = self.service.fetch_registers(&node_ids).await;
    Ok(Response::new(lycoris_api::proto::FetchRegistersResponse {
      registers,
    }))
  }

  async fn push_registers(
    &self, request: Request<lycoris_api::proto::PushRegistersRequest>,
  ) -> Result<Response<lycoris_api::proto::FetchRegistersResponse>, Status> {
    let registers = request.into_inner().registers;
    let _ = self.service.sync_nodes(registers).await;
    Ok(Response::new(lycoris_api::proto::FetchRegistersResponse {
      registers: Vec::new(),
    }))
  }

  async fn probe(&self, request: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
    self.handle_probe(request).await
  }

  async fn gossip(
    &self, request: Request<GossipMessage>,
  ) -> Result<Response<lycoris_api::proto::GossipResponse>, Status> {
    self.handle_gossip(request).await
  }
}

fn merkle_diff(
  local: &[(String, MerkleHash)], remote: &[(String, MerkleHash)],
) -> (Vec<String>, Vec<String>) {
  let mut need_from_remote = Vec::new();
  let mut need_from_local = Vec::new();
  let mut i = 0usize;
  let mut j = 0usize;

  while i < local.len() && j < remote.len() {
    match local[i].0.cmp(&remote[j].0) {
      std::cmp::Ordering::Less => {
        need_from_local.push(local[i].0.clone());
        i += 1;
      }
      std::cmp::Ordering::Greater => {
        need_from_remote.push(remote[j].0.clone());
        j += 1;
      }
      std::cmp::Ordering::Equal => {
        if local[i].1 != remote[j].1 {
          need_from_remote.push(local[i].0.clone());
          need_from_local.push(local[i].0.clone());
        }
        i += 1;
        j += 1;
      }
    }
  }

  while i < local.len() {
    need_from_local.push(local[i].0.clone());
    i += 1;
  }
  while j < remote.len() {
    need_from_remote.push(remote[j].0.clone());
    j += 1;
  }

  (need_from_remote, need_from_local)
}

fn register_to_proto(register: &crate::membership::MemberRegister) -> ProtoNodeInfo {
  ProtoNodeInfo {
    id: register.node_id.clone(),
    address: register.address.clone(),
    labels: register.labels.clone(),
    annotations: register.annotations.clone(),
    last_heartbeat_unix_ms: register.updated_at_ms,
    state: match register.state {
      crate::membership::MemberState::Active => "active".to_string(),
      crate::membership::MemberState::Suspected => "suspected".to_string(),
      crate::membership::MemberState::Leaving => "leaving".to_string(),
      crate::membership::MemberState::Offline => "offline".to_string(),
    },
    incarnation: register.incarnation,
    heartbeat: register.heartbeat,
  }
}
