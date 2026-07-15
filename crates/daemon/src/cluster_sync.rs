use std::{
  collections::{HashMap, HashSet},
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use lycoris_client::{ClientError, PeerClient};
use lycoris_core::time::now_ms;
use lycoris_proto::node::{
  LeaveMessage as ProtoLeave, NodeInfo as ProtoNodeInfo, ProbeRequest, ProbeResponse,
  PushNodeRequest, PushNodeResponse, StateMessage, SuspectMessage as ProtoSuspect,
  SyncNodesRequest, SyncNodesResponse,
  membership_server::{Membership, MembershipServer},
  sync_server::{Sync, SyncServer},
};
use lycoris_storage::NodeDomain;
use lycoris_tls::TlsBundle;
use tokio::{
  sync::Mutex,
  time::{self, MissedTickBehavior, timeout},
};
use tonic::{Request, Response, Status, transport::ClientTlsConfig};

use crate::membership::{
  MembershipService, SwimAction, SwimMessage, merkle::Hash as MerkleHash, register_to_proto,
};

/// Orchestrates peer-to-peer membership synchronization.
///
/// `ClusterSync` combines backward-compatible `Sync` RPCs, the new `Membership`
/// RPCs (Merkle anti-entropy, SWIM probes), and a background loop that drives
/// the SWIM failure detector.
#[derive(Debug, Clone)]
pub struct ClusterSync {
  local_node_id: String,
  service: Arc<MembershipService>,
  storage: NodeDomain,
  tls: ClientTlsConfig,
  clients: Arc<Mutex<HashMap<String, PeerClient>>>,
  seen_pushes: Arc<Mutex<HashSet<(String, u64)>>>,
  seen_states: Arc<Mutex<HashSet<(String, u64, u8)>>>,
  sequence: Arc<AtomicU64>,
}

const RPC_TIMEOUT: Duration = Duration::from_secs(3);

impl ClusterSync {
  pub fn new(
    local_node_id: String, service: Arc<MembershipService>, storage: NodeDomain,
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
      seen_states: Arc::new(Mutex::new(HashSet::new())),
      sequence: Arc::new(AtomicU64::new(1)),
    }
  }

  /// Start background anti-entropy sync and SWIM failure detection.
  pub async fn run(&self, interval: Duration) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
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
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
      ticker.tick().await;
      let actions = self.service.tick().await;
      let sync = self.clone();
      tokio::spawn(async move {
        sync.dispatch(actions).await;
      });
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
            .seen_states
            .lock()
            .await
            .insert((node_id.clone(), incarnation, 1));
          self
            .broadcast_state_message(StateMessage {
              payload: Some(lycoris_proto::node::state_message::Payload::Suspect(
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
            .seen_states
            .lock()
            .await
            .insert((node_id.clone(), incarnation, 2));
          self
            .broadcast_state_message(StateMessage {
              payload: Some(lycoris_proto::node::state_message::Payload::Leave(
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

    let mut client = match self.connect_peer(&address).await {
      Ok(client) => client,
      Err(_) => {
        self.remove_client(&address).await;
        return false;
      }
    };

    match timeout(RPC_TIMEOUT, client.membership.probe(seq, "")).await {
      Ok(Ok(response)) => {
        if response.ack {
          self
            .service
            .on_probe(target_id, SwimMessage::Ack { seq })
            .await;
        }
        response.ack
      }
      Ok(Err(error)) => {
        tracing::warn!(%target_id, %error, "probe failed");
        self.remove_client(&address).await;
        false
      }
      Err(_) => {
        tracing::warn!(%target_id, "probe timed out");
        self.remove_client(&address).await;
        false
      }
    }
  }

  async fn resolve_address(&self, node_id: &str) -> Option<String> {
    self.service.member_address(node_id).await
  }

  async fn sync_with_peers(&self) {
    let snapshot = self.service.list_nodes(&HashMap::new()).await;
    let local_address = self.local_address().await;

    if let Some(primary) = self.storage.peers.get_primary().unwrap_or(None) {
      match timeout(RPC_TIMEOUT, self.sync_with_peer(&primary, snapshot.clone())).await {
        Ok(Ok(())) => return,
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

    let fallbacks = self.storage.peers.fallback_addresses().unwrap_or_default();
    let mut join_set = tokio::task::JoinSet::new();
    for peer in fallbacks {
      let sync = self.clone();
      let snapshot = snapshot.clone();
      join_set.spawn(async move {
        let result = timeout(RPC_TIMEOUT, sync.sync_with_peer(&peer, snapshot)).await;
        (peer, result)
      });
    }

    let mut promoted = false;
    while let Some(result) = join_set.join_next().await {
      match result {
        Ok((peer, Ok(Ok(())))) => {
          if !promoted && local_address.as_deref() != Some(peer.as_str()) {
            if let Err(error) = self.storage.peers.set_primary(&peer) {
              tracing::warn!(%peer, %error, "failed to promote fallback to primary");
            }
            promoted = true;
          }
        }
        Ok((peer, Ok(Err(error)))) => {
          tracing::warn!(%peer, %error, "fallback peer sync failed");
          self.remove_client(&peer).await;
        }
        Ok((peer, Err(_))) => {
          tracing::warn!(%peer, "fallback peer sync timed out");
          self.remove_client(&peer).await;
        }
        Err(error) => {
          tracing::warn!(%error, "sync task panicked");
        }
      }
    }
  }

  async fn sync_with_peer(
    &self, peer: &str, _snapshot: Vec<ProtoNodeInfo>,
  ) -> Result<(), ClientError> {
    let mut client = self.connect_peer(peer).await?;
    let local_address = self.local_address().await.unwrap_or_default();

    let (remote_root, remote_leaves) =
      match timeout(RPC_TIMEOUT, client.membership.merkle_root()).await {
        Ok(Ok(root)) => root,
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "merkle root failed, falling back to full sync");
          return timeout(RPC_TIMEOUT, self.full_sync_with_peer(peer))
            .await
            .map_err(|_| ClientError::Timeout)
            .and_then(|result| result);
        }
        Err(_) => {
          tracing::warn!(%peer, "merkle root timed out, falling back to full sync");
          return timeout(RPC_TIMEOUT, self.full_sync_with_peer(peer))
            .await
            .map_err(|_| ClientError::Timeout)
            .and_then(|result| result);
        }
      };

    let local_root = self.service.merkle_root().await;
    if remote_root == local_root.root_hash {
      let now = now_ms();
      let _ = self.storage.peers.mark_seen(peer, now);
      return Ok(());
    }

    let remote_leaves_parsed: Vec<(String, MerkleHash)> = remote_leaves
      .into_iter()
      .filter_map(|leaf| {
        let hash = leaf.hash.try_into().ok()?;
        Some((leaf.node_id, hash))
      })
      .collect();

    let (need_from_remote, need_from_local) = self.service.merkle_diff(&remote_leaves_parsed).await;

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
      if info.address != local_address {
        let _ = self.storage.peers.seed(&info.address);
      }
    }

    let _ = self.storage.peers.mark_seen(peer, now_ms());
    Ok(())
  }

  async fn full_sync_with_peer(&self, peer: &str) -> Result<(), ClientError> {
    let mut client = self.connect_peer(peer).await?;
    let snapshot = self.service.list_nodes(&HashMap::new()).await;
    let response = client.sync.sync_nodes(snapshot).await?;
    let _ = self.service.sync_nodes(response.nodes).await;
    let local_address = self.local_address().await.unwrap_or_default();

    for info in self.service.list_nodes(&HashMap::new()).await {
      if info.address != local_address {
        let _ = self.storage.peers.seed(&info.address);
      }
    }

    let _ = self.storage.peers.mark_seen(peer, now_ms());
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
          let _ = self.storage.peers.mark_seen(&peer, now_ms());
        }
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "push to peer failed");
          let _ = self.storage.peers.mark_attempt(&peer, false);
          self.remove_client(&peer).await;
        }
        Err(_) => {
          tracing::warn!(%peer, "push to peer timed out");
          let _ = self.storage.peers.mark_attempt(&peer, false);
          self.remove_client(&peer).await;
        }
      }
    }
  }

  async fn push_to_peer(
    &self, peer: &str, info: ProtoNodeInfo, origin: String, sequence: u64,
  ) -> Result<(), ClientError> {
    let mut client = self.connect_peer(peer).await?;
    client.sync.push_node(info, origin, sequence).await?;
    Ok(())
  }

  async fn broadcast_state_message(&self, message: StateMessage) {
    let targets = self.current_targets().await;
    for peer in targets {
      let message = message.clone();
      match timeout(RPC_TIMEOUT, self.send_state_message_to_peer(&peer, message)).await {
        Ok(Ok(())) => {
          let _ = self.storage.peers.mark_seen(&peer, now_ms());
        }
        Ok(Err(error)) => {
          tracing::warn!(%peer, %error, "state message to peer failed");
          let _ = self.storage.peers.mark_attempt(&peer, false);
          self.remove_client(&peer).await;
        }
        Err(_) => {
          tracing::warn!(%peer, "state message to peer timed out");
          let _ = self.storage.peers.mark_attempt(&peer, false);
          self.remove_client(&peer).await;
        }
      }
    }
  }

  async fn send_state_message_to_peer(
    &self, peer: &str, message: StateMessage,
  ) -> Result<(), ClientError> {
    let mut client = self.connect_peer(peer).await?;
    client.membership.state(message).await?;
    Ok(())
  }

  async fn current_targets(&self) -> Vec<String> {
    let local_address = self.local_address().await.unwrap_or_default();
    let mut seen = HashSet::new();
    let mut targets = Vec::new();
    if let Ok(Some(primary)) = self.storage.peers.get_primary()
      && primary != local_address
    {
      seen.insert(primary.clone());
      targets.push(primary);
    }
    if let Ok(fallbacks) = self.storage.peers.fallback_addresses() {
      for peer in fallbacks {
        if peer != local_address && seen.insert(peer.clone()) {
          targets.push(peer);
        }
      }
    }
    targets
  }

  async fn local_address(&self) -> Option<String> {
    self.service.member_address(&self.local_node_id).await
  }

  async fn connect_peer(&self, address: &str) -> Result<PeerClient, ClientError> {
    {
      let clients = self.clients.lock().await;
      if let Some(client) = clients.get(address) {
        return Ok(client.clone());
      }
    }

    let connect = PeerClient::connect(address, self.tls.clone());
    let client = match timeout(Duration::from_secs(3), connect).await {
      Ok(Ok(client)) => client,
      Ok(Err(error)) => return Err(error),
      Err(_) => return Err(ClientError::Timeout),
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
    let local_address = self.local_address().await.unwrap_or_default();
    for info in &nodes {
      if info.address != local_address {
        let _ = self.storage.peers.seed(&info.address);
      }
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

    let local_address = self.local_address().await.unwrap_or_default();
    if info.address != local_address {
      let _ = self.storage.peers.seed(&info.address);
    }
    let actions = self.service.register(&info).await;

    let origin = push.origin_node_id;
    let sequence = push.sequence;
    let sync = self.clone();
    tokio::spawn(async move {
      sync.dispatch(actions).await;
      sync.broadcast_push(info, origin, sequence).await;
    });

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
      let ack = actions
        .iter()
        .any(|action| matches!(action, SwimAction::SendAck { .. }));
      let sync = self.clone();
      tokio::spawn(async move {
        sync.dispatch_non_acks(actions).await;
      });
      ack
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

  /// Handle an incoming state message.
  #[allow(clippy::result_large_err)]
  pub async fn handle_state_message(
    &self, request: Request<StateMessage>,
  ) -> Result<Response<lycoris_proto::node::StateResponse>, Status> {
    let from = self.local_node_id.clone();
    let message = request.into_inner();

    // Deduplicate gossiped Suspect/Leave/Alive state messages to prevent them
    // from cycling around the graph forever.
    let state_key = match &message.payload {
      Some(lycoris_proto::node::state_message::Payload::Alive(info)) => {
        Some((info.id.clone(), info.incarnation, 0u8))
      }
      Some(lycoris_proto::node::state_message::Payload::Suspect(suspect)) => {
        Some((suspect.node_id.clone(), suspect.incarnation, 1u8))
      }
      Some(lycoris_proto::node::state_message::Payload::Leave(leave)) => {
        Some((leave.node_id.clone(), leave.incarnation, 2u8))
      }
      None => None,
    };

    if let Some(key) = state_key {
      let already_seen = !self.seen_states.lock().await.insert(key);
      if already_seen {
        return Ok(Response::new(lycoris_proto::node::StateResponse {
          accepted: true,
        }));
      }
    }

    let actions = match message.payload {
      Some(lycoris_proto::node::state_message::Payload::Alive(info)) => {
        self.service.register(&info).await
      }
      Some(lycoris_proto::node::state_message::Payload::Suspect(suspect)) => {
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
      Some(lycoris_proto::node::state_message::Payload::Leave(leave)) => {
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
    let sync = self.clone();
    tokio::spawn(async move {
      sync.dispatch(actions).await;
    });
    Ok(Response::new(lycoris_proto::node::StateResponse {
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

pub type SyncServerHandle = SyncServer<ClusterSync>;
pub type MembershipServerHandle = MembershipServer<ClusterSync>;

impl ClusterSync {
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
impl Sync for ClusterSync {
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
impl Membership for ClusterSync {
  async fn merkle_root(
    &self, _request: Request<lycoris_proto::node::MerkleRootRequest>,
  ) -> Result<Response<lycoris_proto::node::MerkleRootResponse>, Status> {
    let root = self.service.merkle_root().await;
    Ok(Response::new(lycoris_proto::node::MerkleRootResponse {
      root_hash: root.root_hash,
      leaf_hashes: root
        .leaf_hashes
        .into_iter()
        .map(|(node_id, hash)| lycoris_proto::node::LeafHash { node_id, hash })
        .collect(),
    }))
  }

  async fn fetch_registers(
    &self, request: Request<lycoris_proto::node::FetchRegistersRequest>,
  ) -> Result<Response<lycoris_proto::node::FetchRegistersResponse>, Status> {
    let inner = request.into_inner();
    let node_ids: Vec<&str> = inner.node_ids.iter().map(String::as_str).collect();
    let registers = self.service.fetch_registers(&node_ids).await;
    Ok(Response::new(lycoris_proto::node::FetchRegistersResponse {
      registers,
    }))
  }

  async fn push_registers(
    &self, request: Request<lycoris_proto::node::PushRegistersRequest>,
  ) -> Result<Response<lycoris_proto::node::FetchRegistersResponse>, Status> {
    let registers = request.into_inner().registers;
    let _ = self.service.sync_nodes(registers).await;
    Ok(Response::new(lycoris_proto::node::FetchRegistersResponse {
      registers: Vec::new(),
    }))
  }

  async fn probe(&self, request: Request<ProbeRequest>) -> Result<Response<ProbeResponse>, Status> {
    self.handle_probe(request).await
  }

  async fn state(
    &self, request: Request<StateMessage>,
  ) -> Result<Response<lycoris_proto::node::StateResponse>, Status> {
    self.handle_state_message(request).await
  }
}
