//! Daemon-side bootstrap of the libp2p overlay: node identity, authorization
//! registry, admission, registry exchange, and protocol dispatch.
//!
//! Membership, shared resources, and extension forwarding now ride this
//! module's `LinkHandle`. This module owns who the node is (`NodeIdentity`),
//! which cluster it belongs to (the persisted `AuthorizationRegistry`), and how
//! new nodes enroll (the bounded admission protocol plus checkpoint gossip).

use std::{
  path::Path,
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

use lycoris_core::ClusterKey;
use lycoris_overlay::{
  AdmissionCandidate, AdmissionError, AdmissionRequest, AdmissionResponse, AuthorizationRecord,
  AuthorizationRegistry, ClusterId, Enrollment, Envelope, EnvelopeHeader, IdentityError, JoinProof,
  LinkConfig, LinkError, LinkHandle, LinkRuntime, MessageKind, Multiaddr, NodeId, NodeIdentity,
  PROTOCOL_VERSION, ProtocolId,
};
use lycoris_proto::node::{
  ExtensionForwardResponse, ExtensionInvokeRequest, NodeRequest, NodeResponse,
  SyncResourcesRequest, SyncResourcesResponse,
};
use lycoris_storage::{AuthorizationStorage, NodeDomain};
use prost::Message as _;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use crate::overlay_transport::OverlayPool;

const REGISTRY_REQUEST_TIMEOUT_MS: i64 = 5_000;
const REGISTRY_REQUEST_HOPS: u8 = 0;
const JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
enum RegistryResponse {
  Applied(Vec<AuthorizationRecord>),
  Rejected(String),
}

/// Handler for inbound membership-plane requests arriving over the overlay.
/// Installed after `ClusterSync` is built (the sync layer implements it).
#[async_trait::async_trait]
pub(crate) trait NodeRequestHandler: Send + Sync {
  async fn handle(&self, request: NodeRequest) -> NodeResponse;
}

#[async_trait::async_trait]
pub(crate) trait ResourceRequestHandler: Send + Sync {
  async fn handle(&self, request: SyncResourcesRequest) -> SyncResourcesResponse;
}

#[async_trait::async_trait]
pub(crate) trait ExtensionRequestHandler: Send + Sync {
  async fn handle(&self, request: ExtensionInvokeRequest) -> ExtensionForwardResponse;
}

#[async_trait::async_trait]
trait RegistryCommitter: Send + Sync {
  async fn commit_registry(
    &self, store: AuthorizationStorage, registry: AuthorizationRegistry, adopt: bool,
  ) -> Result<(), LinkError>;
}

#[async_trait::async_trait]
impl RegistryCommitter for LinkHandle {
  async fn commit_registry(
    &self, store: AuthorizationStorage, registry: AuthorizationRegistry, adopt: bool,
  ) -> Result<(), LinkError> {
    let records = registry.records();
    let persist = move || store.replace(&records);
    if adopt {
      self.commit_adopt_authorization(registry, persist).await
    } else {
      self.commit_authorization(registry, persist).await
    }
  }
}

async fn commit_enrollment(
  committer: &impl RegistryCommitter, store: AuthorizationStorage, current: &mut Enrollment,
  proposed: Enrollment,
) -> Result<(), OverlayError> {
  if current.registry().records() == proposed.registry().records() {
    *current = proposed;
    return Ok(());
  }
  committer
    .commit_registry(store, proposed.registry().clone(), false)
    .await?;
  *current = proposed;
  Ok(())
}

async fn merge_registry_checkpoint(
  committer: &impl RegistryCommitter, store: AuthorizationStorage, current: &mut Enrollment,
  records: Vec<AuthorizationRecord>,
) -> (RegistryResponse, bool) {
  let mut proposed = current.clone();
  match proposed.merge_checkpoint(records) {
    Ok(changed) if changed > 0 => {
      match commit_enrollment(committer, store, current, proposed).await {
        Ok(()) => (
          RegistryResponse::Applied(current.registry().records()),
          true,
        ),
        Err(error) => {
          tracing::error!(%error, "failed to commit a registry checkpoint");
          (
            RegistryResponse::Rejected("authorization commit failed".to_string()),
            false,
          )
        }
      }
    }
    Ok(_) => (
      RegistryResponse::Applied(current.registry().records()),
      false,
    ),
    Err(error) => (RegistryResponse::Rejected(error.to_string()), false),
  }
}

#[derive(Debug, Error)]
pub enum OverlayError {
  #[error(transparent)]
  Identity(#[from] IdentityError),
  #[error(transparent)]
  Link(#[from] LinkError),
  #[error(transparent)]
  Authorization(#[from] lycoris_overlay::AuthorizationError),
  #[error(transparent)]
  Storage(#[from] lycoris_storage::StorageError),
  #[error("overlay listen address '{0}' is not a valid multiaddr")]
  InvalidListenAddress(String),
  #[error("stored authorization records contain no genesis record")]
  MissingGenesis,
  #[error("persisted authorization exists but node.identity is missing")]
  PersistedIdentityMissing,
  #[error("identity {0} is a successor and cannot initialize a standalone registry")]
  InitialIdentityRequired(NodeId),
  #[error("local identity {0} is not uniquely active in the persisted registry")]
  LocalIdentityNotActive(NodeId),
  #[error("local identity {0} does not match its active authorization record")]
  LocalIdentityMismatch(NodeId),
  #[error(transparent)]
  Postcard(#[from] postcard::Error),
  #[error(transparent)]
  Admission(#[from] AdmissionError),
  #[error("join rejected by sponsor: {0}")]
  JoinRejected(String),
  #[error("join failed: {0}")]
  JoinFailed(String),
  #[error("bootstrap address has no authorized peer identity")]
  UnknownBootstrapPeer,
}

/// The daemon's overlay identity, registry, and messaging handle.
pub struct OverlayNode {
  runtime: Mutex<Option<LinkRuntime>>,
  handle: LinkHandle,
  identity: NodeIdentity,
  enrollment: Arc<Mutex<Enrollment>>,
  node: NodeDomain,
  node_handler: Arc<RwLock<Option<Arc<dyn NodeRequestHandler>>>>,
  resource_handler: Arc<RwLock<Option<Arc<dyn ResourceRequestHandler>>>>,
  extension_handler: Arc<RwLock<Option<Arc<dyn ExtensionRequestHandler>>>>,
}

impl OverlayNode {
  /// Load (or create) the node identity and authorization registry, then
  /// start the overlay link actor on the configured listen addresses.
  pub fn start(
    data_dir: &Path, node: NodeDomain, cluster_key: Option<ClusterKey>, listen: &[String],
  ) -> Result<Self, OverlayError> {
    let (identity, registry) = initialize_identity_and_registry(data_dir, &node)?;
    let addresses = parse_listen_addresses(listen)?;
    let runtime = LinkRuntime::start(&identity, LinkConfig::new(addresses), registry.clone())?;
    let handle = runtime.handle();
    tracing::info!(node_id = %identity.node_id(), peer_id = %identity.peer_id(), "overlay identity ready");
    Ok(Self {
      runtime: Mutex::new(Some(runtime)),
      handle,
      identity,
      enrollment: Arc::new(Mutex::new(Enrollment::new(registry, cluster_key))),
      node,
      node_handler: Arc::new(RwLock::new(None)),
      resource_handler: Arc::new(RwLock::new(None)),
      extension_handler: Arc::new(RwLock::new(None)),
    })
  }

  pub fn node_id(&self) -> NodeId {
    self.identity.node_id()
  }

  pub fn handle(&self) -> LinkHandle {
    self.handle.clone()
  }

  /// Build the membership-plane pool from the current adopted registry.
  pub(crate) async fn membership_pool(&self) -> OverlayPool {
    let cluster_id = self.enrollment.lock().await.registry().cluster_id();
    OverlayPool::for_daemon(
      self.handle.clone(),
      self.node_id(),
      cluster_id,
      self.enrollment.clone(),
    )
  }

  /// Install the membership-plane request handler (called once `ClusterSync`
  /// exists).
  pub(crate) async fn set_node_handler(&self, handler: Arc<dyn NodeRequestHandler>) {
    *self.node_handler.write().await = Some(handler);
  }

  pub(crate) async fn set_resource_handler(&self, handler: Arc<dyn ResourceRequestHandler>) {
    *self.resource_handler.write().await = Some(handler);
  }

  pub(crate) async fn set_extension_handler(&self, handler: Arc<dyn ExtensionRequestHandler>) {
    *self.extension_handler.write().await = Some(handler);
  }

  /// True when the registry contains only this node's genesis record, i.e.
  /// the node has not joined an existing cluster yet.
  pub(crate) async fn registry_is_solo(&self) -> bool {
    let registry = self.enrollment.lock().await.registry().clone();
    registry.records().len() == 1
      && registry
        .records()
        .first()
        .is_some_and(|record| record.node_id() == self.node_id())
  }

  /// Enroll into an existing cluster through a sponsor's bootstrap address.
  /// On success the sponsor's registry checkpoint replaces the local one
  /// (persisted, adopted into the link actor, and adopted into enrollment).
  pub async fn join(&self, address: Multiaddr, join_key: &ClusterKey) -> Result<(), OverlayError> {
    let sponsor_peer_id = self.handle.dial_admission(address).await?;
    self
      .handle
      .wait_quarantined(sponsor_peer_id, JOIN_TIMEOUT)
      .await?;
    let candidate = AdmissionCandidate::new(&self.identity)?;
    let sentinel = NodeId::from_bytes([0; NodeId::BYTE_LENGTH]);
    let sentinel_cluster = ClusterId::from_bytes([0; ClusterId::BYTE_LENGTH]);
    let (begin_source, begin_response) = self
      .admission_call(
        sponsor_peer_id,
        sentinel,
        sentinel_cluster,
        &AdmissionRequest::Begin(candidate.clone()),
      )
      .await?;
    let challenge = match begin_response {
      AdmissionResponse::Challenge(challenge) => challenge,
      AdmissionResponse::Rejected(reason) => return Err(OverlayError::JoinRejected(reason)),
      other => {
        return Err(OverlayError::JoinFailed(format!(
          "sponsor answered begin with {other:?}"
        )));
      }
    };
    if begin_source != challenge.sponsor_node_id() {
      return Err(OverlayError::JoinFailed(
        "admission challenge source does not match its sponsor identity".to_string(),
      ));
    }
    let proof = JoinProof::create(join_key, candidate.clone(), challenge.clone())?;
    let (_, prove_response) = self
      .admission_call(
        sponsor_peer_id,
        challenge.sponsor_node_id(),
        challenge.cluster_id(),
        &AdmissionRequest::Prove(proof),
      )
      .await?;
    let outcome = match prove_response {
      AdmissionResponse::Admitted(outcome) => outcome,
      AdmissionResponse::Rejected(reason) => return Err(OverlayError::JoinRejected(reason)),
      other => {
        return Err(OverlayError::JoinFailed(format!(
          "sponsor answered prove with {other:?}"
        )));
      }
    };
    let registry = outcome.validate_for(&candidate, &challenge, &sponsor_peer_id)?;
    let records = registry.records();
    let store = self.node.authorization().clone();
    self
      .handle
      .commit_adopt_authorization(registry.clone(), move || store.replace(&records))
      .await?;
    self.enrollment.lock().await.adopt_registry(registry);
    Ok(())
  }

  /// Redial a persisted sponsor after restart using its authorized peer id.
  pub(crate) async fn reconnect(&self, address: Multiaddr) -> Result<(), OverlayError> {
    let peer_id = address.iter().find_map(|protocol| match protocol {
      lycoris_overlay::MultiaddrProtocol::P2p(peer_id) => Some(peer_id),
      _ => None,
    });
    let peer_id = peer_id.ok_or(OverlayError::UnknownBootstrapPeer)?;
    let registry = self.enrollment.lock().await.registry().clone();
    let node_id = registry
      .node_for_peer(&peer_id)
      .ok_or(OverlayError::UnknownBootstrapPeer)?;
    self.handle.dial(node_id, address).await?;
    Ok(())
  }

  async fn admission_call(
    &self, sponsor: lycoris_overlay::PeerId, destination: NodeId, cluster_id: ClusterId,
    request: &AdmissionRequest,
  ) -> Result<(NodeId, AdmissionResponse), OverlayError> {
    let payload = postcard::to_stdvec(request)?;
    let Some(envelope) =
      self.request_envelope(destination, cluster_id, ProtocolId::Admission, payload)?
    else {
      return Err(OverlayError::JoinFailed(
        "admission request exceeds the frame budget".to_string(),
      ));
    };
    let response = self.handle.request_admission(sponsor, envelope).await?;
    let source = response.header().source;
    Ok((source, postcard::from_bytes(response.payload())?))
  }

  /// Stop the link actor; tolerates a dispatcher that already stopped it.
  pub async fn shutdown(&self) {
    if let Some(runtime) = self.runtime.lock().await.take()
      && let Err(error) = runtime.shutdown().await
    {
      tracing::debug!(%error, "overlay link actor was already stopped");
    }
  }

  /// Dispatch inbound overlay envelopes to the installed plane handlers.
  pub async fn run_dispatcher(&self) {
    while let Some(inbound) = self.handle.next_inbound().await {
      match inbound.envelope.header().protocol {
        ProtocolId::Admission => self.handle_admission(inbound).await,
        ProtocolId::Registry => self.handle_registry(inbound).await,
        ProtocolId::Membership => self.handle_membership(inbound).await,
        ProtocolId::Resource => self.handle_resource(inbound).await,
        ProtocolId::Extension => self.handle_extension(inbound).await,
        _ => {}
      }
    }
  }

  /// Push the registry checkpoint to newly connected peers so enrollment
  /// propagates across the sparse graph without waiting for anti-entropy.
  pub async fn run_registry_gossip(&self) {
    let mut snapshots = self.handle.subscribe();
    let mut known = self.handle.snapshot().connected_nodes;
    loop {
      if snapshots.changed().await.is_err() {
        return;
      }
      let connected = snapshots.borrow().connected_nodes.clone();
      let grew = connected.iter().any(|node| !known.contains(node));
      known = connected;
      if grew {
        self.broadcast_checkpoint().await;
      }
    }
  }

  async fn handle_admission(&self, inbound: lycoris_overlay::InboundEnvelope) {
    let request = match postcard::from_bytes::<AdmissionRequest>(inbound.envelope.payload()) {
      Ok(request) => request,
      Err(error) => {
        tracing::debug!(%error, "dropping a malformed admission request");
        return;
      }
    };
    let response = {
      let mut enrollment = self.enrollment.lock().await;
      match request {
        AdmissionRequest::Begin(candidate) => enrollment
          .begin(candidate, &inbound.sender, &self.identity)
          .map_or_else(
            |error| AdmissionResponse::Rejected(error.to_string()),
            AdmissionResponse::Challenge,
          ),
        AdmissionRequest::Prove(proof) => {
          let mut proposed = enrollment.clone();
          match proposed.enroll_with_join_key(&proof, &inbound.sender, &self.identity) {
            Ok(outcome) => {
              let admitted = outcome.record().clone();
              let records = outcome.records().to_vec();
              match commit_enrollment(
                &self.handle,
                self.node.authorization().clone(),
                &mut enrollment,
                proposed,
              )
              .await
              {
                Ok(()) => AdmissionResponse::Admitted(Box::new(
                  lycoris_overlay::AdmissionOutcome::new(admitted, records),
                )),
                Err(error) => {
                  tracing::error!(%error, "failed to commit an admission checkpoint");
                  AdmissionResponse::Rejected("authorization commit failed".to_string())
                }
              }
            }
            Err(error) => AdmissionResponse::Rejected(error.to_string()),
          }
        }
      }
    };
    self.send_reply(&inbound, &response).await;
  }

  async fn send_reply<T: serde::Serialize>(
    &self, inbound: &lycoris_overlay::InboundEnvelope, response: &T,
  ) {
    let payload = match postcard::to_stdvec(response) {
      Ok(payload) => payload,
      Err(error) => {
        tracing::warn!(%error, "failed to encode an overlay reply");
        return;
      }
    };
    self.send_reply_payload(inbound, payload).await;
  }

  async fn send_reply_payload(&self, inbound: &lycoris_overlay::InboundEnvelope, payload: Vec<u8>) {
    let Some(reply) = response_envelope(&inbound.envelope, self.node_id(), payload) else {
      return;
    };
    if let Err(error) = self.handle.respond(inbound.token, reply).await {
      tracing::debug!(%error, "failed to send an overlay reply");
    }
  }

  async fn handle_membership(&self, inbound: lycoris_overlay::InboundEnvelope) {
    let request = match NodeRequest::decode(inbound.envelope.payload()) {
      Ok(request) => request,
      Err(error) => {
        tracing::debug!(%error, "dropping a malformed membership request");
        return;
      }
    };
    let handler = self.node_handler.read().await.clone();
    let Some(handler) = handler else {
      tracing::debug!("dropping a membership request before the handler is installed");
      return;
    };
    let response = handler.handle(request).await;
    self
      .send_reply_payload(&inbound, response.encode_to_vec())
      .await;
  }

  async fn handle_resource(&self, inbound: lycoris_overlay::InboundEnvelope) {
    let request = match SyncResourcesRequest::decode(inbound.envelope.payload()) {
      Ok(request) => request,
      Err(error) => {
        tracing::debug!(%error, "dropping a malformed resource request");
        return;
      }
    };
    let Some(handler) = self.resource_handler.read().await.clone() else {
      tracing::debug!("dropping a resource request before the handler is installed");
      return;
    };
    let response = handler.handle(request).await;
    self
      .send_reply_payload(&inbound, response.encode_to_vec())
      .await;
  }

  async fn handle_extension(&self, inbound: lycoris_overlay::InboundEnvelope) {
    let request = match ExtensionInvokeRequest::decode(inbound.envelope.payload()) {
      Ok(request) => request,
      Err(error) => {
        tracing::debug!(%error, "dropping a malformed extension request");
        return;
      }
    };
    let Some(handler) = self.extension_handler.read().await.clone() else {
      tracing::debug!("dropping an extension request before the handler is installed");
      return;
    };
    let response = handler.handle(request).await;
    self
      .send_reply_payload(&inbound, response.encode_to_vec())
      .await;
  }

  async fn handle_registry(&self, inbound: lycoris_overlay::InboundEnvelope) {
    let records = match postcard::from_bytes::<Vec<AuthorizationRecord>>(inbound.envelope.payload())
    {
      Ok(records) => records,
      Err(error) => {
        tracing::debug!(%error, "rejecting a malformed registry checkpoint");
        self
          .send_reply(
            &inbound,
            &RegistryResponse::Rejected("malformed registry checkpoint".to_string()),
          )
          .await;
        return;
      }
    };
    let (response, changed) = {
      let mut enrollment = self.enrollment.lock().await;
      merge_registry_checkpoint(
        &self.handle,
        self.node.authorization().clone(),
        &mut enrollment,
        records,
      )
      .await
    };
    self.send_reply(&inbound, &response).await;
    if changed {
      self.broadcast_checkpoint().await;
    }
  }

  /// Send the full registry checkpoint to every connected peer and merge
  /// whatever they send back (symmetric exchange, no re-broadcast here — the
  /// receiver-side path cascades further).
  async fn broadcast_checkpoint(&self) {
    let registry = self.enrollment.lock().await.registry().clone();
    let payload = match postcard::to_stdvec(&registry.records()) {
      Ok(payload) => payload,
      Err(error) => {
        tracing::warn!(%error, "failed to encode the registry checkpoint");
        return;
      }
    };
    let cluster_id = registry.cluster_id();
    for destination in self.handle.snapshot().connected_nodes {
      let envelope = match self.request_envelope(
        destination,
        cluster_id,
        ProtocolId::Registry,
        payload.clone(),
      ) {
        Ok(Some(envelope)) => envelope,
        Ok(None) => return,
        Err(error) => {
          tracing::error!(%error, "stopping registry publication");
          return;
        }
      };
      let handle = self.handle.clone();
      let enrollment = self.enrollment.clone();
      let node = self.node.clone();
      tokio::spawn(async move {
        match handle.request(envelope).await {
          Ok(response) => {
            let records = match postcard::from_bytes::<RegistryResponse>(response.payload()) {
              Ok(RegistryResponse::Applied(records)) => records,
              Ok(RegistryResponse::Rejected(reason)) => {
                tracing::warn!(%destination, %reason, "peer rejected a registry checkpoint");
                return;
              }
              Err(error) => {
                tracing::debug!(%error, "ignoring a malformed registry reply");
                return;
              }
            };
            let mut enrollment = enrollment.lock().await;
            let (result, _) = merge_registry_checkpoint(
              &handle,
              node.authorization().clone(),
              &mut enrollment,
              records,
            )
            .await;
            if let RegistryResponse::Rejected(reason) = result {
              tracing::error!(%destination, %reason, "failed to merge a registry reply");
            }
          }
          Err(error) => {
            tracing::debug!(%destination, %error, "registry exchange failed");
          }
        }
      });
    }
  }

  fn request_envelope(
    &self, destination: NodeId, cluster_id: ClusterId, protocol: ProtocolId, payload: Vec<u8>,
  ) -> Result<Option<Envelope>, LinkError> {
    let header = EnvelopeHeader {
      version: PROTOCOL_VERSION,
      cluster_id,
      request_id: self.handle.next_request_id()?,
      source: self.node_id(),
      destination,
      protocol,
      kind: MessageKind::Request,
      deadline_unix_ms: now_unix_ms() + REGISTRY_REQUEST_TIMEOUT_MS,
      remaining_hops: REGISTRY_REQUEST_HOPS,
    };
    match Envelope::new(header, payload) {
      Ok(envelope) => Ok(Some(envelope)),
      Err(error) => {
        tracing::warn!(%error, "registry checkpoint exceeds the overlay frame budget");
        Ok(None)
      }
    }
  }
}

fn response_envelope(request: &Envelope, source: NodeId, payload: Vec<u8>) -> Option<Envelope> {
  let header = EnvelopeHeader {
    source,
    destination: request.header().source,
    kind: MessageKind::Response,
    ..request.header().clone()
  };
  match Envelope::new(header, payload) {
    Ok(envelope) => Some(envelope),
    Err(error) => {
      tracing::warn!(%error, "overlay response exceeds the frame budget");
      None
    }
  }
}

fn initialize_identity_and_registry(
  data_dir: &Path, node: &NodeDomain,
) -> Result<(NodeIdentity, AuthorizationRegistry), OverlayError> {
  let identity_path = data_dir.join("node.identity");
  let records = node.authorization().records()?;
  let (identity, registry) = if records.is_empty() {
    let identity = NodeIdentity::load_or_generate(&identity_path)?;
    if !identity.public_identity().is_initial_identity() {
      return Err(OverlayError::InitialIdentityRequired(identity.node_id()));
    }
    let (cluster_id, genesis) = AuthorizationRecord::genesis(&identity)?;
    node.authorization().put(&genesis)?;
    tracing::info!(cluster_id = %cluster_id, node_id = %identity.node_id(), "initialized a standalone cluster registry");
    let committed = node.authorization().records()?;
    (identity, registry_from_records(committed)?)
  } else {
    let identity = match NodeIdentity::load(&identity_path) {
      Err(IdentityError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
        return Err(OverlayError::PersistedIdentityMissing);
      }
      result => result?,
    };
    (identity, registry_from_records(records)?)
  };
  bind_local_identity(&registry, &identity)?;
  Ok((identity, registry))
}

fn registry_from_records(
  records: Vec<AuthorizationRecord>,
) -> Result<AuthorizationRegistry, OverlayError> {
  let genesis = records
    .iter()
    .find(|record| record.authorizer().is_none())
    .ok_or(OverlayError::MissingGenesis)?;
  let cluster_id = ClusterId::from_genesis(genesis.node_id());
  Ok(AuthorizationRegistry::from_records(cluster_id, records)?)
}

fn bind_local_identity(
  registry: &AuthorizationRegistry, identity: &NodeIdentity,
) -> Result<(), OverlayError> {
  let node_id = identity.node_id();
  let active = registry
    .active_record_for_node(node_id)
    .ok_or(OverlayError::LocalIdentityNotActive(node_id))?;
  let matches_identity = active.node_id() == node_id
    && active.initial_public_key() == identity.initial_public_key()
    && active.public_key() == identity.public_key_bytes()
    && active.peer_id() == identity.peer_id_bytes()
    && registry.active_peer_for_node(node_id) == Some(identity.peer_id());
  if !matches_identity {
    return Err(OverlayError::LocalIdentityMismatch(node_id));
  }
  Ok(())
}

fn parse_listen_addresses(listen: &[String]) -> Result<Vec<Multiaddr>, OverlayError> {
  if listen.is_empty() {
    return Ok(vec![
      "/ip4/0.0.0.0/udp/0/quic-v1".parse().map_err(|_| {
        OverlayError::InvalidListenAddress("/ip4/0.0.0.0/udp/0/quic-v1".to_string())
      })?,
      "/ip4/0.0.0.0/tcp/0"
        .parse()
        .map_err(|_| OverlayError::InvalidListenAddress("/ip4/0.0.0.0/tcp/0".to_string()))?,
    ]);
  }
  listen
    .iter()
    .map(|address| {
      address
        .parse()
        .map_err(|_| OverlayError::InvalidListenAddress(address.clone()))
    })
    .collect()
}

fn now_unix_ms() -> i64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|duration| duration.as_millis() as i64)
    .unwrap_or(0)
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::{AtomicBool, Ordering};

  use super::*;

  #[derive(Debug)]
  struct TestRegistryCommitter {
    called: AtomicBool,
    fail: bool,
  }

  #[async_trait::async_trait]
  impl RegistryCommitter for TestRegistryCommitter {
    async fn commit_registry(
      &self, _store: AuthorizationStorage, _registry: AuthorizationRegistry, adopt: bool,
    ) -> Result<(), LinkError> {
      assert!(!adopt);
      self.called.store(true, Ordering::SeqCst);
      if self.fail {
        return Err(LinkError::AuthorizationCommit(
          "injected commit failure".to_string(),
        ));
      }
      Ok(())
    }
  }

  fn test_enrollments() -> TestResult<(Enrollment, Enrollment)> {
    let sponsor = NodeIdentity::generate();
    let (cluster_id, genesis) = AuthorizationRecord::genesis(&sponsor)?;
    let current_registry = AuthorizationRegistry::from_records(cluster_id, vec![genesis.clone()])?;
    let member = NodeIdentity::generate();
    let admission = AuthorizationRecord::admit(
      cluster_id,
      &member.public_identity(),
      &genesis,
      &genesis,
      &sponsor,
    )?;
    let proposed_registry =
      AuthorizationRegistry::from_records(cluster_id, vec![genesis, admission])?;
    Ok((
      Enrollment::new(current_registry, None),
      Enrollment::new(proposed_registry, None),
    ))
  }

  type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

  #[tokio::test]
  async fn registry_commit_failure_keeps_the_current_enrollment() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let committer = TestRegistryCommitter {
      called: AtomicBool::new(false),
      fail: true,
    };
    let (mut current, proposed) = test_enrollments()?;

    let result = commit_enrollment(
      &committer,
      storage.node().authorization().clone(),
      &mut current,
      proposed,
    )
    .await;

    assert!(matches!(
      result,
      Err(OverlayError::Link(LinkError::AuthorizationCommit(_)))
    ));
    assert_eq!(current.registry().records().len(), 1);
    assert!(committer.called.load(Ordering::SeqCst));
    Ok(())
  }

  #[tokio::test]
  async fn registry_commit_success_adopts_the_proposal() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let committer = TestRegistryCommitter {
      called: AtomicBool::new(false),
      fail: false,
    };
    let (mut current, proposed) = test_enrollments()?;

    commit_enrollment(
      &committer,
      storage.node().authorization().clone(),
      &mut current,
      proposed,
    )
    .await?;

    assert_eq!(current.registry().records().len(), 2);
    assert!(committer.called.load(Ordering::SeqCst));
    Ok(())
  }

  #[tokio::test]
  async fn unchanged_registry_adopts_without_a_durable_rewrite() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let committer = TestRegistryCommitter {
      called: AtomicBool::new(false),
      fail: true,
    };
    let (mut current, _) = test_enrollments()?;
    let proposed = current.clone();

    commit_enrollment(
      &committer,
      storage.node().authorization().clone(),
      &mut current,
      proposed,
    )
    .await?;

    assert!(!committer.called.load(Ordering::SeqCst));
    assert_eq!(current.registry().records().len(), 1);
    Ok(())
  }

  #[tokio::test]
  async fn registry_checkpoint_commit_failure_returns_rejection() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let committer = TestRegistryCommitter {
      called: AtomicBool::new(false),
      fail: true,
    };
    let (mut current, proposed) = test_enrollments()?;

    let (response, changed) = merge_registry_checkpoint(
      &committer,
      storage.node().authorization().clone(),
      &mut current,
      proposed.registry().records(),
    )
    .await;

    assert_eq!(
      response,
      RegistryResponse::Rejected("authorization commit failed".to_string())
    );
    assert!(!changed);
    assert_eq!(current.registry().records().len(), 1);
    Ok(())
  }

  #[tokio::test]
  async fn foreign_registry_checkpoint_is_rejected_before_commit() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let committer = TestRegistryCommitter {
      called: AtomicBool::new(false),
      fail: false,
    };
    let (mut current, _) = test_enrollments()?;
    let foreign = test_registry_for_identity(&NodeIdentity::generate())?;

    let (response, changed) = merge_registry_checkpoint(
      &committer,
      storage.node().authorization().clone(),
      &mut current,
      foreign.records(),
    )
    .await;

    assert!(matches!(response, RegistryResponse::Rejected(_)));
    assert!(!changed);
    assert!(!committer.called.load(Ordering::SeqCst));
    assert_eq!(current.registry().records().len(), 1);
    Ok(())
  }

  #[test]
  fn registry_rejection_round_trips_on_the_wire() -> TestResult {
    let rejected = RegistryResponse::Rejected("malformed registry checkpoint".to_string());
    let encoded = postcard::to_stdvec(&rejected)?;
    assert_eq!(
      postcard::from_bytes::<RegistryResponse>(&encoded)?,
      rejected
    );
    Ok(())
  }

  fn test_registry_for_identity(identity: &NodeIdentity) -> TestResult<AuthorizationRegistry> {
    let (cluster_id, genesis) = AuthorizationRecord::genesis(identity)?;
    Ok(AuthorizationRegistry::from_records(
      cluster_id,
      vec![genesis],
    )?)
  }

  fn persist_records(directory: &Path, records: &[AuthorizationRecord]) -> TestResult {
    let storage = lycoris_storage::Storage::open(directory.join("lycoris.redb"))?;
    storage.node().authorization().replace(records)?;
    Ok(())
  }

  #[test]
  fn empty_registry_creates_bound_identity_and_reopens_exactly() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let first_public;
    let first_records;
    {
      let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
      let (identity, registry) = initialize_identity_and_registry(dir.path(), storage.node())?;
      first_public = identity.public_identity();
      first_records = registry.records();
      let active = registry.active_record_for_node(identity.node_id()).unwrap();
      assert_eq!(active.public_key(), identity.public_key_bytes());
      assert_eq!(active.peer_id(), identity.peer_id_bytes());
    }
    {
      let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
      let (identity, registry) = initialize_identity_and_registry(dir.path(), storage.node())?;
      assert_eq!(identity.public_identity(), first_public);
      assert_eq!(registry.records(), first_records);
    }
    Ok(())
  }

  #[tokio::test]
  async fn overlay_restart_rejects_missing_and_foreign_identity_without_mutation() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let database = dir.path().join("lycoris.redb");
    let identity_path = dir.path().join("node.identity");
    let original_bytes;
    let records;
    {
      let storage = lycoris_storage::Storage::open(&database)?;
      let overlay = OverlayNode::start(dir.path(), storage.node().clone(), None, &[])?;
      original_bytes = std::fs::read(&identity_path)?;
      records = storage.node().authorization().records()?;
      overlay.shutdown().await;
    }

    std::fs::remove_file(&identity_path)?;
    {
      let storage = lycoris_storage::Storage::open(&database)?;
      assert!(matches!(
        OverlayNode::start(dir.path(), storage.node().clone(), None, &[]),
        Err(OverlayError::PersistedIdentityMissing)
      ));
      assert!(!identity_path.exists());
      assert_eq!(storage.node().authorization().records()?, records);
    }

    NodeIdentity::generate().save(&identity_path)?;
    {
      let storage = lycoris_storage::Storage::open(&database)?;
      assert!(matches!(
        OverlayNode::start(dir.path(), storage.node().clone(), None, &[]),
        Err(OverlayError::LocalIdentityNotActive(_))
      ));
      assert_eq!(storage.node().authorization().records()?, records);
    }

    lycoris_core::write_private_file(&identity_path, &original_bytes)?;
    {
      let storage = lycoris_storage::Storage::open(&database)?;
      let overlay = OverlayNode::start(dir.path(), storage.node().clone(), None, &[])?;
      assert_eq!(storage.node().authorization().records()?, records);
      overlay.shutdown().await;
    }
    Ok(())
  }

  #[test]
  fn empty_registry_reuses_an_existing_initial_identity() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let identity = NodeIdentity::generate();
    let path = dir.path().join("node.identity");
    identity.save(&path)?;
    let before = std::fs::read(&path)?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;

    let (loaded, registry) = initialize_identity_and_registry(dir.path(), storage.node())?;

    assert_eq!(loaded.public_identity(), identity.public_identity());
    assert_eq!(std::fs::read(path)?, before);
    assert_eq!(registry.records().len(), 1);
    Ok(())
  }

  #[test]
  fn empty_registry_rejects_a_successor_identity() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let initial = NodeIdentity::generate();
    let successor =
      NodeIdentity::generate_successor(initial.node_id(), initial.initial_public_key().to_vec())?;
    successor.save(dir.path().join("node.identity"))?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;

    assert!(matches!(
      initialize_identity_and_registry(dir.path(), storage.node()),
      Err(OverlayError::InitialIdentityRequired(node_id)) if node_id == initial.node_id()
    ));
    assert!(storage.node().authorization().records()?.is_empty());
    Ok(())
  }

  #[test]
  fn populated_registry_never_generates_a_missing_identity() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let identity = NodeIdentity::generate();
    let (_, genesis) = AuthorizationRecord::genesis(&identity)?;
    persist_records(dir.path(), std::slice::from_ref(&genesis))?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let before = storage.node().authorization().records()?;

    assert!(matches!(
      initialize_identity_and_registry(dir.path(), storage.node()),
      Err(OverlayError::PersistedIdentityMissing)
    ));
    assert!(!dir.path().join("node.identity").exists());
    assert_eq!(storage.node().authorization().records()?, before);
    Ok(())
  }

  #[test]
  fn populated_registry_rejects_foreign_and_corrupt_identity_without_mutation() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let authorized = NodeIdentity::generate();
    let (_, genesis) = AuthorizationRecord::genesis(&authorized)?;
    persist_records(dir.path(), std::slice::from_ref(&genesis))?;
    let identity_path = dir.path().join("node.identity");
    let foreign = NodeIdentity::generate();
    foreign.save(&identity_path)?;
    let foreign_bytes = std::fs::read(&identity_path)?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let records = storage.node().authorization().records()?;

    assert!(matches!(
      initialize_identity_and_registry(dir.path(), storage.node()),
      Err(OverlayError::LocalIdentityNotActive(node_id)) if node_id == foreign.node_id()
    ));
    assert_eq!(std::fs::read(&identity_path)?, foreign_bytes);
    assert_eq!(storage.node().authorization().records()?, records);

    let corrupt = b"corrupt identity";
    std::fs::write(&identity_path, corrupt)?;
    assert!(matches!(
      initialize_identity_and_registry(dir.path(), storage.node()),
      Err(OverlayError::Identity(IdentityError::Serialization(_)))
    ));
    assert_eq!(std::fs::read(identity_path)?, corrupt);
    assert_eq!(storage.node().authorization().records()?, records);
    Ok(())
  }

  #[test]
  fn stale_rotation_key_fails_and_active_successor_reopens() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let initial = NodeIdentity::generate();
    let (_, genesis) = AuthorizationRecord::genesis(&initial)?;
    let successor =
      NodeIdentity::generate_successor(initial.node_id(), initial.initial_public_key().to_vec())?;
    let rotation = AuthorizationRecord::rotate(&genesis, &genesis, &successor, &initial)?;
    persist_records(dir.path(), &[genesis, rotation])?;
    let identity_path = dir.path().join("node.identity");
    initial.save(&identity_path)?;
    let identity_bytes = std::fs::read(&identity_path)?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let records = storage.node().authorization().records()?;

    assert!(matches!(
      initialize_identity_and_registry(dir.path(), storage.node()),
      Err(OverlayError::LocalIdentityMismatch(node_id)) if node_id == initial.node_id()
    ));
    assert_eq!(std::fs::read(&identity_path)?, identity_bytes);
    assert_eq!(storage.node().authorization().records()?, records);

    successor.save(&identity_path)?;
    let (loaded, _) = initialize_identity_and_registry(dir.path(), storage.node())?;
    assert_eq!(loaded.public_identity(), successor.public_identity());
    Ok(())
  }

  #[test]
  fn successor_identity_with_predecessor_registry_fails_without_mutation() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let initial = NodeIdentity::generate();
    let (_, genesis) = AuthorizationRecord::genesis(&initial)?;
    persist_records(dir.path(), std::slice::from_ref(&genesis))?;
    let successor =
      NodeIdentity::generate_successor(initial.node_id(), initial.initial_public_key().to_vec())?;
    let identity_path = dir.path().join("node.identity");
    successor.save(&identity_path)?;
    let identity_bytes = std::fs::read(&identity_path)?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let records = storage.node().authorization().records()?;

    assert!(matches!(
      initialize_identity_and_registry(dir.path(), storage.node()),
      Err(OverlayError::LocalIdentityMismatch(node_id)) if node_id == successor.node_id()
    ));
    assert_eq!(std::fs::read(identity_path)?, identity_bytes);
    assert_eq!(storage.node().authorization().records()?, records);
    Ok(())
  }

  #[test]
  fn revoked_and_conflicted_local_lineages_fail_closed() -> TestResult {
    let revoked_dir = tempfile::TempDir::new()?;
    let identity = NodeIdentity::generate();
    let (_, genesis) = AuthorizationRecord::genesis(&identity)?;
    let revocation =
      AuthorizationRecord::revoke(&genesis, &genesis, &genesis, &genesis, &identity)?;
    persist_records(revoked_dir.path(), &[genesis.clone(), revocation])?;
    let revoked_identity_path = revoked_dir.path().join("node.identity");
    identity.save(&revoked_identity_path)?;
    let revoked_identity_bytes = std::fs::read(&revoked_identity_path)?;
    let revoked_storage = lycoris_storage::Storage::open(revoked_dir.path().join("lycoris.redb"))?;
    let revoked_records = revoked_storage.node().authorization().records()?;
    assert!(matches!(
      initialize_identity_and_registry(revoked_dir.path(), revoked_storage.node()),
      Err(OverlayError::LocalIdentityNotActive(node_id)) if node_id == identity.node_id()
    ));
    assert_eq!(
      std::fs::read(revoked_identity_path)?,
      revoked_identity_bytes
    );
    assert_eq!(
      revoked_storage.node().authorization().records()?,
      revoked_records
    );

    let conflicted_dir = tempfile::TempDir::new()?;
    let first =
      NodeIdentity::generate_successor(identity.node_id(), identity.initial_public_key().to_vec())?;
    let second =
      NodeIdentity::generate_successor(identity.node_id(), identity.initial_public_key().to_vec())?;
    let first_rotation = AuthorizationRecord::rotate(&genesis, &genesis, &first, &identity)?;
    let second_rotation = AuthorizationRecord::rotate(&genesis, &genesis, &second, &identity)?;
    persist_records(
      conflicted_dir.path(),
      &[genesis, first_rotation, second_rotation],
    )?;
    let conflicted_identity_path = conflicted_dir.path().join("node.identity");
    identity.save(&conflicted_identity_path)?;
    let conflicted_identity_bytes = std::fs::read(&conflicted_identity_path)?;
    let conflicted_storage =
      lycoris_storage::Storage::open(conflicted_dir.path().join("lycoris.redb"))?;
    let conflicted_records = conflicted_storage.node().authorization().records()?;
    assert!(matches!(
      initialize_identity_and_registry(conflicted_dir.path(), conflicted_storage.node()),
      Err(OverlayError::LocalIdentityNotActive(node_id)) if node_id == identity.node_id()
    ));
    assert_eq!(
      std::fs::read(conflicted_identity_path)?,
      conflicted_identity_bytes
    );
    assert_eq!(
      conflicted_storage.node().authorization().records()?,
      conflicted_records
    );
    Ok(())
  }

  #[test]
  fn duplicate_active_peer_id_fails_exact_local_binding_without_mutation() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let sponsor = NodeIdentity::generate();
    let (cluster_id, sponsor_record) = AuthorizationRecord::genesis(&sponsor)?;
    let original = NodeIdentity::generate();
    let admission = AuthorizationRecord::admit(
      cluster_id,
      &original.public_identity(),
      &sponsor_record,
      &sponsor_record,
      &sponsor,
    )?;
    let successor =
      NodeIdentity::generate_successor(original.node_id(), original.initial_public_key().to_vec())?;
    let rotation = AuthorizationRecord::rotate(&admission, &admission, &successor, &original)?;
    let alias_key = successor.public_key_bytes();
    let alias_node = NodeId::from_initial_public_key(&alias_key);
    let alias = lycoris_overlay::PublicIdentity::new(
      alias_node,
      successor.peer_id_bytes(),
      alias_key.clone(),
      alias_key,
    )?;
    let alias_record =
      AuthorizationRecord::admit(cluster_id, &alias, &sponsor_record, &admission, &sponsor)?;
    persist_records(
      dir.path(),
      &[sponsor_record, admission, rotation, alias_record],
    )?;
    let identity_path = dir.path().join("node.identity");
    successor.save(&identity_path)?;
    let identity_bytes = std::fs::read(&identity_path)?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let records = storage.node().authorization().records()?;

    assert!(matches!(
      initialize_identity_and_registry(dir.path(), storage.node()),
      Err(OverlayError::LocalIdentityMismatch(node_id)) if node_id == original.node_id()
    ));
    assert_eq!(std::fs::read(identity_path)?, identity_bytes);
    assert_eq!(storage.node().authorization().records()?, records);
    Ok(())
  }

  #[test]
  fn empty_listen_config_falls_back_to_ephemeral_transports() -> TestResult {
    let addresses = parse_listen_addresses(&[])?;
    assert_eq!(addresses.len(), 2);
    let parsed = parse_listen_addresses(&["/ip4/127.0.0.1/tcp/4001".to_string()])?;
    assert_eq!(parsed.len(), 1);
    assert!(matches!(
      parse_listen_addresses(&["not-a-multiaddr".to_string()]),
      Err(OverlayError::InvalidListenAddress(_))
    ));
    Ok(())
  }
}
