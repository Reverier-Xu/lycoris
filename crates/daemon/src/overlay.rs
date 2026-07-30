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
use lycoris_storage::NodeDomain;
use prost::Message as _;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use crate::overlay_transport::OverlayPool;

const REGISTRY_REQUEST_TIMEOUT_MS: i64 = 5_000;
const REGISTRY_REQUEST_HOPS: u8 = 0;
const JOIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

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
    let identity = NodeIdentity::load_or_generate(data_dir.join("node.identity"))?;
    let registry = load_or_create_registry(&node, &identity)?;
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
    self.handle.dial_admission(address).await?;
    let deadline = std::time::Instant::now() + JOIN_TIMEOUT;
    while self.handle.snapshot().quarantined_count == 0 {
      if std::time::Instant::now() > deadline {
        return Err(OverlayError::JoinFailed(
          "sponsor connection was not quarantined in time".to_string(),
        ));
      }
      tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let candidate = AdmissionCandidate::new(&self.identity)?;
    let sentinel = NodeId::from_bytes([0; NodeId::BYTE_LENGTH]);
    let sentinel_cluster = ClusterId::from_bytes([0; ClusterId::BYTE_LENGTH]);
    let challenge = match self
      .admission_call(
        sentinel,
        sentinel_cluster,
        &AdmissionRequest::Begin(candidate.clone()),
      )
      .await?
    {
      AdmissionResponse::Challenge(challenge) => challenge,
      AdmissionResponse::Rejected(reason) => return Err(OverlayError::JoinRejected(reason)),
      other => {
        return Err(OverlayError::JoinFailed(format!(
          "sponsor answered begin with {other:?}"
        )));
      }
    };
    let proof = JoinProof::create(join_key, candidate, challenge.clone())?;
    let outcome = match self
      .admission_call(
        sentinel,
        challenge.cluster_id(),
        &AdmissionRequest::Prove(proof),
      )
      .await?
    {
      AdmissionResponse::Admitted(outcome) => outcome,
      AdmissionResponse::Rejected(reason) => return Err(OverlayError::JoinRejected(reason)),
      other => {
        return Err(OverlayError::JoinFailed(format!(
          "sponsor answered prove with {other:?}"
        )));
      }
    };
    let registry =
      AuthorizationRegistry::from_records(challenge.cluster_id(), outcome.records().to_vec())?;
    self.node.authorization().replace(&registry.records())?;
    self.handle.adopt_authorization(registry.clone()).await?;
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
    &self, destination: NodeId, cluster_id: ClusterId, request: &AdmissionRequest,
  ) -> Result<AdmissionResponse, OverlayError> {
    let payload = postcard::to_stdvec(request)?;
    let Some(envelope) =
      self.request_envelope(destination, cluster_id, ProtocolId::Admission, payload)
    else {
      return Err(OverlayError::JoinFailed(
        "admission request exceeds the frame budget".to_string(),
      ));
    };
    let response = self.handle.request(envelope).await?;
    Ok(postcard::from_bytes(response.payload())?)
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
          match enrollment.enroll_with_join_key(&proof, &inbound.sender, &self.identity) {
            Ok(outcome) => {
              let registry = enrollment.registry().clone();
              let admitted = outcome.record().clone();
              let records = outcome.records().to_vec();
              drop(enrollment);
              self.apply_registry(registry).await;
              AdmissionResponse::Admitted(Box::new(lycoris_overlay::AdmissionOutcome::new(
                admitted, records,
              )))
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
        tracing::debug!(%error, "dropping a malformed registry checkpoint");
        return;
      }
    };
    let changed = {
      let mut enrollment = self.enrollment.lock().await;
      match enrollment.merge_checkpoint(records) {
        Ok(changed) => {
          let registry = enrollment.registry().clone();
          drop(enrollment);
          if changed > 0 {
            self.apply_registry(registry).await;
          }
          changed
        }
        Err(error) => {
          tracing::debug!(%error, "rejected a registry checkpoint");
          0
        }
      }
    };
    let registry = self.enrollment.lock().await.registry().clone();
    self.send_reply(&inbound, &registry.records()).await;
    if changed > 0 {
      self.broadcast_checkpoint().await;
    }
  }

  /// Persist a registry mutation, push it into the link actor, and let the
  /// actor promote any quarantined connections the mutation authorizes.
  async fn apply_registry(&self, registry: AuthorizationRegistry) {
    for record in registry.records() {
      if let Err(error) = self.node.authorization().put(&record) {
        tracing::warn!(%error, "failed to persist an authorization record");
      }
    }
    if let Err(error) = self.handle.set_authorization(registry).await {
      tracing::warn!(%error, "failed to apply an authorization registry");
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
      let Some(envelope) = self.request_envelope(
        destination,
        cluster_id,
        ProtocolId::Registry,
        payload.clone(),
      ) else {
        return;
      };
      let handle = self.handle.clone();
      let enrollment = self.enrollment.clone();
      let node = self.node.clone();
      tokio::spawn(async move {
        match handle.request(envelope).await {
          Ok(response) => {
            let records = match postcard::from_bytes::<Vec<AuthorizationRecord>>(response.payload())
            {
              Ok(records) => records,
              Err(error) => {
                tracing::debug!(%error, "ignoring a malformed registry reply");
                return;
              }
            };
            let merged = {
              let mut enrollment = enrollment.lock().await;
              enrollment.merge_checkpoint(records)
            };
            match merged {
              Ok(changed) if changed > 0 => {
                let registry = enrollment.lock().await.registry().clone();
                for record in registry.records() {
                  if let Err(error) = node.authorization().put(&record) {
                    tracing::warn!(%error, "failed to persist an authorization record");
                  }
                }
                if let Err(error) = handle.set_authorization(registry).await {
                  tracing::warn!(%error, "failed to apply an authorization registry");
                }
              }
              Ok(_) => {}
              Err(error) => {
                tracing::debug!(%error, "rejected a registry reply");
              }
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
  ) -> Option<Envelope> {
    let header = EnvelopeHeader {
      version: PROTOCOL_VERSION,
      cluster_id,
      request_id: self.handle.next_request_id(),
      source: self.node_id(),
      destination,
      protocol,
      kind: MessageKind::Request,
      deadline_unix_ms: now_unix_ms() + REGISTRY_REQUEST_TIMEOUT_MS,
      remaining_hops: REGISTRY_REQUEST_HOPS,
    };
    match Envelope::new(header, payload) {
      Ok(envelope) => Some(envelope),
      Err(error) => {
        tracing::warn!(%error, "registry checkpoint exceeds the overlay frame budget");
        None
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

fn load_or_create_registry(
  node: &NodeDomain, identity: &NodeIdentity,
) -> Result<AuthorizationRegistry, OverlayError> {
  let records = node.authorization().records()?;
  if records.is_empty() {
    let (cluster_id, genesis) = AuthorizationRecord::genesis(identity)?;
    node.authorization().put(&genesis)?;
    tracing::info!(cluster_id = %cluster_id, node_id = %identity.node_id(), "initialized a standalone cluster registry");
    return Ok(AuthorizationRegistry::from_records(cluster_id, [genesis])?);
  }
  let genesis = records
    .iter()
    .find(|record| record.authorizer().is_none())
    .ok_or(OverlayError::MissingGenesis)?;
  let cluster_id = ClusterId::from_genesis(genesis.node_id());
  Ok(AuthorizationRegistry::from_records(cluster_id, records)?)
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
  use super::*;

  type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

  #[test]
  fn registry_is_created_once_and_reloaded() -> TestResult {
    let dir = tempfile::TempDir::new()?;
    let storage = lycoris_storage::Storage::open(dir.path().join("lycoris.redb"))?;
    let identity = NodeIdentity::generate();

    let created = load_or_create_registry(storage.node(), &identity)?;
    assert_eq!(created.records().len(), 1);

    let reloaded = load_or_create_registry(storage.node(), &NodeIdentity::generate())?;
    assert_eq!(reloaded.cluster_id(), created.cluster_id());
    assert_eq!(reloaded.records().len(), 1);
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
