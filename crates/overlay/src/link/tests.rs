use std::time::Duration;

use libp2p::{Multiaddr, multiaddr::Protocol};

use super::*;
use crate::{
  AdmissionCandidate, AdmissionOutcome, AdmissionRequest, AdmissionResponse, AuthorizationRecord,
  AuthorizationRegistry, ClusterId, Enrollment, Envelope, EnvelopeHeader, JoinProof, MessageKind,
  NodeId, NodeIdentity, PROTOCOL_VERSION, ProtocolId, RequestId,
};

const WAIT: Duration = Duration::from_secs(5);
const FAR_FUTURE_MS: i64 = 4_111_111_111_111;

#[tokio::test]
async fn cloned_handles_share_one_request_id_sequence() -> Result<(), LinkError> {
  let identity = NodeIdentity::generate();
  let runtime = start_tcp(&identity, single_registry(&identity));
  let first = runtime.handle();
  let second = runtime.handle();

  assert_ne!(first.next_request_id(), second.next_request_id());
  runtime.shutdown().await?;
  Ok(())
}

#[tokio::test]
async fn dropping_runtime_stops_its_actor() {
  let identity = NodeIdentity::generate();
  let registry = single_registry(&identity);
  let runtime = start_tcp(&identity, registry);
  let handle = runtime.handle();
  let node_id = identity.node_id();
  drop(runtime);

  let observed = tokio::time::timeout(WAIT, async {
    loop {
      let dial = handle
        .dial(node_id, "/ip4/127.0.0.1/tcp/1".parse().unwrap())
        .await;
      if matches!(dial, Err(LinkError::ActorStopped)) {
        break;
      }
      tokio::task::yield_now().await;
    }
  })
  .await;
  assert!(observed.is_ok(), "runtime drop did not stop the actor");
}

#[tokio::test]
async fn quic_link_connects_pings_bidirectionally_and_disconnects() {
  exercise_link("/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap(), |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::QuicV1))
  })
  .await;
}

#[tokio::test]
async fn tcp_noise_yamux_link_is_a_working_fallback() {
  exercise_link("/ip4/127.0.0.1/tcp/0".parse().unwrap(), |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
  })
  .await;
}

#[tokio::test]
async fn unknown_peer_is_rejected_before_a_logical_edge() {
  let authorized_identity = NodeIdentity::generate();
  let registry = single_registry(&authorized_identity);
  let authorized = start_tcp(&authorized_identity, registry.clone());
  let stranger_identity = NodeIdentity::generate();
  let stranger = start_tcp(&stranger_identity, registry);
  let authorized_handle = authorized.handle();
  let stranger_handle = stranger.handle();
  let authorized_node = authorized_identity.node_id();
  let stranger_node = stranger_identity.node_id();
  let authorized_address = wait_for_listener(&authorized_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
  })
  .await;

  assert!(matches!(
    authorized_handle
      .dial(stranger_node, authorized_address.clone())
      .await,
    Err(LinkError::UnauthorizedNode(node)) if node == stranger_node
  ));
  stranger_handle
    .dial(authorized_node, authorized_address)
    .await
    .unwrap();
  assert_remains_disconnected(
    &authorized_handle,
    stranger_node,
    Duration::from_millis(500),
  )
  .await;

  stranger.shutdown().await.unwrap();
  authorized.shutdown().await.unwrap();
}

#[tokio::test]
async fn revoking_authorization_closes_the_existing_link() {
  let (first_identity, second_identity, first_record, second_record, registry) = authorized_pair();
  let first = start_tcp(&first_identity, registry.clone());
  let second = start_tcp(&second_identity, registry);
  let first_handle = first.handle();
  let second_handle = second.handle();
  let first_node = first_identity.node_id();
  let second_node = second_identity.node_id();
  let first_address = wait_for_listener(&first_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
  })
  .await;
  second_handle.dial(first_node, first_address).await.unwrap();
  first_handle
    .wait_connected(second_node, WAIT)
    .await
    .unwrap();

  let revocation = AuthorizationRecord::revoke(
    &second_record,
    &second_record,
    &first_record,
    &second_record,
    &first_identity,
  )
  .unwrap();
  let revoked_registry = AuthorizationRegistry::from_records(
    first_record.cluster_id(),
    [first_record, second_record, revocation],
  )
  .unwrap();
  first_handle
    .set_authorization(revoked_registry)
    .await
    .unwrap();

  wait_for_disconnected(&first_handle, second_node).await;
  assert!(matches!(
    first_handle
      .dial(second_node, "/ip4/127.0.0.1/tcp/1".parse().unwrap())
      .await,
    Err(LinkError::UnauthorizedNode(node)) if node == second_node
  ));

  second.shutdown().await.unwrap();
  first.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_links_arbitrate_to_one_connection_without_losing_the_edge() {
  let (first_identity, second_identity, _, _, registry) = authorized_pair();
  let listen_addresses = vec![
    "/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap(),
    "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
  ];
  let first = LinkRuntime::start(
    &first_identity,
    LinkConfig::new(listen_addresses.clone()),
    registry.clone(),
  )
  .unwrap();
  let second = LinkRuntime::start(
    &second_identity,
    LinkConfig::new(listen_addresses),
    registry,
  )
  .unwrap();
  let first_handle = first.handle();
  let second_handle = second.handle();
  let first_node = first_identity.node_id();
  let second_node = second_identity.node_id();
  let first_tcp = wait_for_listener(&first_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
  })
  .await;
  let second_quic = wait_for_listener(&second_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::QuicV1))
  })
  .await;

  let (first_dial, second_dial) = tokio::join!(
    first_handle.dial(second_node, second_quic),
    second_handle.dial(first_node, first_tcp),
  );
  assert!(first_dial.is_ok() || second_dial.is_ok());
  first_handle
    .wait_connected(second_node, WAIT)
    .await
    .unwrap();
  second_handle
    .wait_connected(first_node, WAIT)
    .await
    .unwrap();
  wait_for_connection_count(&first_handle, 1).await;
  wait_for_connection_count(&second_handle, 1).await;
  first_handle
    .wait_connected(second_node, WAIT)
    .await
    .unwrap();
  second_handle
    .wait_connected(first_node, WAIT)
    .await
    .unwrap();

  second.shutdown().await.unwrap();
  first.shutdown().await.unwrap();
}

#[tokio::test]
async fn relayed_nodes_connect_through_a_reservation() {
  let (relay_identity, first_identity, second_identity, registry) = authorized_trio();
  let relay = start_quiet_tcp(&relay_identity, registry.clone());
  let first = start_quiet_tcp(&first_identity, registry.clone());
  let second = start_quiet_tcp(&second_identity, registry);
  let relay_handle = relay.handle();
  let first_handle = first.handle();
  let second_handle = second.handle();
  let relay_node = relay_identity.node_id();
  let first_node = first_identity.node_id();
  let second_node = second_identity.node_id();
  let relay_address = wait_for_listener(&relay_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
  })
  .await;
  first_handle
    .listen_via_relay(relay_node, relay_address.clone())
    .await
    .unwrap();
  second_handle
    .listen_via_relay(relay_node, relay_address)
    .await
    .unwrap();
  let first_circuit = wait_for_listener(&first_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::P2pCircuit))
  })
  .await;
  let second_circuit = wait_for_listener(&second_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::P2pCircuit))
  })
  .await;
  assert!(first_circuit != second_circuit);

  first_handle
    .dial(second_node, second_circuit)
    .await
    .unwrap();
  first_handle
    .wait_connected(second_node, WAIT)
    .await
    .unwrap();
  second_handle
    .wait_connected(first_node, WAIT)
    .await
    .unwrap();
  wait_for_connection_count(&first_handle, 2).await;
  wait_for_connection_count(&second_handle, 2).await;

  second.shutdown().await.unwrap();
  first.shutdown().await.unwrap();
  relay.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_requests_echo_through_the_overlay() {
  let (first_identity, second_identity, _, _, registry) = authorized_pair();
  let first = start_quiet_tcp(&first_identity, registry.clone());
  let second = start_quiet_tcp(&second_identity, registry.clone());
  let first_handle = first.handle();
  let second_handle = second.handle();
  let echo = spawn_echo(second_handle.clone());
  let second_address = wait_for_listener(&second_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
  })
  .await;
  first_handle
    .dial(second_identity.node_id(), second_address)
    .await
    .unwrap();
  first_handle
    .wait_connected(second_identity.node_id(), WAIT)
    .await
    .unwrap();

  let response = first_handle
    .request(request_envelope(
      first_identity.node_id(),
      second_identity.node_id(),
      registry.cluster_id(),
      7,
      b"ping",
    ))
    .await
    .unwrap();

  assert_eq!(response.payload(), b"ping");
  assert_eq!(response.header().kind, MessageKind::Response);
  echo.abort();
  second.shutdown().await.unwrap();
  first.shutdown().await.unwrap();
}

#[tokio::test]
async fn routed_requests_cross_a_sparse_chain() {
  let (middle_identity, first_identity, second_identity, registry) = authorized_trio();
  let middle = start_quiet_tcp(&middle_identity, registry.clone());
  let first = start_quiet_tcp(&first_identity, registry.clone());
  let second = start_quiet_tcp(&second_identity, registry.clone());
  let middle_handle = middle.handle();
  let first_handle = first.handle();
  let second_handle = second.handle();
  let echo = spawn_echo(second_handle.clone());
  let middle_node = middle_identity.node_id();
  let first_node = first_identity.node_id();
  let second_node = second_identity.node_id();
  let middle_address = wait_for_listener(&middle_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
  })
  .await;
  first_handle
    .dial(middle_node, middle_address.clone())
    .await
    .unwrap();
  second_handle
    .dial(middle_node, middle_address)
    .await
    .unwrap();
  first_handle
    .wait_connected(middle_node, WAIT)
    .await
    .unwrap();
  second_handle
    .wait_connected(middle_node, WAIT)
    .await
    .unwrap();
  middle_handle
    .wait_connected(first_node, WAIT)
    .await
    .unwrap();
  middle_handle
    .wait_connected(second_node, WAIT)
    .await
    .unwrap();

  let observed = tokio::time::timeout(WAIT, async {
    loop {
      let request = request_envelope(first_node, second_node, registry.cluster_id(), 11, b"hop");
      match first_handle.request(request).await {
        Ok(response) => break response,
        Err(LinkError::NoRoute(_)) => tokio::time::sleep(Duration::from_millis(50)).await,
        Err(error) => panic!("unexpected request failure: {error}"),
      }
    }
  })
  .await;
  let response = match observed {
    Ok(response) => response,
    Err(_) => panic!("routed request did not complete"),
  };

  assert_eq!(response.payload(), b"hop");
  assert_eq!(response.header().source, second_node);
  echo.abort();
  second.shutdown().await.unwrap();
  first.shutdown().await.unwrap();
  middle.shutdown().await.unwrap();
}

#[tokio::test]
async fn unroutable_requests_fail_closed() {
  let (middle_identity, first_identity, second_identity, registry) = authorized_trio();
  let first = start_quiet_tcp(&first_identity, registry.clone());
  let first_handle = first.handle();

  assert!(matches!(
    first_handle
      .request(request_envelope(
        first_identity.node_id(),
        second_identity.node_id(),
        registry.cluster_id(),
        13,
        b"lost",
      ))
      .await,
    Err(LinkError::NoRoute(node)) if node == second_identity.node_id()
  ));
  assert!(matches!(
    first_handle
      .request(request_envelope(
        first_identity.node_id(),
        first_identity.node_id(),
        registry.cluster_id(),
        17,
        b"loop",
      ))
      .await,
    Err(LinkError::NoRoute(node)) if node == first_identity.node_id()
  ));

  drop(middle_identity);
  first.shutdown().await.unwrap();
}

#[tokio::test]
async fn quarantined_peers_enroll_and_promote_to_a_logical_edge() {
  let sponsor_identity = NodeIdentity::generate();
  let sponsor_registry = single_registry(&sponsor_identity);
  let cluster_id = sponsor_registry.cluster_id();
  let join_key = lycoris_core::ClusterKey::generate().unwrap();
  let sponsor = start_quiet_tcp(&sponsor_identity, sponsor_registry.clone());
  let sponsor_handle = sponsor.handle();
  let mut enrollment = Enrollment::new(sponsor_registry, Some(join_key.clone()));
  let responder_identity = sponsor_identity.clone();
  let responder_handle = sponsor_handle.clone();
  let responder = tokio::spawn(async move {
    while let Some(inbound) = responder_handle.next_inbound().await {
      if inbound.envelope.header().protocol != ProtocolId::Admission {
        continue;
      }
      let request: AdmissionRequest = postcard::from_bytes(inbound.envelope.payload()).unwrap();
      let response = match request {
        AdmissionRequest::Begin(candidate) => {
          match enrollment.begin(candidate, &inbound.sender, &responder_identity) {
            Ok(challenge) => AdmissionResponse::Challenge(challenge),
            Err(error) => AdmissionResponse::Rejected(error.to_string()),
          }
        }
        AdmissionRequest::Prove(proof) => {
          match enrollment.enroll_with_join_key(&proof, &inbound.sender, &responder_identity) {
            Ok(outcome) => {
              let admitted = outcome.record().clone();
              let records = outcome.records().to_vec();
              responder_handle
                .set_authorization(enrollment.registry().clone())
                .await
                .unwrap();
              AdmissionResponse::Admitted(Box::new(AdmissionOutcome::new(admitted, records)))
            }
            Err(error) => AdmissionResponse::Rejected(error.to_string()),
          }
        }
      };
      let payload = postcard::to_stdvec(&response).unwrap();
      let reply = response_envelope(&inbound.envelope, responder_identity.node_id(), payload);
      responder_handle
        .respond(inbound.token, reply)
        .await
        .unwrap();
    }
  });
  let joiner_identity = NodeIdentity::generate();
  let joiner = start_quiet_tcp(&joiner_identity, single_registry(&joiner_identity));
  let joiner_handle = joiner.handle();
  let sponsor_address = wait_for_listener(&sponsor_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
  })
  .await;
  let admission_address: Multiaddr =
    format!("{sponsor_address}/p2p/{}", sponsor_identity.peer_id())
      .parse()
      .unwrap();
  joiner_handle
    .dial_admission(admission_address)
    .await
    .unwrap();
  wait_for_snapshot(&joiner_handle, |snapshot| snapshot.quarantined_count > 0).await;
  assert!(sponsor_handle.snapshot().connected_nodes.is_empty());

  let candidate = AdmissionCandidate::new(&joiner_identity).unwrap();
  let begin = AdmissionRequest::Begin(candidate.clone());
  let challenge_response = joiner_handle
    .request(admission_envelope(
      joiner_identity.node_id(),
      21,
      postcard::to_stdvec(&begin).unwrap(),
    ))
    .await
    .unwrap();
  let AdmissionResponse::Challenge(challenge) =
    postcard::from_bytes(challenge_response.payload()).unwrap()
  else {
    panic!("expected an admission challenge");
  };
  assert_eq!(challenge.cluster_id(), cluster_id);

  let proof = JoinProof::create(&join_key, candidate, challenge).unwrap();
  let prove = AdmissionRequest::Prove(proof);
  let admitted_response = joiner_handle
    .request(admission_envelope(
      joiner_identity.node_id(),
      23,
      postcard::to_stdvec(&prove).unwrap(),
    ))
    .await
    .unwrap();
  let AdmissionResponse::Admitted(outcome) =
    postcard::from_bytes(admitted_response.payload()).unwrap()
  else {
    panic!("expected an admission outcome");
  };
  assert_eq!(outcome.record().node_id(), joiner_identity.node_id());
  let adopted =
    AuthorizationRegistry::from_records(cluster_id, outcome.records().to_vec()).unwrap();
  joiner_handle.adopt_authorization(adopted).await.unwrap();

  sponsor_handle
    .wait_connected(joiner_identity.node_id(), WAIT)
    .await
    .unwrap();
  joiner_handle
    .wait_connected(sponsor_identity.node_id(), WAIT)
    .await
    .unwrap();
  assert_eq!(joiner_handle.snapshot().quarantined_count, 0);
  assert_eq!(sponsor_handle.snapshot().quarantined_count, 0);

  responder.abort();
  joiner.shutdown().await.unwrap();
  sponsor.shutdown().await.unwrap();
}

fn admission_envelope(source: NodeId, nonce: u8, payload: Vec<u8>) -> Envelope {
  let header = EnvelopeHeader {
    version: PROTOCOL_VERSION,
    cluster_id: ClusterId::from_bytes([0; ClusterId::BYTE_LENGTH]),
    request_id: RequestId::from_bytes([nonce; RequestId::BYTE_LENGTH]),
    source,
    destination: NodeId::from_bytes([0; NodeId::BYTE_LENGTH]),
    protocol: ProtocolId::Admission,
    kind: MessageKind::Request,
    deadline_unix_ms: FAR_FUTURE_MS,
    remaining_hops: 0,
  };
  Envelope::new(header, payload).unwrap()
}

fn response_envelope(request: &Envelope, source: NodeId, payload: Vec<u8>) -> Envelope {
  let request_header = request.header();
  let header = EnvelopeHeader {
    version: PROTOCOL_VERSION,
    cluster_id: request_header.cluster_id,
    request_id: request_header.request_id,
    source,
    destination: request_header.source,
    protocol: request_header.protocol,
    kind: MessageKind::Response,
    deadline_unix_ms: request_header.deadline_unix_ms,
    remaining_hops: request_header.remaining_hops,
  };
  Envelope::new(header, payload).unwrap()
}

async fn wait_for_snapshot(handle: &LinkHandle, predicate: impl Fn(&LinkSnapshot) -> bool) {
  let mut snapshots = handle.subscribe();
  let observed = tokio::time::timeout(WAIT, async {
    loop {
      if predicate(&snapshots.borrow()) {
        return;
      }
      snapshots.changed().await.unwrap();
    }
  })
  .await;
  assert!(
    observed.is_ok(),
    "snapshot did not reach the expected state"
  );
}

fn request_envelope(
  source: NodeId, destination: NodeId, cluster_id: ClusterId, nonce: u8, payload: &[u8],
) -> Envelope {
  let header = EnvelopeHeader {
    version: PROTOCOL_VERSION,
    cluster_id,
    request_id: RequestId::from_bytes([nonce; RequestId::BYTE_LENGTH]),
    source,
    destination,
    protocol: ProtocolId::Membership,
    kind: MessageKind::Request,
    deadline_unix_ms: FAR_FUTURE_MS,
    remaining_hops: 8,
  };
  Envelope::new(header, payload.to_vec()).unwrap()
}

fn spawn_echo(handle: LinkHandle) -> tokio::task::JoinHandle<()> {
  tokio::spawn(async move {
    while let Some(inbound) = handle.next_inbound().await {
      let source = handle.snapshot().node_id;
      let request = inbound.envelope;
      let payload = request.payload().to_vec();
      let response = response_envelope(&request, source, payload);
      handle.respond(inbound.token, response).await.unwrap();
    }
  })
}

#[tokio::test]
async fn configured_links_reconnect_after_a_restart() {
  let (first_identity, second_identity, _, _, registry) = authorized_pair();
  let first = LinkRuntime::start(
    &first_identity,
    fast_link_config(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()]),
    registry.clone(),
  )
  .unwrap();
  let second = LinkRuntime::start(
    &second_identity,
    fast_link_config(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()]),
    registry.clone(),
  )
  .unwrap();
  let first_handle = first.handle();
  let second_handle = second.handle();
  let second_node = second_identity.node_id();
  let second_address = wait_for_listener(&second_handle, |address| {
    address
      .iter()
      .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
  })
  .await;
  first_handle
    .dial(second_node, second_address.clone())
    .await
    .unwrap();
  first_handle
    .wait_connected(second_node, WAIT)
    .await
    .unwrap();

  second.shutdown().await.unwrap();
  wait_for_disconnected(&first_handle, second_node).await;
  let restarted = LinkRuntime::start(
    &second_identity,
    fast_link_config(vec![second_address]),
    registry,
  )
  .unwrap();

  first_handle
    .wait_connected(second_node, WAIT)
    .await
    .unwrap();

  restarted.shutdown().await.unwrap();
  first.shutdown().await.unwrap();
}

#[tokio::test]
async fn lan_discovery_connects_authorized_nodes_without_a_dial() {
  let (first_identity, second_identity, _, _, registry) = authorized_pair();
  let first = LinkRuntime::start(
    &first_identity,
    fast_link_config(vec!["/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap()]),
    registry.clone(),
  )
  .unwrap();
  let second = LinkRuntime::start(
    &second_identity,
    fast_link_config(vec!["/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap()]),
    registry,
  )
  .unwrap();
  let first_handle = first.handle();
  let second_handle = second.handle();

  first_handle
    .wait_connected(second_identity.node_id(), Duration::from_secs(10))
    .await
    .unwrap();
  second_handle
    .wait_connected(first_identity.node_id(), WAIT)
    .await
    .unwrap();

  second.shutdown().await.unwrap();
  first.shutdown().await.unwrap();
}

async fn exercise_link(listen_address: Multiaddr, matches_transport: impl Fn(&Multiaddr) -> bool) {
  let (first_identity, second_identity, _, _, registry) = authorized_pair();
  let first = LinkRuntime::start(
    &first_identity,
    LinkConfig::new(vec![listen_address.clone()]),
    registry.clone(),
  )
  .unwrap();
  let second = LinkRuntime::start(
    &second_identity,
    LinkConfig::new(vec![listen_address]),
    registry,
  )
  .unwrap();
  let first_handle = first.handle();
  let second_handle = second.handle();
  let first_node = first_identity.node_id();
  let second_node = second_identity.node_id();
  let first_address = wait_for_listener(&first_handle, matches_transport).await;

  second_handle.dial(first_node, first_address).await.unwrap();
  first_handle
    .wait_connected(second_node, WAIT)
    .await
    .unwrap();
  second_handle
    .wait_connected(first_node, WAIT)
    .await
    .unwrap();
  first_handle.wait_healthy(second_node, WAIT).await.unwrap();
  second_handle.wait_healthy(first_node, WAIT).await.unwrap();

  second_handle.disconnect(first_node).await.unwrap();
  wait_for_disconnected(&first_handle, second_node).await;
  wait_for_disconnected(&second_handle, first_node).await;

  second.shutdown().await.unwrap();
  first.shutdown().await.unwrap();
}

fn single_registry(identity: &NodeIdentity) -> AuthorizationRegistry {
  let (cluster_id, record) = AuthorizationRecord::genesis(identity).unwrap();
  AuthorizationRegistry::from_records(cluster_id, [record]).unwrap()
}

fn authorized_trio() -> (
  NodeIdentity,
  NodeIdentity,
  NodeIdentity,
  AuthorizationRegistry,
) {
  let relay = NodeIdentity::generate();
  let first = NodeIdentity::generate();
  let second = NodeIdentity::generate();
  let (cluster_id, relay_record) = AuthorizationRecord::genesis(&relay).unwrap();
  let first_record = AuthorizationRecord::admit(
    cluster_id,
    &first.public_identity(),
    &relay_record,
    &relay_record,
    &relay,
  )
  .unwrap();
  let second_record = AuthorizationRecord::admit(
    cluster_id,
    &second.public_identity(),
    &relay_record,
    &first_record,
    &relay,
  )
  .unwrap();
  let registry =
    AuthorizationRegistry::from_records(cluster_id, [relay_record, first_record, second_record])
      .unwrap();
  (relay, first, second, registry)
}

fn start_quiet_tcp(identity: &NodeIdentity, registry: AuthorizationRegistry) -> LinkRuntime {
  LinkRuntime::start(
    identity,
    LinkConfig::new(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()]).with_lan_discovery(false),
    registry,
  )
  .unwrap()
}

fn fast_link_config(listen_addresses: Vec<Multiaddr>) -> LinkConfig {
  LinkConfig::new(listen_addresses)
    .with_reconnect_timing(
      Duration::from_millis(50),
      Duration::from_millis(50),
      Duration::from_millis(200),
    )
    .with_discovery_timing(Duration::from_secs(5), Duration::from_millis(200))
}

fn start_tcp(identity: &NodeIdentity, registry: AuthorizationRegistry) -> LinkRuntime {
  LinkRuntime::start(
    identity,
    LinkConfig::new(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()]),
    registry,
  )
  .unwrap()
}

fn authorized_pair() -> (
  NodeIdentity,
  NodeIdentity,
  AuthorizationRecord,
  AuthorizationRecord,
  AuthorizationRegistry,
) {
  let first = NodeIdentity::generate();
  let second = NodeIdentity::generate();
  let (cluster_id, first_record) = AuthorizationRecord::genesis(&first).unwrap();
  let second_record = AuthorizationRecord::admit(
    cluster_id,
    &second.public_identity(),
    &first_record,
    &first_record,
    &first,
  )
  .unwrap();
  let registry =
    AuthorizationRegistry::from_records(cluster_id, [first_record.clone(), second_record.clone()])
      .unwrap();
  (first, second, first_record, second_record, registry)
}

async fn wait_for_listener(
  handle: &LinkHandle, matches_transport: impl Fn(&Multiaddr) -> bool,
) -> Multiaddr {
  let mut snapshots = handle.subscribe();
  let observed = tokio::time::timeout(WAIT, async {
    loop {
      if let Some(address) = snapshots
        .borrow()
        .listen_addresses
        .iter()
        .find(|address| matches_transport(address))
        .cloned()
      {
        return address;
      }
      snapshots.changed().await.unwrap();
    }
  })
  .await;
  match observed {
    Ok(address) => address,
    Err(_) => panic!("listener did not become ready"),
  }
}

async fn wait_for_disconnected(handle: &LinkHandle, node_id: NodeId) {
  let mut snapshots = handle.subscribe();
  let observed = tokio::time::timeout(WAIT, async {
    loop {
      if !snapshots.borrow().connected_nodes.contains(&node_id) {
        return;
      }
      snapshots.changed().await.unwrap();
    }
  })
  .await;
  assert!(observed.is_ok(), "node did not disconnect");
}

async fn wait_for_connection_count(handle: &LinkHandle, expected: usize) {
  let mut snapshots = handle.subscribe();
  let observed = tokio::time::timeout(WAIT, async {
    loop {
      if snapshots.borrow().connection_count == expected {
        return;
      }
      snapshots.changed().await.unwrap();
    }
  })
  .await;
  assert!(observed.is_ok(), "connection count did not converge");
}

async fn assert_remains_disconnected(handle: &LinkHandle, node_id: NodeId, duration: Duration) {
  let snapshots = handle.subscribe();
  let observed = tokio::time::timeout(duration, async {
    loop {
      assert!(!snapshots.borrow().connected_nodes.contains(&node_id));
      tokio::time::sleep(Duration::from_millis(20)).await;
    }
  })
  .await;
  assert!(observed.is_err(), "unauthorized node became connected");
}
