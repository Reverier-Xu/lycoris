use std::time::Duration;

use libp2p::{Multiaddr, multiaddr::Protocol};

use super::*;
use crate::{AuthorizationRecord, AuthorizationRegistry, NodeId, NodeIdentity};

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn dropping_runtime_stops_its_actor() {
  let identity = NodeIdentity::generate();
  let registry = single_registry(&identity);
  let runtime = start_tcp(&identity, registry);
  let handle = runtime.handle();
  let node_id = identity.node_id();
  drop(runtime);

  tokio::time::timeout(WAIT, async {
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
  .await
  .expect("runtime drop did not stop the actor");
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
  tokio::time::timeout(WAIT, async {
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
  .await
  .expect("listener did not become ready")
}

async fn wait_for_disconnected(handle: &LinkHandle, node_id: NodeId) {
  let mut snapshots = handle.subscribe();
  tokio::time::timeout(WAIT, async {
    loop {
      if !snapshots.borrow().connected_nodes.contains(&node_id) {
        return;
      }
      snapshots.changed().await.unwrap();
    }
  })
  .await
  .expect("node did not disconnect");
}

async fn wait_for_connection_count(handle: &LinkHandle, expected: usize) {
  let mut snapshots = handle.subscribe();
  tokio::time::timeout(WAIT, async {
    loop {
      if snapshots.borrow().connection_count == expected {
        return;
      }
      snapshots.changed().await.unwrap();
    }
  })
  .await
  .expect("connection count did not converge");
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
