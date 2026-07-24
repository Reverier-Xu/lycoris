use std::time::Duration;

use libp2p::{Multiaddr, multiaddr::Protocol};

use super::*;
use crate::NodeIdentity;

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn dropping_runtime_stops_its_actor() {
  let runtime = LinkRuntime::start(
    &NodeIdentity::generate(),
    LinkConfig::new(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()]),
  )
  .unwrap();
  let handle = runtime.handle();
  drop(runtime);

  tokio::time::timeout(WAIT, async {
    loop {
      let dial = handle
        .dial(
          libp2p::PeerId::random(),
          "/ip4/127.0.0.1/tcp/1".parse().unwrap(),
        )
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

async fn exercise_link(listen_address: Multiaddr, matches_transport: impl Fn(&Multiaddr) -> bool) {
  let first = LinkRuntime::start(
    &NodeIdentity::generate(),
    LinkConfig::new(vec![listen_address.clone()]),
  )
  .unwrap();
  let second = LinkRuntime::start(
    &NodeIdentity::generate(),
    LinkConfig::new(vec![listen_address]),
  )
  .unwrap();
  let first_handle = first.handle();
  let second_handle = second.handle();
  let first_peer = first_handle.snapshot().local_peer_id;
  let second_peer = second_handle.snapshot().local_peer_id;
  let first_address = wait_for_listener(&first_handle, matches_transport).await;

  second_handle.dial(first_peer, first_address).await.unwrap();
  first_handle
    .wait_connected(second_peer, WAIT)
    .await
    .unwrap();
  second_handle
    .wait_connected(first_peer, WAIT)
    .await
    .unwrap();
  first_handle.wait_healthy(second_peer, WAIT).await.unwrap();
  second_handle.wait_healthy(first_peer, WAIT).await.unwrap();

  second_handle.disconnect(first_peer).await.unwrap();
  wait_for_disconnected(&first_handle, second_peer).await;
  wait_for_disconnected(&second_handle, first_peer).await;

  second.shutdown().await.unwrap();
  first.shutdown().await.unwrap();
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

async fn wait_for_disconnected(handle: &LinkHandle, peer_id: libp2p::PeerId) {
  let mut snapshots = handle.subscribe();
  tokio::time::timeout(WAIT, async {
    loop {
      if !snapshots.borrow().connected_peers.contains(&peer_id) {
        return;
      }
      snapshots.changed().await.unwrap();
    }
  })
  .await
  .expect("peer did not disconnect");
}
