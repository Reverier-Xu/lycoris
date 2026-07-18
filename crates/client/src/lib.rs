#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod client;

pub use client::{
  ClientError, ClusterClientHandle as ClusterClient, MAX_RPC_MESSAGE_BYTES,
  MembershipClientHandle as MembershipClient, PeerClient, SyncClientHandle as SyncClient,
};
