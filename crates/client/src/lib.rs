#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod client;

pub use client::{
  ClientError, ClusterClientHandle as ClusterClient, MembershipClientHandle as MembershipClient,
  PeerClient, SyncClientHandle as SyncClient, install_crypto_provider,
};
