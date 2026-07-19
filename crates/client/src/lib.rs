#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod client;

pub use client::{
  ClientError, ClusterClientHandle as ClusterClient, ExtensionClientHandle as ExtensionClient,
  MAX_RPC_MESSAGE_BYTES, PeerClient,
};
