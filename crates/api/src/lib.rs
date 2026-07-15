#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! Compatibility shim re-exporting the split protocol and client crates.
//!
//! New code should depend on `lycoris-proto`, `lycoris-client`, or
//! `lycoris-tls` directly. This crate will be removed once all callers are
//! migrated.

pub use lycoris_client::*;
pub use lycoris_proto::*;
pub use lycoris_tls::*;

/// Compatibility alias for code that imports `lycoris_api::proto`.
pub mod proto {
  pub use lycoris_proto::node::*;
}

/// Compatibility alias for the old RPC client name.
pub type ClusterRpcClient = lycoris_client::ClusterClient;
pub type ClusterClientError = lycoris_client::ClientError;
pub type PeerClient = lycoris_client::PeerClient;
pub type SyncRpcClient = lycoris_client::SyncClient;
pub type MembershipRpcClient = lycoris_client::MembershipClient;
