#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

pub mod node {
  tonic::include_proto!("lycoris.daemon");
}

pub use node::*;
