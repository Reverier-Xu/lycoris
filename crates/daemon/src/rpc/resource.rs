//! Protocol-boundary helpers for resource requests: validation and wire
//! decoding. The resource facade (`ResourceMapper`) lives in
//! `crate::resource`; this module keeps only the request-side parsing shared
//! by the rpc handlers.
#![allow(clippy::result_large_err)]

use lycoris_core::ResourceScope;
use lycoris_proto::{
  node::{ResourceKind, ResourceScope as ProtoResourceScope},
  scope_from_proto,
};
use tonic::Status;

use crate::resource::decode_kind;

pub fn parse_kind(raw: i32) -> Result<ResourceKind, Status> {
  decode_kind(raw).map_err(Status::from)
}

/// Parse the optional scope filter of a list request: `UNSPECIFIED` means no
/// filtering.
pub fn parse_scope_filter(raw: i32) -> Result<Option<ResourceScope>, Status> {
  let scope = ProtoResourceScope::try_from(raw)
    .map_err(|_| Status::invalid_argument(format!("unknown resource scope: {raw}")))?;
  Ok(match scope {
    ProtoResourceScope::Unspecified => None,
    other => Some(scope_from_proto(other)),
  })
}
