//! Daemon-level resource facade.
//!
//! `ResourceMapper` maps between stored domain types and the public
//! `Resource` proto. It is the single entry point shared by the rpc handlers
//! (`crate::rpc::server`) and the resource anti-entropy task
//! (`crate::sync::resource`). Failures are reported as the typed
//! [`MapperError`]; the rpc boundary maps it onto gRPC statuses
//! (`crate::rpc`), keeping this module free of transport concerns.
//!
//! Layout: [`error`] holds the typed failure enum, [`convert`] the
//! bidirectional wire/domain converters, and [`mapper`] the storage-facing
//! facade.

mod convert;
mod error;
mod mapper;

pub(crate) use convert::{decode_kind, scope_from_proto};
pub use error::MapperError;
pub use mapper::ResourceMapper;
