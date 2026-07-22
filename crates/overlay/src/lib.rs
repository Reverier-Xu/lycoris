#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod codec;
mod id;
mod protocol;

pub use codec::{FrameError, decode_frame, encode_frame};
pub use id::{ClusterId, NodeId, ParseIdentifierError, RequestId};
pub use protocol::{
  Envelope, EnvelopeHeader, MAX_FRAME_BYTES, MAX_PAYLOAD_BYTES, MessageKind, PROTOCOL_VERSION,
  ProtocolId,
};
