#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod authorization;
mod codec;
mod id;
mod identity;
mod protocol;

pub use authorization::{
  AuthorizationError, AuthorizationKind, AuthorizationRecord, AuthorizationRegistry,
  AuthorizationStatus, KeyState,
};
pub use codec::{FrameError, decode_frame, encode_frame};
pub use id::{ClusterId, NodeId, ParseIdentifierError, RecordId, RequestId};
pub use identity::{IdentityError, NodeIdentity};
pub use libp2p_identity::PeerId;
pub use protocol::{
  Envelope, EnvelopeHeader, MAX_FRAME_BYTES, MAX_PAYLOAD_BYTES, MessageKind, PROTOCOL_VERSION,
  ProtocolId,
};
