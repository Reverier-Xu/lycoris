use serde::{Deserialize, Serialize};

use crate::{ClusterId, FrameError, NodeId, RequestId};

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = MAX_FRAME_BYTES - 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ProtocolId {
  Admission,
  Registry,
  Membership,
  Resource,
  Extension,
  Route,
}

impl ProtocolId {
  pub const fn path(self) -> &'static str {
    match self {
      Self::Admission => "/lycoris/admission/1",
      Self::Registry => "/lycoris/registry/1",
      Self::Membership => "/lycoris/membership/1",
      Self::Resource => "/lycoris/resource/1",
      Self::Extension => "/lycoris/extension/1",
      Self::Route => "/lycoris/route/1",
    }
  }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum MessageKind {
  Request,
  Response,
  Event,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvelopeHeader {
  pub version: u16,
  pub cluster_id: ClusterId,
  pub request_id: RequestId,
  pub source: NodeId,
  pub destination: NodeId,
  pub protocol: ProtocolId,
  pub kind: MessageKind,
  pub deadline_unix_ms: i64,
  pub remaining_hops: u8,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
  header: EnvelopeHeader,
  payload: Vec<u8>,
}

impl Envelope {
  pub fn new(header: EnvelopeHeader, payload: Vec<u8>) -> Result<Self, FrameError> {
    if payload.len() > MAX_PAYLOAD_BYTES {
      return Err(FrameError::PayloadTooLarge {
        actual: payload.len(),
        maximum: MAX_PAYLOAD_BYTES,
      });
    }
    Ok(Self { header, payload })
  }

  pub const fn header(&self) -> &EnvelopeHeader {
    &self.header
  }

  pub fn payload(&self) -> &[u8] {
    &self.payload
  }

  pub fn into_payload(self) -> Vec<u8> {
    self.payload
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashSet;

  use super::*;

  fn header() -> EnvelopeHeader {
    EnvelopeHeader {
      version: PROTOCOL_VERSION,
      cluster_id: ClusterId::from_bytes([1; ClusterId::BYTE_LENGTH]),
      request_id: RequestId::from_bytes([2; RequestId::BYTE_LENGTH]),
      source: NodeId::from_bytes([3; NodeId::BYTE_LENGTH]),
      destination: NodeId::from_bytes([4; NodeId::BYTE_LENGTH]),
      protocol: ProtocolId::Membership,
      kind: MessageKind::Request,
      deadline_unix_ms: 42,
      remaining_hops: 8,
    }
  }

  #[test]
  fn protocol_paths_are_unique_and_versioned() {
    let protocols = [
      ProtocolId::Admission,
      ProtocolId::Registry,
      ProtocolId::Membership,
      ProtocolId::Resource,
      ProtocolId::Extension,
      ProtocolId::Route,
    ];
    let paths: HashSet<_> = protocols.map(ProtocolId::path).into_iter().collect();

    assert_eq!(paths.len(), protocols.len());
    assert!(paths.iter().all(|path| path.ends_with("/1")));
  }

  #[test]
  fn envelope_rejects_payload_above_the_frame_budget() {
    let error = Envelope::new(header(), vec![0; MAX_PAYLOAD_BYTES + 1]).unwrap_err();

    assert!(matches!(error, FrameError::PayloadTooLarge { .. }));
  }
}
