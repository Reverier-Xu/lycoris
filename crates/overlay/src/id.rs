use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

macro_rules! identifier {
  ($name:ident, $length:expr) => {
    #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct $name([u8; $length]);

    impl $name {
      pub const BYTE_LENGTH: usize = $length;

      pub const fn from_bytes(bytes: [u8; $length]) -> Self {
        Self(bytes)
      }

      pub const fn as_bytes(&self) -> &[u8; $length] {
        &self.0
      }
    }

    impl fmt::Debug for $name {
      fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
      }
    }

    impl fmt::Display for $name {
      fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
      }
    }

    impl FromStr for $name {
      type Err = ParseIdentifierError;

      fn from_str(value: &str) -> Result<Self, Self::Err> {
        let expected = $length * 2;
        if value.len() != expected {
          return Err(ParseIdentifierError::Length {
            expected,
            actual: value.len(),
          });
        }
        let mut bytes = [0_u8; $length];
        hex::decode_to_slice(value, &mut bytes)?;
        Ok(Self(bytes))
      }
    }
  };
}

identifier!(ClusterId, 32);
identifier!(NodeId, 32);
identifier!(RecordId, 32);
identifier!(RequestId, 16);

impl ClusterId {
  pub fn from_genesis(node_id: NodeId) -> Self {
    Self(domain_hash(b"lycoris/cluster-id/1", node_id.as_bytes()))
  }
}

impl NodeId {
  pub fn from_initial_public_key(public_key: &[u8]) -> Self {
    Self(domain_hash(b"lycoris/node-id/1", public_key))
  }
}

impl RequestId {
  /// Derive a deterministic, unique-per-`(node, nonce)` request identifier.
  /// Callers keep their own monotonic nonce, so ids never collide across
  /// reboots and never repeat within a node.
  pub fn derive(node_id: NodeId, nonce: u64) -> Self {
    let mut input = Vec::with_capacity(40);
    input.extend_from_slice(node_id.as_bytes());
    input.extend_from_slice(&nonce.to_be_bytes());
    let hash = blake3::hash(&input);
    let mut bytes = [0_u8; Self::BYTE_LENGTH];
    bytes.copy_from_slice(&hash.as_bytes()[..Self::BYTE_LENGTH]);
    Self::from_bytes(bytes)
  }
}

impl RecordId {
  pub(crate) fn from_signed_record(record: &[u8]) -> Self {
    Self(domain_hash(b"lycoris/authorization-record/1", record))
  }
}

fn domain_hash(domain: &[u8], value: &[u8]) -> [u8; 32] {
  let mut hasher = blake3::Hasher::new();
  hasher.update(domain);
  hasher.update(&[0]);
  hasher.update(value);
  hasher.finalize().into()
}

#[derive(Debug, Error)]
pub enum ParseIdentifierError {
  #[error("identifier has {actual} hex characters; expected {expected}")]
  Length { expected: usize, actual: usize },
  #[error("identifier is not valid hexadecimal: {0}")]
  Hex(#[from] hex::FromHexError),
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn identifier_text_round_trip_is_canonical() {
    let id = NodeId::from_bytes([0xAB; NodeId::BYTE_LENGTH]);
    let encoded = id.to_string();

    assert_eq!(encoded.len(), NodeId::BYTE_LENGTH * 2);
    assert_eq!(encoded.parse::<NodeId>().unwrap(), id);
    assert_eq!(format!("{id:?}"), encoded);
  }

  #[test]
  fn identifier_parser_rejects_wrong_length_and_non_hex() {
    assert!(matches!(
      "00".parse::<ClusterId>(),
      Err(ParseIdentifierError::Length { .. })
    ));
    let invalid = "z".repeat(RequestId::BYTE_LENGTH * 2);
    assert!(matches!(
      invalid.parse::<RequestId>(),
      Err(ParseIdentifierError::Hex(_))
    ));
  }
}
