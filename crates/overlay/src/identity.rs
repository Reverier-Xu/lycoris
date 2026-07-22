use std::{fmt, io, path::Path};

use libp2p_identity::{Keypair, PeerId, PublicKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::NodeId;

#[derive(Clone)]
pub struct NodeIdentity {
  node_id: NodeId,
  initial_public_key: Vec<u8>,
  keypair: Keypair,
}

impl fmt::Debug for NodeIdentity {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter
      .debug_struct("NodeIdentity")
      .field("node_id", &self.node_id)
      .field("peer_id", &self.peer_id())
      .finish_non_exhaustive()
  }
}

impl NodeIdentity {
  pub fn generate() -> Self {
    let keypair = Keypair::generate_ed25519();
    let initial_public_key = keypair.public().encode_protobuf();
    let node_id = NodeId::from_initial_public_key(&initial_public_key);
    Self {
      node_id,
      initial_public_key,
      keypair,
    }
  }

  pub fn generate_successor(
    node_id: NodeId, initial_public_key: Vec<u8>,
  ) -> Result<Self, IdentityError> {
    validate_initial_key(node_id, &initial_public_key)?;
    Ok(Self {
      node_id,
      initial_public_key,
      keypair: Keypair::generate_ed25519(),
    })
  }

  pub fn load_or_generate(path: impl AsRef<Path>) -> Result<Self, IdentityError> {
    match std::fs::read(path.as_ref()) {
      Ok(bytes) => Self::decode(&bytes),
      Err(error) if error.kind() == io::ErrorKind::NotFound => {
        let identity = Self::generate();
        identity.save(path)?;
        Ok(identity)
      }
      Err(error) => Err(error.into()),
    }
  }

  pub fn save(&self, path: impl AsRef<Path>) -> Result<(), IdentityError> {
    let document = IdentityDocument {
      node_id: self.node_id,
      initial_public_key: self.initial_public_key.clone(),
      private_key: self.keypair.to_protobuf_encoding()?,
    };
    let bytes = postcard::to_stdvec(&document)?;
    lycoris_core::write_private_file(path, &bytes)?;
    Ok(())
  }

  pub const fn node_id(&self) -> NodeId {
    self.node_id
  }

  pub fn peer_id(&self) -> PeerId {
    self.keypair.public().to_peer_id()
  }

  pub fn peer_id_bytes(&self) -> Vec<u8> {
    self.peer_id().to_bytes()
  }

  pub fn public_key_bytes(&self) -> Vec<u8> {
    self.keypair.public().encode_protobuf()
  }

  pub fn initial_public_key(&self) -> &[u8] {
    &self.initial_public_key
  }

  pub fn keypair(&self) -> &Keypair {
    &self.keypair
  }

  pub fn sign(&self, message: &[u8]) -> Result<Vec<u8>, IdentityError> {
    Ok(self.keypair.sign(message)?)
  }

  pub fn verify(&self, message: &[u8], signature: &[u8]) -> bool {
    self.keypair.public().verify(message, signature)
  }

  fn decode(bytes: &[u8]) -> Result<Self, IdentityError> {
    let document: IdentityDocument = postcard::from_bytes(bytes)?;
    validate_initial_key(document.node_id, &document.initial_public_key)?;
    Ok(Self {
      node_id: document.node_id,
      initial_public_key: document.initial_public_key,
      keypair: Keypair::from_protobuf_encoding(&document.private_key)?,
    })
  }
}

pub(crate) fn decode_public_key(bytes: &[u8]) -> Result<PublicKey, IdentityError> {
  Ok(PublicKey::try_decode_protobuf(bytes)?)
}

fn validate_initial_key(node_id: NodeId, initial_public_key: &[u8]) -> Result<(), IdentityError> {
  let derived = NodeId::from_initial_public_key(initial_public_key);
  if derived != node_id {
    return Err(IdentityError::NodeIdMismatch {
      expected: node_id,
      derived,
    });
  }
  Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct IdentityDocument {
  node_id: NodeId,
  initial_public_key: Vec<u8>,
  private_key: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum IdentityError {
  #[error("identity io failed: {0}")]
  Io(#[from] io::Error),
  #[error("identity serialization failed: {0}")]
  Serialization(#[from] postcard::Error),
  #[error("identity key encoding failed: {0}")]
  KeyEncoding(#[from] libp2p_identity::DecodingError),
  #[error("identity signing failed: {0}")]
  Signing(#[from] libp2p_identity::SigningError),
  #[error("initial public key derives {derived}, not stored node id {expected}")]
  NodeIdMismatch { expected: NodeId, derived: NodeId },
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  #[test]
  fn identity_survives_reload_without_exposing_key_material_in_debug() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("identity.key");
    let first = NodeIdentity::load_or_generate(&path).unwrap();
    let second = NodeIdentity::load_or_generate(&path).unwrap();

    assert_eq!(second.node_id(), first.node_id());
    assert_eq!(second.peer_id(), first.peer_id());
    assert!(!format!("{first:?}").contains("private"));
  }

  #[cfg(unix)]
  #[test]
  fn persisted_identity_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("identity.key");
    NodeIdentity::generate().save(&path).unwrap();

    assert_eq!(
      std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
      0o600
    );
  }

  #[test]
  fn successor_preserves_node_id_but_changes_peer_id() {
    let first = NodeIdentity::generate();
    let second =
      NodeIdentity::generate_successor(first.node_id(), first.initial_public_key().to_vec())
        .unwrap();

    assert_eq!(second.node_id(), first.node_id());
    assert_ne!(second.peer_id(), first.peer_id());
  }

  #[test]
  fn signature_verifies_only_with_the_signing_identity() {
    let signer = NodeIdentity::generate();
    let other = NodeIdentity::generate();
    let signature = signer.sign(b"record").unwrap();

    assert!(signer.verify(b"record", &signature));
    assert!(!other.verify(b"record", &signature));
  }
}
