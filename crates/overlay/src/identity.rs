use std::{fmt, io, path::Path};

use libp2p_identity::{Keypair, PeerId, PublicKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::NodeId;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicIdentity {
  node_id: NodeId,
  peer_id: Vec<u8>,
  public_key: Vec<u8>,
  initial_public_key: Vec<u8>,
}

impl PublicIdentity {
  pub fn new(
    node_id: NodeId, peer_id: Vec<u8>, public_key: Vec<u8>, initial_public_key: Vec<u8>,
  ) -> Result<Self, IdentityError> {
    validate_initial_key(node_id, &initial_public_key)?;
    let decoded = decode_public_key(&public_key)?;
    if decoded.to_peer_id().to_bytes() != peer_id {
      return Err(IdentityError::PeerIdMismatch);
    }
    Ok(Self {
      node_id,
      peer_id,
      public_key,
      initial_public_key,
    })
  }

  pub const fn node_id(&self) -> NodeId {
    self.node_id
  }

  pub fn peer_id(&self) -> &[u8] {
    &self.peer_id
  }

  pub fn public_key(&self) -> &[u8] {
    &self.public_key
  }

  pub fn initial_public_key(&self) -> &[u8] {
    &self.initial_public_key
  }

  pub fn is_initial_identity(&self) -> bool {
    self.public_key == self.initial_public_key
  }
}

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

  pub fn load(path: impl AsRef<Path>) -> Result<Self, IdentityError> {
    Self::decode(&std::fs::read(path)?)
  }

  pub fn load_or_generate(path: impl AsRef<Path>) -> Result<Self, IdentityError> {
    match Self::load(path.as_ref()) {
      Ok(identity) => Ok(identity),
      Err(IdentityError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
        Self::publish_candidate(path.as_ref(), Self::generate()).map(|(identity, _)| identity)
      }
      Err(error) => Err(error),
    }
  }

  pub fn save(&self, path: impl AsRef<Path>) -> Result<(), IdentityError> {
    lycoris_core::write_private_file(path, &self.encode_document()?)?;
    Ok(())
  }

  fn publish_candidate(
    path: &Path, candidate: Self,
  ) -> Result<(Self, lycoris_core::PrivateFileCreate), IdentityError> {
    let publication =
      lycoris_core::write_private_file_if_absent(path, &candidate.encode_document()?)?;
    let identity = match publication {
      lycoris_core::PrivateFileCreate::Created => candidate,
      lycoris_core::PrivateFileCreate::AlreadyExists => Self::load(path)?,
    };
    Ok((identity, publication))
  }

  fn encode_document(&self) -> Result<Vec<u8>, IdentityError> {
    let document = IdentityDocument {
      node_id: self.node_id,
      initial_public_key: self.initial_public_key.clone(),
      private_key: self.keypair.to_protobuf_encoding()?,
    };
    Ok(postcard::to_stdvec(&document)?)
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

  pub fn public_identity(&self) -> PublicIdentity {
    PublicIdentity {
      node_id: self.node_id,
      peer_id: self.peer_id_bytes(),
      public_key: self.public_key_bytes(),
      initial_public_key: self.initial_public_key.clone(),
    }
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
  decode_public_key(initial_public_key)?;
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
  #[error("peer id does not match the identity public key")]
  PeerIdMismatch,
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

  #[test]
  fn concurrent_generation_returns_one_persisted_winner() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("identity.key");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
      .map(|_| {
        let barrier = barrier.clone();
        let path = path.clone();
        std::thread::spawn(move || {
          barrier.wait();
          NodeIdentity::load_or_generate(path)
            .unwrap()
            .public_identity()
        })
      })
      .collect();
    let identities: Vec<_> = threads
      .into_iter()
      .map(|thread| thread.join().unwrap())
      .collect();

    assert!(identities.iter().all(|identity| identity == &identities[0]));
    assert_eq!(
      NodeIdentity::load(&path).unwrap().public_identity(),
      identities[0]
    );
  }

  #[test]
  fn no_clobber_loser_strictly_loads_the_creation_winner() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("identity.key");
    let winner = NodeIdentity::generate();
    let loser = NodeIdentity::generate();

    let (created, first_outcome) = NodeIdentity::publish_candidate(&path, winner.clone()).unwrap();
    let (selected, second_outcome) = NodeIdentity::publish_candidate(&path, loser).unwrap();

    assert_eq!(first_outcome, lycoris_core::PrivateFileCreate::Created);
    assert_eq!(
      second_outcome,
      lycoris_core::PrivateFileCreate::AlreadyExists
    );
    assert_eq!(created.public_identity(), winner.public_identity());
    assert_eq!(selected.public_identity(), winner.public_identity());
    assert_eq!(
      NodeIdentity::load(path).unwrap().public_identity(),
      winner.public_identity()
    );
  }

  #[test]
  fn no_clobber_loser_propagates_a_malformed_winner() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("identity.key");
    let malformed = b"malformed winner";
    std::fs::write(&path, malformed).unwrap();

    assert!(NodeIdentity::publish_candidate(&path, NodeIdentity::generate()).is_err());
    assert_eq!(std::fs::read(path).unwrap(), malformed);
  }

  #[test]
  fn malformed_identity_is_never_replaced() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("identity.key");
    let malformed = b"not an identity";
    std::fs::write(&path, malformed).unwrap();

    assert!(NodeIdentity::load_or_generate(&path).is_err());
    assert_eq!(std::fs::read(path).unwrap(), malformed);
  }

  #[test]
  fn save_atomically_replaces_an_existing_identity() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("identity.key");
    let first = NodeIdentity::generate();
    let second = NodeIdentity::generate();
    first.save(&path).unwrap();
    second.save(&path).unwrap();

    assert_eq!(
      NodeIdentity::load(path).unwrap().public_identity(),
      second.public_identity()
    );
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

  #[test]
  fn public_identity_validates_the_peer_binding() {
    let identity = NodeIdentity::generate();
    let public = identity.public_identity();
    let validated = PublicIdentity::new(
      public.node_id(),
      public.peer_id().to_vec(),
      public.public_key().to_vec(),
      public.initial_public_key().to_vec(),
    )
    .unwrap();
    assert_eq!(validated, public);

    assert!(matches!(
      PublicIdentity::new(
        public.node_id(),
        NodeIdentity::generate().peer_id_bytes(),
        public.public_key().to_vec(),
        public.initial_public_key().to_vec(),
      ),
      Err(IdentityError::PeerIdMismatch)
    ));
  }
}
