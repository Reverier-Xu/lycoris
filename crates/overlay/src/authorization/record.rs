use serde::{Deserialize, Serialize};

use crate::{ClusterId, NodeId, NodeIdentity, RecordId, authorization::AuthorizationError};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum AuthorizationKind {
  Admit,
  Rotate,
  Recover,
  Revoke,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum KeyState {
  Active,
  Revoked,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorizationRecord {
  id: RecordId,
  body: AuthorizationBody,
  signature: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct AuthorizationBody {
  pub cluster_id: ClusterId,
  pub node_id: NodeId,
  pub peer_id: Vec<u8>,
  pub public_key: Vec<u8>,
  pub initial_public_key: Vec<u8>,
  pub epoch: u64,
  pub kind: AuthorizationKind,
  pub state: KeyState,
  pub predecessor: Option<RecordId>,
  pub authorizer: Option<RecordId>,
  pub authorization_predecessor: Option<RecordId>,
  pub retires: Option<RetiredAuthorization>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RetiredAuthorization {
  pub identity: RecordId,
  pub action_head: RecordId,
}

impl AuthorizationRecord {
  pub fn genesis(identity: &NodeIdentity) -> Result<(ClusterId, Self), AuthorizationError> {
    let cluster_id = ClusterId::from_genesis(identity.node_id());
    let body = AuthorizationBody::active(cluster_id, identity, 0, AuthorizationKind::Admit);
    Ok((cluster_id, Self::sign(body, identity)?))
  }

  pub fn admit(
    cluster_id: ClusterId, identity: &NodeIdentity, authorizer: &Self, authorizer_head: &Self,
    signer: &NodeIdentity,
  ) -> Result<Self, AuthorizationError> {
    let mut body = AuthorizationBody::active(cluster_id, identity, 0, AuthorizationKind::Admit);
    body.set_authorizer(authorizer, authorizer_head);
    Self::sign(body, signer)
  }

  pub fn rotate(
    previous: &Self, previous_action_head: &Self, successor: &NodeIdentity, signer: &NodeIdentity,
  ) -> Result<Self, AuthorizationError> {
    let mut body = AuthorizationBody::active(
      previous.cluster_id(),
      successor,
      previous.epoch() + 1,
      AuthorizationKind::Rotate,
    );
    body.predecessor = Some(previous.id);
    body.set_authorizer(previous, previous_action_head);
    Self::sign(body, signer)
  }

  pub fn recover(
    previous: &Self, previous_action_head: &Self, successor: &NodeIdentity, authorizer: &Self,
    authorizer_head: &Self, signer: &NodeIdentity,
  ) -> Result<Self, AuthorizationError> {
    let mut body = AuthorizationBody::active(
      previous.cluster_id(),
      successor,
      previous.epoch() + 1,
      AuthorizationKind::Recover,
    );
    body.predecessor = Some(previous.id);
    body.set_authorizer(authorizer, authorizer_head);
    body.retires = Some(RetiredAuthorization {
      identity: previous.id,
      action_head: previous_action_head.id,
    });
    Self::sign(body, signer)
  }

  pub fn revoke(
    previous: &Self, previous_action_head: &Self, authorizer: &Self, authorizer_head: &Self,
    signer: &NodeIdentity,
  ) -> Result<Self, AuthorizationError> {
    let mut body = AuthorizationBody {
      cluster_id: previous.cluster_id(),
      node_id: previous.node_id(),
      peer_id: previous.peer_id().to_vec(),
      public_key: previous.public_key().to_vec(),
      initial_public_key: previous.initial_public_key().to_vec(),
      epoch: previous.epoch() + 1,
      kind: AuthorizationKind::Revoke,
      state: KeyState::Revoked,
      predecessor: Some(previous.id),
      authorizer: None,
      authorization_predecessor: None,
      retires: Some(RetiredAuthorization {
        identity: previous.id,
        action_head: previous_action_head.id,
      }),
    };
    body.set_authorizer(authorizer, authorizer_head);
    Self::sign(body, signer)
  }

  pub const fn id(&self) -> RecordId {
    self.id
  }

  pub const fn cluster_id(&self) -> ClusterId {
    self.body.cluster_id
  }

  pub const fn node_id(&self) -> NodeId {
    self.body.node_id
  }

  pub fn peer_id(&self) -> &[u8] {
    &self.body.peer_id
  }

  pub fn public_key(&self) -> &[u8] {
    &self.body.public_key
  }

  pub fn initial_public_key(&self) -> &[u8] {
    &self.body.initial_public_key
  }

  pub const fn epoch(&self) -> u64 {
    self.body.epoch
  }

  pub const fn kind(&self) -> AuthorizationKind {
    self.body.kind
  }

  pub const fn state(&self) -> KeyState {
    self.body.state
  }

  pub const fn predecessor(&self) -> Option<RecordId> {
    self.body.predecessor
  }

  pub const fn authorizer(&self) -> Option<RecordId> {
    self.body.authorizer
  }

  pub const fn authorization_predecessor(&self) -> Option<RecordId> {
    self.body.authorization_predecessor
  }

  pub(crate) const fn retires(&self) -> Option<RetiredAuthorization> {
    self.body.retires
  }

  pub(crate) fn signing_bytes(&self) -> Result<Vec<u8>, AuthorizationError> {
    Ok(postcard::to_stdvec(&self.body)?)
  }

  pub(crate) fn expected_id(&self) -> Result<RecordId, AuthorizationError> {
    record_id(&self.body, &self.signature)
  }

  pub(crate) fn signature(&self) -> &[u8] {
    &self.signature
  }

  fn sign(body: AuthorizationBody, signer: &NodeIdentity) -> Result<Self, AuthorizationError> {
    let signing_bytes = postcard::to_stdvec(&body)?;
    let signature = signer.sign(&signing_bytes)?;
    let id = record_id(&body, &signature)?;
    Ok(Self {
      id,
      body,
      signature,
    })
  }
}

impl AuthorizationBody {
  fn active(
    cluster_id: ClusterId, identity: &NodeIdentity, epoch: u64, kind: AuthorizationKind,
  ) -> Self {
    Self {
      cluster_id,
      node_id: identity.node_id(),
      peer_id: identity.peer_id_bytes(),
      public_key: identity.public_key_bytes(),
      initial_public_key: identity.initial_public_key().to_vec(),
      epoch,
      kind,
      state: KeyState::Active,
      predecessor: None,
      authorizer: None,
      authorization_predecessor: None,
      retires: None,
    }
  }

  fn set_authorizer(&mut self, identity: &AuthorizationRecord, head: &AuthorizationRecord) {
    self.authorizer = Some(identity.id);
    self.authorization_predecessor = Some(head.id);
  }
}

fn record_id(body: &AuthorizationBody, signature: &[u8]) -> Result<RecordId, AuthorizationError> {
  let bytes = postcard::to_stdvec(&(body, signature))?;
  Ok(RecordId::from_signed_record(&bytes))
}
