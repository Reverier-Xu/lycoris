use std::collections::{BTreeMap, BTreeSet};

use libp2p_identity::PeerId;

use crate::{
  ClusterId, NodeId, RecordId,
  authorization::{AuthorizationError, AuthorizationKind, AuthorizationRecord, KeyState},
  identity::decode_public_key,
};

#[derive(Debug, Clone)]
pub struct AuthorizationRegistry {
  cluster_id: ClusterId,
  records: BTreeMap<RecordId, AuthorizationRecord>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AuthorizationStatus<'a> {
  Unknown,
  Active(&'a AuthorizationRecord),
  Revoked(&'a AuthorizationRecord),
  Conflicted(Vec<RecordId>),
}

impl AuthorizationRegistry {
  pub fn new(cluster_id: ClusterId) -> Self {
    Self {
      cluster_id,
      records: BTreeMap::new(),
    }
  }

  pub fn from_records(
    cluster_id: ClusterId, records: impl IntoIterator<Item = AuthorizationRecord>,
  ) -> Result<Self, AuthorizationError> {
    let mut registry = Self::new(cluster_id);
    registry.merge(records)?;
    Ok(registry)
  }

  pub const fn cluster_id(&self) -> ClusterId {
    self.cluster_id
  }

  pub fn insert(&mut self, record: AuthorizationRecord) -> Result<bool, AuthorizationError> {
    if self.records.contains_key(&record.id()) {
      return Ok(false);
    }
    self.validate(&record)?;
    self.records.insert(record.id(), record);
    Ok(true)
  }

  pub fn merge(
    &mut self, records: impl IntoIterator<Item = AuthorizationRecord>,
  ) -> Result<usize, AuthorizationError> {
    let mut pending: Vec<_> = records.into_iter().collect();
    let mut inserted = 0;
    while !pending.is_empty() {
      let before = pending.len();
      let mut deferred = Vec::new();
      let mut missing = None;
      for record in pending {
        match self.insert(record.clone()) {
          Ok(was_inserted) => inserted += usize::from(was_inserted),
          Err(AuthorizationError::MissingDependency(id)) => {
            missing = Some(id);
            deferred.push(record);
          }
          Err(error) => return Err(error),
        }
      }
      if deferred.len() == before {
        return Err(AuthorizationError::MissingDependency(missing.ok_or(
          AuthorizationError::InvalidRecord("unresolvable authorization graph"),
        )?));
      }
      pending = deferred;
    }
    Ok(inserted)
  }

  pub fn records(&self) -> Vec<AuthorizationRecord> {
    self.records.values().cloned().collect()
  }

  pub fn status(&self, node_id: NodeId) -> AuthorizationStatus<'_> {
    let roots: Vec<_> = self
      .records
      .values()
      .filter(|record| record.node_id() == node_id && record.predecessor().is_none())
      .collect();
    let [root] = roots.as_slice() else {
      return if roots.is_empty() {
        AuthorizationStatus::Unknown
      } else {
        AuthorizationStatus::Conflicted(ids(&roots))
      };
    };
    let mut current = *root;

    loop {
      if !self.record_is_effective(current) {
        return AuthorizationStatus::Conflicted(vec![current.id()]);
      }
      let children: Vec<_> = self
        .records
        .values()
        .filter(|record| record.predecessor() == Some(current.id()))
        .collect();
      match children.as_slice() {
        [] => {
          return match current.state() {
            KeyState::Active => AuthorizationStatus::Active(current),
            KeyState::Revoked => AuthorizationStatus::Revoked(current),
          };
        }
        [next] => current = next,
        conflicts => return AuthorizationStatus::Conflicted(ids(conflicts)),
      }
    }
  }

  pub fn node_for_peer(&self, peer_id: &PeerId) -> Option<NodeId> {
    self.records.values().find_map(|record| {
      let AuthorizationStatus::Active(active) = self.status(record.node_id()) else {
        return None;
      };
      (active.peer_id() == peer_id.to_bytes()).then_some(active.node_id())
    })
  }

  pub fn active_record_for_node(&self, node_id: NodeId) -> Option<&AuthorizationRecord> {
    let AuthorizationStatus::Active(record) = self.status(node_id) else {
      return None;
    };
    Some(record)
  }

  pub fn active_peer_for_node(&self, node_id: NodeId) -> Option<PeerId> {
    let record = self.active_record_for_node(node_id)?;
    PeerId::from_bytes(record.peer_id()).ok()
  }

  pub fn authorizer_head(
    &self, identity: &AuthorizationRecord,
  ) -> Result<&AuthorizationRecord, AuthorizationError> {
    let identity_id = identity.id();
    let mut current = self.record(identity_id)?;
    loop {
      let successors = self.action_successors(identity_id, current.id());
      match successors.as_slice() {
        [] => return Ok(current),
        [next] if self.terminates_identity(next, identity_id) => {
          return Err(AuthorizationError::RetiredAuthorizer(identity_id));
        }
        [next] => current = next,
        _ => return Err(AuthorizationError::ConflictedAuthorizer(identity_id)),
      }
    }
  }

  fn validate(&self, record: &AuthorizationRecord) -> Result<(), AuthorizationError> {
    if record.cluster_id() != self.cluster_id {
      return Err(AuthorizationError::ForeignCluster {
        expected: self.cluster_id,
        actual: record.cluster_id(),
      });
    }
    if record.expected_id()? != record.id() {
      return Err(AuthorizationError::InvalidRecord(
        "record id does not match its signed body",
      ));
    }
    let public_key = decode_public_key(record.public_key())?;
    if public_key.to_peer_id().to_bytes() != record.peer_id() {
      return Err(AuthorizationError::InvalidRecord(
        "peer id does not match public key",
      ));
    }
    if NodeId::from_initial_public_key(record.initial_public_key()) != record.node_id() {
      return Err(AuthorizationError::InvalidRecord(
        "initial public key does not match node id",
      ));
    }

    self.validate_lineage(record)?;
    let signing_key = match (record.authorizer(), record.authorization_predecessor()) {
      (Some(identity_id), Some(action_id)) => {
        let identity = self.record(identity_id)?;
        self.validate_action_predecessor(identity, action_id)?;
        decode_public_key(identity.public_key())?
      }
      (None, None) => public_key,
      _ => {
        return Err(AuthorizationError::InvalidRecord(
          "incomplete authorizer chain",
        ));
      }
    };
    if !signing_key.verify(&record.signing_bytes()?, record.signature()) {
      return Err(AuthorizationError::InvalidSignature(record.id()));
    }
    Ok(())
  }

  fn validate_lineage(&self, record: &AuthorizationRecord) -> Result<(), AuthorizationError> {
    match record.predecessor() {
      None => self.validate_admission(record),
      Some(predecessor_id) => {
        let predecessor = self.record(predecessor_id)?;
        if predecessor.node_id() != record.node_id()
          || predecessor.cluster_id() != record.cluster_id()
          || predecessor.epoch().checked_add(1) != Some(record.epoch())
        {
          return Err(AuthorizationError::InvalidRecord(
            "invalid predecessor chain",
          ));
        }
        self.validate_successor(record, predecessor)
      }
    }
  }

  fn validate_admission(&self, record: &AuthorizationRecord) -> Result<(), AuthorizationError> {
    if record.kind() != AuthorizationKind::Admit
      || record.state() != KeyState::Active
      || record.epoch() != 0
      || record.retires().is_some()
      || record.public_key() != record.initial_public_key()
    {
      return Err(AuthorizationError::InvalidRecord(
        "invalid admission record",
      ));
    }
    if record.authorizer().is_none()
      && ClusterId::from_genesis(record.node_id()) != record.cluster_id()
    {
      return Err(AuthorizationError::InvalidRecord(
        "invalid self-signed genesis",
      ));
    }
    Ok(())
  }

  fn validate_successor(
    &self, record: &AuthorizationRecord, predecessor: &AuthorizationRecord,
  ) -> Result<(), AuthorizationError> {
    let retires_predecessor = record
      .retires()
      .is_some_and(|retired| retired.identity == predecessor.id());
    match (record.kind(), record.state()) {
      (AuthorizationKind::Rotate, KeyState::Active)
        if record.authorizer() == Some(predecessor.id()) && record.retires().is_none() =>
      {
        Ok(())
      }
      (AuthorizationKind::Recover, KeyState::Active) if retires_predecessor => {
        self.validate_retirement(record)
      }
      (AuthorizationKind::Revoke, KeyState::Revoked) if retires_predecessor => {
        self.validate_retirement(record)
      }
      _ => Err(AuthorizationError::InvalidRecord(
        "authorization kind and state disagree",
      )),
    }
  }

  fn validate_retirement(&self, record: &AuthorizationRecord) -> Result<(), AuthorizationError> {
    let retired = record.retires().ok_or(AuthorizationError::InvalidRecord(
      "missing retired authorization chain",
    ))?;
    let identity = self.record(retired.identity)?;
    self.validate_action_predecessor(identity, retired.action_head)
  }

  fn validate_action_predecessor(
    &self, identity: &AuthorizationRecord, action_id: RecordId,
  ) -> Result<(), AuthorizationError> {
    if action_id == identity.id() {
      return Ok(());
    }
    let action = self.record(action_id)?;
    if action.authorizer() != Some(identity.id()) {
      return Err(AuthorizationError::InvalidRecord(
        "authorization action belongs to another identity",
      ));
    }
    Ok(())
  }

  fn record_is_effective(&self, record: &AuthorizationRecord) -> bool {
    let signed_on_unique_chain = match record.authorizer() {
      Some(identity_id) => {
        self.action_path_contains(identity_id, record.id())
          && self
            .records
            .get(&identity_id)
            .is_some_and(|identity| self.identity_record_is_effective(identity))
      }
      None => true,
    };
    signed_on_unique_chain
      && record
        .retires()
        .is_none_or(|retired| self.action_path_contains(retired.identity, record.id()))
  }

  fn identity_record_is_effective(&self, identity: &AuthorizationRecord) -> bool {
    if identity.state() != KeyState::Active {
      return false;
    }
    let roots: Vec<_> = self
      .records
      .values()
      .filter(|record| record.node_id() == identity.node_id() && record.predecessor().is_none())
      .collect();
    let [root] = roots.as_slice() else {
      return false;
    };
    let mut current = *root;
    loop {
      if !self.record_is_effective(current) {
        return false;
      }
      if current.id() == identity.id() {
        return true;
      }
      let children: Vec<_> = self
        .records
        .values()
        .filter(|record| record.predecessor() == Some(current.id()))
        .collect();
      let [next] = children.as_slice() else {
        return false;
      };
      current = next;
    }
  }

  fn action_path_contains(&self, identity: RecordId, target: RecordId) -> bool {
    let mut current = identity;
    if current == target {
      return true;
    }
    loop {
      let successors = self.action_successors(identity, current);
      let [next] = successors.as_slice() else {
        return false;
      };
      if next.id() == target {
        return true;
      }
      if self.terminates_identity(next, identity) {
        return false;
      }
      current = next.id();
    }
  }

  fn action_successors(&self, identity: RecordId, action: RecordId) -> Vec<&AuthorizationRecord> {
    let ids: BTreeSet<_> = self
      .records
      .values()
      .filter(|record| {
        (record.authorizer() == Some(identity)
          && record.authorization_predecessor() == Some(action))
          || record
            .retires()
            .is_some_and(|retired| retired.identity == identity && retired.action_head == action)
      })
      .map(AuthorizationRecord::id)
      .collect();
    ids
      .into_iter()
      .filter_map(|id| self.records.get(&id))
      .collect()
  }

  fn terminates_identity(&self, record: &AuthorizationRecord, identity: RecordId) -> bool {
    (record.kind() == AuthorizationKind::Rotate
      && record.predecessor() == Some(identity)
      && record.authorizer() == Some(identity))
      || record
        .retires()
        .is_some_and(|retired| retired.identity == identity)
  }

  fn record(&self, id: RecordId) -> Result<&AuthorizationRecord, AuthorizationError> {
    self
      .records
      .get(&id)
      .ok_or(AuthorizationError::MissingDependency(id))
  }
}

fn ids(records: &[&AuthorizationRecord]) -> Vec<RecordId> {
  records.iter().map(|record| record.id()).collect()
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{AuthorizationRecord, NodeIdentity};

  fn genesis() -> (NodeIdentity, AuthorizationRecord, AuthorizationRegistry) {
    let identity = NodeIdentity::generate();
    let (cluster_id, record) = AuthorizationRecord::genesis(&identity).unwrap();
    let registry = AuthorizationRegistry::from_records(cluster_id, [record.clone()]).unwrap();
    (identity, record, registry)
  }

  fn successor(identity: &NodeIdentity) -> NodeIdentity {
    NodeIdentity::generate_successor(identity.node_id(), identity.initial_public_key().to_vec())
      .unwrap()
  }

  fn admit_member(
    cluster_id: ClusterId, member: &NodeIdentity, sponsor_record: &AuthorizationRecord,
    sponsor: &NodeIdentity,
  ) -> AuthorizationRecord {
    AuthorizationRecord::admit(
      cluster_id,
      &member.public_identity(),
      sponsor_record,
      sponsor_record,
      sponsor,
    )
    .unwrap()
  }

  fn insert_admitted_member(
    registry: &mut AuthorizationRegistry, sponsor_record: &AuthorizationRecord,
    sponsor: &NodeIdentity,
  ) -> (NodeIdentity, AuthorizationRecord) {
    let member = NodeIdentity::generate();
    let admission = admit_member(registry.cluster_id(), &member, sponsor_record, sponsor);
    registry.insert(admission.clone()).unwrap();
    (member, admission)
  }

  #[test]
  fn forged_genesis_for_an_existing_node_is_rejected() {
    let (identity, record, _) = genesis();
    let forged_identity = successor(&identity);
    let (_, forged) = AuthorizationRecord::genesis(&forged_identity).unwrap();
    let mut registry = AuthorizationRegistry::new(record.cluster_id());

    assert!(matches!(
      registry.insert(forged),
      Err(AuthorizationError::InvalidRecord(
        "invalid admission record"
      ))
    ));
  }

  #[test]
  fn sponsored_admission_must_use_the_initial_identity() {
    let (sponsor, sponsor_record, mut registry) = genesis();
    let initial = NodeIdentity::generate();
    let successor = successor(&initial);
    let admission = AuthorizationRecord::admit(
      registry.cluster_id(),
      &successor.public_identity(),
      &sponsor_record,
      &sponsor_record,
      &sponsor,
    )
    .unwrap();

    assert!(matches!(
      registry.insert(admission),
      Err(AuthorizationError::InvalidRecord(
        "invalid admission record"
      ))
    ));
  }

  #[test]
  fn rotation_replaces_the_peer_without_changing_the_node() {
    let (identity, record, mut registry) = genesis();
    let successor = successor(&identity);
    let rotation = AuthorizationRecord::rotate(&record, &record, &successor, &identity).unwrap();
    registry.insert(rotation).unwrap();

    assert_eq!(
      registry.node_for_peer(&successor.peer_id()),
      Some(identity.node_id())
    );
    assert_eq!(
      registry.active_peer_for_node(identity.node_id()),
      Some(successor.peer_id())
    );
    assert_eq!(registry.node_for_peer(&identity.peer_id()), None);
  }

  #[test]
  fn concurrent_successors_fail_closed_as_a_conflict() {
    let (identity, record, mut registry) = genesis();
    for _ in 0..2 {
      let rotation =
        AuthorizationRecord::rotate(&record, &record, &successor(&identity), &identity).unwrap();
      registry.insert(rotation).unwrap();
    }

    assert!(matches!(
      registry.status(identity.node_id()),
      AuthorizationStatus::Conflicted(records) if records.len() == 2
    ));
  }

  #[test]
  fn unordered_merge_resolves_both_causal_chains() {
    let (identity, record, _) = genesis();
    let successor = successor(&identity);
    let rotation = AuthorizationRecord::rotate(&record, &record, &successor, &identity).unwrap();
    let mut registry = AuthorizationRegistry::new(record.cluster_id());

    registry.merge([rotation, record]).unwrap();

    assert_eq!(
      registry.node_for_peer(&successor.peer_id()),
      Some(identity.node_id())
    );
  }

  #[test]
  fn signature_from_a_different_key_is_rejected() {
    let (identity, record, mut registry) = genesis();
    let stranger = NodeIdentity::generate();
    let rotation =
      AuthorizationRecord::rotate(&record, &record, &successor(&identity), &stranger).unwrap();

    assert!(matches!(
      registry.insert(rotation),
      Err(AuthorizationError::InvalidSignature(_))
    ));
  }

  #[test]
  fn historical_admission_survives_sponsor_rotation() {
    let (sponsor, sponsor_record, mut registry) = genesis();
    let (member, admission) = insert_admitted_member(&mut registry, &sponsor_record, &sponsor);
    let rotation =
      AuthorizationRecord::rotate(&sponsor_record, &admission, &successor(&sponsor), &sponsor)
        .unwrap();
    registry.insert(rotation).unwrap();

    assert_eq!(
      registry.node_for_peer(&member.peer_id()),
      Some(member.node_id())
    );
  }

  #[test]
  fn old_key_cannot_admit_after_rotation() {
    let (sponsor, sponsor_record, mut registry) = genesis();
    let rotated = successor(&sponsor);
    let rotation =
      AuthorizationRecord::rotate(&sponsor_record, &sponsor_record, &rotated, &sponsor).unwrap();
    registry.insert(rotation.clone()).unwrap();
    let stranger = NodeIdentity::generate();
    let stale_admission = AuthorizationRecord::admit(
      registry.cluster_id(),
      &stranger.public_identity(),
      &sponsor_record,
      &rotation,
      &sponsor,
    )
    .unwrap();
    registry.insert(stale_admission).unwrap();

    assert!(matches!(
      registry.status(stranger.node_id()),
      AuthorizationStatus::Conflicted(_)
    ));
    assert_eq!(registry.node_for_peer(&stranger.peer_id()), None);

    let downstream = NodeIdentity::generate();
    let stale_record = registry
      .records()
      .into_iter()
      .find(|record| record.node_id() == stranger.node_id())
      .unwrap();
    let laundering = AuthorizationRecord::admit(
      registry.cluster_id(),
      &downstream.public_identity(),
      &stale_record,
      &stale_record,
      &stranger,
    )
    .unwrap();
    registry.insert(laundering).unwrap();
    assert_eq!(registry.node_for_peer(&downstream.peer_id()), None);
  }

  #[test]
  fn old_key_branch_after_recovery_fails_closed() {
    let (sponsor, sponsor_record, mut registry) = genesis();
    let (member, admission) = insert_admitted_member(&mut registry, &sponsor_record, &sponsor);
    let recovered = successor(&member);
    let recovery = AuthorizationRecord::recover(
      &admission,
      &admission,
      &recovered,
      &sponsor_record,
      &admission,
      &sponsor,
    )
    .unwrap();
    registry.insert(recovery.clone()).unwrap();
    assert_eq!(
      registry.node_for_peer(&recovered.peer_id()),
      Some(member.node_id())
    );

    let stranger = NodeIdentity::generate();
    let stale_admission = AuthorizationRecord::admit(
      registry.cluster_id(),
      &stranger.public_identity(),
      &admission,
      &admission,
      &member,
    )
    .unwrap();
    registry.insert(stale_admission).unwrap();

    assert!(matches!(
      registry.status(member.node_id()),
      AuthorizationStatus::Conflicted(_)
    ));
    assert_eq!(registry.node_for_peer(&recovered.peer_id()), None);
    assert_eq!(registry.node_for_peer(&stranger.peer_id()), None);
  }

  #[test]
  fn revocation_removes_the_peer_and_cannot_authorize_downstream() {
    let (identity, record, mut registry) = genesis();
    let revocation =
      AuthorizationRecord::revoke(&record, &record, &record, &record, &identity).unwrap();
    registry.insert(revocation.clone()).unwrap();

    assert!(matches!(
      registry.status(identity.node_id()),
      AuthorizationStatus::Revoked(_)
    ));
    assert_eq!(registry.node_for_peer(&identity.peer_id()), None);

    let downstream = NodeIdentity::generate();
    let laundering = AuthorizationRecord::admit(
      registry.cluster_id(),
      &downstream.public_identity(),
      &revocation,
      &revocation,
      &identity,
    )
    .unwrap();
    registry.insert(laundering).unwrap();
    assert_eq!(registry.node_for_peer(&downstream.peer_id()), None);
  }
}
