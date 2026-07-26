use std::collections::BTreeMap;

use lycoris_core::ClusterKey;
use ring::{
  hmac,
  rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
  AuthorizationError, AuthorizationRecord, AuthorizationRegistry, ClusterId, IdentityError, NodeId,
  NodeIdentity, PROTOCOL_VERSION, PeerId, PublicIdentity,
};

pub const ADMISSION_NONCE_BYTES: usize = 32;
const MAX_QUARANTINED: usize = 128;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdmissionCandidate {
  identity: PublicIdentity,
  nonce: [u8; ADMISSION_NONCE_BYTES],
}

impl AdmissionCandidate {
  pub fn new(identity: &NodeIdentity) -> Result<Self, AdmissionError> {
    Self::with_nonce(identity.public_identity(), random_nonce()?)
  }

  pub fn with_nonce(
    identity: PublicIdentity, nonce: [u8; ADMISSION_NONCE_BYTES],
  ) -> Result<Self, AdmissionError> {
    let identity = PublicIdentity::new(
      identity.node_id(),
      identity.peer_id().to_vec(),
      identity.public_key().to_vec(),
      identity.initial_public_key().to_vec(),
    )?;
    if !identity.is_initial_identity() {
      return Err(AdmissionError::NonInitialIdentity(identity.node_id()));
    }
    Ok(Self { identity, nonce })
  }

  pub const fn identity(&self) -> &PublicIdentity {
    &self.identity
  }

  pub const fn nonce(&self) -> &[u8; ADMISSION_NONCE_BYTES] {
    &self.nonce
  }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdmissionChallenge {
  cluster_id: ClusterId,
  sponsor_peer_id: Vec<u8>,
  nonce: [u8; ADMISSION_NONCE_BYTES],
}

impl AdmissionChallenge {
  pub const fn cluster_id(&self) -> ClusterId {
    self.cluster_id
  }

  pub fn sponsor_peer_id(&self) -> &[u8] {
    &self.sponsor_peer_id
  }

  pub const fn nonce(&self) -> &[u8; ADMISSION_NONCE_BYTES] {
    &self.nonce
  }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct JoinProof {
  candidate: AdmissionCandidate,
  challenge: AdmissionChallenge,
  mac: Vec<u8>,
}

impl JoinProof {
  pub fn create(
    join_key: &ClusterKey, candidate: AdmissionCandidate, challenge: AdmissionChallenge,
  ) -> Result<Self, AdmissionError> {
    let transcript = proof_transcript(&candidate, &challenge)?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, join_key.as_bytes());
    let mac = hmac::sign(&key, &transcript);
    Ok(Self {
      candidate,
      challenge,
      mac: mac.as_ref().to_vec(),
    })
  }

  fn verify(&self, join_key: &ClusterKey) -> Result<(), AdmissionError> {
    let transcript = proof_transcript(&self.candidate, &self.challenge)?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, join_key.as_bytes());
    hmac::verify(&key, &transcript, &self.mac).map_err(|_| AdmissionError::InvalidJoinProof)
  }

  pub const fn candidate(&self) -> &AdmissionCandidate {
    &self.candidate
  }

  pub const fn challenge(&self) -> &AdmissionChallenge {
    &self.challenge
  }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnrollmentOutcome {
  record: AuthorizationRecord,
  records: Vec<AuthorizationRecord>,
}

impl EnrollmentOutcome {
  pub const fn record(&self) -> &AuthorizationRecord {
    &self.record
  }

  pub fn records(&self) -> &[AuthorizationRecord] {
    &self.records
  }
}

/// Bounded admission wire request carried by quarantined overlay channels.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum AdmissionRequest {
  Begin(AdmissionCandidate),
  Prove(JoinProof),
}

/// The admitted record plus the sponsor's full registry checkpoint, so the
/// joiner can merge authorization state in one round.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdmissionOutcome {
  record: AuthorizationRecord,
  records: Vec<AuthorizationRecord>,
}

impl AdmissionOutcome {
  pub fn new(record: AuthorizationRecord, records: Vec<AuthorizationRecord>) -> Self {
    Self { record, records }
  }

  pub const fn record(&self) -> &AuthorizationRecord {
    &self.record
  }

  pub fn records(&self) -> &[AuthorizationRecord] {
    &self.records
  }
}

/// Sponsor reply to an [`AdmissionRequest`].
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum AdmissionResponse {
  Challenge(AdmissionChallenge),
  Admitted(Box<AdmissionOutcome>),
  Rejected(String),
}

#[derive(Debug)]
pub struct Enrollment {
  registry: AuthorizationRegistry,
  join_key: Option<ClusterKey>,
  quarantined: BTreeMap<NodeId, PendingAdmission>,
}

impl Enrollment {
  pub fn new(registry: AuthorizationRegistry, join_key: Option<ClusterKey>) -> Self {
    Self {
      registry,
      join_key,
      quarantined: BTreeMap::new(),
    }
  }

  pub const fn registry(&self) -> &AuthorizationRegistry {
    &self.registry
  }

  pub fn into_registry(self) -> AuthorizationRegistry {
    self.registry
  }

  /// Merge a registry checkpoint received from a peer, returning the number
  /// of records that changed the local registry.
  pub fn merge_checkpoint(
    &mut self, records: Vec<AuthorizationRecord>,
  ) -> Result<usize, AuthorizationError> {
    self.registry.merge(records)
  }

  pub fn quarantined(&self) -> Vec<AdmissionCandidate> {
    self
      .quarantined
      .values()
      .map(|pending| pending.candidate.clone())
      .collect()
  }

  pub fn begin(
    &mut self, candidate: AdmissionCandidate, authenticated_peer: &PeerId, sponsor: &NodeIdentity,
  ) -> Result<AdmissionChallenge, AdmissionError> {
    self.ensure_sponsor(sponsor)?;
    ensure_authenticated_peer(&candidate, authenticated_peer)?;
    let node_id = candidate.identity().node_id();
    if !matches!(
      self.registry.status(node_id),
      crate::AuthorizationStatus::Unknown
    ) {
      return Err(AdmissionError::IdentityConflict(node_id));
    }
    if !self.quarantined.contains_key(&node_id) && self.quarantined.len() >= MAX_QUARANTINED {
      return Err(AdmissionError::QuarantineFull);
    }

    let challenge = AdmissionChallenge {
      cluster_id: self.registry.cluster_id(),
      sponsor_peer_id: sponsor.peer_id_bytes(),
      nonce: random_nonce()?,
    };
    self.quarantined.insert(
      node_id,
      PendingAdmission {
        candidate,
        challenge: challenge.clone(),
        sponsor_node_id: sponsor.node_id(),
      },
    );
    Ok(challenge)
  }

  pub fn enroll_with_join_key(
    &mut self, proof: &JoinProof, authenticated_peer: &PeerId, sponsor: &NodeIdentity,
  ) -> Result<EnrollmentOutcome, AdmissionError> {
    let join_key = self
      .join_key
      .as_ref()
      .ok_or(AdmissionError::JoinKeyRequired)?;
    ensure_authenticated_peer(&proof.candidate, authenticated_peer)?;
    let node_id = proof.candidate.identity().node_id();
    let pending = self
      .quarantined
      .get(&node_id)
      .ok_or(AdmissionError::UnknownCandidate(node_id))?;
    if pending.candidate != proof.candidate || pending.challenge != proof.challenge {
      return Err(AdmissionError::ProofMismatch(node_id));
    }
    proof.verify(join_key)?;
    self.admit_pending(node_id, sponsor)
  }

  pub fn approve(
    &mut self, node_id: NodeId, sponsor: &NodeIdentity,
  ) -> Result<EnrollmentOutcome, AdmissionError> {
    if !self.quarantined.contains_key(&node_id) {
      return Err(AdmissionError::UnknownCandidate(node_id));
    }
    self.admit_pending(node_id, sponsor)
  }

  fn admit_pending(
    &mut self, node_id: NodeId, sponsor: &NodeIdentity,
  ) -> Result<EnrollmentOutcome, AdmissionError> {
    let pending = self
      .quarantined
      .get(&node_id)
      .cloned()
      .ok_or(AdmissionError::UnknownCandidate(node_id))?;
    if pending.sponsor_node_id != sponsor.node_id() {
      return Err(AdmissionError::SponsorMismatch {
        expected: pending.sponsor_node_id,
        actual: sponsor.node_id(),
      });
    }
    let (sponsor_record, authorizer_head) = {
      let sponsor_record = self.ensure_sponsor(sponsor)?;
      let authorizer_head = self.registry.authorizer_head(sponsor_record)?;
      (sponsor_record.clone(), authorizer_head.clone())
    };
    let record = AuthorizationRecord::admit(
      self.registry.cluster_id(),
      pending.candidate.identity(),
      &sponsor_record,
      &authorizer_head,
      sponsor,
    )?;
    self.registry.insert(record.clone())?;
    self.quarantined.remove(&node_id);
    Ok(EnrollmentOutcome {
      record,
      records: self.registry.records(),
    })
  }

  fn ensure_sponsor(&self, sponsor: &NodeIdentity) -> Result<&AuthorizationRecord, AdmissionError> {
    let record = self
      .registry
      .active_record_for_node(sponsor.node_id())
      .ok_or(AdmissionError::SponsorNotAuthorized(sponsor.node_id()))?;
    if record.peer_id() != sponsor.peer_id_bytes() {
      return Err(AdmissionError::SponsorNotAuthorized(sponsor.node_id()));
    }
    Ok(record)
  }
}

#[derive(Debug, Clone)]
struct PendingAdmission {
  candidate: AdmissionCandidate,
  challenge: AdmissionChallenge,
  sponsor_node_id: NodeId,
}

fn ensure_authenticated_peer(
  candidate: &AdmissionCandidate, authenticated_peer: &PeerId,
) -> Result<(), AdmissionError> {
  if candidate.identity().peer_id() != authenticated_peer.to_bytes() {
    return Err(AdmissionError::CandidatePeerMismatch(
      candidate.identity().node_id(),
    ));
  }
  Ok(())
}

fn proof_transcript(
  candidate: &AdmissionCandidate, challenge: &AdmissionChallenge,
) -> Result<Vec<u8>, AdmissionError> {
  let transcript = (
    PROTOCOL_VERSION,
    challenge.cluster_id,
    candidate.identity.peer_id().to_vec(),
    challenge.sponsor_peer_id.clone(),
    candidate.nonce,
    challenge.nonce,
    candidate.identity.node_id(),
    challenge.sponsor_peer_id.clone(),
  );
  Ok(postcard::to_stdvec(&transcript)?)
}

fn random_nonce() -> Result<[u8; ADMISSION_NONCE_BYTES], AdmissionError> {
  let rng = SystemRandom::new();
  let mut nonce = [0; ADMISSION_NONCE_BYTES];
  rng
    .fill(&mut nonce)
    .map_err(|_| AdmissionError::RandomGeneration)?;
  Ok(nonce)
}

#[derive(Debug, Error)]
pub enum AdmissionError {
  #[error(transparent)]
  Identity(#[from] IdentityError),
  #[error(transparent)]
  Authorization(#[from] AuthorizationError),
  #[error("admission serialization failed: {0}")]
  Serialization(#[from] postcard::Error),
  #[error("failed to generate an admission nonce")]
  RandomGeneration,
  #[error("node {0} did not present its initial identity key")]
  NonInitialIdentity(NodeId),
  #[error("authenticated transport peer does not match candidate node {0}")]
  CandidatePeerMismatch(NodeId),
  #[error("node {0} already has an authorization identity")]
  IdentityConflict(NodeId),
  #[error("node {0} is not quarantined")]
  UnknownCandidate(NodeId),
  #[error("admission proof does not match the pending challenge for node {0}")]
  ProofMismatch(NodeId),
  #[error("join key proof is invalid")]
  InvalidJoinProof,
  #[error("a join key is required for this enrollment")]
  JoinKeyRequired,
  #[error("too many candidates are quarantined")]
  QuarantineFull,
  #[error("sponsor {0} is not an active authorized node")]
  SponsorNotAuthorized(NodeId),
  #[error("admission sponsor is {actual}, expected {expected}")]
  SponsorMismatch { expected: NodeId, actual: NodeId },
}

#[cfg(test)]
mod tests {
  use super::*;

  type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

  fn sponsor() -> TestResult<(NodeIdentity, AuthorizationRegistry)> {
    let identity = NodeIdentity::generate();
    let (cluster_id, record) = AuthorizationRecord::genesis(&identity)?;
    let registry = AuthorizationRegistry::from_records(cluster_id, [record])?;
    Ok((identity, registry))
  }

  fn candidate(byte: u8) -> TestResult<(NodeIdentity, AdmissionCandidate)> {
    let identity = NodeIdentity::generate();
    let candidate =
      AdmissionCandidate::with_nonce(identity.public_identity(), [byte; ADMISSION_NONCE_BYTES])?;
    Ok((identity, candidate))
  }

  fn keyed_enrollment() -> TestResult<(NodeIdentity, ClusterKey, Enrollment)> {
    let (sponsor, registry) = sponsor()?;
    let join_key = ClusterKey::generate()?;
    let enrollment = Enrollment::new(registry, Some(join_key.clone()));
    Ok((sponsor, join_key, enrollment))
  }

  fn pending_candidate(
    enrollment: &mut Enrollment, sponsor: &NodeIdentity, byte: u8,
  ) -> TestResult<(NodeIdentity, AdmissionCandidate, AdmissionChallenge)> {
    let (identity, candidate) = candidate(byte)?;
    let challenge = enrollment.begin(candidate.clone(), &identity.peer_id(), sponsor)?;
    Ok((identity, candidate, challenge))
  }

  #[test]
  fn join_key_enrollment_admits_and_returns_a_registry_checkpoint() -> TestResult {
    let (sponsor, join_key, mut enrollment) = keyed_enrollment()?;
    let (candidate_identity, candidate, challenge) =
      pending_candidate(&mut enrollment, &sponsor, 7)?;
    let proof = JoinProof::create(&join_key, candidate, challenge)?;

    let outcome =
      enrollment.enroll_with_join_key(&proof, &candidate_identity.peer_id(), &sponsor)?;

    assert_eq!(outcome.record().node_id(), candidate_identity.node_id());
    assert_eq!(outcome.records().len(), 2);
    assert_eq!(
      enrollment
        .registry()
        .node_for_peer(&candidate_identity.peer_id()),
      Some(candidate_identity.node_id())
    );
    assert!(matches!(
      enrollment.enroll_with_join_key(&proof, &candidate_identity.peer_id(), &sponsor),
      Err(AdmissionError::UnknownCandidate(node)) if node == candidate_identity.node_id()
    ));
    Ok(())
  }

  #[test]
  fn join_proof_contains_a_mac_but_never_the_join_key() -> TestResult {
    let (sponsor, join_key, mut enrollment) = keyed_enrollment()?;
    let (_candidate_identity, candidate, challenge) =
      pending_candidate(&mut enrollment, &sponsor, 13)?;
    let proof = JoinProof::create(&join_key, candidate, challenge)?;
    let encoded = postcard::to_stdvec(&proof)?;

    assert!(
      !encoded
        .windows(join_key.as_bytes().len())
        .any(|window| window == join_key.as_bytes())
    );
    Ok(())
  }

  #[test]
  fn wrong_key_and_replayed_challenge_fail_closed() -> TestResult {
    let (sponsor, registry) = sponsor()?;
    let join_key = ClusterKey::generate()?;
    let wrong_key = ClusterKey::generate()?;
    let mut enrollment = Enrollment::new(registry, Some(join_key.clone()));
    let (candidate_identity, candidate) = candidate(8)?;
    let first_challenge =
      enrollment.begin(candidate.clone(), &candidate_identity.peer_id(), &sponsor)?;
    let wrong_proof = JoinProof::create(&wrong_key, candidate.clone(), first_challenge.clone())?;

    assert!(matches!(
      enrollment.enroll_with_join_key(&wrong_proof, &candidate_identity.peer_id(), &sponsor),
      Err(AdmissionError::InvalidJoinProof)
    ));

    enrollment.begin(candidate.clone(), &candidate_identity.peer_id(), &sponsor)?;
    let replayed = JoinProof::create(&join_key, candidate, first_challenge)?;
    assert!(matches!(
      enrollment.enroll_with_join_key(&replayed, &candidate_identity.peer_id(), &sponsor),
      Err(AdmissionError::ProofMismatch(node)) if node == candidate_identity.node_id()
    ));
    Ok(())
  }

  #[test]
  fn operator_approval_admits_a_quarantined_candidate_without_a_join_key() -> TestResult {
    let (sponsor, registry) = sponsor()?;
    let mut enrollment = Enrollment::new(registry, None);
    let (candidate_identity, candidate) = candidate(9)?;
    enrollment.begin(candidate, &candidate_identity.peer_id(), &sponsor)?;

    assert_eq!(enrollment.quarantined().len(), 1);
    let outcome = enrollment.approve(candidate_identity.node_id(), &sponsor)?;

    assert_eq!(outcome.record().node_id(), candidate_identity.node_id());
    assert!(enrollment.quarantined().is_empty());
    Ok(())
  }

  #[test]
  fn known_node_id_cannot_enter_the_join_path() -> TestResult {
    let (sponsor, registry) = sponsor()?;
    let mut enrollment = Enrollment::new(registry, None);
    let candidate = AdmissionCandidate::new(&sponsor)?;

    assert!(matches!(
      enrollment.begin(candidate, &sponsor.peer_id(), &sponsor),
      Err(AdmissionError::IdentityConflict(node)) if node == sponsor.node_id()
    ));
    Ok(())
  }

  #[test]
  fn rotated_identity_material_is_not_a_new_admission() -> TestResult {
    let initial = NodeIdentity::generate();
    let successor =
      NodeIdentity::generate_successor(initial.node_id(), initial.initial_public_key().to_vec())?;

    assert!(matches!(
      AdmissionCandidate::with_nonce(
        successor.public_identity(),
        [10; ADMISSION_NONCE_BYTES],
      ),
      Err(AdmissionError::NonInitialIdentity(node)) if node == initial.node_id()
    ));
    Ok(())
  }

  #[test]
  fn candidate_material_must_match_the_authenticated_transport_peer() -> TestResult {
    let (sponsor, registry) = sponsor()?;
    let join_key = ClusterKey::generate()?;
    let mut enrollment = Enrollment::new(registry, Some(join_key.clone()));
    let (candidate_identity, candidate) = candidate(12)?;
    let other = NodeIdentity::generate();

    assert!(matches!(
      enrollment.begin(candidate.clone(), &other.peer_id(), &sponsor),
      Err(AdmissionError::CandidatePeerMismatch(node)) if node == candidate_identity.node_id()
    ));
    assert!(enrollment.quarantined().is_empty());

    let challenge = enrollment.begin(candidate.clone(), &candidate_identity.peer_id(), &sponsor)?;
    let proof = JoinProof::create(&join_key, candidate, challenge)?;
    assert!(matches!(
      enrollment.enroll_with_join_key(&proof, &other.peer_id(), &sponsor),
      Err(AdmissionError::CandidatePeerMismatch(node)) if node == candidate_identity.node_id()
    ));
    Ok(())
  }

  #[test]
  fn an_unauthorized_identity_cannot_sponsor_admission() -> TestResult {
    let (_, registry) = sponsor()?;
    let stranger = NodeIdentity::generate();
    let mut enrollment = Enrollment::new(registry, None);
    let (candidate_identity, candidate) = candidate(11)?;

    assert!(matches!(
      enrollment.begin(candidate, &candidate_identity.peer_id(), &stranger),
      Err(AdmissionError::SponsorNotAuthorized(node)) if node == stranger.node_id()
    ));
    Ok(())
  }
}
