//! Peer selection policy (D9): the single place that turns peer bookkeeping
//! into an ordered candidate list for every sync plane.
//!
//! Gossip fan-out, membership anti-entropy, and resource anti-entropy all
//! consume [`targets`]; none of them selects endpoints on its own.

use std::collections::HashMap;

use lycoris_storage::{NodeDomain, PeerRecord};

/// Failure backoff window: an endpoint whose latest attempt failed is skipped
/// for 30s — six 5s anti-entropy cycles. The rule is deliberately time-based
/// and deterministic: it needs no state beyond the health fields already
/// persisted in [`PeerRecord`], so backoff survives restarts for free.
const FAILURE_BACKOFF_MS: i64 = 30_000;

/// Return the ordered candidate endpoints for sync/gossip fan-out.
///
/// Semantics:
/// - The local endpoint is never a candidate.
/// - The stored primary heads the list, so the cluster converges on one
///   preferred endpoint — unless the primary itself is in failure backoff.
/// - Remaining candidates are ordered by `last_seen_ms` descending (recently
///   reachable first, never-seen last), ties broken by address so the result is
///   deterministic.
/// - An endpoint whose last attempt failed within [`FAILURE_BACKOFF_MS`] is
///   *excluded*, not merely demoted: that is what frees the attempts the
///   isolation guard spends on the full seed set. Exclusion is time-bounded, so
///   every endpoint is retried once the window lapses.
///
/// An empty result therefore means "nothing is worth trying right now":
/// either no peers are known at all, or every known endpoint is backing off.
/// The anti-entropy loop treats that as isolation and retries the complete
/// seed set once per cycle (see `super::antientropy`).
pub(super) fn targets(node: &NodeDomain, local_address: &str, now_ms: i64) -> Vec<String> {
  let primary = node.peers().get_primary().unwrap_or(None);
  let records = node.peers().records().unwrap_or_default();
  let by_address: HashMap<&str, &PeerRecord> = records
    .iter()
    .map(|record| (record.address.as_str(), record))
    .collect();

  let mut targets = Vec::new();
  if let Some(address) = &primary
    && address != local_address
    && !by_address
      .get(address.as_str())
      .is_some_and(|record| backing_off(record, now_ms))
  {
    targets.push(address.clone());
  }

  let mut healthy: Vec<&PeerRecord> = records
    .iter()
    .filter(|record| {
      record.address != local_address
        && primary.as_ref() != Some(&record.address)
        && !backing_off(record, now_ms)
    })
    .collect();
  healthy.sort_by(|a, b| {
    b.last_seen_ms
      .unwrap_or(i64::MIN)
      .cmp(&a.last_seen_ms.unwrap_or(i64::MIN))
      .then_with(|| a.address.cmp(&b.address))
  });
  targets.extend(healthy.into_iter().map(|record| record.address.clone()));

  targets
}

/// True while `record` is inside the failure backoff window: the last attempt
/// failed (`online == false`) less than [`FAILURE_BACKOFF_MS`] ago.
/// Never-attempted endpoints (`last_attempt_ms == None`) are never in
/// backoff — freshly seeded bootstrap peers must be tried immediately.
fn backing_off(record: &PeerRecord, now_ms: i64) -> bool {
  !record.online
    && record
      .last_attempt_ms
      .is_some_and(|attempt| now_ms.saturating_sub(attempt) < FAILURE_BACKOFF_MS)
}

#[cfg(test)]
mod tests {
  use lycoris_core::now_ms;
  use lycoris_storage::Storage;
  use tempfile::TempDir;

  use super::*;

  fn test_node() -> (TempDir, NodeDomain) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("test.redb")).unwrap();
    (dir, storage.node().clone())
  }

  #[test]
  fn primary_heads_candidates_and_local_is_excluded() {
    let (_dir, node) = test_node();
    node.peers().seed("local:1").unwrap();
    node.peers().seed("peer-a:1").unwrap();
    node.peers().seed("peer-b:1").unwrap();
    node.peers().set_primary("peer-b:1", "local:1").unwrap();

    let candidates = targets(&node, "local:1", now_ms());

    assert_eq!(
      candidates,
      vec!["peer-b:1".to_string(), "peer-a:1".to_string()]
    );
  }

  #[test]
  fn recently_seen_fallback_ranks_first() {
    let (_dir, node) = test_node();
    node.peers().seed("peer-a:1").unwrap();
    node.peers().seed("peer-b:1").unwrap();
    node.peers().mark_seen("peer-b:1", 1_000).unwrap();

    let candidates = targets(&node, "local:1", now_ms());

    assert_eq!(
      candidates,
      vec!["peer-b:1".to_string(), "peer-a:1".to_string()]
    );
  }

  #[test]
  fn recent_failure_backs_off_until_the_window_lapses() {
    let (_dir, node) = test_node();
    node.peers().seed("peer-a:1").unwrap();
    node.peers().seed("peer-b:1").unwrap();
    node.peers().mark_attempt("peer-a:1", false).unwrap();

    let now = now_ms();
    assert_eq!(targets(&node, "local:1", now), vec!["peer-b:1".to_string()]);

    // Once the backoff window lapses the endpoint is retried (never-seen
    // ranks after the healthy, recently-seen peer-b).
    node.peers().mark_seen("peer-b:1", now).unwrap();
    let after = now + FAILURE_BACKOFF_MS + 1;
    assert_eq!(
      targets(&node, "local:1", after),
      vec!["peer-b:1".to_string(), "peer-a:1".to_string()]
    );
  }

  #[test]
  fn successful_contact_clears_backoff_immediately() {
    let (_dir, node) = test_node();
    node.peers().seed("peer-a:1").unwrap();
    node.peers().mark_attempt("peer-a:1", false).unwrap();

    let now = now_ms();
    assert!(targets(&node, "local:1", now).is_empty());

    node.peers().mark_seen("peer-a:1", now).unwrap();
    assert_eq!(targets(&node, "local:1", now), vec!["peer-a:1".to_string()]);
  }

  #[test]
  fn backing_off_primary_loses_its_head_position() {
    let (_dir, node) = test_node();
    node.peers().seed("peer-a:1").unwrap();
    node.peers().seed("peer-b:1").unwrap();
    node.peers().set_primary("peer-a:1", "local:1").unwrap();
    node.peers().mark_attempt("peer-a:1", false).unwrap();

    // The primary is in backoff: the healthy fallback takes over, and with
    // no healthy endpoint left the result is empty — the isolation signal.
    assert_eq!(
      targets(&node, "local:1", now_ms()),
      vec!["peer-b:1".to_string()]
    );
    node.peers().mark_attempt("peer-b:1", false).unwrap();
    assert!(targets(&node, "local:1", now_ms()).is_empty());
  }
}
