use blake3::Hasher;

use crate::{membership::Membership, register::MemberRegister};

const LEAF_PREFIX: &[u8] = b"L";
const INNER_PREFIX: &[u8] = b"I";
const EMPTY_PREFIX: &[u8] = b"E";

/// Depth of the fixed-depth Merkle tree.
///
/// A depth of 16 yields 2^16 leaf buckets, sparse enough that members of
/// typical clusters rarely share a bucket. One tree instance holds a flat
/// node vector of 2^17 32-byte hashes (4 MiB, index 0 unused) plus 2^16
/// bucket `Vec` headers (~1.5 MiB), so it costs ~5.5 MiB of memory. That is
/// affordable only because the tree is not rebuilt per query: consumers cache
/// it keyed by `Membership::version` and rebuild only when a hash-relevant
/// mutation advanced the version. The build itself hashes only non-empty
/// subtrees, so its CPU cost scales with the number of members, not with the
/// fixed 2^17 node count.
pub const MERKLE_TREE_DEPTH: u8 = 16;

/// Number of bytes used for a Merkle hash. 32 bytes is the blake3 output size.
pub type Hash = [u8; 32];

/// A fixed-depth complete binary Merkle tree over the cluster membership.
///
/// Leaves are keyed by the first `MERKLE_TREE_DEPTH` bits of
/// `blake3(node_id)`. Each leaf bucket contains all registers whose node id
/// hashes to that bucket, sorted by node id. Internal nodes hash their two
/// children. Empty subtrees hash to `hash_empty()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleTree {
  nodes: Vec<Hash>,
  buckets: Vec<Vec<(String, Hash)>>,
  empty_hashes: Vec<Hash>,
}

impl MerkleTree {
  /// Build a Merkle tree from a membership snapshot.
  ///
  /// Fully empty subtrees are never hashed: every level of the flat node
  /// vector is pre-filled with the canonical empty-subtree hash for that
  /// depth, and the bottom-up pass skips nodes whose children are both
  /// empty. The result is byte-for-byte identical to hashing all 2^17 nodes,
  /// but the work scales with the number of members instead of the tree size.
  pub fn from_membership(membership: &Membership) -> Self {
    let leaf_count = 1usize << MERKLE_TREE_DEPTH;
    let total_nodes = 1usize << (MERKLE_TREE_DEPTH + 1);

    // Canonical hash of a fully empty subtree per depth, from the root
    // (`empty_hashes[0]`) down to an empty leaf (`empty_hashes[DEPTH]`).
    let mut empty_hashes = Vec::with_capacity((MERKLE_TREE_DEPTH + 1) as usize);
    let mut current = hash_empty();
    empty_hashes.push(current);
    for _ in 0..MERKLE_TREE_DEPTH {
      current = hash_inner(current, current);
      empty_hashes.push(current);
    }
    empty_hashes.reverse();

    // Pre-fill every level with its empty-subtree hash (the leaf level keeps
    // `hash_empty()`; index 0 is unused). Subtrees never touched below are
    // empty and therefore already hold the correct hash.
    let mut nodes = vec![hash_empty(); total_nodes];
    for depth in 0..MERKLE_TREE_DEPTH {
      let start = 1usize << depth;
      nodes[start..(start << 1)].fill(empty_hashes[depth as usize]);
    }

    let mut buckets: Vec<Vec<(String, Hash)>> = (0..leaf_count).map(|_| Vec::new()).collect();
    for register in membership.all() {
      let idx = leaf_index(register.node_id());
      let register_hash = hash_register(register);
      buckets[idx as usize].push((register.node_id().to_string(), register_hash));
    }

    for (i, bucket) in buckets.iter_mut().enumerate() {
      if bucket.is_empty() {
        continue;
      }
      bucket.sort_by(|a, b| a.0.cmp(&b.0));
      nodes[flat_index(MERKLE_TREE_DEPTH, i as u64)] = hash_leaf_bucket(bucket);
    }

    for depth in (0..MERKLE_TREE_DEPTH).rev() {
      let empty_child = empty_hashes[(depth + 1) as usize];
      let width = 1usize << depth;
      for i in 0..width {
        let left = nodes[flat_index(depth + 1, 2 * i as u64)];
        let right = nodes[flat_index(depth + 1, (2 * i + 1) as u64)];
        if left == empty_child && right == empty_child {
          continue;
        }
        nodes[flat_index(depth, i as u64)] = hash_inner(left, right);
      }
    }

    Self {
      nodes,
      buckets,
      empty_hashes,
    }
  }

  /// Return the hash of an empty subtree at the given depth.
  pub fn empty_subtree_hash(&self, depth: u8) -> Option<Hash> {
    self.empty_hashes.get(depth as usize).copied()
  }

  /// Return the root hash.
  pub fn root_hash(&self) -> Hash {
    self.nodes[1]
  }

  /// Return the hash for a node at `(depth, index)`.
  ///
  /// Returns `None` if `index >= 2^depth`.
  pub fn node_hash(&self, depth: u8, index: u64) -> Option<Hash> {
    if depth > MERKLE_TREE_DEPTH {
      return None;
    }
    let max_index = 1u64 << depth;
    if index >= max_index {
      return None;
    }
    Some(self.nodes[flat_index(depth, index)])
  }

  /// Return the sorted leaf bucket at `index` (depth = `MERKLE_TREE_DEPTH`).
  ///
  /// Returns `None` if `index` is out of range.
  pub fn leaf_entries(&self, index: u64) -> Option<&[(String, Hash)]> {
    let max_index = 1u64 << MERKLE_TREE_DEPTH;
    if index >= max_index {
      return None;
    }
    Some(&self.buckets[index as usize])
  }

  /// Return all node ids in the subtree rooted at `(depth, index)`.
  pub fn collect_node_ids(&self, depth: u8, index: u64) -> Vec<String> {
    if depth > MERKLE_TREE_DEPTH {
      return Vec::new();
    }
    if depth == MERKLE_TREE_DEPTH {
      return self
        .leaf_entries(index)
        .map(|entries| entries.iter().map(|(id, _)| id.clone()).collect())
        .unwrap_or_default();
    }
    let mut ids = Vec::new();
    ids.extend(self.collect_node_ids(depth + 1, 2 * index));
    ids.extend(self.collect_node_ids(depth + 1, 2 * index + 1));
    ids
  }
}

/// Compute the flat vector index for `(depth, index)`.
///
/// Index 0 is unused; the root lives at index 1. For depth `d` and index `i`,
/// the flat index is `(1 << d) + i`.
fn flat_index(depth: u8, index: u64) -> usize {
  ((1u64 << depth) + index) as usize
}

/// Compute the leaf bucket index for a node id.
///
/// Uses the first `MERKLE_TREE_DEPTH` bits of `blake3(node_id)` interpreted as
/// a big-endian unsigned integer.
pub fn leaf_index(node_id: &str) -> u64 {
  let hash = blake3::hash(node_id.as_bytes());
  let bytes = hash.as_bytes();
  let depth = MERKLE_TREE_DEPTH;
  let byte_count = (depth / 8) as usize;
  let extra_bits = depth % 8;

  let mut idx: u64 = 0;
  for byte in bytes.iter().take(byte_count) {
    idx = (idx << 8) | u64::from(*byte);
  }

  if extra_bits > 0 {
    idx = (idx << extra_bits) | u64::from(bytes[byte_count] >> (8 - extra_bits));
  }

  idx
}

/// Deterministic, canonical hash of a member register.
///
/// Labels and annotations are sorted by key to ensure that equivalent registers
/// always hash to the same value regardless of HashMap iteration order.
/// Volatile fields are intentionally excluded so that background churn does not
/// rewrite the whole Merkle tree and trigger pointless anti-entropy exchanges:
/// `updated_at_ms` is wall-clock time, and `heartbeat` is only the final
/// tiebreak of the merge order (D3) — state transitions that matter
/// (`incarnation`, `state`, `address`, labels, annotations) still change the
/// hash, and a stale heartbeat rides along with the next real change.
pub fn hash_register(register: &MemberRegister) -> Hash {
  let mut hasher = Hasher::new();
  hasher.update(LEAF_PREFIX);
  hasher.update(register.node_id().as_bytes());
  hasher.update(b"\0");
  hasher.update(register.address().as_bytes());
  hasher.update(b"\0");
  hasher.update(&[register.state().as_u8()]);
  hasher.update(&register.incarnation().to_be_bytes());

  let mut labels: Vec<(&String, &String)> = register.labels().iter().collect();
  labels.sort_by(|a, b| a.0.cmp(b.0));
  for (key, value) in labels {
    hasher.update(key.as_bytes());
    hasher.update(b"=");
    hasher.update(value.as_bytes());
    hasher.update(b"\n");
  }

  let mut annotations: Vec<(&String, &String)> = register.annotations().iter().collect();
  annotations.sort_by(|a, b| a.0.cmp(b.0));
  for (key, value) in annotations {
    hasher.update(key.as_bytes());
    hasher.update(b"=");
    hasher.update(value.as_bytes());
    hasher.update(b"\n");
  }

  hasher.finalize().into()
}

/// Hash a leaf bucket deterministically.
///
/// The bucket is assumed to be sorted by node id. The hash incorporates each
/// `(node_id, register_hash)` pair so that leaf contents are tamper-evident.
pub fn hash_leaf_bucket(bucket: &[(String, Hash)]) -> Hash {
  if bucket.is_empty() {
    return hash_empty();
  }

  let mut hasher = Hasher::new();
  hasher.update(LEAF_PREFIX);
  for (node_id, register_hash) in bucket {
    hasher.update(node_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(register_hash);
  }
  hasher.finalize().into()
}

pub fn hash_inner(left: Hash, right: Hash) -> Hash {
  let mut hasher = Hasher::new();
  hasher.update(INNER_PREFIX);
  hasher.update(&left);
  hasher.update(&right);
  hasher.finalize().into()
}

pub fn hash_empty() -> Hash {
  let mut hasher = Hasher::new();
  hasher.update(EMPTY_PREFIX);
  hasher.finalize().into()
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use super::*;
  use crate::register::MemberState;

  /// Build a register whose Merkle hash differs per `incarnation`. Heartbeat
  /// is fixed at 0 because it is excluded from the hash (D3) and would not
  /// distinguish test registers anyway.
  fn register(id: &str, incarnation: u64) -> MemberRegister {
    MemberRegister::new(id, "127.0.0.1:1", incarnation, 0)
      .with_state(MemberState::Active)
      .with_updated_at_ms(0)
  }

  #[test]
  fn empty_tree_hash_is_stable() {
    let m = Membership::new();
    let tree = MerkleTree::from_membership(&m);
    let expected = MerkleTree::from_membership(&m);
    assert_eq!(tree.root_hash(), expected.root_hash());
  }

  #[test]
  fn same_membership_same_root() {
    let mut m = Membership::new();
    m.merge_register(&register("a", 1));
    m.merge_register(&register("b", 2));

    let t1 = MerkleTree::from_membership(&m);
    let t2 = MerkleTree::from_membership(&m);
    assert_eq!(t1.root_hash(), t2.root_hash());
  }

  #[test]
  fn different_membership_different_root() {
    let mut m1 = Membership::new();
    m1.merge_register(&register("a", 1));

    let mut m2 = Membership::new();
    m2.merge_register(&register("a", 2));

    assert_ne!(
      MerkleTree::from_membership(&m1).root_hash(),
      MerkleTree::from_membership(&m2).root_hash()
    );
  }

  #[test]
  fn heartbeat_bump_does_not_change_root() {
    let mut m = Membership::new();
    m.merge_register(&register("a", 1));
    let root_before = MerkleTree::from_membership(&m).root_hash();

    // A pure heartbeat bump must not perturb the tree (D3).
    assert!(m.heartbeat("a", 1_000));
    assert_eq!(MerkleTree::from_membership(&m).root_hash(), root_before);
  }

  #[test]
  fn state_change_changes_root() {
    let mut m = Membership::new();
    m.merge_register(&register("a", 1));
    let root_before = MerkleTree::from_membership(&m).root_hash();

    assert!(m.suspect("a", 1_000).is_some());
    assert_ne!(MerkleTree::from_membership(&m).root_hash(), root_before);
  }

  #[test]
  fn incarnation_change_changes_root() {
    let mut m = Membership::new();
    m.merge_register(&register("a", 1));
    let root_before = MerkleTree::from_membership(&m).root_hash();

    assert!(m.refute("a", 1_000));
    assert_ne!(MerkleTree::from_membership(&m).root_hash(), root_before);
  }

  #[test]
  fn node_hash_matches_at_every_level() {
    let mut m = Membership::new();
    m.merge_register(&register("a", 1));
    m.merge_register(&register("b", 2));

    let tree = MerkleTree::from_membership(&m);

    for depth in 0..=MERKLE_TREE_DEPTH {
      let width = 1u64 << depth;
      for i in 0..width {
        let hash = tree.node_hash(depth, i);
        assert!(hash.is_some(), "missing node at depth {depth} index {i}");
      }
    }

    // Recompute the root from its two children to confirm structural correctness.
    let left = tree.node_hash(1, 0).unwrap();
    let right = tree.node_hash(1, 1).unwrap();
    assert_eq!(tree.root_hash(), hash_inner(left, right));
  }

  #[test]
  fn label_order_does_not_affect_hash() {
    let mut labels1 = HashMap::new();
    labels1.insert("x".to_string(), "1".to_string());
    labels1.insert("y".to_string(), "2".to_string());
    let r1 = register("a", 1).with_labels(labels1);

    let mut labels2 = HashMap::new();
    labels2.insert("y".to_string(), "2".to_string());
    labels2.insert("x".to_string(), "1".to_string());
    let r2 = register("a", 1).with_labels(labels2);

    let mut m1 = Membership::new();
    m1.merge_register(&r1);
    let mut m2 = Membership::new();
    m2.merge_register(&r2);

    assert_eq!(
      MerkleTree::from_membership(&m1).root_hash(),
      MerkleTree::from_membership(&m2).root_hash()
    );
  }
}
