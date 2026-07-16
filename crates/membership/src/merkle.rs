use blake3::Hasher;

use crate::{membership::Membership, register::MemberRegister};

const LEAF_PREFIX: &[u8] = b"L";
const INNER_PREFIX: &[u8] = b"I";
const EMPTY_PREFIX: &[u8] = b"E";

/// Depth of the fixed-depth Merkle tree.
///
/// A depth of 16 yields 2^16 leaf buckets. With 32-byte hashes, the flat vector
/// has 1 + 2 + 4 + ... + 2^16 = 2^(16+1) - 1 usable entries (index 0 unused),
/// which is small enough to allocate once (~130 KiB) while keeping leaf buckets
/// sparse for typical cluster sizes.
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
  pub fn from_membership(membership: &Membership) -> Self {
    let leaf_count = 1usize << MERKLE_TREE_DEPTH;
    let total_nodes = 1usize << (MERKLE_TREE_DEPTH + 1);

    let empty = hash_empty();
    let mut nodes = vec![empty; total_nodes];
    let mut buckets: Vec<Vec<(String, Hash)>> = (0..leaf_count).map(|_| Vec::new()).collect();

    for register in membership.all() {
      let idx = leaf_index(register.node_id());
      let register_hash = hash_register(register);
      buckets[idx as usize].push((register.node_id().to_string(), register_hash));
    }

    for (i, bucket) in buckets.iter_mut().enumerate() {
      bucket.sort_by(|a, b| a.0.cmp(&b.0));
      let leaf_hash = hash_leaf_bucket(bucket);
      nodes[flat_index(MERKLE_TREE_DEPTH, i as u64)] = leaf_hash;
    }

    for depth in (0..MERKLE_TREE_DEPTH).rev() {
      let width = 1usize << depth;
      for i in 0..width {
        let left = nodes[flat_index(depth + 1, 2 * i as u64)];
        let right = nodes[flat_index(depth + 1, (2 * i + 1) as u64)];
        nodes[flat_index(depth, i as u64)] = hash_inner(left, right);
      }
    }

    let mut empty_hashes = Vec::with_capacity((MERKLE_TREE_DEPTH + 1) as usize);
    empty_hashes.push(empty);
    for _ in 0..MERKLE_TREE_DEPTH {
      let child = *empty_hashes.last().unwrap_or(&empty);
      empty_hashes.push(hash_inner(child, child));
    }
    empty_hashes.reverse();

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

  /// Return true if the subtree rooted at `(depth, index)` contains no
  /// registers.
  pub fn is_empty_subtree(&self, depth: u8, index: u64) -> bool {
    self
      .node_hash(depth, index)
      .and_then(|hash| self.empty_subtree_hash(depth).map(|empty| hash == empty))
      .unwrap_or(true)
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
/// Volatile fields such as `updated_at_ms` are intentionally excluded so that
/// heartbeats do not rewrite the whole Merkle tree on every tick.
pub fn hash_register(register: &MemberRegister) -> Hash {
  let mut hasher = Hasher::new();
  hasher.update(LEAF_PREFIX);
  hasher.update(register.node_id().as_bytes());
  hasher.update(b"\0");
  hasher.update(register.address().as_bytes());
  hasher.update(b"\0");
  hasher.update(&[register.state().as_u8()]);
  hasher.update(&register.incarnation().to_be_bytes());
  hasher.update(&register.heartbeat().to_be_bytes());

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

  fn register(id: &str, heartbeat: u64) -> MemberRegister {
    MemberRegister::new(id, "127.0.0.1:1", 1, heartbeat)
      .with_state(MemberState::Active)
      .with_updated_at_ms(i64::try_from(heartbeat).unwrap_or(i64::MAX))
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
  fn diff_empty_vs_populated() {
    let mut m = Membership::new();
    m.merge_register(&register("a", 1));

    let empty = MerkleTree::from_membership(&Membership::new());
    let populated = MerkleTree::from_membership(&m);

    let diff = merkle_diff(&empty, &populated);
    assert_eq!(diff.need_from_remote, vec!["a".to_string()]);
    assert!(diff.need_from_local.is_empty());
  }

  #[test]
  fn diff_single_changed_node() {
    let mut m1 = Membership::new();
    m1.merge_register(&register("a", 1));
    m1.merge_register(&register("b", 1));
    m1.merge_register(&register("c", 1));

    let mut m2 = m1.clone();
    m2.merge_register(&register("b", 2));

    let diff = merkle_diff(
      &MerkleTree::from_membership(&m1),
      &MerkleTree::from_membership(&m2),
    );
    assert_eq!(diff.need_from_remote, vec!["b".to_string()]);
    assert_eq!(diff.need_from_local, vec!["b".to_string()]);
  }

  #[test]
  fn diff_single_added_node() {
    let mut m1 = Membership::new();
    m1.merge_register(&register("a", 1));

    let mut m2 = Membership::new();
    m2.merge_register(&register("a", 1));
    m2.merge_register(&register("b", 1));

    let diff = merkle_diff(
      &MerkleTree::from_membership(&m1),
      &MerkleTree::from_membership(&m2),
    );
    assert_eq!(diff.need_from_remote, vec!["b".to_string()]);
    assert!(diff.need_from_local.is_empty());
  }

  #[test]
  fn diff_symmetric() {
    let mut m1 = Membership::new();
    m1.merge_register(&register("a", 1));
    m1.merge_register(&register("b", 1));

    let mut m2 = Membership::new();
    m2.merge_register(&register("b", 2));
    m2.merge_register(&register("c", 1));

    let diff_ab = merkle_diff(
      &MerkleTree::from_membership(&m1),
      &MerkleTree::from_membership(&m2),
    );
    let diff_ba = merkle_diff(
      &MerkleTree::from_membership(&m2),
      &MerkleTree::from_membership(&m1),
    );

    assert_eq!(diff_ab.need_from_remote, diff_ba.need_from_local);
    assert_eq!(diff_ab.need_from_local, diff_ba.need_from_remote);

    let mut expected_remote: Vec<String> = vec!["b".to_string(), "c".to_string()];
    expected_remote.sort();
    assert_eq!(diff_ab.need_from_remote, expected_remote);

    let mut expected_local: Vec<String> = vec!["a".to_string(), "b".to_string()];
    expected_local.sort();
    assert_eq!(diff_ab.need_from_local, expected_local);
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

  #[derive(Debug, PartialEq, Eq)]
  struct DiffResult {
    need_from_remote: Vec<String>,
    need_from_local: Vec<String>,
  }

  fn merkle_diff(local: &MerkleTree, remote: &MerkleTree) -> DiffResult {
    let mut need_from_remote = Vec::new();
    let mut need_from_local = Vec::new();
    let mut queue = vec![(0u8, 0u64)];

    while !queue.is_empty() {
      let mut next_queue = Vec::new();
      for (depth, index) in queue {
        let local_hash = local.node_hash(depth, index).unwrap_or_else(hash_empty);
        let remote_hash = remote.node_hash(depth, index).unwrap_or_else(hash_empty);
        if local_hash == remote_hash {
          continue;
        }

        if depth == MERKLE_TREE_DEPTH {
          let local_entries = local.leaf_entries(index).unwrap_or_default();
          let remote_entries = remote.leaf_entries(index).unwrap_or_default();
          diff_leaf_bucket(
            local_entries,
            remote_entries,
            &mut need_from_remote,
            &mut need_from_local,
          );
        } else {
          let left_local = local
            .node_hash(depth + 1, 2 * index)
            .unwrap_or_else(hash_empty);
          let left_remote = remote
            .node_hash(depth + 1, 2 * index)
            .unwrap_or_else(hash_empty);
          if left_local != left_remote {
            next_queue.push((depth + 1, 2 * index));
          }

          let right_local = local
            .node_hash(depth + 1, 2 * index + 1)
            .unwrap_or_else(hash_empty);
          let right_remote = remote
            .node_hash(depth + 1, 2 * index + 1)
            .unwrap_or_else(hash_empty);
          if right_local != right_remote {
            next_queue.push((depth + 1, 2 * index + 1));
          }
        }
      }
      queue = next_queue;
    }

    need_from_remote.sort();
    need_from_remote.dedup();
    need_from_local.sort();
    need_from_local.dedup();

    DiffResult {
      need_from_remote,
      need_from_local,
    }
  }

  fn diff_leaf_bucket(
    local: &[(String, Hash)], remote: &[(String, Hash)], need_from_remote: &mut Vec<String>,
    need_from_local: &mut Vec<String>,
  ) {
    let mut i = 0usize;
    let mut j = 0usize;

    while i < local.len() && j < remote.len() {
      match local[i].0.cmp(&remote[j].0) {
        std::cmp::Ordering::Less => {
          need_from_local.push(local[i].0.clone());
          i += 1;
        }
        std::cmp::Ordering::Greater => {
          need_from_remote.push(remote[j].0.clone());
          j += 1;
        }
        std::cmp::Ordering::Equal => {
          if local[i].1 != remote[j].1 {
            need_from_remote.push(local[i].0.clone());
            need_from_local.push(local[i].0.clone());
          }
          i += 1;
          j += 1;
        }
      }
    }

    while i < local.len() {
      need_from_local.push(local[i].0.clone());
      i += 1;
    }
    while j < remote.len() {
      need_from_remote.push(remote[j].0.clone());
      j += 1;
    }
  }
}
