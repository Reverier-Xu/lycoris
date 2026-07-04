use blake3::Hasher;

use crate::membership::crdt::{MemberRegister, Membership};

const LEAF_PREFIX: &[u8] = b"L";
const INNER_PREFIX: &[u8] = b"I";
const EMPTY_PREFIX: &[u8] = b"E";

/// Number of bytes used for a Merkle hash. 32 bytes is the blake3 output size.
pub type Hash = [u8; 32];

/// A Merkle tree over the cluster membership.
///
/// Leaves are hashes of canonical `MemberRegister` serializations. Internal
/// nodes hash their two children. The tree is balanced and sorted by node id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleTree {
  root: MerkleNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MerkleNode {
  Empty,
  Leaf {
    hash: Hash,
    node_id: String,
  },
  Inner {
    hash: Hash,
    left: Box<MerkleNode>,
    right: Box<MerkleNode>,
  },
}

impl MerkleTree {
  /// Build a Merkle tree from a membership snapshot.
  pub fn from_membership(membership: &Membership) -> Self {
    let mut registers: Vec<&MemberRegister> = membership.all();
    registers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let leaves: Vec<MerkleNode> = registers
      .into_iter()
      .map(|register| MerkleNode::Leaf {
        hash: hash_register(register),
        node_id: register.node_id.clone(),
      })
      .collect();
    let root = build_balanced(&leaves);
    Self { root }
  }

  /// Return the root hash.
  pub fn root_hash(&self) -> Hash {
    self.root.hash()
  }

  /// Compute the symmetric difference between two trees.
  ///
  /// Returns the set of node ids whose leaf hashes differ or exist only on one
  /// side. The result is deterministic and sorted.
  pub fn diff(&self, other: &Self) -> Vec<String> {
    let mut left = self.leaf_hashes();
    let mut right = other.leaf_hashes();
    left.sort_by(|a, b| a.0.cmp(&b.0));
    right.sort_by(|a, b| a.0.cmp(&b.0));

    let mut ids = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;

    while i < left.len() && j < right.len() {
      match left[i].0.cmp(&right[j].0) {
        std::cmp::Ordering::Less => {
          ids.push(left[i].0.clone());
          i += 1;
        }
        std::cmp::Ordering::Greater => {
          ids.push(right[j].0.clone());
          j += 1;
        }
        std::cmp::Ordering::Equal => {
          if left[i].1 != right[j].1 {
            ids.push(left[i].0.clone());
          }
          i += 1;
          j += 1;
        }
      }
    }

    while i < left.len() {
      ids.push(left[i].0.clone());
      i += 1;
    }
    while j < right.len() {
      ids.push(right[j].0.clone());
      j += 1;
    }

    ids.sort();
    ids.dedup();
    ids
  }

  /// Collect all `(node_id, leaf_hash)` pairs from the tree.
  pub fn leaf_hashes(&self) -> Vec<(String, Hash)> {
    let mut out = Vec::new();
    collect_leaf_hashes(&self.root, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
  }
}

impl MerkleNode {
  fn hash(&self) -> Hash {
    match self {
      MerkleNode::Empty => hash_empty(),
      MerkleNode::Leaf { hash, .. } => *hash,
      MerkleNode::Inner { hash, .. } => *hash,
    }
  }
}

fn build_balanced(leaves: &[MerkleNode]) -> MerkleNode {
  match leaves.len() {
    0 => MerkleNode::Empty,
    1 => leaves[0].clone(),
    _ => {
      let mid = leaves.len() / 2;
      let left = build_balanced(&leaves[..mid]);
      let right = build_balanced(&leaves[mid..]);
      MerkleNode::Inner {
        hash: hash_inner(left.hash(), right.hash()),
        left: Box::new(left),
        right: Box::new(right),
      }
    }
  }
}

fn collect_leaf_hashes(node: &MerkleNode, out: &mut Vec<(String, Hash)>) {
  match node {
    MerkleNode::Empty => {}
    MerkleNode::Leaf { node_id, hash } => out.push((node_id.clone(), *hash)),
    MerkleNode::Inner { left, right, .. } => {
      collect_leaf_hashes(left, out);
      collect_leaf_hashes(right, out);
    }
  }
}

/// Deterministic, canonical hash of a member register.
///
/// Labels and annotations are sorted by key to ensure that equivalent registers
/// always hash to the same value regardless of HashMap iteration order.
fn hash_register(register: &MemberRegister) -> Hash {
  let mut hasher = Hasher::new();
  hasher.update(LEAF_PREFIX);
  hasher.update(register.node_id.as_bytes());
  hasher.update(b"\0");
  hasher.update(register.address.as_bytes());
  hasher.update(b"\0");
  hasher.update(&[register.state.as_u8()]);
  hasher.update(&register.incarnation.to_be_bytes());
  hasher.update(&register.heartbeat.to_be_bytes());
  hasher.update(&register.updated_at_ms.to_be_bytes());

  let mut labels: Vec<(&String, &String)> = register.labels.iter().collect();
  labels.sort_by(|a, b| a.0.cmp(b.0));
  for (key, value) in labels {
    hasher.update(key.as_bytes());
    hasher.update(b"=");
    hasher.update(value.as_bytes());
    hasher.update(b"\n");
  }

  let mut annotations: Vec<(&String, &String)> = register.annotations.iter().collect();
  annotations.sort_by(|a, b| a.0.cmp(b.0));
  for (key, value) in annotations {
    hasher.update(key.as_bytes());
    hasher.update(b"=");
    hasher.update(value.as_bytes());
    hasher.update(b"\n");
  }

  hasher.finalize().into()
}

fn hash_inner(left: Hash, right: Hash) -> Hash {
  let mut hasher = Hasher::new();
  hasher.update(INNER_PREFIX);
  hasher.update(&left);
  hasher.update(&right);
  hasher.finalize().into()
}

fn hash_empty() -> Hash {
  let mut hasher = Hasher::new();
  hasher.update(EMPTY_PREFIX);
  hasher.finalize().into()
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::membership::crdt::{MemberRegister, MemberState, Membership};

  fn register(id: &str, heartbeat: u64) -> MemberRegister {
    let mut r = MemberRegister::new(id, "127.0.0.1:1", 1, heartbeat);
    r.state = MemberState::Active;
    r.updated_at_ms = i64::try_from(heartbeat).unwrap_or(i64::MAX);
    r
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
  fn diff_finds_single_changed_node() {
    let mut m1 = Membership::new();
    m1.merge_register(&register("a", 1));
    m1.merge_register(&register("b", 1));
    m1.merge_register(&register("c", 1));

    let mut m2 = m1.clone();
    m2.merge_register(&register("b", 2));

    let diff = MerkleTree::from_membership(&m1).diff(&MerkleTree::from_membership(&m2));
    assert_eq!(diff, vec!["b".to_string()]);
  }

  #[test]
  fn diff_finds_added_node() {
    let mut m1 = Membership::new();
    m1.merge_register(&register("a", 1));

    let mut m2 = Membership::new();
    m2.merge_register(&register("a", 1));
    m2.merge_register(&register("b", 1));

    let diff = MerkleTree::from_membership(&m1).diff(&MerkleTree::from_membership(&m2));
    assert_eq!(diff, vec!["b".to_string()]);
  }

  #[test]
  fn diff_is_symmetric() {
    let mut m1 = Membership::new();
    m1.merge_register(&register("a", 1));
    m1.merge_register(&register("b", 1));

    let mut m2 = Membership::new();
    m2.merge_register(&register("b", 2));
    m2.merge_register(&register("c", 1));

    let diff_ab = MerkleTree::from_membership(&m1).diff(&MerkleTree::from_membership(&m2));
    let diff_ba = MerkleTree::from_membership(&m2).diff(&MerkleTree::from_membership(&m1));
    assert_eq!(diff_ab, diff_ba);
    assert_eq!(
      diff_ab,
      vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
  }

  #[test]
  fn label_order_does_not_affect_hash() {
    let mut r1 = register("a", 1);
    r1.labels.insert("x".to_string(), "1".to_string());
    r1.labels.insert("y".to_string(), "2".to_string());

    let mut r2 = register("a", 1);
    r2.labels.insert("y".to_string(), "2".to_string());
    r2.labels.insert("x".to_string(), "1".to_string());

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
