//! Anti-entropy Merkle diff: the pure, transport-free protocol for locating
//! divergence between the local membership tree and a remote peer's tree.
//!
//! The caller (typically a network service) drives the two RPC round trips
//! and feeds the peer's replies back into a [`MerkleDiff`] session:
//!
//! 1. Round one compares the fixed top of the tree — every node down to
//!    [`SPLIT_DEPTH`], requested with [`MerkleDiff::top_refs`]. Subtrees whose
//!    hashes match are pruned; subtrees the peer reports as empty resolve
//!    immediately to "push everything we hold under them".
//! 2. Round two compares the leaf buckets of the subtrees that diverged
//!    ([`MerkleDiff::plan_leaf_refs`] returns the refs to request). Leaf
//!    entries are reconciled with a sorted two-pointer walk by
//!    [`diff_leaf_buckets`].
//!
//! The resulting [`DiffResult`] lists which node ids to fetch from the peer
//! and which to push to it; the register payloads themselves travel on
//! separate RPCs that are none of this module's business.
//!
//! Missing nodes in a peer reply are treated as empty subtrees. The worst
//! case is therefore an over-exchange (pushing registers the peer already
//! has), never a divergence this round could have found but missed.

use std::{cmp::Ordering, collections::HashMap};

use crate::merkle::{Hash, MERKLE_TREE_DEPTH, MerkleTree, hash_empty};

/// Depth at which round one stops and round two switches to per-leaf diffing.
///
/// Round one requests the `2^(SPLIT_DEPTH+1) - 1` nodes of the tree top; a
/// divergent subtree at this depth then costs `2^(MERKLE_TREE_DEPTH -
/// SPLIT_DEPTH)` leaf refs in round two.
pub const SPLIT_DEPTH: u8 = 8;

const _: () = assert!(
  SPLIT_DEPTH <= MERKLE_TREE_DEPTH,
  "SPLIT_DEPTH must not exceed MERKLE_TREE_DEPTH"
);

/// A reference to a Merkle tree node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeRef {
  /// Tree depth of the node: the root is at depth 0, leaves at
  /// [`MERKLE_TREE_DEPTH`].
  pub depth: u8,
  /// Index of the node within its level, counting from the left.
  pub index: u64,
}

/// A node reported by the remote peer; the transport-free mirror of the wire
/// reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteNode {
  /// Tree depth of the node.
  pub depth: u8,
  /// Index of the node within its level.
  pub index: u64,
  /// The hash the peer computed for this node.
  pub hash: Hash,
  /// Leaf entries `(node_id, register_hash)`, present iff the node is a leaf
  /// (`depth == MERKLE_TREE_DEPTH`). Not necessarily sorted by node id.
  pub entries: Option<Vec<(String, Hash)>>,
}

/// The deterministic outcome of a [`MerkleDiff`] session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiffResult {
  /// Node ids whose registers the peer holds (or holds with a different
  /// register hash) and that we must fetch from it. Sorted, deduplicated.
  pub need_from_remote: Vec<String>,
  /// Node ids whose registers we hold (or hold with a different register
  /// hash) and that we must push to the peer. Sorted, deduplicated.
  pub need_from_local: Vec<String>,
}

/// A two-round Merkle diff session against one remote peer.
///
/// The session borrows a snapshot of the local tree; the caller performs the
/// network round trips and feeds the replies back. The protocol is symmetric:
/// either side can drive it, and both reach the mirrored conclusion about who
/// holds what.
#[derive(Debug)]
pub struct MerkleDiff<'a> {
  tree: &'a MerkleTree,
  /// Ids resolved to "push" during round one (remote-empty subtrees).
  need_from_local: Vec<String>,
  /// Indices of the divergent subtrees at [`SPLIT_DEPTH`] that round two
  /// must inspect leaf by leaf.
  divergent: Vec<u64>,
}

impl<'a> MerkleDiff<'a> {
  /// Start a session over a snapshot of the local tree.
  pub fn new(tree: &'a MerkleTree) -> Self {
    Self {
      tree,
      need_from_local: Vec::new(),
      divergent: Vec::new(),
    }
  }

  /// Node references for round one: the full top of the tree down to
  /// [`SPLIT_DEPTH`] inclusive (`2^(SPLIT_DEPTH+1) - 1` refs).
  pub fn top_refs() -> Vec<NodeRef> {
    let mut refs = Vec::with_capacity((1usize << (SPLIT_DEPTH + 1)) - 1);
    for depth in 0..=SPLIT_DEPTH {
      for index in 0..(1u64 << depth) {
        refs.push(NodeRef { depth, index });
      }
    }
    refs
  }

  /// Consume the peer's round-one reply and return the leaf references to
  /// request in round two. An empty return means the diff is already
  /// complete; call [`MerkleDiff::finish`] with an empty reply.
  ///
  /// Nodes missing from the reply are treated as empty subtrees.
  pub fn plan_leaf_refs(&mut self, remote: Vec<RemoteNode>) -> Vec<NodeRef> {
    let remote_hashes: HashMap<(u8, u64), Hash> = remote
      .into_iter()
      .map(|node| ((node.depth, node.index), node.hash))
      .collect();
    let empty_split = self
      .tree
      .empty_subtree_hash(SPLIT_DEPTH)
      .unwrap_or_else(hash_empty);

    let mut refs = Vec::new();
    for index in 0..(1u64 << SPLIT_DEPTH) {
      let local_hash = self
        .tree
        .node_hash(SPLIT_DEPTH, index)
        .unwrap_or(empty_split);
      let remote_hash = remote_hashes
        .get(&(SPLIT_DEPTH, index))
        .copied()
        .unwrap_or(empty_split);
      if local_hash == remote_hash {
        continue;
      }
      // The peer holds nothing under this subtree: everything we hold must be
      // pushed, and no leaf-level fetch is needed for it.
      if remote_hash == empty_split {
        self
          .need_from_local
          .extend(self.tree.collect_node_ids(SPLIT_DEPTH, index));
        continue;
      }
      self.divergent.push(index);
      for leaf in 0..(1u64 << (MERKLE_TREE_DEPTH - SPLIT_DEPTH)) {
        refs.push(NodeRef {
          depth: MERKLE_TREE_DEPTH,
          index: (index << (MERKLE_TREE_DEPTH - SPLIT_DEPTH)) | leaf,
        });
      }
    }
    refs
  }

  /// Consume the peer's round-two reply (an empty vec when
  /// [`MerkleDiff::plan_leaf_refs`] returned no refs) and produce the final
  /// diff.
  ///
  /// Leaf nodes missing from the reply are treated as empty leaves: every
  /// local entry under them must be pushed.
  pub fn finish(mut self, remote: Vec<RemoteNode>) -> DiffResult {
    let mut leaf_hashes: HashMap<u64, Hash> = HashMap::new();
    let mut leaf_entries: HashMap<u64, Vec<(String, Hash)>> = HashMap::new();
    for node in remote {
      if node.depth != MERKLE_TREE_DEPTH {
        continue;
      }
      leaf_hashes.insert(node.index, node.hash);
      if let Some(entries) = node.entries {
        leaf_entries.insert(node.index, entries);
      }
    }

    let divergent = std::mem::take(&mut self.divergent);
    let mut need_from_remote = Vec::new();
    for split_index in divergent {
      for leaf in 0..(1u64 << (MERKLE_TREE_DEPTH - SPLIT_DEPTH)) {
        let index = (split_index << (MERKLE_TREE_DEPTH - SPLIT_DEPTH)) | leaf;
        let local_hash = self
          .tree
          .node_hash(MERKLE_TREE_DEPTH, index)
          .unwrap_or_else(hash_empty);
        let remote_hash = leaf_hashes.get(&index).copied().unwrap_or_else(hash_empty);
        if local_hash == remote_hash {
          continue;
        }
        match leaf_entries.get(&index) {
          Some(remote_bucket) => {
            let local_bucket = self.tree.leaf_entries(index).unwrap_or_default();
            let (mut pull, mut push) = diff_leaf_buckets(local_bucket, remote_bucket);
            need_from_remote.append(&mut pull);
            self.need_from_local.append(&mut push);
          }
          // The peer's leaf is empty: every local entry must be pushed.
          None => self.need_from_local.extend(
            self
              .tree
              .leaf_entries(index)
              .unwrap_or_default()
              .iter()
              .map(|(id, _)| id.clone()),
          ),
        }
      }
    }

    need_from_remote.sort();
    need_from_remote.dedup();
    self.need_from_local.sort();
    self.need_from_local.dedup();
    DiffResult {
      need_from_remote,
      need_from_local: self.need_from_local,
    }
  }
}

/// Reconcile two leaf buckets with a sorted two-pointer walk.
///
/// `local` must be sorted by node id (`MerkleTree::leaf_entries` guarantees
/// this); `remote` is sorted internally, so wire data can be passed as
/// received. Returns `(need_from_remote, need_from_local)`: ids present only
/// on one side go to the side that lacks them; ids present on both sides with
/// different register hashes go to both — each side fetches and pushes, and
/// the CRDT merge resolves the conflict deterministically.
pub fn diff_leaf_buckets(
  local: &[(String, Hash)], remote: &[(String, Hash)],
) -> (Vec<String>, Vec<String>) {
  let mut remote_sorted = remote.to_vec();
  remote_sorted.sort_by(|a, b| a.0.cmp(&b.0));

  let mut need_from_remote = Vec::new();
  let mut need_from_local = Vec::new();
  let mut i = 0usize;
  let mut j = 0usize;

  while i < local.len() && j < remote_sorted.len() {
    match local[i].0.cmp(&remote_sorted[j].0) {
      Ordering::Less => {
        need_from_local.push(local[i].0.clone());
        i += 1;
      }
      Ordering::Greater => {
        need_from_remote.push(remote_sorted[j].0.clone());
        j += 1;
      }
      Ordering::Equal => {
        if local[i].1 != remote_sorted[j].1 {
          need_from_remote.push(local[i].0.clone());
          need_from_local.push(local[i].0.clone());
        }
        i += 1;
        j += 1;
      }
    }
  }

  need_from_local.extend(local[i..].iter().map(|(id, _)| id.clone()));
  need_from_remote.extend(remote_sorted[j..].iter().map(|(id, _)| id.clone()));

  (need_from_remote, need_from_local)
}

/// Answer a batch of node references against a tree, as the serving side of
/// the diff protocol does.
///
/// Leaf refs (`depth == MERKLE_TREE_DEPTH`) carry the bucket entries; inner
/// refs carry only the hash. Invalid refs (depth beyond the tree or index out
/// of range) are skipped silently.
pub fn answer_refs(tree: &MerkleTree, refs: &[NodeRef]) -> Vec<RemoteNode> {
  refs
    .iter()
    .filter_map(|node_ref| {
      let hash = tree.node_hash(node_ref.depth, node_ref.index)?;
      let entries = if node_ref.depth == MERKLE_TREE_DEPTH {
        Some(
          tree
            .leaf_entries(node_ref.index)
            .unwrap_or_default()
            .to_vec(),
        )
      } else {
        None
      };
      Some(RemoteNode {
        depth: node_ref.depth,
        index: node_ref.index,
        hash,
        entries,
      })
    })
    .collect()
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    membership::Membership,
    merkle::leaf_index,
    register::{MemberRegister, MemberState},
  };

  fn register(id: &str, incarnation: u64) -> MemberRegister {
    MemberRegister::new(id, "127.0.0.1:1", incarnation, 0)
      .with_state(MemberState::Active)
      .with_updated_at_ms(0)
  }

  fn tree_of(registers: &[(&str, u64)]) -> MerkleTree {
    let mut membership = Membership::new();
    for (id, incarnation) in registers {
      membership.merge_register(&register(id, *incarnation));
    }
    MerkleTree::from_membership(&membership)
  }

  /// Drive a full diff session locally, with `remote` playing the peer.
  fn run_diff(local: &MerkleTree, remote: &MerkleTree) -> DiffResult {
    let mut diff = MerkleDiff::new(local);
    let top = answer_refs(remote, &MerkleDiff::top_refs());
    let leaf_refs = diff.plan_leaf_refs(top);
    let bottom = if leaf_refs.is_empty() {
      Vec::new()
    } else {
      answer_refs(remote, &leaf_refs)
    };
    diff.finish(bottom)
  }

  #[test]
  fn top_refs_cover_the_tree_top() {
    let refs = MerkleDiff::top_refs();
    assert_eq!(refs.len(), (1usize << (SPLIT_DEPTH + 1)) - 1);
    assert!(refs.contains(&NodeRef { depth: 0, index: 0 }));
    assert!(
      refs
        .iter()
        .all(|r| r.depth <= SPLIT_DEPTH && r.index < (1u64 << r.depth))
    );
  }

  #[test]
  fn identical_trees_have_no_divergence() {
    let local = tree_of(&[("a", 1), ("b", 1)]);
    let remote = local.clone();

    let mut diff = MerkleDiff::new(&local);
    let leaf_refs = diff.plan_leaf_refs(answer_refs(&remote, &MerkleDiff::top_refs()));
    assert!(leaf_refs.is_empty());

    let result = diff.finish(Vec::new());
    assert_eq!(result, DiffResult::default());
  }

  #[test]
  fn empty_local_pulls_everything() {
    let local = MerkleTree::from_membership(&Membership::new());
    let remote = tree_of(&[("a", 1)]);

    let result = run_diff(&local, &remote);
    assert_eq!(result.need_from_remote, vec!["a".to_string()]);
    assert!(result.need_from_local.is_empty());
  }

  #[test]
  fn empty_remote_pushes_everything_without_leaf_round() {
    let local = tree_of(&[("a", 1), ("b", 1)]);
    let remote = MerkleTree::from_membership(&Membership::new());

    let mut diff = MerkleDiff::new(&local);
    let leaf_refs = diff.plan_leaf_refs(answer_refs(&remote, &MerkleDiff::top_refs()));
    // Remote-empty subtrees resolve in round one; no leaf fetch is needed.
    assert!(leaf_refs.is_empty());

    let result = diff.finish(Vec::new());
    assert_eq!(
      result.need_from_local,
      vec!["a".to_string(), "b".to_string()]
    );
    assert!(result.need_from_remote.is_empty());
  }

  #[test]
  fn changed_register_is_found_in_both_directions() {
    let local = tree_of(&[("a", 1), ("b", 1), ("c", 1)]);
    let remote = tree_of(&[("a", 1), ("b", 2), ("c", 1)]);

    let result = run_diff(&local, &remote);
    assert_eq!(result.need_from_remote, vec!["b".to_string()]);
    assert_eq!(result.need_from_local, vec!["b".to_string()]);
  }

  #[test]
  fn added_node_is_pulled() {
    let local = tree_of(&[("a", 1)]);
    let remote = tree_of(&[("a", 1), ("b", 1)]);

    let result = run_diff(&local, &remote);
    assert_eq!(result.need_from_remote, vec!["b".to_string()]);
    assert!(result.need_from_local.is_empty());
  }

  #[test]
  fn diff_is_symmetric() {
    let local = tree_of(&[("a", 1), ("b", 1)]);
    let remote = tree_of(&[("b", 2), ("c", 1)]);

    let forward = run_diff(&local, &remote);
    let backward = run_diff(&remote, &local);

    assert_eq!(forward.need_from_remote, backward.need_from_local);
    assert_eq!(forward.need_from_local, backward.need_from_remote);
    assert_eq!(
      forward.need_from_remote,
      vec!["b".to_string(), "c".to_string()]
    );
    assert_eq!(
      forward.need_from_local,
      vec!["a".to_string(), "b".to_string()]
    );
  }

  #[test]
  fn divergence_is_localized_to_the_changed_bucket() {
    // With 64 nodes only the changed id may be exchanged, even though round
    // two fetches the whole leaf span of the divergent subtree.
    let local_ids: Vec<String> = (0..64).map(|i| format!("node-{i:03}")).collect();
    let local = {
      let mut membership = Membership::new();
      for id in &local_ids {
        membership.merge_register(&register(id, 1));
      }
      MerkleTree::from_membership(&membership)
    };
    let remote = {
      let mut membership = Membership::new();
      for id in &local_ids {
        let incarnation = if id == "node-007" { 2 } else { 1 };
        membership.merge_register(&register(id, incarnation));
      }
      MerkleTree::from_membership(&membership)
    };

    let result = run_diff(&local, &remote);
    assert_eq!(result.need_from_remote, vec!["node-007".to_string()]);
    assert_eq!(result.need_from_local, vec!["node-007".to_string()]);
  }

  #[test]
  fn unsorted_remote_entries_and_missing_leaves_are_handled() {
    let local = tree_of(&[("a", 1)]);
    let a_leaf = leaf_index("a");
    let split = a_leaf >> (MERKLE_TREE_DEPTH - SPLIT_DEPTH);
    // A hash that matches neither the local subtree nor the canonical empty
    // hash, standing in for "the peer holds something different here".
    let foreign = [0xAB; 32];

    let mut diff = MerkleDiff::new(&local);
    let top = vec![RemoteNode {
      depth: SPLIT_DEPTH,
      index: split,
      hash: foreign,
      entries: None,
    }];
    let leaf_refs = diff.plan_leaf_refs(top);
    assert_eq!(leaf_refs.len(), 1usize << (MERKLE_TREE_DEPTH - SPLIT_DEPTH));

    // Round two answers only the one leaf, with unsorted entries: "z" is
    // unknown locally, "a" carries a different register hash.
    let bottom = vec![RemoteNode {
      depth: MERKLE_TREE_DEPTH,
      index: a_leaf,
      hash: foreign,
      entries: Some(vec![
        ("z".to_string(), foreign),
        ("a".to_string(), [0xCD; 32]),
      ]),
    }];
    let result = diff.finish(bottom);
    assert_eq!(
      result.need_from_remote,
      vec!["a".to_string(), "z".to_string()]
    );
    assert_eq!(result.need_from_local, vec!["a".to_string()]);
  }

  fn h(byte: u8) -> Hash {
    [byte; 32]
  }

  #[test]
  fn leaf_diff_with_both_buckets_empty() {
    let (pull, push) = diff_leaf_buckets(&[], &[]);
    assert!(pull.is_empty());
    assert!(push.is_empty());
  }

  #[test]
  fn leaf_diff_with_one_side_empty() {
    let local = vec![("a".to_string(), h(1)), ("b".to_string(), h(2))];

    let (pull, push) = diff_leaf_buckets(&local, &[]);
    assert!(pull.is_empty());
    assert_eq!(push, vec!["a".to_string(), "b".to_string()]);

    let (pull, push) = diff_leaf_buckets(&[], &local);
    assert_eq!(pull, vec!["a".to_string(), "b".to_string()]);
    assert!(push.is_empty());
  }

  #[test]
  fn leaf_diff_with_equal_buckets() {
    let bucket = vec![("a".to_string(), h(1)), ("b".to_string(), h(2))];
    let (pull, push) = diff_leaf_buckets(&bucket, &bucket);
    assert!(pull.is_empty());
    assert!(push.is_empty());
  }

  #[test]
  fn leaf_diff_interleaved() {
    let local = vec![("a".to_string(), h(1)), ("c".to_string(), h(1))];
    // Deliberately unsorted: the wire order must not matter.
    let remote = vec![
      ("c".to_string(), h(2)),
      ("d".to_string(), h(1)),
      ("b".to_string(), h(1)),
    ];

    let (pull, push) = diff_leaf_buckets(&local, &remote);
    assert_eq!(
      pull,
      vec!["b".to_string(), "c".to_string(), "d".to_string()]
    );
    assert_eq!(push, vec!["a".to_string(), "c".to_string()]);
  }

  #[test]
  fn answer_refs_returns_hashes_and_leaf_entries() {
    let tree = tree_of(&[("a", 1)]);
    let a_leaf = leaf_index("a");

    let results = answer_refs(
      &tree,
      &[
        NodeRef { depth: 0, index: 0 },
        NodeRef {
          depth: MERKLE_TREE_DEPTH,
          index: a_leaf,
        },
      ],
    );

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].hash, tree.root_hash());
    assert!(results[0].entries.is_none());
    let entries = results[1].entries.as_deref().unwrap_or(&[]);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, "a");
  }

  #[test]
  fn answer_refs_skips_invalid_refs() {
    let tree = tree_of(&[("a", 1)]);
    let results = answer_refs(
      &tree,
      &[
        NodeRef {
          depth: MERKLE_TREE_DEPTH + 1,
          index: 0,
        },
        NodeRef {
          depth: 1,
          index: 10,
        },
      ],
    );
    assert!(results.is_empty());
  }
}
