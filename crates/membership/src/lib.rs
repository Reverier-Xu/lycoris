#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod membership;
mod merkle;
mod register;
mod swim;
mod sync;

pub use membership::Membership;
pub use merkle::{Hash, MERKLE_TREE_DEPTH, MerkleTree, hash_empty};
pub use register::{MemberRegister, MemberState};
pub use swim::{Swim, SwimAction, SwimConfig, SwimMessage};
pub use sync::{
  DiffResult, MerkleDiff, NodeRef, RemoteNode, SPLIT_DEPTH, answer_refs, diff_leaf_buckets,
};
