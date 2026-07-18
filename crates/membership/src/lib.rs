#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod membership;
mod merkle;
mod register;
mod swim;
mod sync;

pub use membership::Membership;
pub use merkle::{Hash, MERKLE_TREE_DEPTH, MerkleTree};
pub use register::{MemberRegister, MemberState};
pub use swim::{Swim, SwimAction, SwimConfig, SwimMessage};
pub use sync::{DiffResult, MerkleDiff, NodeRef, RemoteNode, answer_refs};
