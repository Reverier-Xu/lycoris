#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod cluster_key;
pub mod membership;
pub mod node_info;
pub mod paths;
pub mod selector;
pub mod time;

pub mod validation;

pub use cluster_key::{ClusterKey, ClusterKeyError, default_cluster_key_path};
pub use membership::{MemberRegister, MemberState, Membership};
pub use node_info::{NodeInfo, SimpleNode};
pub use selector::matches_selector;
pub use time::now_ms;
pub use validation::non_empty_string;

/// Default vector dimension used by embedding models in the agent memory store.
pub const DEFAULT_EMBEDDING_DIM: usize = 384;
