#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod cluster_key;
mod node_info;
mod paths;
mod resource_scope;
mod selector;
mod time;
mod validation;

pub use cluster_key::{ClusterKey, ClusterKeyError, default_cluster_key_path};
pub use node_info::{NodeInfo, SimpleNode};
pub use paths::{default_data_dir, user_data_dir};
pub use resource_scope::ResourceScope;
pub use selector::matches_selector;
pub use time::now_ms;
pub use validation::non_empty_string;

/// Default vector dimension used by embedding models in the agent memory store.
pub const DEFAULT_EMBEDDING_DIM: usize = 384;
