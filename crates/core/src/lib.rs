#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod cluster_key;
mod paths;
mod resource_scope;
mod time;

pub use cluster_key::{ClusterKey, ClusterKeyError, default_cluster_key_path};
pub use paths::{cluster_key_path_in, default_data_dir, user_data_dir};
pub use resource_scope::{ResourceScope, UnknownResourceScope};
pub use time::now_ms;
