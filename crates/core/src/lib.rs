#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod cluster_key;
mod fs;
mod paths;
mod resource_scope;
mod time;

pub use cluster_key::{ClusterKey, ClusterKeyError, default_cluster_key_path};
pub use fs::{PrivateFileCreate, write_private_file, write_private_file_if_absent};
pub use paths::{cluster_key_path_in, default_data_dir, project_dirs, user_data_dir};
pub use resource_scope::{ResourceScope, UnknownResourceScope};
pub use time::now_ms;
