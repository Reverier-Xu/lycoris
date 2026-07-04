pub mod cluster;
pub mod info;
pub mod local;
pub mod peers;
pub mod registry;

use std::sync::Arc;

pub use cluster::{ClusterNodeRecord, ClusterNodeStorage, NodeState};
pub use info::LocalNode;
pub use local::LocalStorage;
pub use peers::{PeerRecord, PeerStorage};
use redb::Database;
pub use registry::NodeRegistry;
use tokio::sync::Notify;

/// Node-local storage domain.
///
/// This is the first of the three planned storage domains. It owns the
/// cluster-membership, peer-endpoint, and local-attribute state that is
/// persisted per node. A single `redb::Database` is shared with the other
/// domains through an `Arc`.
#[derive(Debug, Clone)]
pub struct NodeDomain {
  pub local: LocalStorage,
  pub cluster: ClusterNodeStorage,
  pub peers: PeerStorage,
  notify: Arc<Notify>,
}

impl NodeDomain {
  pub(crate) fn new(db: Arc<Database>, notify: Arc<Notify>) -> Self {
    Self {
      local: LocalStorage::new(db.clone()),
      cluster: ClusterNodeStorage::new(db.clone()),
      peers: PeerStorage::new(db),
      notify,
    }
  }

  /// Subscribe to changes that should trigger an immediate sync.
  pub fn change_notify(&self) -> Arc<Notify> {
    self.notify.clone()
  }
}
