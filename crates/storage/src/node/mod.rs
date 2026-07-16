pub mod local;
pub mod peers;

use std::sync::Arc;

pub use local::LocalStorage;
pub use peers::{PeerRecord, PeerStorage};
use redb::Database;

/// Node-local storage domain.
///
/// This is the first of the three planned storage domains. It owns the
/// peer-endpoint and local-attribute state that is persisted per node. A single
/// `redb::Database` is shared with the other domains through an `Arc`.
#[derive(Debug, Clone)]
pub struct NodeDomain {
  local: LocalStorage,
  peers: PeerStorage,
}

impl NodeDomain {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self {
      local: LocalStorage::new(db.clone()),
      peers: PeerStorage::new(db),
    }
  }

  /// Access local node attribute storage.
  pub fn local(&self) -> &LocalStorage {
    &self.local
  }

  /// Access peer endpoint storage.
  pub fn peers(&self) -> &PeerStorage {
    &self.peers
  }
}
