pub mod local;
pub mod meta;
pub mod peers;

use std::sync::Arc;

pub use local::LocalStorage;
pub use meta::MetaStorage;
pub use peers::{PeerRecord, PeerStorage};
use redb::Database;

/// Node-local storage domain.
///
/// Owns the peer-endpoint and local-attribute state that is persisted per
/// node. A single `redb::Database` is shared with the other domains through
/// an `Arc`.
#[derive(Debug, Clone)]
pub struct NodeDomain {
  local: LocalStorage,
  peers: PeerStorage,
  meta: MetaStorage,
}

impl NodeDomain {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self {
      local: LocalStorage::new(db.clone()),
      peers: PeerStorage::new(db.clone()),
      meta: MetaStorage::new(db),
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

  /// Access the node-local metadata table (restart-monotonic counters).
  pub fn meta(&self) -> &MetaStorage {
    &self.meta
  }
}
