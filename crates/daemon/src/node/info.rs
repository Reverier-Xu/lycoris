use std::{collections::HashMap, sync::Arc};

use lycoris_config::{NodeConfig, NodeInfo};
use lycoris_storage::ClusterStorage;

/// The local node representation built from configuration and dynamic storage.
#[derive(Debug, Clone)]
pub struct LocalNode {
  id: String,
  address: String,
  storage: Arc<ClusterStorage>,
}

impl LocalNode {
  pub fn from_config(config: &NodeConfig, storage: ClusterStorage) -> Self {
    Self {
      id: config.id.clone(),
      address: config.address.clone(),
      storage: Arc::new(storage),
    }
  }
}

impl NodeInfo for LocalNode {
  fn id(&self) -> &str {
    &self.id
  }

  fn address(&self) -> &str {
    &self.address
  }

  fn labels(&self) -> HashMap<String, String> {
    self.storage.local_labels().unwrap_or_default()
  }

  fn annotations(&self) -> HashMap<String, String> {
    self.storage.local_annotations().unwrap_or_default()
  }
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  #[test]
  fn local_node_from_config_and_storage() {
    let dir = TempDir::new().unwrap();
    let storage = ClusterStorage::open(dir.path().join("node.db")).unwrap();
    storage.set_local_label("arch", "x86_64").unwrap();

    let config = NodeConfig {
      id: "node-1".to_string(),
      address: "127.0.0.1:5001".to_string(),
    };
    let node = LocalNode::from_config(&config, storage);
    assert_eq!(node.id(), "node-1");
    assert_eq!(node.labels().get("arch"), Some(&"x86_64".to_string()));
  }
}
