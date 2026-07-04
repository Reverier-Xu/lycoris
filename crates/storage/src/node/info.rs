use std::collections::HashMap;

use lycoris_config::{NodeConfig, NodeInfo};

use crate::node::local::LocalStorage;

/// The local node representation built from configuration and dynamic storage.
#[derive(Debug, Clone)]
pub struct LocalNode {
  id: String,
  address: String,
  local: LocalStorage,
}

impl LocalNode {
  pub fn from_config(config: &NodeConfig, local: LocalStorage) -> Self {
    Self {
      id: config.id.clone(),
      address: config.address.clone(),
      local,
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
    self.local.labels().unwrap_or_default()
  }

  fn annotations(&self) -> HashMap<String, String> {
    self.local.annotations().unwrap_or_default()
  }
}
