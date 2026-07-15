use std::collections::HashMap;

use lycoris_config::NodeConfig;
use lycoris_core::NodeInfo;

use crate::node::local::LocalStorage;

/// The local node representation built from configuration and dynamic storage.
#[derive(Debug, Clone)]
pub struct LocalNode {
  id: String,
  address: String,
  labels: HashMap<String, String>,
  annotations: HashMap<String, String>,
}

impl LocalNode {
  pub fn from_config(
    config: &NodeConfig, local: LocalStorage,
  ) -> Result<Self, crate::StorageError> {
    Ok(Self {
      id: config.id.clone(),
      address: config.address.clone(),
      labels: local.labels()?,
      annotations: local.annotations()?,
    })
  }
}

impl NodeInfo for LocalNode {
  fn id(&self) -> &str {
    &self.id
  }

  fn address(&self) -> &str {
    &self.address
  }

  fn labels(&self) -> &HashMap<String, String> {
    &self.labels
  }

  fn annotations(&self) -> &HashMap<String, String> {
    &self.annotations
  }
}
