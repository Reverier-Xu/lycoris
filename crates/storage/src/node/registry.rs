use std::{collections::HashMap, time::Duration};

use lycoris_core::{NodeInfo, matches_selector, time::now_ms};

use crate::node::{
  NodeDomain, NodeState,
  cluster::{ClusterNodeRecord, ClusterNodeStorage},
};

/// TTL-aware registry view over the persisted cluster node records.
#[derive(Debug, Clone)]
pub struct NodeRegistry {
  cluster: ClusterNodeStorage,
  ttl: Duration,
}

impl NodeRegistry {
  pub fn new(node: NodeDomain, ttl: Duration) -> Self {
    Self {
      cluster: node.cluster,
      ttl,
    }
  }

  /// Register or update a node, marking it alive with the current timestamp.
  pub fn register_or_update<T: NodeInfo>(&self, node: &T) -> Result<(), crate::StorageError> {
    let record = ClusterNodeRecord {
      id: node.id().to_string(),
      address: node.address().to_string(),
      last_heartbeat_ms: now_ms(),
      state: NodeState::Alive,
      labels: node.labels().clone(),
      annotations: node.annotations().clone(),
    };
    self.cluster.upsert(&record)
  }

  /// Merge a batch of records, keeping the latest heartbeat per node.
  pub fn merge(&self, nodes: Vec<ClusterNodeRecord>) -> Result<(), crate::StorageError> {
    let existing: HashMap<String, ClusterNodeRecord> = self
      .cluster
      .list()?
      .into_iter()
      .map(|record| (record.id.clone(), record))
      .collect();

    for incoming in nodes {
      let keep = existing
        .get(&incoming.id)
        .map(|record| incoming.last_heartbeat_ms >= record.last_heartbeat_ms)
        .unwrap_or(true);

      if keep {
        self.cluster.upsert(&incoming)?;
      }
    }
    Ok(())
  }

  /// Return a snapshot of all nodes currently in the registry.
  pub fn snapshot(&self) -> Result<Vec<ClusterNodeRecord>, crate::StorageError> {
    self.cluster.list()
  }

  /// Return all alive nodes whose labels match the optional selector.
  pub fn list_alive(
    &self, selector: &HashMap<String, String>,
  ) -> Result<Vec<ClusterNodeRecord>, crate::StorageError> {
    let cutoff = now_ms() - i64::try_from(self.ttl.as_millis()).unwrap_or(i64::MAX);

    let records: Vec<ClusterNodeRecord> = self
      .cluster
      .list()?
      .into_iter()
      .filter(|record| {
        record.state == NodeState::Alive
          && record.last_heartbeat_ms >= cutoff
          && matches_selector(&record.labels, selector)
      })
      .collect();
    Ok(records)
  }

  /// Mark nodes that have not sent a heartbeat within the TTL as offline.
  pub fn cleanup_offline(&self) -> Result<(), crate::StorageError> {
    let cutoff = now_ms() - i64::try_from(self.ttl.as_millis()).unwrap_or(i64::MAX);
    self.cluster.cleanup_offline(cutoff)
  }
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use lycoris_config::NodeConfig;
  use tempfile::TempDir;

  use super::*;
  use crate::{Storage, node::info::LocalNode};

  fn test_registry() -> (TempDir, NodeRegistry) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("registry.redb")).unwrap();
    (
      dir,
      NodeRegistry::new(storage.node(), Duration::from_secs(60)),
    )
  }

  fn node_storage(dir: &TempDir) -> crate::node::NodeDomain {
    Storage::open(dir.path().join("node.redb")).unwrap().node()
  }

  #[test]
  fn register_and_list() {
    let (_dir, registry) = test_registry();
    let dir = TempDir::new().unwrap();
    let node_domain = node_storage(&dir);
    let config = NodeConfig {
      id: "node-1".to_string(),
      address: "127.0.0.1:5001".to_string(),
    };
    let node = LocalNode::from_config(&config, node_domain.local.clone()).unwrap();
    registry.register_or_update(&node).unwrap();

    let all = registry.list_alive(&HashMap::new()).unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, "node-1");
  }

  #[test]
  fn merge_keeps_latest_heartbeat() {
    let (_dir, registry) = test_registry();
    let old = ClusterNodeRecord {
      id: "node-1".to_string(),
      address: "127.0.0.1:1".to_string(),
      last_heartbeat_ms: 100,
      state: NodeState::Alive,
      labels: HashMap::new(),
      annotations: HashMap::new(),
    };
    let new = ClusterNodeRecord {
      id: "node-1".to_string(),
      address: "127.0.0.1:2".to_string(),
      last_heartbeat_ms: 200,
      state: NodeState::Alive,
      labels: HashMap::new(),
      annotations: HashMap::new(),
    };

    registry.merge(vec![new.clone()]).unwrap();
    registry.merge(vec![old.clone()]).unwrap();

    let snapshot = registry.snapshot().unwrap();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].address, "127.0.0.1:2");
    assert_eq!(snapshot[0].last_heartbeat_ms, 200);
  }

  #[test]
  fn selector_filters_labels() {
    let (_dir, registry) = test_registry();

    let dir = TempDir::new().unwrap();
    let node_domain_a = node_storage(&dir);
    node_domain_a.local.set_label("zone", "cn").unwrap();
    let node_a = LocalNode::from_config(
      &NodeConfig {
        id: "a".to_string(),
        address: "127.0.0.1:1".to_string(),
      },
      node_domain_a.local.clone(),
    )
    .unwrap();

    let dir = TempDir::new().unwrap();
    let node_domain_b = node_storage(&dir);
    node_domain_b.local.set_label("zone", "us").unwrap();
    let node_b = LocalNode::from_config(
      &NodeConfig {
        id: "b".to_string(),
        address: "127.0.0.1:2".to_string(),
      },
      node_domain_b.local.clone(),
    )
    .unwrap();

    registry.register_or_update(&node_a).unwrap();
    registry.register_or_update(&node_b).unwrap();

    let selector: HashMap<String, String> = [("zone".to_string(), "cn".to_string())]
      .into_iter()
      .collect();
    let matched = registry.list_alive(&selector).unwrap();
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].id, "a");
  }
}
