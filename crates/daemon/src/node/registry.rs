use std::{collections::HashMap, time::Duration};

use lycoris_api::proto::NodeInfo as ProtoNodeInfo;
use lycoris_config::NodeInfo;

use crate::storage::{ClusterNodeRecord, NodeState, Storage};

/// In-memory/registry view of cluster nodes, backed by persistent storage.
#[derive(Debug, Clone)]
pub struct NodeRegistry {
  storage: Storage,
  ttl: Duration,
}

impl NodeRegistry {
  pub fn new(storage: Storage, ttl: Duration) -> Self {
    Self { storage, ttl }
  }

  /// Register or update the local node in the cluster registry.
  pub fn register_or_update<T: NodeInfo>(&self, node: &T) {
    let record = ClusterNodeRecord {
      id: node.id().to_string(),
      address: node.address().to_string(),
      last_heartbeat_ms: now_ms(),
      state: NodeState::Alive,
      labels: node.labels(),
      annotations: node.annotations(),
    };
    let _ = self.storage.upsert_cluster_node(&record);
  }

  /// Update heartbeat for a node.
  pub fn heartbeat<T: NodeInfo>(&self, node: &T) {
    self.register_or_update(node);
  }

  /// Merge a batch of nodes into the registry, keeping the latest heartbeat per
  /// node.
  pub fn merge(&self, nodes: Vec<ProtoNodeInfo>) {
    for info in nodes {
      let existing = self
        .storage
        .list_cluster_nodes()
        .unwrap_or_default()
        .into_iter()
        .find(|n| n.id == info.id);

      let keep = match existing {
        Some(existing) => info.last_heartbeat_unix_ms >= existing.last_heartbeat_ms,
        None => true,
      };

      if keep {
        let record = ClusterNodeRecord {
          id: info.id,
          address: info.address,
          last_heartbeat_ms: info.last_heartbeat_unix_ms,
          state: NodeState::Alive,
          labels: info.labels,
          annotations: info.annotations,
        };
        let _ = self.storage.upsert_cluster_node(&record);
      }
    }
  }

  /// Return a snapshot of all nodes currently in the registry.
  pub fn snapshot(&self) -> Vec<ProtoNodeInfo> {
    self
      .storage
      .list_cluster_nodes()
      .unwrap_or_default()
      .into_iter()
      .map(record_to_proto)
      .collect()
  }

  /// Return all alive nodes whose labels match the optional selector.
  pub fn list_alive(&self, selector: &HashMap<String, String>) -> Vec<ProtoNodeInfo> {
    let cutoff = now_ms() - i64::try_from(self.ttl.as_millis()).unwrap_or(i64::MAX);

    self
      .storage
      .list_cluster_nodes()
      .unwrap_or_default()
      .into_iter()
      .filter(|record| {
        record.state == NodeState::Alive
          && record.last_heartbeat_ms >= cutoff
          && matches_selector(&record.labels, selector)
      })
      .map(record_to_proto)
      .collect()
  }

  /// Mark nodes that have not sent a heartbeat within the TTL as offline.
  pub fn cleanup_offline(&self) {
    let cutoff = now_ms() - i64::try_from(self.ttl.as_millis()).unwrap_or(i64::MAX);
    let _ = self.storage.cleanup_offline_nodes(cutoff);
  }
}

fn record_to_proto(record: ClusterNodeRecord) -> ProtoNodeInfo {
  ProtoNodeInfo {
    id: record.id,
    address: record.address,
    labels: record.labels,
    annotations: record.annotations,
    last_heartbeat_unix_ms: record.last_heartbeat_ms,
  }
}

fn matches_selector(labels: &HashMap<String, String>, selector: &HashMap<String, String>) -> bool {
  if selector.is_empty() {
    return true;
  }
  selector
    .iter()
    .all(|(key, value)| labels.get(key) == Some(value))
}

fn now_ms() -> i64 {
  use std::time::{SystemTime, UNIX_EPOCH};
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(0))
    .unwrap_or(0)
}

#[cfg(test)]
mod tests {
  use lycoris_config::NodeConfig;
  use tempfile::TempDir;

  use super::*;
  use crate::node::info::LocalNode;

  fn test_registry() -> (TempDir, NodeRegistry) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("registry.db")).unwrap();
    (dir, NodeRegistry::new(storage, Duration::from_secs(60)))
  }

  fn node_storage() -> (TempDir, Storage) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("node.db")).unwrap();
    (dir, storage)
  }

  #[test]
  fn register_and_list() {
    let (_dir, registry) = test_registry();
    let (_node_dir, node_storage) = node_storage();
    let config = NodeConfig {
      id: "node-1".to_string(),
      address: "127.0.0.1:5001".to_string(),
    };
    let node = LocalNode::from_config(&config, node_storage);
    registry.register_or_update(&node);

    let all = registry.list_alive(&HashMap::new());
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, "node-1");
  }

  #[test]
  fn merge_keeps_latest_heartbeat() {
    let (_dir, registry) = test_registry();
    let old = ProtoNodeInfo {
      id: "node-1".to_string(),
      address: "127.0.0.1:1".to_string(),
      labels: HashMap::new(),
      annotations: HashMap::new(),
      last_heartbeat_unix_ms: 100,
    };
    let new = ProtoNodeInfo {
      id: "node-1".to_string(),
      address: "127.0.0.1:2".to_string(),
      labels: HashMap::new(),
      annotations: HashMap::new(),
      last_heartbeat_unix_ms: 200,
    };

    registry.merge(vec![new.clone()]);
    registry.merge(vec![old.clone()]);

    let snapshot = registry.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].address, "127.0.0.1:2");
    assert_eq!(snapshot[0].last_heartbeat_unix_ms, 200);
  }

  #[test]
  fn selector_filters_labels() {
    let (_dir, registry) = test_registry();

    let (_dir_a, storage_a) = node_storage();
    storage_a.set_local_label("zone", "cn").unwrap();
    let node_a = LocalNode::from_config(
      &NodeConfig {
        id: "a".to_string(),
        address: "127.0.0.1:1".to_string(),
      },
      storage_a,
    );

    let (_dir_b, storage_b) = node_storage();
    storage_b.set_local_label("zone", "us").unwrap();
    let node_b = LocalNode::from_config(
      &NodeConfig {
        id: "b".to_string(),
        address: "127.0.0.1:2".to_string(),
      },
      storage_b,
    );

    registry.register_or_update(&node_a);
    registry.register_or_update(&node_b);

    let selector: HashMap<String, String> = [("zone".to_string(), "cn".to_string())]
      .into_iter()
      .collect();
    let matched = registry.list_alive(&selector);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].id, "a");
  }
}
