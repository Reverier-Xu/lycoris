use std::{
  collections::HashMap,
  path::Path,
  sync::{Arc, Mutex},
};

use lycoris_config::time::now_ms;
use rusqlite::{Connection, params};
use tokio::sync::Notify;

use crate::error::StorageError;

/// Persistent storage for dynamic cluster node state.
///
/// Each daemon node keeps its own local SQLite database for peer list, primary
/// endpoint, node labels/annotations, and peer reachability information. The
/// on-disk config file only supplies bootstrap identity and networking
/// information; all runtime-discovered and runtime-modified state lives here.
#[derive(Debug, Clone)]
pub struct ClusterStorage {
  connection: Arc<Mutex<Connection>>,
  change_notify: Arc<Notify>,
}

#[derive(Debug, Clone)]
pub struct PeerRecord {
  pub address: String,
  pub is_primary: bool,
  pub online: bool,
  pub last_seen_ms: Option<i64>,
  pub last_attempt_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ClusterNodeRecord {
  pub id: String,
  pub address: String,
  pub last_heartbeat_ms: i64,
  pub state: NodeState,
  pub labels: HashMap<String, String>,
  pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
  Alive,
  Offline,
}

impl ClusterStorage {
  /// Open or create the SQLite database at the given path.
  pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
    let connection = Connection::open(path)?;
    Self::init_schema(&connection)?;
    Ok(Self {
      connection: Arc::new(Mutex::new(connection)),
      change_notify: Arc::new(Notify::new()),
    })
  }

  /// Subscribe to changes that should trigger an immediate sync.
  pub fn change_notify(&self) -> Arc<Notify> {
    self.change_notify.clone()
  }

  fn notify_change(&self) {
    self.change_notify.notify_one();
  }

  fn with_connection<R, F>(&self, operation: F) -> Result<R, StorageError>
  where
    F: FnOnce(&Connection) -> Result<R, StorageError>, {
    let connection = self
      .connection
      .lock()
      .map_err(|_| StorageError::LockPoisoned)?;
    operation(&connection)
  }

  fn with_connection_mutating<R, F>(&self, operation: F) -> Result<R, StorageError>
  where
    F: FnOnce(&Connection) -> Result<R, StorageError>, {
    let result = self.with_connection(operation)?;
    self.notify_change();
    Ok(result)
  }

  fn init_schema(connection: &Connection) -> Result<(), StorageError> {
    connection.execute(
      "CREATE TABLE IF NOT EXISTS local_node_labels (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
      [],
    )?;
    connection.execute(
      "CREATE TABLE IF NOT EXISTS local_node_annotations (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
      [],
    )?;
    connection.execute(
      "CREATE TABLE IF NOT EXISTS cluster_nodes (
                id TEXT PRIMARY KEY,
                address TEXT NOT NULL,
                last_heartbeat_ms INTEGER NOT NULL,
                state TEXT NOT NULL
            )",
      [],
    )?;
    connection.execute(
      "CREATE TABLE IF NOT EXISTS cluster_node_attributes (
                node_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (node_id, kind, key)
            )",
      [],
    )?;
    connection.execute(
      "CREATE TABLE IF NOT EXISTS peers (
                address TEXT PRIMARY KEY,
                is_primary BOOLEAN NOT NULL DEFAULT 0,
                online BOOLEAN NOT NULL DEFAULT 0,
                last_seen_ms INTEGER,
                last_attempt_ms INTEGER
            )",
      [],
    )?;
    Ok(())
  }

  // --- Local node attributes (not synced directly, but included in local node
  // info) ---

  pub fn set_local_label(&self, key: &str, value: &str) -> Result<(), StorageError> {
    self.with_connection_mutating(|conn| {
      conn.execute(
        "INSERT INTO local_node_labels (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
      )?;
      Ok(())
    })
  }

  pub fn set_local_annotation(&self, key: &str, value: &str) -> Result<(), StorageError> {
    self.with_connection_mutating(|conn| {
      conn.execute(
        "INSERT INTO local_node_annotations (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
      )?;
      Ok(())
    })
  }

  pub fn local_labels(&self) -> Result<HashMap<String, String>, StorageError> {
    self.with_connection(|conn| {
      let mut statement = conn.prepare("SELECT key, value FROM local_node_labels")?;
      let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
      })?;
      rows
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(Into::into)
    })
  }

  pub fn local_annotations(&self) -> Result<HashMap<String, String>, StorageError> {
    self.with_connection(|conn| {
      let mut statement = conn.prepare("SELECT key, value FROM local_node_annotations")?;
      let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
      })?;
      rows
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(Into::into)
    })
  }

  // --- Cluster node registry (synced) ---

  pub fn upsert_cluster_node(&self, node: &ClusterNodeRecord) -> Result<(), StorageError> {
    self.with_connection_mutating(|conn| {
      conn.execute(
        "INSERT INTO cluster_nodes (id, address, last_heartbeat_ms, state)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET
                address = excluded.address,
                last_heartbeat_ms = excluded.last_heartbeat_ms,
                state = excluded.state",
        params![
          node.id,
          node.address,
          node.last_heartbeat_ms,
          state_to_string(node.state),
        ],
      )?;

      conn.execute(
        "DELETE FROM cluster_node_attributes WHERE node_id = ?1",
        [&node.id],
      )?;
      let mut stmt = conn.prepare(
        "INSERT INTO cluster_node_attributes (node_id, kind, key, value) VALUES (?1, ?2, ?3, ?4)",
      )?;
      for (key, value) in &node.labels {
        stmt.execute(params![&node.id, "label", key, value])?;
      }
      for (key, value) in &node.annotations {
        stmt.execute(params![&node.id, "annotation", key, value])?;
      }
      Ok(())
    })
  }

  pub fn list_cluster_nodes(&self) -> Result<Vec<ClusterNodeRecord>, StorageError> {
    self.with_connection(|conn| {
      let mut statement =
        conn.prepare("SELECT id, address, last_heartbeat_ms, state FROM cluster_nodes")?;
      let rows = statement.query_map([], |row| {
        Ok((
          row.get::<_, String>(0)?,
          row.get::<_, String>(1)?,
          row.get::<_, i64>(2)?,
          row.get::<_, String>(3)?,
        ))
      })?;
      rows
        .map(|row| {
          let (id, address, last_heartbeat_ms, state_str) = row?;
          Ok(ClusterNodeRecord {
            id: id.clone(),
            address,
            last_heartbeat_ms,
            state: string_to_state(&state_str)?,
            labels: Self::node_attributes(conn, &id, "label").unwrap_or_default(),
            annotations: Self::node_attributes(conn, &id, "annotation").unwrap_or_default(),
          })
        })
        .collect()
    })
  }

  pub fn cleanup_offline_nodes(&self, cutoff_ms: i64) -> Result<(), StorageError> {
    self.with_connection(|conn| {
      conn.execute(
        "UPDATE cluster_nodes SET state = 'offline' WHERE last_heartbeat_ms < ?1 AND state = 'alive'",
        [cutoff_ms],
      )?;
      Ok(())
    })
  }

  fn node_attributes(
    connection: &Connection, node_id: &str, kind: &str,
  ) -> Result<HashMap<String, String>, StorageError> {
    let mut statement = connection
      .prepare("SELECT key, value FROM cluster_node_attributes WHERE node_id = ?1 AND kind = ?2")?;
    let rows = statement.query_map(params![node_id, kind], |row| {
      Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    rows
      .collect::<Result<HashMap<_, _>, _>>()
      .map_err(Into::into)
  }

  // --- Peers ---

  /// Insert a bootstrap peer if it is not already known.
  pub fn seed_peer(&self, address: &str) -> Result<(), StorageError> {
    self.with_connection(|conn| {
      conn.execute(
        "INSERT OR IGNORE INTO peers (address, is_primary, online)
             VALUES (?1, 0, 0)",
        [address],
      )?;
      Ok(())
    })
  }

  /// Record that a peer was reachable at the given timestamp.
  pub fn mark_peer_seen(&self, address: &str, seen_ms: i64) -> Result<(), StorageError> {
    self.with_connection(|conn| {
      conn.execute(
        "INSERT INTO peers (address, is_primary, online, last_seen_ms, last_attempt_ms)
             VALUES (?1, 0, 1, ?2, ?2)
             ON CONFLICT(address) DO UPDATE SET
                online = 1,
                last_seen_ms = excluded.last_seen_ms,
                last_attempt_ms = excluded.last_attempt_ms",
        params![address, seen_ms],
      )?;
      Ok(())
    })
  }

  /// Record that a communication attempt with a peer happened now.
  pub fn mark_peer_attempt(&self, address: &str, online: bool) -> Result<(), StorageError> {
    let now = now_ms();
    self.with_connection(|conn| {
      conn.execute(
        "INSERT INTO peers (address, is_primary, online, last_attempt_ms)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(address) DO UPDATE SET
                online = excluded.online,
                last_attempt_ms = excluded.last_attempt_ms",
        params![address, online as i32, now],
      )?;
      Ok(())
    })
  }

  /// Promote a peer to primary communication endpoint.
  pub fn set_primary(&self, address: &str) -> Result<(), StorageError> {
    self.with_connection(|conn| {
      conn.execute("UPDATE peers SET is_primary = 0", [])?;
      conn.execute(
        "INSERT INTO peers (address, is_primary, online, last_attempt_ms)
             VALUES (?1, 1, 1, ?2)
             ON CONFLICT(address) DO UPDATE SET
                is_primary = 1,
                online = 1,
                last_attempt_ms = excluded.last_attempt_ms",
        params![address, now_ms()],
      )?;
      Ok(())
    })
  }

  /// Get the current primary endpoint, if any.
  pub fn get_primary(&self) -> Result<Option<String>, StorageError> {
    self.with_connection(|conn| {
      let mut statement = conn.prepare("SELECT address FROM peers WHERE is_primary = 1 LIMIT 1")?;
      let mut rows = statement.query([])?;
      Ok(
        rows
          .next()?
          .map(|row| row.get::<_, String>(0))
          .transpose()?,
      )
    })
  }

  /// Return candidate peer addresses excluding the current primary.
  pub fn fallback_peers(&self) -> Result<Vec<String>, StorageError> {
    self.with_connection(|conn| {
      let mut statement = conn.prepare(
        "SELECT address FROM peers WHERE is_primary = 0 ORDER BY online DESC, last_seen_ms DESC",
      )?;
      let rows = statement.query_map([], |row| row.get(0))?;
      rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    })
  }
}

fn state_to_string(state: NodeState) -> &'static str {
  match state {
    NodeState::Alive => "alive",
    NodeState::Offline => "offline",
  }
}

fn string_to_state(s: &str) -> Result<NodeState, StorageError> {
  match s {
    "alive" => Ok(NodeState::Alive),
    "offline" => Ok(NodeState::Offline),
    other => Err(StorageError::CorruptState(other.to_string())),
  }
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  #[test]
  fn seed_and_list_peers() {
    let dir = TempDir::new().unwrap();
    let storage = ClusterStorage::open(dir.path().join("peers.db")).unwrap();
    storage.seed_peer("https://peer-a:5000").unwrap();
    storage.seed_peer("https://peer-b:5000").unwrap();

    let peers = storage.list_cluster_nodes().unwrap();
    assert!(peers.is_empty());
  }

  #[test]
  fn primary_round_trip() {
    let dir = TempDir::new().unwrap();
    let storage = ClusterStorage::open(dir.path().join("peers.db")).unwrap();
    storage.seed_peer("https://peer-a:5000").unwrap();
    storage.set_primary("https://peer-a:5000").unwrap();

    let primary = storage.get_primary().unwrap();
    assert_eq!(primary, Some("https://peer-a:5000".to_string()));
  }

  #[test]
  fn local_and_cluster_attributes() {
    let dir = TempDir::new().unwrap();
    let storage = ClusterStorage::open(dir.path().join("nodes.db")).unwrap();

    storage.set_local_label("zone", "cn").unwrap();
    storage.set_local_annotation("note", "test").unwrap();

    assert_eq!(
      storage.local_labels().unwrap().get("zone"),
      Some(&"cn".to_string())
    );
    assert_eq!(
      storage.local_annotations().unwrap().get("note"),
      Some(&"test".to_string())
    );

    storage
      .upsert_cluster_node(&ClusterNodeRecord {
        id: "node-1".to_string(),
        address: "127.0.0.1:1".to_string(),
        last_heartbeat_ms: 100,
        state: NodeState::Alive,
        labels: [("role".to_string(), "worker".to_string())]
          .into_iter()
          .collect(),
        annotations: HashMap::new(),
      })
      .unwrap();

    let nodes = storage.list_cluster_nodes().unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].labels.get("role"), Some(&"worker".to_string()));
  }
}
