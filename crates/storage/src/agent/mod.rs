//! Agent orchestration storage.
//!
//! This domain persists agent session metadata and vector-backed memories.
//!
//! - `Session` metadata is stored in the shared `redb` database via
//!   `RedbSessionStorage`.
//! - `MemoryEntry` records (including their embeddings) are stored in an
//!   embedded LanceDB table for fast vector similarity search.
//! - Memories can be scoped as `NodeLocal` or `ClusterShared`; only shared
//!   memories participate in cluster anti-entropy synchronization.

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use arrow_array::{
  Array, BinaryArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
  types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures_util::stream::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use lycoris_core::{DEFAULT_EMBEDDING_DIM, ResourceScope};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::{
  StorageError,
  bytes::{Bytes, decode, encode},
  error::{is_missing_table, redb_err},
};

const SESSIONS: TableDefinition<&str, Bytes> = TableDefinition::new("agent_sessions");
const MEMORY_TABLE: &str = "memories";

/// A stored agent session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
  pub id: String,
  pub metadata: HashMap<String, String>,
}

/// A memory entry (short- or long-term).
#[derive(Debug, Clone)]
pub struct MemoryEntry {
  pub id: String,
  pub content: Vec<u8>,
  pub embedding: Vec<f32>,
  pub metadata: HashMap<String, String>,
  pub scope: ResourceScope,
  /// `None` means this memory originated on the local node.
  pub source_node_id: Option<String>,
  pub updated_at_ms: i64,
  pub content_hash: String,
  pub version: u64,
}

impl MemoryEntry {
  /// Compute the content hash for the entry body.
  pub fn compute_content_hash(content: &[u8]) -> String {
    blake3::hash(content).to_hex().to_string()
  }

  /// Return the logical version used for conflict resolution.
  pub fn version(&self) -> u64 {
    self.version
  }
}

impl crate::versioned::VersionedRecord for MemoryEntry {
  fn version(&self) -> u64 {
    self.version
  }

  fn updated_at_ms(&self) -> i64 {
    self.updated_at_ms
  }

  fn scope(&self) -> ResourceScope {
    self.scope
  }
}

/// Storage for active agent sessions.
pub trait SessionStorage: std::fmt::Debug + Send + Sync {
  fn create(&self, session: &Session) -> Result<(), AgentStorageError>;
  fn upsert(&self, session: &Session) -> Result<(), AgentStorageError>;
  fn get(&self, id: &str) -> Result<Option<Session>, AgentStorageError>;
  fn list(&self) -> Result<Vec<Session>, AgentStorageError>;
  fn delete(&self, id: &str) -> Result<(), AgentStorageError>;
}

/// Storage for agent memory.
#[async_trait]
pub trait MemoryStorage: std::fmt::Debug + Send + Sync {
  async fn store(&self, entry: &MemoryEntry) -> Result<(), AgentStorageError>;
  async fn recall(
    &self, query: Vec<f32>, limit: usize,
  ) -> Result<Vec<MemoryEntry>, AgentStorageError>;
  /// Return the memory with the given id, if any.
  async fn get(&self, id: &str) -> Result<Option<MemoryEntry>, AgentStorageError>;
  /// Return all memories.
  async fn list(&self) -> Result<Vec<MemoryEntry>, AgentStorageError>;
  /// Return memories whose scope is `ClusterShared`.
  async fn list_shared(&self) -> Result<Vec<MemoryEntry>, AgentStorageError>;
  /// Return memories whose scope is `NodeLocal`.
  async fn list_local(&self) -> Result<Vec<MemoryEntry>, AgentStorageError>;
}

/// Errors that can occur in agent storage backends.
#[derive(Debug, thiserror::Error)]
pub enum AgentStorageError {
  #[error("backend error: {0}")]
  Backend(String),
}

impl From<StorageError> for AgentStorageError {
  fn from(error: StorageError) -> Self {
    Self::Backend(error.to_string())
  }
}

impl From<std::io::Error> for AgentStorageError {
  fn from(error: std::io::Error) -> Self {
    Self::Backend(error.to_string())
  }
}

/// redb-backed implementation of `SessionStorage`.
#[derive(Debug, Clone)]
pub struct RedbSessionStorage {
  db: Arc<Database>,
}

impl RedbSessionStorage {
  pub(crate) fn new(db: Arc<Database>) -> Self {
    Self { db }
  }
}

impl SessionStorage for RedbSessionStorage {
  fn create(&self, session: &Session) -> Result<(), AgentStorageError> {
    self.upsert(session)
  }

  fn upsert(&self, session: &Session) -> Result<(), AgentStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(SESSIONS).map_err(redb_err)?;
      table
        .insert(session.id.as_str(), Bytes(encode(session)?))
        .map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }

  fn get(&self, id: &str) -> Result<Option<Session>, AgentStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(SESSIONS).map_err(redb_err) {
      Ok(table) => table,
      Err(error) if is_missing_table(&error) => return Ok(None),
      Err(error) => return Err(error.into()),
    };

    table
      .get(id)
      .map_err(redb_err)?
      .map(|guard| decode::<Session>(&guard.value().0))
      .transpose()
      .map_err(Into::into)
  }

  fn list(&self) -> Result<Vec<Session>, AgentStorageError> {
    let read_txn = self.db.begin_read().map_err(redb_err)?;
    let table = match read_txn.open_table(SESSIONS).map_err(redb_err) {
      Ok(table) => table,
      Err(error) if is_missing_table(&error) => return Ok(Vec::new()),
      Err(error) => return Err(error.into()),
    };

    table
      .iter()
      .map_err(redb_err)?
      .map(|row| {
        let (_, value) = row.map_err(redb_err)?;
        decode::<Session>(&value.value().0)
      })
      .collect::<Result<Vec<_>, _>>()
      .map_err(Into::into)
  }

  fn delete(&self, id: &str) -> Result<(), AgentStorageError> {
    let write_txn = self.db.begin_write().map_err(redb_err)?;
    {
      let mut table = write_txn.open_table(SESSIONS).map_err(redb_err)?;
      table.remove(id).map_err(redb_err)?;
    }
    write_txn.commit().map_err(redb_err)?;
    Ok(())
  }
}

/// LanceDB-backed implementation of `MemoryStorage`.
pub struct LanceDbMemoryStorage {
  uri: PathBuf,
  embedding_dim: usize,
  connection: tokio::sync::RwLock<Option<lancedb::Connection>>,
}

impl std::fmt::Debug for LanceDbMemoryStorage {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("LanceDbMemoryStorage")
      .field("uri", &self.uri)
      .field("embedding_dim", &self.embedding_dim)
      .finish_non_exhaustive()
  }
}

impl LanceDbMemoryStorage {
  pub(crate) fn new(uri: PathBuf) -> Self {
    Self::with_embedding_dim(uri, DEFAULT_EMBEDDING_DIM)
  }

  pub(crate) fn with_embedding_dim(uri: PathBuf, embedding_dim: usize) -> Self {
    Self {
      uri,
      embedding_dim,
      connection: tokio::sync::RwLock::new(None),
    }
  }

  async fn connection(&self) -> Result<lancedb::Connection, AgentStorageError> {
    let guard = self.connection.read().await;
    if let Some(conn) = guard.as_ref() {
      return Ok(conn.clone());
    }
    drop(guard);

    let mut guard = self.connection.write().await;
    if let Some(conn) = guard.as_ref() {
      return Ok(conn.clone());
    }

    let uri = self.uri.to_string_lossy().to_string();
    let conn = lancedb::connect(&uri)
      .execute()
      .await
      .map_err(|error| AgentStorageError::Backend(error.to_string()))?;
    *guard = Some(conn.clone());
    Ok(conn)
  }

  fn schema(&self) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
      Field::new("id", DataType::Utf8, false),
      Field::new("content", DataType::Binary, true),
      Field::new("metadata", DataType::Utf8, true),
      Field::new(
        "embedding",
        DataType::FixedSizeList(
          Arc::new(Field::new("item", DataType::Float32, true)),
          self.embedding_dim as i32,
        ),
        true,
      ),
      Field::new("scope", DataType::Utf8, false),
      Field::new("source_node_id", DataType::Utf8, true),
      Field::new("updated_at_ms", DataType::Int64, false),
      Field::new("content_hash", DataType::Utf8, false),
      Field::new("version", DataType::UInt64, false),
    ]))
  }

  fn check_dim(&self, embedding: &[f32]) -> Result<(), AgentStorageError> {
    if embedding.len() != self.embedding_dim {
      return Err(AgentStorageError::Backend(format!(
        "embedding dimension mismatch: expected {}, got {}",
        self.embedding_dim,
        embedding.len()
      )));
    }
    Ok(())
  }

  async fn query_filtered(&self, filter: &str) -> Result<Vec<MemoryEntry>, AgentStorageError> {
    let conn = self.connection().await?;

    let table = match conn.open_table(MEMORY_TABLE).execute().await {
      Ok(table) => table,
      Err(_) => return Ok(Vec::new()),
    };

    let stream = table
      .query()
      .only_if(filter)
      .execute()
      .await
      .map_err(|error| AgentStorageError::Backend(error.to_string()))?;

    let batches = stream
      .try_collect::<Vec<_>>()
      .await
      .map_err(|error| AgentStorageError::Backend(error.to_string()))?;

    let mut entries = Vec::new();
    for batch in batches {
      entries.extend(parse_memory_batch(&batch)?);
    }
    Ok(entries)
  }

  fn batch_for(&self, entries: &[MemoryEntry]) -> Result<RecordBatch, AgentStorageError> {
    let schema = self.schema();
    let ids = StringArray::from_iter_values(entries.iter().map(|entry| entry.id.as_str()));
    let contents = BinaryArray::from(
      entries
        .iter()
        .map(|entry| entry.content.as_slice())
        .collect::<Vec<_>>(),
    );
    let metadata = StringArray::from_iter_values(
      entries
        .iter()
        .map(|entry| toml::to_string(&entry.metadata))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
          AgentStorageError::Backend(format!("metadata serialization failed: {error}"))
        })?,
    );
    let embedding_values: Vec<_> = entries
      .iter()
      .map(|entry| {
        entry
          .embedding
          .iter()
          .map(|value| Some(*value))
          .collect::<Vec<_>>()
      })
      .collect();
    let embeddings = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
      embedding_values.into_iter().map(Some),
      self.embedding_dim as i32,
    );
    let scopes = StringArray::from_iter_values(entries.iter().map(|entry| match entry.scope {
      ResourceScope::ClusterShared => "shared",
      ResourceScope::NodeLocal => "local",
    }));
    let source_node_ids = StringArray::from(
      entries
        .iter()
        .map(|entry| entry.source_node_id.as_deref())
        .collect::<Vec<_>>(),
    );
    let updated_at_ms =
      Int64Array::from_iter_values(entries.iter().map(|entry| entry.updated_at_ms));
    let content_hashes =
      StringArray::from_iter_values(entries.iter().map(|entry| entry.content_hash.as_str()));
    let versions =
      arrow_array::UInt64Array::from_iter_values(entries.iter().map(|entry| entry.version));

    RecordBatch::try_new(
      schema,
      vec![
        Arc::new(ids),
        Arc::new(contents),
        Arc::new(metadata),
        Arc::new(embeddings),
        Arc::new(scopes),
        Arc::new(source_node_ids),
        Arc::new(updated_at_ms),
        Arc::new(content_hashes),
        Arc::new(versions),
      ],
    )
    .map_err(|error| AgentStorageError::Backend(error.to_string()))
  }
}

#[async_trait]
impl MemoryStorage for LanceDbMemoryStorage {
  async fn store(&self, entry: &MemoryEntry) -> Result<(), AgentStorageError> {
    self.check_dim(&entry.embedding)?;
    let conn = self.connection().await?;
    let batch = self.batch_for(std::slice::from_ref(entry))?;

    match conn.open_table(MEMORY_TABLE).execute().await {
      Ok(table) => {
        table
          .add(batch)
          .execute()
          .await
          .map_err(|error| AgentStorageError::Backend(error.to_string()))?;
      }
      Err(_) => {
        conn
          .create_table(MEMORY_TABLE, batch)
          .execute()
          .await
          .map_err(|error| AgentStorageError::Backend(error.to_string()))?;
      }
    }
    Ok(())
  }

  async fn recall(
    &self, query: Vec<f32>, limit: usize,
  ) -> Result<Vec<MemoryEntry>, AgentStorageError> {
    self.check_dim(&query)?;
    let conn = self.connection().await?;
    let table = conn
      .open_table(MEMORY_TABLE)
      .execute()
      .await
      .map_err(|error| AgentStorageError::Backend(error.to_string()))?;

    let stream = table
      .query()
      .nearest_to(query.as_slice())
      .map_err(|error| AgentStorageError::Backend(error.to_string()))?
      .limit(limit)
      .execute()
      .await
      .map_err(|error| AgentStorageError::Backend(error.to_string()))?;

    let batches = stream
      .try_collect::<Vec<_>>()
      .await
      .map_err(|error| AgentStorageError::Backend(error.to_string()))?;

    let mut entries = Vec::new();
    for batch in batches {
      entries.extend(parse_memory_batch(&batch)?);
    }
    Ok(entries)
  }

  async fn get(&self, id: &str) -> Result<Option<MemoryEntry>, AgentStorageError> {
    let entries = self
      .query_filtered(&format!("id = '{}'", escape_id_for_filter(id)))
      .await?;
    Ok(entries.into_iter().next())
  }

  async fn list(&self) -> Result<Vec<MemoryEntry>, AgentStorageError> {
    let conn = self.connection().await?;

    let table = match conn.open_table(MEMORY_TABLE).execute().await {
      Ok(table) => table,
      Err(_) => return Ok(Vec::new()),
    };

    let stream = table
      .query()
      .execute()
      .await
      .map_err(|error| AgentStorageError::Backend(error.to_string()))?;

    let batches = stream
      .try_collect::<Vec<_>>()
      .await
      .map_err(|error| AgentStorageError::Backend(error.to_string()))?;

    let mut entries = Vec::new();
    for batch in batches {
      entries.extend(parse_memory_batch(&batch)?);
    }
    Ok(entries)
  }

  async fn list_shared(&self) -> Result<Vec<MemoryEntry>, AgentStorageError> {
    self.query_filtered("scope = 'shared'").await
  }

  async fn list_local(&self) -> Result<Vec<MemoryEntry>, AgentStorageError> {
    self.query_filtered("scope = 'local'").await
  }
}

fn escape_id_for_filter(id: &str) -> String {
  id.replace('\'', "''")
}

fn parse_memory_batch(batch: &RecordBatch) -> Result<Vec<MemoryEntry>, AgentStorageError> {
  let id_col = batch
    .column_by_name("id")
    .ok_or_else(|| AgentStorageError::Backend("missing id column".to_string()))?;
  let content_col = batch
    .column_by_name("content")
    .ok_or_else(|| AgentStorageError::Backend("missing content column".to_string()))?;
  let metadata_col = batch
    .column_by_name("metadata")
    .ok_or_else(|| AgentStorageError::Backend("missing metadata column".to_string()))?;
  let embedding_col = batch
    .column_by_name("embedding")
    .ok_or_else(|| AgentStorageError::Backend("missing embedding column".to_string()))?;
  let scope_col = batch
    .column_by_name("scope")
    .ok_or_else(|| AgentStorageError::Backend("missing scope column".to_string()))?;
  let source_node_id_col = batch
    .column_by_name("source_node_id")
    .ok_or_else(|| AgentStorageError::Backend("missing source_node_id column".to_string()))?;
  let updated_at_ms_col = batch
    .column_by_name("updated_at_ms")
    .ok_or_else(|| AgentStorageError::Backend("missing updated_at_ms column".to_string()))?;
  let content_hash_col = batch
    .column_by_name("content_hash")
    .ok_or_else(|| AgentStorageError::Backend("missing content_hash column".to_string()))?;
  let version_col = batch
    .column_by_name("version")
    .ok_or_else(|| AgentStorageError::Backend("missing version column".to_string()))?;

  let ids = id_col
    .as_any()
    .downcast_ref::<StringArray>()
    .ok_or_else(|| AgentStorageError::Backend("id column has wrong type".to_string()))?;
  let contents = content_col
    .as_any()
    .downcast_ref::<BinaryArray>()
    .ok_or_else(|| AgentStorageError::Backend("content column has wrong type".to_string()))?;
  let metadata = metadata_col
    .as_any()
    .downcast_ref::<StringArray>()
    .ok_or_else(|| AgentStorageError::Backend("metadata column has wrong type".to_string()))?;
  let embeddings = embedding_col
    .as_any()
    .downcast_ref::<FixedSizeListArray>()
    .ok_or_else(|| AgentStorageError::Backend("embedding column has wrong type".to_string()))?;
  let scopes = scope_col
    .as_any()
    .downcast_ref::<StringArray>()
    .ok_or_else(|| AgentStorageError::Backend("scope column has wrong type".to_string()))?;
  let source_node_ids = source_node_id_col
    .as_any()
    .downcast_ref::<StringArray>()
    .ok_or_else(|| {
      AgentStorageError::Backend("source_node_id column has wrong type".to_string())
    })?;
  let updated_at_ms = updated_at_ms_col
    .as_any()
    .downcast_ref::<Int64Array>()
    .ok_or_else(|| AgentStorageError::Backend("updated_at_ms column has wrong type".to_string()))?;
  let content_hashes = content_hash_col
    .as_any()
    .downcast_ref::<StringArray>()
    .ok_or_else(|| AgentStorageError::Backend("content_hash column has wrong type".to_string()))?;
  let versions = version_col
    .as_any()
    .downcast_ref::<arrow_array::UInt64Array>()
    .ok_or_else(|| AgentStorageError::Backend("version column has wrong type".to_string()))?;

  let mut entries = Vec::new();
  for i in 0..batch.num_rows() {
    let embedding_array = embeddings.value(i);
    let embedding = embedding_array
      .as_any()
      .downcast_ref::<Float32Array>()
      .ok_or_else(|| AgentStorageError::Backend("embedding values have wrong type".to_string()))?
      .values()
      .to_vec();
    let metadata_str = metadata.value(i);
    let metadata = toml::from_str(metadata_str).map_err(|error| {
      AgentStorageError::Backend(format!(
        "metadata deserialization failed for row {i}: {error}"
      ))
    })?;
    let scope = match scopes.value(i) {
      "shared" => ResourceScope::ClusterShared,
      _ => ResourceScope::NodeLocal,
    };
    let source_node_id = {
      let value = source_node_ids.value(i);
      if value.is_empty() {
        None
      } else {
        Some(value.to_string())
      }
    };

    entries.push(MemoryEntry {
      id: ids.value(i).to_string(),
      content: contents.value(i).to_vec(),
      embedding,
      metadata,
      scope,
      source_node_id,
      updated_at_ms: updated_at_ms.value(i),
      content_hash: content_hashes.value(i).to_string(),
      version: versions.value(i),
    });
  }
  Ok(entries)
}

/// Agent storage facade.
#[derive(Debug, Clone)]
pub struct AgentDomain {
  sessions: Arc<dyn SessionStorage>,
  memory: Arc<dyn MemoryStorage>,
}

impl AgentDomain {
  pub(crate) fn new(db: Arc<Database>, data_dir: PathBuf) -> Self {
    let memory_uri = data_dir.join("memory.lancedb");
    Self {
      sessions: Arc::new(RedbSessionStorage::new(db)),
      memory: Arc::new(LanceDbMemoryStorage::new(memory_uri)),
    }
  }

  #[allow(dead_code)]
  pub(crate) fn with_embedding_dim(db: Arc<Database>, data_dir: PathBuf, dim: usize) -> Self {
    let memory_uri = data_dir.join("memory.lancedb");
    Self {
      sessions: Arc::new(RedbSessionStorage::new(db)),
      memory: Arc::new(LanceDbMemoryStorage::with_embedding_dim(memory_uri, dim)),
    }
  }

  /// Access session storage.
  pub fn sessions(&self) -> &dyn SessionStorage {
    self.sessions.as_ref()
  }

  /// Access memory storage.
  pub fn memory(&self) -> &dyn MemoryStorage {
    self.memory.as_ref()
  }

  /// Apply a remote memory entry if it wins the version/scope conflict check.
  ///
  /// Returns `true` when the entry was stored, `false` when it was skipped.
  pub async fn apply_remote_memory(
    &self, entry: MemoryEntry, content: &[u8],
  ) -> Result<bool, AgentStorageError> {
    if content.is_empty() {
      return Ok(false);
    }
    let actual_hash = blake3::hash(content).to_hex().to_string();
    if actual_hash != entry.content_hash {
      return Err(AgentStorageError::Backend(
        "content hash mismatch".to_string(),
      ));
    }
    let local = self.memory.get(&entry.id).await?;
    if !crate::versioned::should_apply_versioned(
      local
        .as_ref()
        .map(|local| local as &dyn crate::versioned::VersionedRecord),
      &entry,
    ) {
      return Ok(false);
    }
    self.memory.store(&entry).await?;
    Ok(true)
  }
}

#[cfg(test)]
mod tests {
  use lycoris_core::now_ms;
  use tempfile::TempDir;

  use super::*;
  use crate::Storage;

  fn test_domain() -> (TempDir, AgentDomain) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    (dir, (*storage.agent()).clone())
  }

  fn memory_entry(
    id: &str, embedding: Vec<f32>, scope: ResourceScope, updated_at_ms: i64,
  ) -> MemoryEntry {
    let content = id.as_bytes().to_vec();
    let content_hash = MemoryEntry::compute_content_hash(&content);
    MemoryEntry {
      id: id.to_string(),
      content,
      embedding,
      metadata: [("source".to_string(), "test".to_string())]
        .into_iter()
        .collect(),
      scope,
      source_node_id: None,
      updated_at_ms,
      content_hash,
      version: updated_at_ms as u64,
    }
  }

  #[test]
  fn session_round_trip() {
    let (_dir, domain) = test_domain();
    let session = Session {
      id: "session-1".to_string(),
      metadata: [("title".to_string(), "hello".to_string())]
        .into_iter()
        .collect(),
    };

    domain.sessions().create(&session).unwrap();
    let loaded = domain.sessions().get("session-1").unwrap().unwrap();
    assert_eq!(loaded.id, "session-1");
    assert_eq!(loaded.metadata.get("title"), Some(&"hello".to_string()));
  }

  #[test]
  fn session_list_and_delete() {
    let (_dir, domain) = test_domain();
    domain
      .sessions()
      .create(&Session {
        id: "session-a".to_string(),
        metadata: HashMap::new(),
      })
      .unwrap();
    domain
      .sessions()
      .create(&Session {
        id: "session-b".to_string(),
        metadata: HashMap::new(),
      })
      .unwrap();

    let list = domain.sessions().list().unwrap();
    assert_eq!(list.len(), 2);

    domain.sessions().delete("session-a").unwrap();
    let list = domain.sessions().list().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, "session-b");
  }

  #[tokio::test]
  async fn memory_store_and_recall() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    let agent = storage.agent();
    let memory = agent.memory();

    let dim = DEFAULT_EMBEDDING_DIM;
    let mut near = vec![0.0_f32; dim];
    near[0] = 1.0;
    let mut far = vec![0.0_f32; dim];
    far[0] = -1.0;

    memory
      .store(&memory_entry(
        "near",
        near.clone(),
        ResourceScope::NodeLocal,
        now_ms(),
      ))
      .await
      .unwrap();
    memory
      .store(&memory_entry(
        "far",
        far.clone(),
        ResourceScope::NodeLocal,
        now_ms(),
      ))
      .await
      .unwrap();

    let mut query = vec![0.0_f32; dim];
    query[0] = 0.9;
    let results = memory.recall(query, 1).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "near");
  }

  #[tokio::test]
  async fn memory_scope_filtering() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    let agent = storage.agent();
    let memory = agent.memory();
    let dim = DEFAULT_EMBEDDING_DIM;

    memory
      .store(&memory_entry(
        "shared",
        vec![1.0_f32; dim],
        ResourceScope::ClusterShared,
        now_ms(),
      ))
      .await
      .unwrap();
    memory
      .store(&memory_entry(
        "local",
        vec![-1.0_f32; dim],
        ResourceScope::NodeLocal,
        now_ms(),
      ))
      .await
      .unwrap();

    let shared = memory.list_shared().await.unwrap();
    assert_eq!(shared.len(), 1);
    assert_eq!(shared[0].id, "shared");

    let local = memory.list_local().await.unwrap();
    assert_eq!(local.len(), 1);
    assert_eq!(local[0].id, "local");
  }

  #[tokio::test]
  async fn memory_get_round_trip() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    let agent = storage.agent();
    let memory = agent.memory();
    let dim = DEFAULT_EMBEDDING_DIM;

    let entry = memory_entry(
      "entry-1",
      vec![0.5_f32; dim],
      ResourceScope::ClusterShared,
      42,
    );
    memory.store(&entry).await.unwrap();

    let loaded = memory.get("entry-1").await.unwrap().unwrap();
    assert_eq!(loaded.id, "entry-1");
    assert_eq!(loaded.scope, ResourceScope::ClusterShared);
    assert_eq!(loaded.updated_at_ms, 42);
    assert_eq!(loaded.content_hash, entry.content_hash);
  }

  #[tokio::test]
  async fn apply_remote_memory_stores_new_shared_entry() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    let agent = storage.agent();
    let dim = DEFAULT_EMBEDDING_DIM;

    let content = b"remote memory";
    let mut entry = memory_entry(
      "remote-1",
      vec![0.1_f32; dim],
      ResourceScope::ClusterShared,
      100,
    );
    entry.content = content.to_vec();
    entry.content_hash = MemoryEntry::compute_content_hash(content);
    entry.version = 1;

    let applied = agent
      .apply_remote_memory(entry.clone(), content)
      .await
      .unwrap();
    assert!(applied);

    let loaded = agent.memory().get("remote-1").await.unwrap().unwrap();
    assert_eq!(loaded.content, content.to_vec());
  }

  #[tokio::test]
  async fn apply_remote_memory_skips_older_version() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    let agent = storage.agent();
    let dim = DEFAULT_EMBEDDING_DIM;

    let local_content = b"local memory";
    let mut local = memory_entry(
      "conflict",
      vec![0.2_f32; dim],
      ResourceScope::ClusterShared,
      200,
    );
    local.content = local_content.to_vec();
    local.content_hash = MemoryEntry::compute_content_hash(local_content);
    local.version = 2;
    agent.memory().store(&local).await.unwrap();

    let remote_content = b"remote memory";
    let mut remote = memory_entry(
      "conflict",
      vec![0.3_f32; dim],
      ResourceScope::ClusterShared,
      300,
    );
    remote.content = remote_content.to_vec();
    remote.content_hash = MemoryEntry::compute_content_hash(remote_content);
    remote.version = 1;

    let applied = agent
      .apply_remote_memory(remote, remote_content)
      .await
      .unwrap();
    assert!(!applied);

    let loaded = agent.memory().get("conflict").await.unwrap().unwrap();
    assert_eq!(loaded.content, local_content.to_vec());
  }

  #[tokio::test]
  async fn apply_remote_memory_rejects_local_scope() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    let agent = storage.agent();
    let dim = DEFAULT_EMBEDDING_DIM;

    let content = b"nodelocal memory";
    let mut entry = memory_entry(
      "local-remote",
      vec![0.4_f32; dim],
      ResourceScope::NodeLocal,
      100,
    );
    entry.content = content.to_vec();
    entry.content_hash = MemoryEntry::compute_content_hash(content);
    entry.version = 1;

    let applied = agent.apply_remote_memory(entry, content).await.unwrap();
    assert!(!applied);
  }

  #[tokio::test]
  async fn apply_remote_memory_rejects_hash_mismatch() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    let agent = storage.agent();
    let dim = DEFAULT_EMBEDDING_DIM;

    let content = b"real content";
    let mut entry = memory_entry(
      "hash-bad",
      vec![0.5_f32; dim],
      ResourceScope::ClusterShared,
      100,
    );
    entry.content = content.to_vec();
    entry.content_hash = "wrong-hash".to_string();
    entry.version = 1;

    let error = agent.apply_remote_memory(entry, content).await.unwrap_err();
    assert!(error.to_string().contains("content hash mismatch"));
  }

  #[tokio::test]
  async fn apply_remote_memory_rejects_empty_content() {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    let agent = storage.agent();
    let dim = DEFAULT_EMBEDDING_DIM;

    let mut entry = memory_entry(
      "empty",
      vec![0.6_f32; dim],
      ResourceScope::ClusterShared,
      100,
    );
    entry.content = Vec::new();
    entry.content_hash = MemoryEntry::compute_content_hash(&[]);
    entry.version = 1;

    let applied = agent.apply_remote_memory(entry, &[]).await.unwrap();
    assert!(!applied);
  }
}
