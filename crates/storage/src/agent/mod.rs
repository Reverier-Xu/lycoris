//! Agent orchestration storage.
//!
//! This domain persists agent session metadata and vector-backed memories.
//!
//! - `Session` metadata is stored in the shared `redb` database via
//!   `RedbSessionStorage`.
//! - `MemoryEntry` records (including their embeddings) are stored in an
//!   embedded LanceDB table for fast vector similarity search.

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use arrow_array::{
  Array, BinaryArray, FixedSizeListArray, Float32Array, RecordBatch, StringArray,
  types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures_util::stream::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use lycoris_core::DEFAULT_EMBEDDING_DIM;
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
}

/// Errors that can occur in agent storage backends.
#[derive(Debug, thiserror::Error)]
pub enum AgentStorageError {
  #[error("agent storage not implemented")]
  NotImplemented,
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
        .map(|entry| toml::to_string(&entry.metadata).unwrap_or_default()),
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

    RecordBatch::try_new(
      schema,
      vec![
        Arc::new(ids),
        Arc::new(contents),
        Arc::new(metadata),
        Arc::new(embeddings),
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
    let metadata = toml::from_str(metadata_str).unwrap_or_default();

    entries.push(MemoryEntry {
      id: ids.value(i).to_string(),
      content: contents.value(i).to_vec(),
      embedding,
      metadata,
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
}

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;
  use crate::Storage;

  fn test_domain() -> (TempDir, AgentDomain) {
    let dir = TempDir::new().unwrap();
    let storage = Storage::open(dir.path().join("agent.redb")).unwrap();
    (dir, storage.agent())
  }

  fn memory_entry(id: &str, embedding: Vec<f32>) -> MemoryEntry {
    MemoryEntry {
      id: id.to_string(),
      content: id.as_bytes().to_vec(),
      embedding,
      metadata: [("source".to_string(), "test".to_string())]
        .into_iter()
        .collect(),
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
      .store(&memory_entry("near", near.clone()))
      .await
      .unwrap();
    memory
      .store(&memory_entry("far", far.clone()))
      .await
      .unwrap();

    let mut query = vec![0.0_f32; dim];
    query[0] = 0.9;
    let results = memory.recall(query, 1).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "near");
  }
}
