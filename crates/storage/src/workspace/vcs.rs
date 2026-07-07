//! Version-controlled content storage for skills and rules.
//!
//! Skill and rule bodies are kept in a real Git repository. The backend is
//! intentionally thin: it shells out to the system `git` binary instead of
//! linking a Git library, avoiding C dependencies such as `libgit2`.

use std::path::PathBuf;

/// Errors that can occur in workspace storage backends.
#[derive(Debug, thiserror::Error)]
pub enum WorkspaceStorageError {
  #[error("storage error: {0}")]
  Storage(#[from] crate::StorageError),
  #[error("workspace not found: {0}")]
  NotFound(String),
  #[error("git command failed: {0}")]
  GitCommandFailed(String),
}

impl From<std::io::Error> for WorkspaceStorageError {
  fn from(error: std::io::Error) -> Self {
    Self::Storage(crate::StorageError::Io(error))
  }
}

impl From<std::string::FromUtf8Error> for WorkspaceStorageError {
  fn from(error: std::string::FromUtf8Error) -> Self {
    Self::GitCommandFailed(format!("invalid utf-8 in git output: {error}"))
  }
}

/// A single version of a versioned resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInfo {
  pub hash: String,
  pub message: String,
  pub timestamp_ms: i64,
}

/// A content store backed by a version-control system.
pub trait VersionedContentStore: std::fmt::Debug + Send + Sync {
  fn initialize(&self) -> Result<(), WorkspaceStorageError>;
  fn write(&self, id: &str, content: &str, message: &str) -> Result<String, WorkspaceStorageError>;
  fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError>;
  fn latest_hash(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError>;
  fn history(&self, id: &str) -> Result<Vec<VersionInfo>, WorkspaceStorageError>;
}

/// A thin Git-backed content store using the system `git` binary.
#[derive(Debug, Clone)]
pub struct GitContentStore {
  pub(crate) repo_path: PathBuf,
}

impl GitContentStore {
  pub fn new(repo_path: PathBuf) -> Self {
    Self { repo_path }
  }

  fn git(&self) -> std::process::Command {
    let mut cmd = std::process::Command::new("git");
    cmd
      .current_dir(&self.repo_path)
      .env("GIT_AUTHOR_NAME", "lycoris")
      .env("GIT_AUTHOR_EMAIL", "lycoris@localhost")
      .env("GIT_COMMITTER_NAME", "lycoris")
      .env("GIT_COMMITTER_EMAIL", "lycoris@localhost");
    cmd
  }

  fn ok(
    &self, output: std::process::Output,
  ) -> Result<std::process::Output, WorkspaceStorageError> {
    if output.status.success() {
      Ok(output)
    } else {
      Err(WorkspaceStorageError::GitCommandFailed(
        String::from_utf8_lossy(&output.stderr).to_string(),
      ))
    }
  }
}

impl VersionedContentStore for GitContentStore {
  fn initialize(&self) -> Result<(), WorkspaceStorageError> {
    std::fs::create_dir_all(&self.repo_path)?;
    if self.repo_path.join(".git").exists() {
      return Ok(());
    }
    self.ok(self.git().arg("init").arg("--quiet").output()?)?;
    Ok(())
  }

  fn write(&self, id: &str, content: &str, message: &str) -> Result<String, WorkspaceStorageError> {
    self.initialize()?;
    let relative_path = format!("{id}.toml");
    std::fs::write(self.repo_path.join(&relative_path), content)?;

    self.ok(self.git().arg("add").arg(&relative_path).output()?)?;
    let output = self
      .git()
      .arg("commit")
      .arg("--quiet")
      .arg("--no-gpg-sign")
      .arg("-m")
      .arg(message)
      .arg("--")
      .arg(&relative_path)
      .output()?;
    if !output.status.success() && output.status.code() != Some(1) {
      return Err(WorkspaceStorageError::GitCommandFailed(
        String::from_utf8_lossy(&output.stderr).to_string(),
      ));
    }

    let output = self.ok(
      self
        .git()
        .arg("log")
        .arg("-1")
        .arg("--format=%H")
        .arg("--")
        .arg(&relative_path)
        .output()?,
    )?;
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
  }

  fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    match std::fs::read_to_string(self.repo_path.join(format!("{id}.toml"))) {
      Ok(content) => Ok(Some(content)),
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
      Err(error) => Err(error.into()),
    }
  }

  fn latest_hash(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    let relative_path = format!("{id}.toml");
    let output = self
      .git()
      .arg("log")
      .arg("-1")
      .arg("--format=%H")
      .arg("--")
      .arg(&relative_path)
      .output()?;
    if !output.status.success() {
      return Ok(None);
    }
    let hash = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(if hash.is_empty() { None } else { Some(hash) })
  }

  fn history(&self, id: &str) -> Result<Vec<VersionInfo>, WorkspaceStorageError> {
    let relative_path = format!("{id}.toml");
    let output = self
      .git()
      .arg("log")
      .arg("--format=%H%x00%s%x00%at")
      .arg("--")
      .arg(&relative_path)
      .output()?;
    if !output.status.success() {
      return Ok(Vec::new());
    }

    let stdout = String::from_utf8(output.stdout)?;
    let mut history = Vec::new();
    for record in stdout.split('\n').filter(|line| !line.is_empty()) {
      let mut fields = record.split('\0');
      let hash = fields.next().unwrap_or("").to_string();
      let message = fields.next().unwrap_or("").to_string();
      let timestamp_s: i64 = fields.next().unwrap_or("0").parse().unwrap_or(0);
      history.push(VersionInfo {
        hash,
        message,
        timestamp_ms: timestamp_s * 1000,
      });
    }
    Ok(history)
  }
}

pub type ContentStore = GitContentStore;

#[cfg(test)]
mod tests {
  use tempfile::TempDir;

  use super::*;

  fn test_store() -> (TempDir, GitContentStore) {
    let dir = TempDir::new().unwrap();
    let store = GitContentStore::new(dir.path().to_path_buf());
    (dir, store)
  }

  #[test]
  fn initialize_creates_git_repository() {
    let (dir, store) = test_store();
    store.initialize().unwrap();
    assert!(dir.path().join(".git").exists());
  }

  #[test]
  fn write_and_read_version() {
    let (_dir, store) = test_store();
    store.write("skill-1", "v1", "initial").unwrap();
    store.write("skill-1", "v2", "update").unwrap();
    assert_eq!(store.read("skill-1").unwrap().unwrap(), "v2");
  }

  #[test]
  fn latest_hash_returns_most_recent_change() {
    let (_dir, store) = test_store();
    let first = store.write("skill-1", "v1", "initial").unwrap();
    let second = store.write("skill-1", "v2", "update").unwrap();

    let latest = store.latest_hash("skill-1").unwrap().unwrap();
    assert_eq!(latest, second);
    assert_ne!(latest, first);
  }

  #[test]
  fn latest_hash_missing_returns_none() {
    let (_dir, store) = test_store();
    assert!(store.latest_hash("missing").unwrap().is_none());
  }

  #[test]
  fn history_returns_versions_newest_first() {
    let (_dir, store) = test_store();
    store.write("skill-1", "v1", "initial").unwrap();
    store.write("skill-1", "v2", "update").unwrap();
    store.write("skill-1", "v3", "final").unwrap();

    let history = store.history("skill-1").unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].message, "final");
    assert_eq!(history[1].message, "update");
    assert_eq!(history[2].message, "initial");
    assert!(!history[0].hash.is_empty());
    assert!(history[0].timestamp_ms >= history[1].timestamp_ms);
    assert!(history[1].timestamp_ms >= history[2].timestamp_ms);
  }
}
