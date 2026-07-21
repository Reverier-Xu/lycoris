//! Version-controlled content storage for skills and rules.
//!
//! Skill and rule bodies are kept in a real Git repository. The backend is
//! intentionally thin: it shells out to the system `git` binary instead of
//! linking a Git library, avoiding C dependencies such as `libgit2`.

use std::path::PathBuf;

use super::WorkspaceStorageError;
use crate::resource_id;

/// A content store backed by a version-control system.
pub trait VersionedContentStore: std::fmt::Debug + Send + Sync {
  fn write(&self, id: &str, content: &str, message: &str) -> Result<String, WorkspaceStorageError>;
  fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError>;
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

  /// Create the backing git repository if it does not exist yet.
  fn initialize(&self) -> Result<(), WorkspaceStorageError> {
    std::fs::create_dir_all(&self.repo_path)?;
    if self.repo_path.join(".git").exists() {
      return Ok(());
    }
    self.ok(self.git().arg("init").arg("--quiet").output()?)?;
    Ok(())
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
      let stderr = String::from_utf8_lossy(&output.stderr).to_string();
      tracing::error!(%stderr, "git command failed");
      Err(WorkspaceStorageError::GitCommandFailed(stderr))
    }
  }
}

impl VersionedContentStore for GitContentStore {
  #[tracing::instrument(name = "git_write", skip_all, fields(id = %id))]
  fn write(&self, id: &str, content: &str, message: &str) -> Result<String, WorkspaceStorageError> {
    resource_id::validate(id)?;
    self.initialize()?;
    let relative_path = format!("{id}.toml");
    std::fs::write(self.repo_path.join(&relative_path), content)?;

    tracing::debug!(id = %id, "git add");
    self.ok(self.git().arg("add").arg(&relative_path).output()?)?;
    tracing::debug!(id = %id, message = %message, "git commit");
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
      let stderr = String::from_utf8_lossy(&output.stderr).to_string();
      tracing::error!(%stderr, "git commit failed");
      return Err(WorkspaceStorageError::GitCommandFailed(stderr));
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
    let hash = String::from_utf8(output.stdout)?.trim().to_string();
    tracing::debug!(id = %id, commit = %hash, "git write complete");
    Ok(hash)
  }

  #[tracing::instrument(name = "git_read", skip_all, fields(id = %id))]
  fn read(&self, id: &str) -> Result<Option<String>, WorkspaceStorageError> {
    resource_id::validate(id)?;
    match std::fs::read_to_string(self.repo_path.join(format!("{id}.toml"))) {
      Ok(content) => Ok(Some(content)),
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
      Err(error) => Err(error.into()),
    }
  }
}

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
  fn rejects_ids_that_escape_the_repository() {
    let (dir, store) = test_store();
    for id in ["../escape", "a/b", "", ".hidden", "a..b", "a\\b", "a b"] {
      let error = store.write(id, "content", "msg").unwrap_err();
      assert!(
        matches!(error, WorkspaceStorageError::InvalidResourceId(_)),
        "write id: {id:?}"
      );
      let error = store.read(id).unwrap_err();
      assert!(
        matches!(error, WorkspaceStorageError::InvalidResourceId(_)),
        "read id: {id:?}"
      );
    }
    // No file may have been created anywhere for the rejected ids.
    assert!(!dir.path().join("escape.toml").exists());
  }

  #[test]
  fn accepts_ids_within_the_whitelist() {
    let (_dir, store) = test_store();
    for id in ["skill-1", "a_b.C", "UPPER.lower-9", "x"] {
      store.write(id, "content", "msg").unwrap();
      assert_eq!(store.read(id).unwrap().unwrap(), "content");
    }
  }
}
