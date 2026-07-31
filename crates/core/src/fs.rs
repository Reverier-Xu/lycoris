//! Filesystem helpers shared across the workspace.

use std::{
  fs,
  io::{self, Write},
  path::{Path, PathBuf},
};

use ring::rand::{SecureRandom, SystemRandom};

const TEMP_CREATE_ATTEMPTS: usize = 16;

/// Result of publishing a private file without replacing an existing winner.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PrivateFileCreate {
  Created,
  AlreadyExists,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PublishMode {
  Replace,
  IfAbsent,
}

trait PrivateFileOps {
  type Writer: Write;

  fn create_parent(&self, parent: &Path) -> io::Result<()>;
  fn create_temp(&self, path: &Path) -> io::Result<Self::Writer>;
  fn sync_temp(&self, writer: &Self::Writer) -> io::Result<()>;
  fn replace(&self, source: &Path, destination: &Path) -> io::Result<()>;
  fn link_if_absent(&self, source: &Path, destination: &Path) -> io::Result<()>;
  fn remove_temp(&self, path: &Path) -> io::Result<()>;
  fn sync_parent(&self, parent: &Path) -> io::Result<()>;
}

struct StdPrivateFileOps;

impl PrivateFileOps for StdPrivateFileOps {
  type Writer = fs::File;

  fn create_parent(&self, parent: &Path) -> io::Result<()> {
    fs::create_dir_all(parent)
  }

  fn create_temp(&self, path: &Path) -> io::Result<Self::Writer> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
      use std::os::unix::fs::OpenOptionsExt;
      options.mode(0o600);
    }
    options.open(path)
  }

  fn sync_temp(&self, writer: &Self::Writer) -> io::Result<()> {
    writer.sync_all()
  }

  fn replace(&self, source: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(source, destination)
  }

  fn link_if_absent(&self, source: &Path, destination: &Path) -> io::Result<()> {
    fs::hard_link(source, destination)
  }

  fn remove_temp(&self, path: &Path) -> io::Result<()> {
    fs::remove_file(path)
  }

  #[cfg(unix)]
  fn sync_parent(&self, parent: &Path) -> io::Result<()> {
    fs::File::open(parent)?.sync_all()
  }

  #[cfg(not(unix))]
  fn sync_parent(&self, _parent: &Path) -> io::Result<()> {
    // Portable Rust exposes no directory fsync handle on Windows. The file is
    // flushed before its native atomic namespace operation; native tests cover
    // replacement and reopen semantics.
    Ok(())
  }
}

/// Atomically replace `path` with `content`, creating parent directories as
/// needed and restricting the temporary and published file to owner-only
/// access (`0o600` on Unix).
///
/// The temporary file is written beside the destination and synced before the
/// atomic rename. On Unix the containing directory is synced after publication.
pub fn write_private_file<P: AsRef<Path>>(path: P, content: &[u8]) -> io::Result<()> {
  publish_private_file(
    &StdPrivateFileOps,
    path.as_ref(),
    content,
    PublishMode::Replace,
  )
  .map(|_| ())
}

/// Publish `content` only when `path` does not already exist.
///
/// Concurrent creators cannot replace the winner.
/// [`PrivateFileCreate::AlreadyExists`] means the caller must discard its
/// candidate and load the existing file.
pub fn write_private_file_if_absent<P: AsRef<Path>>(
  path: P, content: &[u8],
) -> io::Result<PrivateFileCreate> {
  publish_private_file(
    &StdPrivateFileOps,
    path.as_ref(),
    content,
    PublishMode::IfAbsent,
  )
}

fn publish_private_file<O: PrivateFileOps>(
  operations: &O, destination: &Path, content: &[u8], mode: PublishMode,
) -> io::Result<PrivateFileCreate> {
  let parent = destination
    .parent()
    .filter(|parent| !parent.as_os_str().is_empty())
    .unwrap_or_else(|| Path::new("."));
  operations.create_parent(parent)?;
  let (temporary, mut writer) = create_temporary(operations, destination, parent)?;

  if let Err(error) = writer
    .write_all(content)
    .and_then(|()| operations.sync_temp(&writer))
  {
    drop(writer);
    let _ = operations.remove_temp(&temporary);
    return Err(error);
  }
  drop(writer);

  match mode {
    PublishMode::Replace => {
      if let Err(error) = operations.replace(&temporary, destination) {
        let _ = operations.remove_temp(&temporary);
        return Err(error);
      }
      operations.sync_parent(parent)?;
      Ok(PrivateFileCreate::Created)
    }
    PublishMode::IfAbsent => publish_if_absent(operations, parent, &temporary, destination),
  }
}

fn publish_if_absent<O: PrivateFileOps>(
  operations: &O, parent: &Path, temporary: &Path, destination: &Path,
) -> io::Result<PrivateFileCreate> {
  let publication = match operations.link_if_absent(temporary, destination) {
    Ok(()) => {
      if let Err(error) = operations.sync_parent(parent) {
        let _ = operations.remove_temp(temporary);
        return Err(error);
      }
      PrivateFileCreate::Created
    }
    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => PrivateFileCreate::AlreadyExists,
    Err(error) => {
      let _ = operations.remove_temp(temporary);
      return Err(error);
    }
  };

  operations.remove_temp(temporary)?;
  operations.sync_parent(parent)?;
  Ok(publication)
}

fn create_temporary<O: PrivateFileOps>(
  operations: &O, destination: &Path, parent: &Path,
) -> io::Result<(PathBuf, O::Writer)> {
  let random = SystemRandom::new();
  let name = destination
    .file_name()
    .and_then(|name| name.to_str())
    .unwrap_or("private");
  for _ in 0..TEMP_CREATE_ATTEMPTS {
    let mut suffix = [0_u8; 16];
    random
      .fill(&mut suffix)
      .map_err(|_| io::Error::other("private file temporary name generation failed"))?;
    let temporary = parent.join(format!(".{name}.{}.tmp", hex::encode(suffix)));
    match operations.create_temp(&temporary) {
      Ok(writer) => return Ok((temporary, writer)),
      Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
      Err(error) => return Err(error),
    }
  }
  Err(io::Error::new(
    io::ErrorKind::AlreadyExists,
    "private file temporary name allocation exhausted",
  ))
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

  use tempfile::TempDir;

  use super::*;

  #[derive(Debug, Clone, Copy, Eq, PartialEq)]
  enum Failure {
    CreateTemp,
    Write,
    SyncTemp,
    Publish,
    SyncParent,
  }

  #[derive(Default)]
  struct FakeState {
    events: Vec<&'static str>,
    files: BTreeMap<PathBuf, Vec<u8>>,
    failure: Option<Failure>,
  }

  struct FakeWriter {
    path: PathBuf,
    state: Rc<RefCell<FakeState>>,
  }

  impl Write for FakeWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
      let mut state = self.state.borrow_mut();
      state.events.push("write");
      if state.failure == Some(Failure::Write) {
        return Err(io::Error::other("injected write failure"));
      }
      state
        .files
        .entry(self.path.clone())
        .or_default()
        .extend(bytes);
      Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
      Ok(())
    }
  }

  struct FakeOps {
    state: Rc<RefCell<FakeState>>,
  }

  impl FakeOps {
    fn new(failure: Option<Failure>) -> Self {
      Self {
        state: Rc::new(RefCell::new(FakeState {
          failure,
          ..FakeState::default()
        })),
      }
    }
  }

  impl PrivateFileOps for FakeOps {
    type Writer = FakeWriter;

    fn create_parent(&self, _parent: &Path) -> io::Result<()> {
      self.state.borrow_mut().events.push("create_parent");
      Ok(())
    }

    fn create_temp(&self, path: &Path) -> io::Result<Self::Writer> {
      let mut state = self.state.borrow_mut();
      state.events.push("create_temp");
      if state.failure == Some(Failure::CreateTemp) {
        return Err(io::Error::other("injected temp creation failure"));
      }
      state.files.insert(path.to_path_buf(), Vec::new());
      Ok(FakeWriter {
        path: path.to_path_buf(),
        state: self.state.clone(),
      })
    }

    fn sync_temp(&self, _writer: &Self::Writer) -> io::Result<()> {
      let mut state = self.state.borrow_mut();
      state.events.push("sync_temp");
      if state.failure == Some(Failure::SyncTemp) {
        return Err(io::Error::other("injected temp sync failure"));
      }
      Ok(())
    }

    fn replace(&self, source: &Path, destination: &Path) -> io::Result<()> {
      let mut state = self.state.borrow_mut();
      state.events.push("replace");
      if state.failure == Some(Failure::Publish) {
        return Err(io::Error::other("injected publication failure"));
      }
      let bytes = state
        .files
        .remove(source)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing fake temp"))?;
      state.files.insert(destination.to_path_buf(), bytes);
      Ok(())
    }

    fn link_if_absent(&self, source: &Path, destination: &Path) -> io::Result<()> {
      let mut state = self.state.borrow_mut();
      state.events.push("link");
      if state.failure == Some(Failure::Publish) {
        return Err(io::Error::other("injected publication failure"));
      }
      if state.files.contains_key(destination) {
        return Err(io::Error::new(
          io::ErrorKind::AlreadyExists,
          "winner exists",
        ));
      }
      let bytes = state
        .files
        .get(source)
        .cloned()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing fake temp"))?;
      state.files.insert(destination.to_path_buf(), bytes);
      Ok(())
    }

    fn remove_temp(&self, path: &Path) -> io::Result<()> {
      let mut state = self.state.borrow_mut();
      state.events.push("remove_temp");
      state.files.remove(path);
      Ok(())
    }

    fn sync_parent(&self, _parent: &Path) -> io::Result<()> {
      let mut state = self.state.borrow_mut();
      state.events.push("sync_parent");
      if state.failure == Some(Failure::SyncParent) {
        return Err(io::Error::other("injected parent sync failure"));
      }
      Ok(())
    }
  }

  #[test]
  fn replacement_orders_sync_before_publication_and_parent_sync() {
    let operations = FakeOps::new(None);
    let destination = Path::new("/data/identity");

    assert_eq!(
      publish_private_file(&operations, destination, b"new", PublishMode::Replace).unwrap(),
      PrivateFileCreate::Created
    );
    assert_eq!(
      operations.state.borrow().events,
      [
        "create_parent",
        "create_temp",
        "write",
        "sync_temp",
        "replace",
        "sync_parent"
      ]
    );
    assert_eq!(
      operations.state.borrow().files.get(destination),
      Some(&b"new".to_vec())
    );
  }

  #[test]
  fn failures_before_publication_preserve_the_previous_destination() {
    for failure in [Failure::Write, Failure::SyncTemp, Failure::Publish] {
      let operations = FakeOps::new(Some(failure));
      let destination = Path::new("/data/identity");
      operations
        .state
        .borrow_mut()
        .files
        .insert(destination.to_path_buf(), b"old".to_vec());

      assert!(
        publish_private_file(&operations, destination, b"new", PublishMode::Replace).is_err()
      );
      assert_eq!(
        operations.state.borrow().files.get(destination),
        Some(&b"old".to_vec())
      );
    }
  }

  #[test]
  fn parent_sync_failure_reports_error_after_complete_publication() {
    let operations = FakeOps::new(Some(Failure::SyncParent));
    let destination = Path::new("/data/identity");
    operations
      .state
      .borrow_mut()
      .files
      .insert(destination.to_path_buf(), b"old".to_vec());

    assert!(publish_private_file(&operations, destination, b"new", PublishMode::Replace).is_err());
    assert_eq!(
      operations.state.borrow().files.get(destination),
      Some(&b"new".to_vec())
    );
  }

  #[test]
  fn temp_creation_failure_never_touches_the_destination() {
    let operations = FakeOps::new(Some(Failure::CreateTemp));
    let destination = Path::new("/data/identity");
    operations
      .state
      .borrow_mut()
      .files
      .insert(destination.to_path_buf(), b"old".to_vec());

    assert!(publish_private_file(&operations, destination, b"new", PublishMode::IfAbsent).is_err());
    assert_eq!(
      operations.state.borrow().events,
      ["create_parent", "create_temp"]
    );
    assert_eq!(
      operations.state.borrow().files.get(destination),
      Some(&b"old".to_vec())
    );
  }

  #[test]
  fn no_clobber_success_orders_both_parent_syncs_around_temp_removal() {
    let operations = FakeOps::new(None);
    let destination = Path::new("/data/identity");

    assert_eq!(
      publish_private_file(&operations, destination, b"winner", PublishMode::IfAbsent).unwrap(),
      PrivateFileCreate::Created
    );
    assert_eq!(
      operations.state.borrow().events,
      [
        "create_parent",
        "create_temp",
        "write",
        "sync_temp",
        "link",
        "sync_parent",
        "remove_temp",
        "sync_parent"
      ]
    );
    assert_eq!(
      operations.state.borrow().files.get(destination),
      Some(&b"winner".to_vec())
    );
    assert_eq!(operations.state.borrow().files.len(), 1);
  }

  #[test]
  fn no_clobber_link_failure_publishes_nothing_and_cleans_the_temp() {
    let operations = FakeOps::new(Some(Failure::Publish));
    let destination = Path::new("/data/identity");

    assert!(
      publish_private_file(
        &operations,
        destination,
        b"candidate",
        PublishMode::IfAbsent
      )
      .is_err()
    );
    assert!(!operations.state.borrow().files.contains_key(destination));
    assert_eq!(
      operations.state.borrow().events,
      [
        "create_parent",
        "create_temp",
        "write",
        "sync_temp",
        "link",
        "remove_temp"
      ]
    );
    assert!(operations.state.borrow().files.is_empty());
  }

  #[test]
  fn no_clobber_parent_sync_failure_leaves_a_complete_winner() {
    let operations = FakeOps::new(Some(Failure::SyncParent));
    let destination = Path::new("/data/identity");

    assert!(
      publish_private_file(&operations, destination, b"winner", PublishMode::IfAbsent).is_err()
    );
    assert_eq!(
      operations.state.borrow().files.get(destination),
      Some(&b"winner".to_vec())
    );
    assert_eq!(
      operations.state.borrow().events,
      [
        "create_parent",
        "create_temp",
        "write",
        "sync_temp",
        "link",
        "sync_parent",
        "remove_temp"
      ]
    );
    assert_eq!(operations.state.borrow().files.len(), 1);
  }

  #[test]
  fn real_filesystem_replaces_reopens_and_cleans_temporary_files() {
    let directory = TempDir::new().unwrap();
    let destination = directory.path().join("identity");

    write_private_file(&destination, b"first").unwrap();
    write_private_file(&destination, b"second").unwrap();

    assert_eq!(fs::read(&destination).unwrap(), b"second");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
  }

  #[test]
  fn no_clobber_publication_preserves_the_winner() {
    let directory = TempDir::new().unwrap();
    let destination = directory.path().join("identity");

    assert_eq!(
      write_private_file_if_absent(&destination, b"winner").unwrap(),
      PrivateFileCreate::Created
    );
    assert_eq!(
      write_private_file_if_absent(&destination, b"loser").unwrap(),
      PrivateFileCreate::AlreadyExists
    );
    assert_eq!(fs::read(&destination).unwrap(), b"winner");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
  }

  #[cfg(unix)]
  #[test]
  fn first_creation_and_replacement_are_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TempDir::new().unwrap();
    let destination = directory.path().join("identity");
    write_private_file_if_absent(&destination, b"first").unwrap();
    assert_eq!(
      fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
      0o600
    );

    write_private_file(&destination, b"second").unwrap();
    assert_eq!(
      fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
      0o600
    );
  }
}
