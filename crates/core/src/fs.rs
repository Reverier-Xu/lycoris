//! Filesystem helpers shared across the workspace.

use std::{
  fs,
  io::{self, Write},
  path::Path,
};

/// Write `content` to `path`, creating parent directories as needed, and
/// restrict the file to owner-only access (`0o600` on unix).
///
/// This is the single writer for secret material such as the cluster key and
/// TLS private keys, so the permission hardening cannot drift between call
/// sites.
pub fn write_private_file<P: AsRef<Path>>(path: P, content: &[u8]) -> io::Result<()> {
  if let Some(parent) = path.as_ref().parent() {
    fs::create_dir_all(parent)?;
  }

  let mut file = fs::File::create(path.as_ref())?;
  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = file.metadata()?.permissions();
    permissions.set_mode(0o600);
    file.set_permissions(permissions)?;
  }

  file.write_all(content)?;
  file.flush()?;
  Ok(())
}
