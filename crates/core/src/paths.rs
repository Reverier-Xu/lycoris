use std::path::PathBuf;

use directories::{BaseDirs, ProjectDirs};

fn project_dirs() -> Option<ProjectDirs> {
  ProjectDirs::from("", "", "lycoris")
}

/// Return the default data directory for lycoris.
///
/// Privileged processes use the platform's system data directory:
/// - Linux: `/var/lib/lycoris`
/// - macOS: `/Library/Application Support/lycoris`
/// - Windows: `%PROGRAMDATA%\lycoris`
///
/// Unprivileged processes use the user-local data directory:
/// - Linux: `$XDG_DATA_HOME/lycoris` or `$HOME/.local/share/lycoris`
/// - macOS: `$HOME/Library/Application Support/lycoris`
/// - Windows: `%LOCALAPPDATA%\lycoris`
///
/// If the platform directories cannot be determined, falls back to a
/// temporary directory.
pub fn default_data_dir() -> PathBuf {
  if is_root::is_root() {
    system_data_dir().unwrap_or_else(fallback_data_dir)
  } else {
    user_data_dir().unwrap_or_else(fallback_data_dir)
  }
}

fn system_data_dir() -> Option<PathBuf> {
  BaseDirs::new().map(|dirs| dirs.data_local_dir().join("lycoris"))
}

pub fn user_data_dir() -> Option<PathBuf> {
  project_dirs().map(|dirs| dirs.data_dir().to_path_buf())
}

fn fallback_data_dir() -> PathBuf {
  std::env::temp_dir().join("lycoris")
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn default_data_dir_contains_lycoris() {
    let dir = default_data_dir();
    assert!(dir.components().any(|c| c.as_os_str() == "lycoris"));
  }
}
