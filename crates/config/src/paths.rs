use std::path::{Path, PathBuf};

use directories::{BaseDirs, ProjectDirs};

pub const DAEMON_CONFIG_FILE_NAME: &str = "lycoris.toml";
pub const CLIENT_CONFIG_FILE_NAME: &str = "lycoris.client.conf";

fn project_dirs() -> Option<ProjectDirs> {
  ProjectDirs::from("", "", "lycoris")
}

/// Return the path to the user-specific configuration directory for lycoris.
///
/// Uses the platform's user configuration directory:
/// - Linux: `$XDG_CONFIG_HOME/lycoris` or `$HOME/.config/lycoris`
/// - macOS: `$HOME/Library/Application Support/lycoris`
/// - Windows: `%APPDATA%\lycoris\config`
pub fn user_config_dir() -> Option<PathBuf> {
  project_dirs().map(|dirs| dirs.config_dir().to_path_buf())
}

/// Return the system-wide configuration directory for lycoris.
///
/// Uses the platform's system configuration directory:
/// - Linux: `/etc/lycoris`
/// - macOS: `/Library/Application Support/lycoris`
/// - Windows: `%PROGRAMDATA%\lycoris`
pub fn system_config_dir() -> Option<PathBuf> {
  BaseDirs::new().map(|dirs| dirs.config_dir().join("lycoris"))
}

/// Return candidate configuration directories in precedence order.
///
/// User configuration takes precedence over system configuration.
pub fn config_dirs() -> Vec<PathBuf> {
  user_config_dir()
    .into_iter()
    .chain(system_config_dir())
    .collect()
}

/// Select the best configuration file path from a list of candidate
/// directories.
///
/// Returns the first existing config file found. If none exist, returns the
/// path in the first candidate directory.
pub fn select_config_path(dirs: &[PathBuf], file_name: &str) -> Option<PathBuf> {
  for dir in dirs {
    let path = dir.join(file_name);
    if path.is_file() {
      return Some(path);
    }
  }
  dirs.first().map(|dir| dir.join(file_name))
}

/// Return the default daemon configuration file path.
///
/// If a configuration file exists in the user-specific directory, use it.
/// Otherwise, fall back to the system-wide directory. If neither exists, the
/// returned path points to the user-specific location so callers can report a
/// helpful "not found" error.
pub fn default_daemon_config_path() -> Option<PathBuf> {
  select_config_path(&config_dirs(), DAEMON_CONFIG_FILE_NAME)
}

/// Return the default client configuration file path in the user-specific
/// configuration directory.
pub fn default_client_config_path() -> Option<PathBuf> {
  user_config_dir().map(|dir| dir.join(CLIENT_CONFIG_FILE_NAME))
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

fn user_data_dir() -> Option<PathBuf> {
  project_dirs().map(|dirs| dirs.data_dir().to_path_buf())
}

fn fallback_data_dir() -> PathBuf {
  std::env::temp_dir().join("lycoris")
}

/// Ensure a directory exists, creating it and its parents if necessary.
pub fn ensure_dir<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
  std::fs::create_dir_all(path.as_ref())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn user_config_dir_contains_lycoris() {
    if let Some(dir) = user_config_dir() {
      assert!(dir.components().any(|c| c.as_os_str() == "lycoris"));
    }
  }

  #[test]
  fn system_config_dir_contains_lycoris() {
    if let Some(dir) = system_config_dir() {
      assert!(dir.components().any(|c| c.as_os_str() == "lycoris"));
    }
  }

  #[test]
  fn default_data_dir_contains_lycoris() {
    let dir = default_data_dir();
    assert!(dir.components().any(|c| c.as_os_str() == "lycoris"));
  }

  #[test]
  fn select_config_path_prefers_existing_user_file() {
    let tmp = std::env::temp_dir().join("lycoris-config-precedence");
    let _ = std::fs::remove_dir_all(&tmp);
    let user_dir = tmp.join("user");
    let system_dir = tmp.join("system");
    std::fs::create_dir_all(&user_dir).unwrap();
    std::fs::create_dir_all(&system_dir).unwrap();
    std::fs::write(
      system_dir.join(DAEMON_CONFIG_FILE_NAME),
      "data_dir = \"/var\"",
    )
    .unwrap();

    // With only the system file present, it should be chosen.
    let path = select_config_path(std::slice::from_ref(&system_dir), DAEMON_CONFIG_FILE_NAME);
    assert_eq!(path, Some(system_dir.join(DAEMON_CONFIG_FILE_NAME)));

    // Once a user file exists, it takes precedence over the system file.
    std::fs::write(
      user_dir.join(DAEMON_CONFIG_FILE_NAME),
      "data_dir = \"/home\"",
    )
    .unwrap();
    let path = select_config_path(
      &[user_dir.clone(), system_dir.clone()],
      DAEMON_CONFIG_FILE_NAME,
    );
    assert_eq!(path, Some(user_dir.join(DAEMON_CONFIG_FILE_NAME)));

    let _ = std::fs::remove_dir_all(&tmp);
  }
}
