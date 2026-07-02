use std::{
  env,
  ffi::OsString,
  path::{Path, PathBuf},
};

pub const DAEMON_CONFIG_FILE_NAME: &str = "lycoris.toml";
pub const CLIENT_CONFIG_FILE_NAME: &str = "lycoris.client.conf";

/// Return the path to the user-specific configuration directory for lycoris.
///
/// Respects `$XDG_CONFIG_HOME`; otherwise falls back to
/// `$HOME/.config/lycoris`. Returns `None` when neither variable is set.
pub fn user_config_dir() -> Option<PathBuf> {
  user_config_dir_from_env(env::var_os("XDG_CONFIG_HOME"), env::var_os("HOME"))
}

fn user_config_dir_from_env(
  xdg_config_home: Option<OsString>, home: Option<OsString>,
) -> Option<PathBuf> {
  xdg_config_home
    .map(PathBuf::from)
    .or_else(|| home.map(|home| PathBuf::from(home).join(".config")))
    .map(|dir| dir.join("lycoris"))
}

/// Return the system-wide configuration directory for lycoris.
pub fn system_config_dir() -> PathBuf {
  PathBuf::from("/etc/lycoris")
}

/// Return candidate configuration directories in precedence order.
///
/// User configuration takes precedence over system configuration.
pub fn config_dirs() -> Vec<PathBuf> {
  let mut dirs = Vec::with_capacity(2);
  if let Some(user) = user_config_dir() {
    dirs.push(user);
  }
  dirs.push(system_config_dir());
  dirs
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
/// On Linux, root processes use `/var/lib/lycoris`; unprivileged processes use
/// `$XDG_DATA_HOME/lycoris` or `$HOME/.local/share/lycoris`. On other platforms
/// the function falls back to a user-local directory under `$HOME`.
pub fn default_data_dir() -> PathBuf {
  default_data_dir_from_env(is_root(), env::var_os("XDG_DATA_HOME"), env::var_os("HOME"))
}

fn default_data_dir_from_env(
  root: bool, xdg_data_home: Option<OsString>, home: Option<OsString>,
) -> PathBuf {
  if root {
    return PathBuf::from("/var/lib/lycoris");
  }

  xdg_data_home
    .map(PathBuf::from)
    .or_else(|| home.map(|home| PathBuf::from(home).join(".local/share")))
    .map(|dir| dir.join("lycoris"))
    .unwrap_or_else(|| PathBuf::from("/tmp/lycoris"))
}

/// Best-effort root detection on Linux by reading `/proc/self/status`.
///
/// Falls back to `false` on non-Linux platforms or if the status file cannot be
/// parsed.
fn is_root() -> bool {
  current_uid() == Some(0)
}

fn current_uid() -> Option<u32> {
  let status = std::fs::read_to_string("/proc/self/status").ok()?;
  status
    .lines()
    .find(|line| line.starts_with("Uid:"))?
    .split_whitespace()
    .nth(1)?
    .parse()
    .ok()
}

/// Ensure a directory exists, creating it and its parents if necessary.
pub fn ensure_dir<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
  std::fs::create_dir_all(path.as_ref())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn user_config_dir_uses_xdg_when_set() {
    let base = PathBuf::from("/tmp/lycoris-xdg-test");
    let dir = user_config_dir_from_env(
      Some(OsString::from(&base)),
      Some(OsString::from("/unused/home")),
    );
    assert_eq!(dir, Some(base.join("lycoris")));
  }

  #[test]
  fn user_config_dir_falls_back_to_home() {
    let home = PathBuf::from("/tmp/lycoris-home-test");
    let dir = user_config_dir_from_env(None, Some(OsString::from(&home)));
    assert_eq!(dir, Some(home.join(".config/lycoris")));
  }

  #[test]
  fn select_config_path_prefers_existing_user_file() {
    let tmp = env::temp_dir().join("lycoris-config-precedence");
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

  #[test]
  fn default_data_dir_uses_xdg_when_set() {
    let base = PathBuf::from("/tmp/lycoris-xdg-data");
    let dir = default_data_dir_from_env(false, Some(OsString::from(&base)), None);
    assert_eq!(dir, base.join("lycoris"));
  }

  #[test]
  fn default_data_dir_falls_back_to_home_local_share() {
    let home = PathBuf::from("/tmp/lycoris-home-data");
    let dir = default_data_dir_from_env(false, None, Some(OsString::from(&home)));
    assert_eq!(dir, home.join(".local/share/lycoris"));
  }

  #[test]
  fn default_data_dir_for_root_uses_var_lib() {
    let dir = default_data_dir_from_env(true, None, None);
    assert_eq!(dir, PathBuf::from("/var/lib/lycoris"));
  }
}
