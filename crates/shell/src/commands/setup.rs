use std::{collections::HashMap, env, ffi::OsString, path::Path};

use lycoris_config::{DAEMON_CONFIG_FILE_NAME, DaemonConfig, ExtensionsConfig};
use lycoris_tls::ensure_tls_bundle;
use service_manager::{
  RestartPolicy, ServiceInstallCtx, ServiceLabel, ServiceLevel, ServiceManager, ServiceStartCtx,
};

use crate::error::ShellError;

const SERVICE_LABEL: &str = "tech.woooo.lycoris";

#[tracing::instrument(name = "setup", skip_all)]
pub(crate) fn run(
  node_id: Option<String>, port: u16, advertise_addr: Option<String>, no_start: bool,
) -> Result<(), ShellError> {
  let current_exe = env::current_exe().map_err(|error| {
    ShellError::setup(format!(
      "failed to determine current executable path: {error}"
    ))
  })?;

  let node_id = resolve_node_id(node_id)?;
  let advertise_addr = advertise_addr.unwrap_or_else(|| format!("https://127.0.0.1:{port}"));

  let server_binary = resolve_install_path(&current_exe)?;
  let (config_dir, data_dir) = lycoris_dirs()?;

  bootstrap_node_assets(&node_id, port, &advertise_addr, &config_dir, &data_dir)?;

  install_service(&server_binary, &config_dir, &data_dir, no_start)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Resolve the node id: return the explicit value, or fall back to the
/// machine hostname.
fn resolve_node_id(node_id: Option<String>) -> Result<String, ShellError> {
  if let Some(id) = node_id {
    return Ok(id);
  }
  let output = std::process::Command::new("hostname")
    .output()
    .map_err(|error| ShellError::setup(format!("failed to execute hostname: {error}")))?;
  if !output.status.success() {
    return Err(ShellError::setup("hostname command failed"));
  }
  let hostname = String::from_utf8_lossy(&output.stdout).trim().to_string();
  if hostname.is_empty() {
    return Err(ShellError::setup("hostname returned an empty string"));
  }
  Ok(hostname)
}

/// Create `path` and all missing parents, with the shared setup error shape.
fn ensure_dir<P: AsRef<Path>>(path: P) -> Result<(), ShellError> {
  let path = path.as_ref();
  std::fs::create_dir_all(path)
    .map_err(|error| ShellError::setup(format!("failed to create {}: {error}", path.display())))
}

/// Return the lycoris config and data directories.
fn lycoris_dirs() -> Result<(std::path::PathBuf, std::path::PathBuf), ShellError> {
  let config_dir = lycoris_config::user_config_dir()
    .ok_or_else(|| ShellError::setup("failed to determine user config directory"))?;
  let data_dir = lycoris_config::user_data_dir()
    .ok_or_else(|| ShellError::setup("failed to determine user data directory"))?;
  Ok((config_dir, data_dir))
}

/// True when `binary_dir` appears in the process `PATH`.
fn is_in_path(binary_dir: &Path) -> bool {
  env::var_os("PATH").is_some_and(|path| {
    env::split_paths(&path).any(|entry| {
      let lossy = entry.to_string_lossy();
      let normalized = lossy.trim_end_matches(std::path::MAIN_SEPARATOR);
      Path::new(normalized) == binary_dir
    })
  })
}

/// The canonical stable installation path for the server binary.
///
/// root → `/usr/local/bin/lycoris`,
/// normal user → `$HOME/.local/bin/lycoris`,
/// windows → `%LOCALAPPDATA%\lycoris\bin\lycoris.exe`.
fn canonical_binary_path() -> Result<std::path::PathBuf, ShellError> {
  #[cfg(target_os = "windows")]
  {
    let local = env::var_os("LOCALAPPDATA")
      .ok_or_else(|| ShellError::setup("LOCALAPPDATA environment variable is not set"))?;
    return Ok(
      std::path::PathBuf::from(local)
        .join("lycoris")
        .join("bin")
        .join("lycoris.exe"),
    );
  }
  #[cfg(not(target_os = "windows"))]
  {
    if is_root::is_root() {
      Ok(std::path::PathBuf::from(format!(
        "/usr/local/bin/lycoris{}",
        env::consts::EXE_SUFFIX
      )))
    } else {
      let home = home_dir()?;
      Ok(
        home
          .join(".local/bin/lycoris")
          .with_extension(env::consts::EXE_SUFFIX),
      )
    }
  }
}

/// Return the current user's home directory.
fn home_dir() -> Result<std::path::PathBuf, ShellError> {
  #[cfg(not(target_os = "windows"))]
  {
    env::var("HOME")
      .map(std::path::PathBuf::from)
      .map_err(|_| ShellError::setup("HOME environment variable is not set"))
  }
  #[cfg(target_os = "windows")]
  {
    env::var("USERPROFILE")
      .map(std::path::PathBuf::from)
      .map_err(|_| ShellError::setup("USERPROFILE environment variable is not set"))
  }
}

/// Ensure the server binary sits at a stable, PATH-accessible location.
///
/// - If `current_exe` is already in PATH, use it as-is.
/// - Otherwise copy it to the canonical location. If that location is also not
///   in PATH a message is logged so the user can add it manually.
fn resolve_install_path(current_exe: &Path) -> Result<std::path::PathBuf, ShellError> {
  let parent = current_exe
    .parent()
    .ok_or_else(|| ShellError::setup("current executable has no parent directory"))?;

  if is_in_path(parent) {
    tracing::info!(
      path = %current_exe.display(),
      "binary is already in PATH, using as-is"
    );
    return Ok(current_exe.to_path_buf());
  }

  let target = canonical_binary_path()?;

  if target.exists() {
    tracing::info!(
      path = %target.display(),
      "binary already exists at the canonical path, using it"
    );
    return Ok(target);
  }

  if let Some(target_dir) = target.parent() {
    ensure_dir(target_dir)?;
  }

  std::fs::copy(current_exe, &target).map_err(|error| {
    ShellError::setup(format!(
      "failed to copy binary to {}: {error}",
      target.display()
    ))
  })?;

  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt;
    let metadata = target.metadata().map_err(|error| {
      ShellError::setup(format!(
        "failed to read metadata for {}: {error}",
        target.display()
      ))
    })?;
    let mut perms = metadata.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&target, perms).map_err(|error| {
      ShellError::setup(format!(
        "failed to set permissions on {}: {error}",
        target.display()
      ))
    })?;
  }

  tracing::info!(
    from = %current_exe.display(),
    to = %target.display(),
    "copied binary to canonical PATH location"
  );

  if let Some(target_dir) = target.parent()
    && !is_in_path(target_dir)
  {
    tracing::warn!(
      dir = %target_dir.display(),
      "the canonical install directory is not in PATH; add it to your shell profile"
    );
  }

  Ok(target)
}

/// Generate TLS certificates and write the daemon configuration file.
///
/// Shared across platforms: certificates go under `{data_dir}/certs/`, the
/// config file lands at `{config_dir}/lycoris.toml`. Both operations are
/// idempotent — certificates are only regenerated when missing or expired,
/// and an existing config file is never overwritten.
fn bootstrap_node_assets(
  node_id: &str, port: u16, advertise_addr: &str, config_dir: &Path, data_dir: &Path,
) -> Result<(), ShellError> {
  let cert_dir = data_dir.join("certs");
  ensure_dir(&cert_dir)?;
  ensure_dir(config_dir)?;

  // 1. TLS certificates — idempotent.
  let ca_cert = cert_dir.join("ca.crt");
  let ca_key = cert_dir.join("ca.key");
  let node_cert = cert_dir.join("node.crt");
  let node_key = cert_dir.join("node.key");
  ensure_tls_bundle(
    &ca_cert,
    &ca_key,
    &node_cert,
    &node_key,
    node_id,
    advertise_addr,
  )
  .map_err(|error| ShellError::setup(format!("failed to generate TLS certificates: {error}")))?;
  tracing::info!(dir = %cert_dir.display(), "TLS certificates ready");

  // 2. Daemon configuration — write only when the file does not exist.
  let config_path = config_dir.join(DAEMON_CONFIG_FILE_NAME);
  if config_path.exists() {
    tracing::info!(path = %config_path.display(), "config file already exists, skipping");
    return Ok(());
  }
  let daemon_config = DaemonConfig {
    node: lycoris_config::NodeConfig {
      id: node_id.to_string(),
      address: advertise_addr.to_string(),
      labels: HashMap::new(),
    },
    cluster: lycoris_config::ClusterConfig {
      listen_address: format!("0.0.0.0:{port}"),
      bootstrap_peers: Vec::new(),
    },
    tls: lycoris_config::TlsConfig {
      ca_cert: ca_cert.to_string_lossy().to_string(),
      ca_key: ca_key.to_string_lossy().to_string(),
      cert: node_cert.to_string_lossy().to_string(),
      key: node_key.to_string_lossy().to_string(),
    },
    data_dir: data_dir.to_string_lossy().to_string(),
    extensions: ExtensionsConfig::default(),
  };
  daemon_config
    .write_to_file(&config_path)
    .map_err(|error| ShellError::setup(format!("failed to write config file: {error}")))?;
  tracing::info!(path = %config_path.display(), "wrote daemon configuration");

  Ok(())
}

// ---------------------------------------------------------------------------
// service installation via service-manager crate
// ---------------------------------------------------------------------------

fn install_service(
  server_binary: &Path, config_dir: &Path, data_dir: &Path, no_start: bool,
) -> Result<(), ShellError> {
  let label: ServiceLabel = SERVICE_LABEL
    .parse()
    .map_err(|error| ShellError::setup(format!("invalid service label: {error}")))?;

  let mut manager = <dyn ServiceManager>::native().map_err(|error| {
    ShellError::setup(format!(
      "failed to detect service management platform: {error}"
    ))
  })?;

  // User-level services on Linux (systemd --user) and macOS (user LaunchAgents).
  manager.set_level(ServiceLevel::User).ok();

  let config_path = config_dir.join(DAEMON_CONFIG_FILE_NAME);
  let args: Vec<OsString> = vec![
    "daemon".into(),
    "--config".into(),
    config_path.into_os_string(),
  ];

  manager
    .install(ServiceInstallCtx {
      label: label.clone(),
      program: server_binary.to_path_buf(),
      args,
      contents: None,
      username: None,
      working_directory: Some(data_dir.to_path_buf()),
      environment: None,
      autostart: true,
      restart_policy: RestartPolicy::OnFailure {
        delay_secs: Some(5),
        max_retries: Some(3),
        reset_after_secs: Some(60),
      },
    })
    .map_err(|error| ShellError::setup(format!("failed to install service: {error}")))?;

  tracing::info!(
    binary = %server_binary.display(),
    config = %config_dir.display(),
    data = %data_dir.display(),
    "service installed"
  );

  if no_start {
    tracing::info!("next: lycoris daemon  (or start via your service manager)");
    return Ok(());
  }

  manager
    .start(ServiceStartCtx {
      label: label.clone(),
    })
    .map_err(|error| ShellError::setup(format!("failed to start service: {error}")))?;

  tracing::info!("service started");

  Ok(())
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn is_in_path_detects_entry() {
    let tmp = std::env::temp_dir();
    assert!(!is_in_path(&tmp));
  }

  #[test]
  fn canonical_binary_path_for_normal_user_is_under_home() {
    if cfg!(unix) && !is_root::is_root() {
      let path = canonical_binary_path().unwrap();
      let home = home_dir().unwrap();
      assert!(path.starts_with(home));
    }
  }
}
