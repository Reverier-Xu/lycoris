use std::{
  env, fs,
  os::unix::fs::PermissionsExt,
  path::{Path, PathBuf},
  process::Command,
};

use anyhow::{Context, bail};

const SERVICE_NAME: &str = "lycoris-server";
const DEFAULT_CONFIG_NAME: &str = "lycoris.toml";

/// Install `lycoris-server` as a user-mode systemd service.
///
/// The server binary is expected to live in the same directory as the running
/// `lycoris` executable. This matches the production layout where both binaries
/// are shipped side-by-side.
pub fn run(binary_name: &str) -> anyhow::Result<()> {
  let current_exe = env::current_exe().context("failed to determine current executable path")?;
  let bin_dir = current_exe
    .parent()
    .context("current executable has no parent directory")?;

  let home = home_dir()?;
  let systemd_dir = home.join(".config/systemd/user");
  let config_dir = home.join(".config/lycoris");
  let data_dir = home.join(".local/share/lycoris");

  install(bin_dir, binary_name, &systemd_dir, &config_dir, &data_dir)?;
  reload_and_enable_user_systemd()?;

  println!(
    "installed user service: {}",
    systemd_dir
      .join(format!("{SERVICE_NAME}.service"))
      .display()
  );
  println!("config directory:       {}", config_dir.display());
  println!("data directory:         {}", data_dir.display());
  println!();
  println!("next steps:");
  println!(
    "  1. create {}",
    config_dir.join(DEFAULT_CONFIG_NAME).display()
  );
  println!("  2. systemctl --user start {SERVICE_NAME}");

  Ok(())
}

fn home_dir() -> anyhow::Result<PathBuf> {
  env::var("HOME")
    .context("HOME environment variable is not set")
    .map(PathBuf::from)
}

fn install(
  bin_dir: &Path, binary_name: &str, systemd_dir: &Path, config_dir: &Path, data_dir: &Path,
) -> anyhow::Result<()> {
  let server_binary = bin_dir.join(binary_name);

  if !server_binary.is_file() {
    bail!(
      "server binary not found at {}; ensure '{}' is in the same directory as the lycoris binary",
      server_binary.display(),
      binary_name
    );
  }

  let metadata = server_binary
    .metadata()
    .with_context(|| format!("failed to read metadata for {}", server_binary.display()))?;
  let mode = metadata.permissions().mode();
  if mode & 0o111 == 0 {
    bail!(
      "server binary at {} is not executable",
      server_binary.display()
    );
  }

  fs::create_dir_all(systemd_dir)
    .with_context(|| format!("failed to create {}", systemd_dir.display()))?;
  fs::create_dir_all(config_dir)
    .with_context(|| format!("failed to create {}", config_dir.display()))?;
  fs::create_dir_all(data_dir)
    .with_context(|| format!("failed to create {}", data_dir.display()))?;

  let service_path = systemd_dir.join(format!("{SERVICE_NAME}.service"));
  let unit_content = render_user_unit(&server_binary);
  fs::write(&service_path, unit_content)
    .with_context(|| format!("failed to write {}", service_path.display()))?;

  Ok(())
}

fn render_user_unit(server_binary: &Path) -> String {
  let exec_start = format!(
    "\"{}\" --config \"%h/.config/lycoris/{DEFAULT_CONFIG_NAME}\"",
    server_binary.to_string_lossy()
  );

  format!(
    "[Unit]\n\
     Description=Lycoris server daemon (user)\n\
     Documentation=https://lycoris.woooo.tech\n\
     After=network-online.target\n\
     Wants=network-online.target\n\n\
     [Service]\n\
     Type=exec\n\
     ExecStart={exec_start}\n\
     WorkingDirectory=%h/.local/share/lycoris\n\n\
     Restart=on-failure\n\
     RestartSec=5s\n\
     StartLimitInterval=60s\n\
     StartLimitBurst=3\n\n\
     StandardOutput=journal\n\
     StandardError=journal\n\
     SyslogIdentifier={SERVICE_NAME}\n\n\
     [Install]\n\
     WantedBy=default.target\n",
  )
}

fn reload_and_enable_user_systemd() -> anyhow::Result<()> {
  run_systemctl(&["--user", "daemon-reload"]).context("failed to reload user systemd daemon")?;
  run_systemctl(&[
    "--user",
    "enable",
    format!("{SERVICE_NAME}.service").as_str(),
  ])
  .context("failed to enable user service")?;
  Ok(())
}

fn run_systemctl(args: &[&str]) -> anyhow::Result<()> {
  let output = Command::new("systemctl")
    .args(args)
    .output()
    .context("failed to execute systemctl")?;

  if !output.status.success() {
    let stderr = String::from_utf8_lossy(&output.stderr);
    bail!("systemctl {} failed: {}", args.join(" "), stderr);
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn make_executable(path: &Path) {
    fs::write(path, "#!/bin/sh\n").unwrap();
    let mut perms = fs::metadata(path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).unwrap();
  }

  #[test]
  fn install_creates_service_file_with_expected_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let bin_dir = tmp.path().join("bin");
    let systemd_dir = tmp.path().join("systemd");
    let config_dir = tmp.path().join("config");
    let data_dir = tmp.path().join("data");

    fs::create_dir_all(&bin_dir).unwrap();
    let server_binary = bin_dir.join("lycoris-server");
    make_executable(&server_binary);

    install(
      &bin_dir,
      "lycoris-server",
      &systemd_dir,
      &config_dir,
      &data_dir,
    )
    .unwrap();

    assert!(systemd_dir.join("lycoris-server.service").is_file());
    assert!(config_dir.is_dir());
    assert!(data_dir.is_dir());

    let content = fs::read_to_string(systemd_dir.join("lycoris-server.service")).unwrap();
    assert!(content.contains(&format!("ExecStart=\"{}\"", server_binary.display())));
    assert!(content.contains("WorkingDirectory=%h/.local/share/lycoris"));
    assert!(content.contains("--config \"%h/.config/lycoris/lycoris.toml\""));
  }

  #[test]
  fn install_fails_when_server_binary_is_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let bin_dir = tmp.path().join("bin");
    fs::create_dir_all(&bin_dir).unwrap();

    let result = install(
      &bin_dir,
      "lycoris-server",
      &tmp.path().join("systemd"),
      &tmp.path().join("config"),
      &tmp.path().join("data"),
    );

    assert!(result.is_err());
  }

  #[test]
  fn install_fails_when_server_binary_is_not_executable() {
    let tmp = tempfile::tempdir().unwrap();
    let bin_dir = tmp.path().join("bin");
    fs::create_dir_all(&bin_dir).unwrap();

    let server_binary = bin_dir.join("lycoris-server");
    fs::write(&server_binary, "not executable").unwrap();

    let result = install(
      &bin_dir,
      "lycoris-server",
      &tmp.path().join("systemd"),
      &tmp.path().join("config"),
      &tmp.path().join("data"),
    );

    assert!(result.is_err());
  }
}
