use std::env;

use crate::error::ShellError;

const SERVICE_NAME: &str = "lycoris";

/// Install `lycoris` as a user service.
///
/// The binary is expected to live in the same directory as the running
/// `lycoris` executable. This matches the production layout where both the
/// CLI and daemon code ship as a single binary.
pub(crate) fn run() -> Result<(), ShellError> {
  let current_exe = env::current_exe().map_err(|error| {
    ShellError::setup(format!(
      "failed to determine current executable path: {error}"
    ))
  })?;
  let bin_dir = current_exe
    .parent()
    .ok_or_else(|| ShellError::setup("current executable has no parent directory"))?;

  platform::install(bin_dir)
}

/// Return the server binary path inside `bin_dir`, verifying it exists.
///
/// The daemon and the CLI ship as a single binary, so the server binary must
/// sit next to the running executable. The platform executable suffix matters
/// here: on windows the binary is `lycoris.exe`.
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn server_binary_path(bin_dir: &std::path::Path) -> Result<std::path::PathBuf, ShellError> {
  let server_binary = bin_dir.join(format!("lycoris{}", env::consts::EXE_SUFFIX));
  if !server_binary.is_file() {
    return Err(ShellError::setup(format!(
      "server binary not found at {}; ensure 'lycoris' is in the same directory as the lycoris binary",
      server_binary.display()
    )));
  }
  Ok(server_binary)
}

/// Create `path` and all missing parents, with the shared setup error shape.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn ensure_dir(path: &std::path::Path) -> Result<(), ShellError> {
  std::fs::create_dir_all(path)
    .map_err(|error| ShellError::setup(format!("failed to create {}: {error}", path.display())))
}

/// Return the current user's home directory (used by the unix service
/// layouts; windows resolves directories through `lycoris_config` instead).
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn home_dir() -> Result<std::path::PathBuf, ShellError> {
  env::var("HOME")
    .map(std::path::PathBuf::from)
    .map_err(|_| ShellError::setup("HOME environment variable is not set"))
}

#[cfg(target_os = "linux")]
mod platform {
  use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

  use lycoris_config::DAEMON_CONFIG_FILE_NAME;

  use super::{SERVICE_NAME, ensure_dir, home_dir, server_binary_path};
  use crate::error::ShellError;

  pub(crate) fn install(bin_dir: &Path) -> Result<(), ShellError> {
    let home = home_dir()?;
    let systemd_dir = home.join(".config/systemd/user");
    let config_dir =
      lycoris_config::user_config_dir().unwrap_or_else(|| home.join(".config/lycoris"));
    let data_dir =
      lycoris_config::user_data_dir().unwrap_or_else(|| home.join(".local/share/lycoris"));

    install_common(bin_dir, &systemd_dir, &config_dir, &data_dir)?;
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
      config_dir.join(DAEMON_CONFIG_FILE_NAME).display()
    );
    println!("  2. systemctl --user start {SERVICE_NAME}");

    Ok(())
  }

  fn install_common(
    bin_dir: &Path, systemd_dir: &Path, config_dir: &Path, data_dir: &Path,
  ) -> Result<(), ShellError> {
    let server_binary = server_binary_path(bin_dir)?;

    let metadata = server_binary.metadata().map_err(|error| {
      ShellError::setup(format!(
        "failed to read metadata for {}: {error}",
        server_binary.display()
      ))
    })?;
    let mode = metadata.permissions().mode();
    if mode & 0o111 == 0 {
      return Err(ShellError::setup(format!(
        "server binary at {} is not executable",
        server_binary.display()
      )));
    }

    ensure_dir(systemd_dir)?;
    ensure_dir(config_dir)?;
    ensure_dir(data_dir)?;

    let service_path = systemd_dir.join(format!("{SERVICE_NAME}.service"));
    let unit_content = render_user_unit(&server_binary, config_dir, data_dir);
    fs::write(&service_path, unit_content).map_err(|error| {
      ShellError::setup(format!(
        "failed to write {}: {error}",
        service_path.display()
      ))
    })?;

    Ok(())
  }

  fn render_user_unit(server_binary: &Path, config_dir: &Path, data_dir: &Path) -> String {
    let exec_start = format!(
      "\"{}\" daemon --config \"{}\"",
      server_binary.to_string_lossy(),
      config_dir.join(DAEMON_CONFIG_FILE_NAME).display()
    );

    format!(
      "[Unit]\n\
       Description=Lycoris daemon (user)\n\
       Documentation=https://lycoris.woooo.tech\n\
       After=network-online.target\n\
       Wants=network-online.target\n\n\
       [Service]\n\
       Type=exec\n\
       ExecStart={exec_start}\n\
       WorkingDirectory=\"{}\"\n\n\
       Restart=on-failure\n\
       RestartSec=5s\n\
       StartLimitInterval=60s\n\
       StartLimitBurst=3\n\n\
       StandardOutput=journal\n\
       StandardError=journal\n\
       SyslogIdentifier={SERVICE_NAME}\n\n\
       [Install]\n\
       WantedBy=default.target\n",
      data_dir.display()
    )
  }

  fn reload_and_enable_user_systemd() -> Result<(), ShellError> {
    run_systemctl(&["--user", "daemon-reload"]).map_err(|error| {
      ShellError::setup(format!("failed to reload user systemd daemon: {error}"))
    })?;
    run_systemctl(&["--user", "enable", &format!("{SERVICE_NAME}.service")])
      .map_err(|error| ShellError::setup(format!("failed to enable user service: {error}")))?;
    Ok(())
  }

  fn run_systemctl(args: &[&str]) -> Result<(), ShellError> {
    let output = Command::new("systemctl")
      .args(args)
      .output()
      .map_err(|error| ShellError::setup(format!("failed to execute systemctl: {error}")))?;

    if !output.status.success() {
      let stderr = String::from_utf8_lossy(&output.stderr);
      return Err(ShellError::setup(format!(
        "systemctl {} failed: {}",
        args.join(" "),
        stderr
      )));
    }

    Ok(())
  }

  #[cfg(test)]
  mod tests {
    use std::{fs, path::Path};

    use super::*;

    fn make_executable(path: &Path) {
      use std::os::unix::fs::PermissionsExt;

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
      let server_binary = bin_dir.join("lycoris");
      make_executable(&server_binary);

      install_common(&bin_dir, &systemd_dir, &config_dir, &data_dir).unwrap();

      assert!(systemd_dir.join("lycoris.service").is_file());
      assert!(config_dir.is_dir());
      assert!(data_dir.is_dir());

      let content = fs::read_to_string(systemd_dir.join("lycoris.service")).unwrap();
      assert!(content.contains(&format!("ExecStart=\"{}\" daemon", server_binary.display())));
      assert!(content.contains(&format!("WorkingDirectory=\"{}\"", data_dir.display())));
    }

    #[test]
    fn install_fails_when_server_binary_is_missing() {
      let tmp = tempfile::tempdir().unwrap();
      let bin_dir = tmp.path().join("bin");
      fs::create_dir_all(&bin_dir).unwrap();

      let result = install_common(
        &bin_dir,
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

      let server_binary = bin_dir.join("lycoris");
      fs::write(&server_binary, "not executable").unwrap();

      let result = install_common(
        &bin_dir,
        &tmp.path().join("systemd"),
        &tmp.path().join("config"),
        &tmp.path().join("data"),
      );

      assert!(result.is_err());
    }
  }
}

#[cfg(target_os = "macos")]
mod platform {
  use std::{fs, path::Path, process::Command};

  use lycoris_config::DAEMON_CONFIG_FILE_NAME;

  use super::{SERVICE_NAME, ensure_dir, home_dir, server_binary_path};
  use crate::error::ShellError;

  pub(crate) fn install(bin_dir: &Path) -> Result<(), ShellError> {
    let home = home_dir()?;
    let launchd_dir = home.join("Library/LaunchAgents");
    let config_dir = lycoris_config::user_config_dir()
      .unwrap_or_else(|| home.join("Library/Application Support/lycoris"));
    let data_dir = lycoris_config::user_data_dir()
      .unwrap_or_else(|| home.join("Library/Application Support/lycoris"));

    install_common(bin_dir, &launchd_dir, &config_dir, &data_dir)?;
    load_launchd_agent(&launchd_dir)?;

    println!(
      "installed launchd agent: {}",
      launchd_dir.join(format!("{SERVICE_NAME}.plist")).display()
    );
    println!("config directory:        {}", config_dir.display());
    println!("data directory:          {}", data_dir.display());
    println!();
    println!("next steps:");
    println!(
      "  1. create {}",
      config_dir.join(DAEMON_CONFIG_FILE_NAME).display()
    );
    println!("  2. launchctl start {SERVICE_NAME}");

    Ok(())
  }

  fn install_common(
    bin_dir: &Path, launchd_dir: &Path, config_dir: &Path, data_dir: &Path,
  ) -> Result<(), ShellError> {
    let server_binary = server_binary_path(bin_dir)?;

    ensure_dir(launchd_dir)?;
    ensure_dir(config_dir)?;
    ensure_dir(data_dir)?;

    let plist_path = launchd_dir.join(format!("{SERVICE_NAME}.plist"));
    let plist_content = render_plist(&server_binary, config_dir, data_dir);
    fs::write(&plist_path, plist_content).map_err(|error| {
      ShellError::setup(format!("failed to write {}: {error}", plist_path.display()))
    })?;

    Ok(())
  }

  fn render_plist(server_binary: &Path, config_dir: &Path, data_dir: &Path) -> String {
    let config_path = config_dir.join(DAEMON_CONFIG_FILE_NAME);

    format!(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
       <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
       <plist version=\"1.0\">\n\
       <dict>\n\
         <key>Label</key>\n\
         <string>{SERVICE_NAME}</string>\n\
         <key>ProgramArguments</key>\n\
         <array>\n\
           <string>{}</string>\n\
           <string>daemon</string>\n\
           <string>--config</string>\n\
           <string>{}</string>\n\
         </array>\n\
         <key>WorkingDirectory</key>\n\
         <string>{}</string>\n\
         <key>RunAtLoad</key>\n\
         <true/>\n\
         <key>KeepAlive</key>\n\
         <true/>\n\
         <key>StandardOutPath</key>\n\
         <string>{}/lycoris.log</string>\n\
         <key>StandardErrorPath</key>\n\
         <string>{}/lycoris.log</string>\n\
       </dict>\n\
       </plist>\n",
      xml_escape(&server_binary.to_string_lossy()),
      xml_escape(&config_path.to_string_lossy()),
      xml_escape(&data_dir.to_string_lossy()),
      xml_escape(&data_dir.to_string_lossy()),
      xml_escape(&data_dir.to_string_lossy())
    )
  }

  /// Escape the XML predefined entities so paths containing `&`, `<`, quotes
  /// or similar characters still produce a valid plist.
  fn xml_escape(raw: &str) -> String {
    raw
      .replace('&', "&amp;")
      .replace('<', "&lt;")
      .replace('>', "&gt;")
      .replace('"', "&quot;")
      .replace('\'', "&apos;")
  }

  fn load_launchd_agent(launchd_dir: &Path) -> Result<(), ShellError> {
    let plist_path = launchd_dir.join(format!("{SERVICE_NAME}.plist"));
    let output = Command::new("launchctl")
      .args(["load", "-w", plist_path.to_string_lossy().as_ref()])
      .output()
      .map_err(|error| ShellError::setup(format!("failed to execute launchctl: {error}")))?;

    if !output.status.success() {
      let stderr = String::from_utf8_lossy(&output.stderr);
      return Err(ShellError::setup(format!(
        "launchctl load failed: {}",
        stderr
      )));
    }

    Ok(())
  }
}

#[cfg(target_os = "windows")]
mod platform {
  use std::path::Path;

  use lycoris_config::DAEMON_CONFIG_FILE_NAME;

  use super::{SERVICE_NAME, server_binary_path};
  use crate::error::ShellError;

  pub(crate) fn install(bin_dir: &Path) -> Result<(), ShellError> {
    let server_binary = server_binary_path(bin_dir)?;

    let config_dir = lycoris_config::user_config_dir()
      .ok_or_else(|| ShellError::setup("failed to determine user config directory"))?;
    let config_path = config_dir.join(DAEMON_CONFIG_FILE_NAME);

    // Registering a windows service requires elevated rights and sc.exe
    // syntax that is error-prone to automate; fail loudly with the manual
    // steps instead of pretending the setup succeeded.
    Err(ShellError::setup(format!(
      "windows service setup is not automated; nothing was installed.\n\
       to run lycoris as a windows service manually:\n\
       \x20 1. create {}\n\
       \x20 2. register the service: sc create {SERVICE_NAME} binPath= \"{} daemon --config \\\"{}\\\"\" start= auto\n\
       \x20 3. start the service: sc start {SERVICE_NAME}",
      config_path.display(),
      server_binary.display(),
      config_path.display()
    )))
  }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod platform {
  use std::path::Path;

  use crate::error::ShellError;

  pub fn install(_bin_dir: &Path) -> Result<(), ShellError> {
    Err(ShellError::setup(
      "setup is only supported on linux, macos and windows",
    ))
  }
}
