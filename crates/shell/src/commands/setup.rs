use std::env;

use anyhow::Context;

const SERVICE_NAME: &str = "lycoris";
const DEFAULT_CONFIG_NAME: &str = "lycoris.toml";

/// Install `lycoris-server` as a user service.
///
/// The server binary is expected to live in the same directory as the running
/// `lycoris` executable. This matches the production layout where both binaries
/// are shipped side-by-side.
pub fn run(binary_name: &str) -> anyhow::Result<()> {
  let current_exe = env::current_exe().context("failed to determine current executable path")?;
  let bin_dir = current_exe
    .parent()
    .context("current executable has no parent directory")?;

  platform::install(bin_dir, binary_name)
}

#[cfg(target_os = "linux")]
mod platform {
  use std::{env, fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

  use anyhow::{Context, bail};

  use super::{DEFAULT_CONFIG_NAME, SERVICE_NAME};

  pub fn install(bin_dir: &Path, binary_name: &str) -> anyhow::Result<()> {
    let home = home_dir()?;
    let systemd_dir = home.join(".config/systemd/user");
    let config_dir =
      lycoris_config::paths::user_config_dir().unwrap_or_else(|| home.join(".config/lycoris"));
    let data_dir =
      lycoris_config::paths::user_data_dir().unwrap_or_else(|| home.join(".local/share/lycoris"));

    install_common(bin_dir, binary_name, &systemd_dir, &config_dir, &data_dir)?;
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

  fn install_common(
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
    let unit_content = render_user_unit(&server_binary, config_dir, data_dir);
    fs::write(&service_path, unit_content)
      .with_context(|| format!("failed to write {}", service_path.display()))?;

    Ok(())
  }

  fn render_user_unit(server_binary: &Path, config_dir: &Path, data_dir: &Path) -> String {
    let exec_start = format!(
      "\"{}\" --config \"{}\"",
      server_binary.to_string_lossy(),
      config_dir.join(DEFAULT_CONFIG_NAME).display()
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
       WorkingDirectory={}\n\n\
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

  fn reload_and_enable_user_systemd() -> anyhow::Result<()> {
    run_systemctl(&["--user", "daemon-reload"]).context("failed to reload user systemd daemon")?;
    run_systemctl(&["--user", "enable", &format!("{SERVICE_NAME}.service")])
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

  fn home_dir() -> anyhow::Result<std::path::PathBuf> {
    env::var("HOME")
      .context("HOME environment variable is not set")
      .map(std::path::PathBuf::from)
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
      let server_binary = bin_dir.join("lycoris-server");
      make_executable(&server_binary);

      install_common(
        &bin_dir,
        "lycoris-server",
        &systemd_dir,
        &config_dir,
        &data_dir,
      )
      .unwrap();

      assert!(systemd_dir.join("lycoris.service").is_file());
      assert!(config_dir.is_dir());
      assert!(data_dir.is_dir());

      let content = fs::read_to_string(systemd_dir.join("lycoris.service")).unwrap();
      assert!(content.contains(&format!("ExecStart=\"{}\"", server_binary.display())));
      assert!(content.contains(&format!("WorkingDirectory={}", data_dir.display())));
    }

    #[test]
    fn install_fails_when_server_binary_is_missing() {
      let tmp = tempfile::tempdir().unwrap();
      let bin_dir = tmp.path().join("bin");
      fs::create_dir_all(&bin_dir).unwrap();

      let result = install_common(
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

      let result = install_common(
        &bin_dir,
        "lycoris-server",
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
  use std::{env, fs, path::Path, process::Command};

  use anyhow::{Context, bail};

  use super::{DEFAULT_CONFIG_NAME, SERVICE_NAME};

  pub fn install(bin_dir: &Path, binary_name: &str) -> anyhow::Result<()> {
    let home = home_dir()?;
    let launchd_dir = home.join("Library/LaunchAgents");
    let config_dir = lycoris_config::paths::user_config_dir()
      .unwrap_or_else(|| home.join("Library/Application Support/lycoris"));
    let data_dir = lycoris_config::paths::user_data_dir()
      .unwrap_or_else(|| home.join("Library/Application Support/lycoris"));

    install_common(bin_dir, binary_name, &launchd_dir, &config_dir, &data_dir)?;
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
      config_dir.join(DEFAULT_CONFIG_NAME).display()
    );
    println!("  2. launchctl start {SERVICE_NAME}");

    Ok(())
  }

  fn install_common(
    bin_dir: &Path, binary_name: &str, launchd_dir: &Path, config_dir: &Path, data_dir: &Path,
  ) -> anyhow::Result<()> {
    let server_binary = bin_dir.join(binary_name);

    if !server_binary.is_file() {
      bail!(
        "server binary not found at {}; ensure '{}' is in the same directory as the lycoris binary",
        server_binary.display(),
        binary_name
      );
    }

    fs::create_dir_all(launchd_dir)
      .with_context(|| format!("failed to create {}", launchd_dir.display()))?;
    fs::create_dir_all(config_dir)
      .with_context(|| format!("failed to create {}", config_dir.display()))?;
    fs::create_dir_all(data_dir)
      .with_context(|| format!("failed to create {}", data_dir.display()))?;

    let plist_path = launchd_dir.join(format!("{SERVICE_NAME}.plist"));
    let plist_content = render_plist(&server_binary, config_dir, data_dir);
    fs::write(&plist_path, plist_content)
      .with_context(|| format!("failed to write {}", plist_path.display()))?;

    Ok(())
  }

  fn render_plist(server_binary: &Path, config_dir: &Path, data_dir: &Path) -> String {
    let config_path = config_dir.join(DEFAULT_CONFIG_NAME);

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
      server_binary.to_string_lossy(),
      config_path.display(),
      data_dir.display(),
      data_dir.display(),
      data_dir.display()
    )
  }

  fn load_launchd_agent(launchd_dir: &Path) -> anyhow::Result<()> {
    let plist_path = launchd_dir.join(format!("{SERVICE_NAME}.plist"));
    let output = Command::new("launchctl")
      .args(["load", "-w", plist_path.to_string_lossy().as_ref()])
      .output()
      .context("failed to execute launchctl")?;

    if !output.status.success() {
      let stderr = String::from_utf8_lossy(&output.stderr);
      bail!("launchctl load failed: {}", stderr);
    }

    Ok(())
  }

  fn home_dir() -> anyhow::Result<std::path::PathBuf> {
    env::var("HOME")
      .context("HOME environment variable is not set")
      .map(std::path::PathBuf::from)
  }
}

#[cfg(target_os = "windows")]
mod platform {
  use std::{fs, path::Path};

  use anyhow::{Context, bail};

  use super::{DEFAULT_CONFIG_NAME, SERVICE_NAME};

  pub fn install(bin_dir: &Path, binary_name: &str) -> anyhow::Result<()> {
    let server_binary = bin_dir.join(binary_name);

    if !server_binary.is_file() {
      bail!(
        "server binary not found at {}; ensure '{}' is in the same directory as the lycoris binary",
        server_binary.display(),
        binary_name
      );
    }

    let config_dir = lycoris_config::paths::user_config_dir()
      .context("failed to determine user config directory")?;
    let data_dir =
      lycoris_config::paths::user_data_dir().context("failed to determine user data directory")?;

    fs::create_dir_all(&config_dir)
      .with_context(|| format!("failed to create {}", config_dir.display()))?;
    fs::create_dir_all(&data_dir)
      .with_context(|| format!("failed to create {}", data_dir.display()))?;

    println!("windows service setup is not yet automated.");
    println!();
    println!("prepared directories:");
    println!("  config: {}", config_dir.display());
    println!("  data:   {}", data_dir.display());
    println!();
    println!("next steps:");
    println!(
      "  1. create {}",
      config_dir.join(DEFAULT_CONFIG_NAME).display()
    );
    println!("  2. register {} as a windows service, e.g.:", SERVICE_NAME);
    println!(
      "     sc create {SERVICE_NAME} binPath= \"{} --config \\\"{}\\\"\" start= auto",
      server_binary.display(),
      config_dir.join(DEFAULT_CONFIG_NAME).display()
    );
    println!("  3. start the service: sc start {SERVICE_NAME}");

    Ok(())
  }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod platform {
  use std::path::Path;

  use anyhow::bail;

  pub fn install(_bin_dir: &Path, _binary_name: &str) -> anyhow::Result<()> {
    bail!("setup is only supported on linux, macos and windows")
  }
}
