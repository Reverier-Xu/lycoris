//! Build the real wasm extension artifacts the end-to-end tests load.

use std::{path::PathBuf, process::Command};

/// Build the release wasm artifact of `package` with the workspace's own
/// cargo and return its path. Uses `--locked` so the committed lockfile is
/// what gets built.
///
/// Panics with remediation when the `wasm32-unknown-unknown` target is not
/// installed or the build fails: a wasm end-to-end test must never pass
/// without having exercised the real artifact.
pub fn build_wasm_artifact(package: &str) -> PathBuf {
  ensure_wasm32_target();
  let root = workspace_root();
  let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()))
    .args([
      "build",
      "--release",
      "--locked",
      "--target",
      "wasm32-unknown-unknown",
      "--package",
      package,
    ])
    .current_dir(&root)
    .status()
    .unwrap_or_else(|err| panic!("spawn the wasm build: {err}"));
  assert!(status.success(), "the wasm32 build of {package} failed");

  let target_dir = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
    || root.join("target"),
    |dir| {
      let dir = PathBuf::from(dir);
      if dir.is_absolute() {
        dir
      } else {
        root.join(dir)
      }
    },
  );
  let artifact = target_dir.join(format!(
    "wasm32-unknown-unknown/release/{}.wasm",
    package.replace('-', "_")
  ));
  assert!(
    artifact.is_file(),
    "expected the wasm artifact at {}",
    artifact.display()
  );
  artifact
}

/// Fail loudly, with the remediation, when the wasm target is missing.
fn ensure_wasm32_target() {
  let Ok(output) = Command::new("rustup")
    .args(["target", "list", "--installed"])
    .output()
  else {
    return; // No rustup: the build below reports its own failure.
  };
  let installed = String::from_utf8_lossy(&output.stdout);
  assert!(
    installed
      .lines()
      .any(|line| line.trim() == "wasm32-unknown-unknown"),
    "the wasm32-unknown-unknown target is not installed; run `rustup target add \
     wasm32-unknown-unknown` first"
  );
}

/// The workspace root (the testkit crate lives directly below it).
fn workspace_root() -> PathBuf {
  let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("..")
    .canonicalize()
    .unwrap_or_else(|err| panic!("resolve the workspace root: {err}"));
  assert!(
    root.join("Cargo.toml").is_file(),
    "no workspace manifest at {}",
    root.display()
  );
  root
}
