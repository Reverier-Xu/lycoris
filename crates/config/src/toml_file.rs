//! Shared TOML file IO for configuration types: both the daemon and client
//! configurations read and write TOML files the same way, so the file
//! handling lives here exactly once.

use std::{fs, path::Path};

use serde::{Serialize, de::DeserializeOwned};

use crate::error::ConfigError;

/// Read and parse a TOML configuration file.
pub(crate) fn read<T: DeserializeOwned>(path: &Path) -> Result<T, ConfigError> {
  let content = fs::read_to_string(path)?;
  Ok(toml::from_str(&content).map_err(Box::new)?)
}

/// Write a configuration value as pretty TOML, creating parent directories if
/// necessary.
pub(crate) fn write<T: Serialize>(value: &T, path: &Path) -> Result<(), ConfigError> {
  if let Some(parent) = path.parent() {
    fs::create_dir_all(parent)?;
  }
  fs::write(path, toml::to_string_pretty(value)?)?;
  Ok(())
}
