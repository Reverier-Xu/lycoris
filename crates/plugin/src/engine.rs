//! Common engine contract shared by the WASM and Lua execution engines
//! (design document section 5.1).
//!
//! Invocation payloads are JSON bytes end-to-end: proto bytes -> engine ->
//! guest -> engine -> proto bytes. Engines validate that guest output is
//! well-formed JSON before returning it.

use std::{fmt, str::FromStr, time::Duration};

use async_trait::async_trait;

use crate::{
  error::{PluginError, Result},
  package::PluginPackage,
};

/// The execution engine backing a plugin package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineKind {
  /// Core WASM module executed by wasmtime (`lycoris-abi-v1`).
  Wasm,
  /// Embedded Lua 5.4 script executed by mlua.
  Lua,
}

impl EngineKind {
  /// The canonical wire representation used in manifests and records.
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Wasm => "wasm",
      Self::Lua => "lua",
    }
  }
}

impl fmt::Display for EngineKind {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

impl FromStr for EngineKind {
  type Err = PluginError;

  fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
    match s {
      "wasm" => Ok(Self::Wasm),
      "lua" => Ok(Self::Lua),
      other => Err(PluginError::Manifest(format!("unknown engine: {other:?}"))),
    }
  }
}

/// Node-local engine limits (design document section 9).
#[derive(Debug, Clone, Copy)]
pub struct EngineLimits {
  /// Fuel a WASM guest may consume per invocation (deterministic timeout).
  pub wasm_fuel_per_call: u64,
  /// Maximum linear memory a WASM guest may reach, in bytes.
  pub wasm_max_memory_bytes: usize,
  /// VM instructions a Lua script may execute per invocation.
  pub lua_instructions_per_call: u64,
  /// Maximum memory a Lua state may allocate, in bytes.
  pub lua_max_memory_bytes: usize,
  /// Wall-clock deadline for a single invocation.
  pub invoke_timeout: Duration,
}

impl Default for EngineLimits {
  fn default() -> Self {
    Self {
      wasm_fuel_per_call: 5_000_000,
      wasm_max_memory_bytes: 64 * 1024 * 1024,
      lua_instructions_per_call: 1_000_000,
      lua_max_memory_bytes: 32 * 1024 * 1024,
      invoke_timeout: Duration::from_millis(10_000),
    }
  }
}

/// An execution engine: loads [`PluginPackage`]s into runnable instances.
#[async_trait]
pub trait PluginEngine: Send + Sync {
  /// The engine kind this loader handles.
  fn kind(&self) -> EngineKind;

  /// Load a package into an instance, verifying the content hash, the
  /// manifest engine kind and all engine-specific shape (ABI exports for
  /// WASM, entry function presence for Lua).
  async fn load(&self, package: &PluginPackage) -> Result<Box<dyn PluginInstance>>;
}

/// A loaded plugin ready to serve invocations.
#[async_trait]
pub trait PluginInstance: Send + Sync {
  /// Invoke the plugin's entry point. `payload` is JSON; the return value is
  /// JSON.
  async fn invoke(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>>;
}
