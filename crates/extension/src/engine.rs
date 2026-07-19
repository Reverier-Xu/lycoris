//! Common engine contract shared by the WASM and Lua execution engines
//! (design document section 5.1).
//!
//! Invocation payloads are JSON bytes end-to-end: proto bytes -> engine ->
//! guest -> engine -> proto bytes. Engines validate that guest output is
//! well-formed JSON before returning it.

use std::{fmt, str::FromStr, time::Duration};

use async_trait::async_trait;

use crate::{
  error::{ExtensionError, Result},
  package::ExtensionPackage,
};

/// The execution engine backing an extension package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineKind {
  /// Core WASM module executed by wasmtime (`lycoris-abi-v1`).
  Wasm,
  /// Embedded Lua 5.4 script executed by mlua.
  Lua,
}

impl EngineKind {
  /// The canonical wire representation used in records.
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
  type Err = ExtensionError;

  fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
    match s {
      "wasm" => Ok(Self::Wasm),
      "lua" => Ok(Self::Lua),
      other => Err(ExtensionError::Manifest(format!(
        "unknown engine: {other:?}"
      ))),
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

/// An execution engine: loads [`ExtensionPackage`]s into runnable instances.
#[async_trait]
pub trait ExtensionEngine: Send + Sync {
  /// The engine kind this loader handles.
  fn kind(&self) -> EngineKind;

  /// Load a package into an instance, verifying the content hash, the
  /// package engine kind and all engine-specific shape (ABI exports for
  /// WASM, entry function presence for Lua).
  ///
  /// After instantiation the engine itself delivers `settings` to the guest
  /// by invoking the `configure` method (llm-provider design section 5);
  /// only a successfully configured — or configure-less — instance is handed
  /// back. See [`configure_instance`] for the compatibility policy.
  async fn load(
    &self, package: &ExtensionPackage, settings: serde_json::Value,
  ) -> Result<Box<dyn ExtensionInstance>>;
}

/// A loaded extension ready to serve invocations.
#[async_trait]
pub trait ExtensionInstance: Send + Sync {
  /// Invoke the extension's entry point. `payload` is JSON; the return value is
  /// JSON.
  async fn invoke(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>>;
}

/// Method name carrying the resolved per-node settings (llm-provider design
/// section 5).
pub const CONFIGURE_METHOD: &str = "configure";

/// Deliver the resolved settings to a freshly instantiated guest by invoking
/// its `configure` method, and classify the outcome:
///
/// - success — the guest accepted the settings (the response payload is not
///   interpreted: a guest rejecting settings raises instead of returning an
///   error document, keeping "success" unambiguous);
/// - a *method not found* class failure — the guest predates the configure
///   convention (a Lua dispatch `error("unknown method: ...")`, or a wasm guest
///   shim reporting the same through its error channel); this is not a failure:
///   the instance simply runs without settings, so it is logged at debug level
///   and load proceeds;
/// - any other failure — the guest has `configure` and it failed: the load
///   fails, so a misconfigured extension never serves traffic.
///
/// Detection is a substring match on the error message — the simplest
/// workable convention until the ABI grows a typed "no such method" signal.
pub(crate) async fn configure_instance(
  instance: &dyn ExtensionInstance, extension_id: &str, settings: &serde_json::Value,
) -> Result<()> {
  let payload = serde_json::to_vec(settings).map_err(|err| {
    ExtensionError::InvalidPayload(format!("failed to encode the configure payload: {err}"))
  })?;
  match instance.invoke(CONFIGURE_METHOD, &payload).await {
    Ok(_) => Ok(()),
    Err(error) if is_method_not_found(&error) => {
      tracing::debug!(extension = %extension_id, %error, "guest does not implement configure; continuing without settings delivery");
      Ok(())
    }
    Err(error) => Err(error),
  }
}

/// Classify an invoke failure as the guest not implementing the method.
fn is_method_not_found(error: &ExtensionError) -> bool {
  let message = match error {
    ExtensionError::Script(message) | ExtensionError::GuestTrap(message) => message,
    _ => return false,
  };
  message.to_ascii_lowercase().contains("unknown method")
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn method_not_found_matches_the_guest_error_channels() {
    for error in [
      ExtensionError::Script("runtime error: unknown method: configure".to_string()),
      ExtensionError::GuestTrap("guest failed: Unknown Method configure".to_string()),
    ] {
      assert!(
        is_method_not_found(&error),
        "expected tolerance for {error}"
      );
    }
  }

  #[test]
  fn method_not_found_rejects_real_failures() {
    for error in [
      ExtensionError::Script("runtime error: bad settings".to_string()),
      ExtensionError::GuestTrap("wasm `unreachable` instruction executed".to_string()),
      ExtensionError::Timeout(Duration::from_secs(1)),
      ExtensionError::Engine("failed to prepare lua state".to_string()),
    ] {
      assert!(
        !is_method_not_found(&error),
        "expected a failure for {error}"
      );
    }
  }
}
