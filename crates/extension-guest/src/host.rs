//! Safe wrappers over the `lycoris` host-module imports (extension system
//! design, section 5.2; http in llm-provider design, section 4).
//!
//! The extern declarations exist only for `wasm32`; on every other target
//! the wrappers are stubs returning an error, so guest crates compile and
//! their pure logic stays testable on the host.

use serde_json::Value;

/// The `lycoris` host module, exactly as the engine links it.
#[cfg(target_arch = "wasm32")]
#[allow(unsafe_code)] // Extern declarations of the host imports.
mod ffi {
  #[link(wasm_import_module = "lycoris")]
  unsafe extern "C" {
    /// `log(level: i32, ptr: i32, len: i32)` — forwarded to `tracing`.
    pub fn log(level: i32, ptr: i32, len: i32);
    /// `http(ptr: i32, len: i32) -> i64` — outbound HTTP; the request is the
    /// section 4 JSON document and the response address packs as
    /// `(ptr << 32) | len`.
    pub fn http(ptr: i32, len: i32) -> i64;
  }
}

/// Log levels for [`log`], mirroring the host's tracing mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
  /// `tracing::trace`.
  Trace = 0,
  /// `tracing::debug`.
  Debug = 1,
  /// `tracing::info`.
  Info  = 2,
  /// `tracing::warn`.
  Warn  = 3,
  /// `tracing::error`.
  Error = 4,
}

/// Forward `message` to the host's tracing at `level`. Fire-and-forget: the
/// host accepts every level (out-of-range falls back to info).
///
/// Off `wasm32` this is a stub returning an error.
pub fn log(level: LogLevel, message: &str) -> Result<(), String> {
  log_impl(level, message)
}

#[cfg(target_arch = "wasm32")]
#[allow(unsafe_code)] // Raw host-import call; invariants at the call site.
fn log_impl(level: LogLevel, message: &str) -> Result<(), String> {
  let len = i32::try_from(message.len())
    .map_err(|_| "log message exceeds the guest addressable size".to_string())?;
  // SAFETY: `message` outlives the call, `len` counts exactly its bytes, and
  // the host reads them synchronously before returning.
  unsafe { ffi::log(level as i32, message.as_ptr() as i32, len) };
  Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn log_impl(_level: LogLevel, _message: &str) -> Result<(), String> {
  Err("the lycoris.log host import is only available inside the wasm engine".to_string())
}

/// Execute an outbound HTTP request through the host. `request` is the
/// section 4 request document (`{method, url, headers, body?}`); the
/// response is the section 4 response document (`{status, headers, body}` —
/// or `{"error": {...}}` for protocol-level failures, which the caller must
/// inspect).
///
/// Off `wasm32` this is a stub returning an error.
pub fn http(request: &Value) -> Result<Value, String> {
  http_impl(request)
}

#[cfg(target_arch = "wasm32")]
#[allow(unsafe_code)] // Raw host-import call and response read; invariants at the call sites.
fn http_impl(request: &Value) -> Result<Value, String> {
  let request = serde_json::to_vec(request)
    .map_err(|err| format!("failed to encode the http request document: {err}"))?;
  let len = i32::try_from(request.len())
    .map_err(|_| "http request document exceeds the guest addressable size".to_string())?;
  // SAFETY: `request` outlives the call, `len` counts exactly its bytes, and
  // the host copies them out before doing any work.
  let packed = unsafe { ffi::http(request.as_ptr() as i32, len) };
  let (ptr, len) = crate::shim::unpack(packed);
  // SAFETY: the host allocated the response buffer through this instance's
  // own `lycoris_alloc` and wrote exactly `len` bytes before returning.
  let body = unsafe { crate::shim::read(ptr, len) };
  serde_json::from_slice(&body).map_err(|err| format!("http response is not valid JSON: {err}"))
}

#[cfg(not(target_arch = "wasm32"))]
fn http_impl(_request: &Value) -> Result<Value, String> {
  Err("the lycoris.http host import is only available inside the wasm engine".to_string())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn log_levels_match_the_host_mapping() {
    // The host maps 0..=4 to trace..error; keep the numbers pinned.
    assert_eq!(LogLevel::Trace as i32, 0);
    assert_eq!(LogLevel::Debug as i32, 1);
    assert_eq!(LogLevel::Info as i32, 2);
    assert_eq!(LogLevel::Warn as i32, 3);
    assert_eq!(LogLevel::Error as i32, 4);
  }

  #[cfg(not(target_arch = "wasm32"))]
  #[test]
  fn host_functions_are_error_stubs_off_wasm() {
    assert!(
      log(LogLevel::Info, "hello")
        .unwrap_err()
        .contains("only available inside the wasm")
    );
    assert!(
      http(&serde_json::json!({"method": "GET", "url": "http://localhost/", "headers": {}}))
        .unwrap_err()
        .contains("only available inside the wasm")
    );
  }
}
