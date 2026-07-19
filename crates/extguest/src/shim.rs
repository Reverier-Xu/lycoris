//! The shims between the raw `lycoris-abi-v1` exports and a typed guest
//! handler. Everything here is pure and host-testable except [`run`] and
//! [`read`], which touch linear memory and exist only for `wasm32`.

use serde_json::Value;

/// Bump-allocate `len` zeroed bytes in linear memory and return the start
/// pointer as the host sees it.
///
/// The allocation is a leaked `Box<[u8]>`: bytes are never freed and the
/// heap frontier only moves forward, so an instance's linear memory grows
/// monotonically across invocations until it hits the engine's memory cap
/// (`wasm_max_memory_bytes`), where allocation failure traps. v1 accepts
/// this: instances are loaded per extension and the cap bounds the growth,
/// which is the same trade-off the ABI's test fixtures make. A negative
/// `len` (never produced by the engine) allocates nothing.
pub fn alloc(len: i32) -> i32 {
  let len = usize::try_from(len).unwrap_or(0);
  let ptr = alloc_raw(len);
  // On wasm32 a raw pointer is the linear-memory offset, which is 32-bit by
  // definition; the engine mirrors this wrap (`ptr as u32`) when unpacking.
  ptr as i32
}

/// The `usize`-typed core of [`alloc`], split out so host tests can inspect
/// the buffer through a real pointer (host addresses do not fit the ABI's
/// `i32`).
fn alloc_raw(len: usize) -> *mut u8 {
  Box::leak(vec![0u8; len].into_boxed_slice()).as_mut_ptr()
}

/// Pack a buffer address as `(ptr << 32) | len`, the `lycoris-abi-v1`
/// response convention.
pub fn pack(ptr: i32, len: i32) -> i64 {
  (i64::from(ptr as u32) << 32) | i64::from(len as u32)
}

/// Split a packed `(ptr << 32) | len` address back into pointer and length.
pub fn unpack(packed: i64) -> (i32, i32) {
  let packed = packed as u64;
  let ptr = (packed >> 32) as u32 as i32;
  let len = (packed & 0xFFFF_FFFF) as u32 as i32;
  (ptr, len)
}

/// Run the typed handler over raw ABI buffers: decode the method name and
/// the JSON payload, and encode the handler's JSON response back to bytes.
/// All failures are structured error strings; the caller (the generated
/// export) turns them into a log line plus a trap.
pub fn dispatch(
  handler: &dyn Fn(&str, Value) -> Result<Value, String>, method: &[u8], payload: &[u8],
) -> Result<Vec<u8>, String> {
  let method =
    std::str::from_utf8(method).map_err(|err| format!("method name is not valid UTF-8: {err}"))?;
  let payload: Value = serde_json::from_slice(payload)
    .map_err(|err| format!("request payload is not valid JSON: {err}"))?;
  let response = handler(method, payload)?;
  serde_json::to_vec(&response)
    .map_err(|err| format!("failed to encode the response payload: {err}"))
}

/// The body of the generated `lycoris_invoke` export: reads the method and
/// payload out of linear memory, dispatches to `handler`, and returns the
/// response buffer packed as `(ptr << 32) | len`.
///
/// A handler failure is logged through the `log` host import and then traps
/// (`unreachable`): the ABI has no typed error channel, and the engine maps
/// traps to `ExtensionError::GuestTrap`, which is what makes a failing
/// `configure` fail the load (llm-provider design, section 5). The log line
/// carries the real message; the trap itself is the generic wasm one.
///
/// # Safety
///
/// `method_ptr..method_ptr + method_len` and `payload_ptr..payload_ptr +
/// payload_len` must be readable in this instance's linear memory. The
/// engine guarantees it: it allocated both buffers through `lycoris_alloc`
/// and wrote the bytes itself before calling.
#[cfg(target_arch = "wasm32")]
#[allow(unsafe_code)] // ABI boundary: raw linear-memory reads, as contracted above.
pub unsafe fn run(
  method_ptr: i32, method_len: i32, payload_ptr: i32, payload_len: i32,
  handler: &dyn Fn(&str, Value) -> Result<Value, String>,
) -> i64 {
  // SAFETY: upheld by the caller; see the function-level contract.
  let method = unsafe { read(method_ptr, method_len) };
  // SAFETY: upheld by the caller; see the function-level contract.
  let payload = unsafe { read(payload_ptr, payload_len) };
  match dispatch(handler, &method, &payload) {
    Ok(response) => {
      let len = i32::try_from(response.len()).unwrap_or(i32::MAX);
      let ptr = Box::leak(response.into_boxed_slice()).as_mut_ptr();
      pack(ptr as i32, len)
    }
    Err(message) => {
      let _ = crate::host::log(crate::host::LogLevel::Error, &message);
      // A trap is the ABI's only error signal; the message reached the host
      // through the log import above.
      core::arch::wasm32::unreachable()
    }
  }
}

/// Copy a buffer out of linear memory.
///
/// # Safety
///
/// `ptr..ptr + len` must be readable in this instance's linear memory.
#[cfg(target_arch = "wasm32")]
#[allow(unsafe_code)] // The whole function is the raw-memory read it documents.
pub(crate) unsafe fn read(ptr: i32, len: i32) -> Vec<u8> {
  let ptr = usize::try_from(ptr).unwrap_or(0) as *const u8;
  let len = usize::try_from(len).unwrap_or(0);
  // SAFETY: upheld by the caller; see the function-level contract.
  unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
}

#[cfg(test)]
#[allow(unsafe_code)] // Tests reconstruct the leaked buffers they allocated.
mod tests {
  use super::*;

  fn ok_handler(method: &str, payload: Value) -> Result<Value, String> {
    Ok(serde_json::json!({"method": method, "payload": payload}))
  }

  #[test]
  fn alloc_returns_zeroed_buffers_of_the_requested_size() {
    let ptr = alloc_raw(8);
    assert!(!ptr.is_null());
    // SAFETY: the buffer was just allocated above with exactly 8 bytes.
    let bytes = unsafe { std::slice::from_raw_parts(ptr, 8) };
    assert_eq!(bytes, &[0u8; 8]);
    // Distinct allocations never alias.
    assert_ne!(ptr, alloc_raw(8));
  }

  #[test]
  fn pack_unpack_round_trips() {
    for (ptr, len) in [(0, 0), (1024, 33), (i32::MAX, i32::MAX), (-1, -1)] {
      assert_eq!(unpack(pack(ptr, len)), (ptr, len));
    }
  }

  #[test]
  fn dispatch_runs_the_handler_over_decoded_json() {
    let out = dispatch(&ok_handler, b"echo", br#"{"a":1}"#).unwrap();
    assert_eq!(
      serde_json::from_slice::<Value>(&out).unwrap(),
      serde_json::json!({"method": "echo", "payload": {"a": 1}})
    );
  }

  #[test]
  fn dispatch_propagates_handler_errors() {
    let result = dispatch(&|_, _| Err("boom".to_string()), b"m", b"{}");
    assert_eq!(result, Err("boom".to_string()));
  }

  #[test]
  fn dispatch_rejects_bad_method_and_payload_bytes() {
    let result = dispatch(&ok_handler, &[0xFF, 0xFE], b"{}");
    assert!(result.unwrap_err().contains("UTF-8"));
    let result = dispatch(&ok_handler, b"m", b"not json");
    assert!(result.unwrap_err().contains("not valid JSON"));
  }
}
