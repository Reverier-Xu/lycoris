//! WASM execution engine: ABI `lycoris-abi-v1` (design document section
//! 5.2).
//!
//! Core wasm on wasmtime with WASI disabled: the linker exposes only the
//! `lycoris` host module with a `log(level, ptr, len)` import forwarded to
//! `tracing` and — only when the manifest declares the `http` capability —
//! an `http(ptr, len) -> i64` import executing outbound HTTP requests
//! (llm-provider design section 4). Guests must export `memory`,
//! `lycoris_alloc(len: i32) -> i32` and
//! `lycoris_invoke(method_ptr, method_len, payload_ptr, payload_len) -> i64`;
//! the invoke return value packs the response buffer as `(ptr << 32) | len`
//! in guest memory.
//!
//! Enforcement: fuel per call gives a deterministic execution bound,
//! [`StoreLimits`] caps linear memory, and every invocation is wrapped in a
//! wall-clock deadline. A guest exceeding any limit traps, and the trap
//! surfaces as [`ExtensionError::GuestTrap`], never as a host panic.

use std::{
  sync::{Arc, RwLock},
  time::Duration,
};

use async_trait::async_trait;
use tokio::sync::Mutex;
use wasmtime::{
  Caller, Config, Engine, ExternType, FuncType, Linker, Memory, Module, Store, StoreLimits,
  StoreLimitsBuilder, TypedFunc, ValType,
};

use crate::{
  engine::{EngineKind, EngineLimits, ExtensionEngine, ExtensionInstance},
  error::{ExtensionError, Result},
  http,
  package::ExtensionPackage,
};

/// Host module name exposed to guests.
const HOST_MODULE: &str = "lycoris";
/// Name of the exported guest allocator.
const ALLOC_EXPORT: &str = "lycoris_alloc";
/// Name of the exported guest entry point.
const INVOKE_EXPORT: &str = "lycoris_invoke";
/// Name of the exported guest linear memory.
const MEMORY_EXPORT: &str = "memory";
/// Name of the log host import.
const LOG_IMPORT: &str = "log";
/// Name of the http host import (linked only with the `http` capability).
const HTTP_IMPORT: &str = "http";

/// Per-instance store state: the resource limits, the extension id for log
/// attribution, and the http egress policy.
struct WasmState {
  limits: StoreLimits,
  extension_id: String,
  /// Host allowlist for the http import; `None` allows every host. Shared
  /// with the instance so settings applied at configure time take effect on
  /// the running guest (llm-provider design sections 4 and 5).
  http_allow_hosts: Arc<RwLock<Option<Vec<String>>>>,
}

/// The WASM execution engine.
pub struct WasmEngine {
  engine: Engine,
  limits: EngineLimits,
  /// Shared HTTP client for the `lycoris.http` import (one connection pool
  /// per engine).
  http: ureq::Agent,
}

impl WasmEngine {
  /// Create an engine enforcing `limits` on every instance it loads.
  pub fn new(limits: EngineLimits) -> Result<Self> {
    let mut config = Config::new();
    // Async support is built in since wasmtime 43 (`Config::async_support` is
    // a deprecated no-op); fuel metering still needs opting into.
    config.consume_fuel(true);
    let engine = Engine::new(&config)
      .map_err(|err| ExtensionError::Engine(format!("failed to create wasmtime engine: {err}")))?;
    Ok(Self {
      engine,
      limits,
      http: http::agent(),
    })
  }

  /// Build the linker exposing exactly the `lycoris` host module. The `http`
  /// import is linked only when `http_capability` is set, so a guest
  /// importing it without declaring the capability fails to instantiate
  /// (llm-provider design section 4).
  fn linker(&self, http_capability: bool) -> Result<Linker<WasmState>> {
    let mut linker = Linker::new(&self.engine);
    linker
      .func_wrap(
        HOST_MODULE,
        LOG_IMPORT,
        |mut caller: Caller<'_, WasmState>, level: i32, ptr: i32, len: i32| {
          let extension_id = caller.data().extension_id.clone();
          let message = read_guest_string(&mut caller, ptr, len)
            .unwrap_or_else(|| "<out-of-bounds log message>".to_string());
          match level {
            0 => tracing::trace!(extension = extension_id, "{message}"),
            1 => tracing::debug!(extension = extension_id, "{message}"),
            3 => tracing::warn!(extension = extension_id, "{message}"),
            4 => tracing::error!(extension = extension_id, "{message}"),
            // 2 is info; out-of-range levels fall back to info as well.
            _ => tracing::info!(extension = extension_id, "{message}"),
          }
        },
      )
      .map_err(|err| {
        ExtensionError::Engine(format!("failed to register the lycoris host module: {err}"))
      })?;
    if http_capability {
      let agent = self.http.clone();
      linker
        .func_wrap_async(
          HOST_MODULE,
          HTTP_IMPORT,
          move |mut caller: Caller<'_, WasmState>, (ptr, len): (i32, i32)| {
            let agent = agent.clone();
            Box::new(async move { run_http_import(&mut caller, &agent, ptr, len).await })
          },
        )
        .map_err(|err| {
          ExtensionError::Engine(format!("failed to register the lycoris http import: {err}"))
        })?;
    }
    Ok(linker)
  }
}

#[async_trait]
impl ExtensionEngine for WasmEngine {
  fn kind(&self) -> EngineKind {
    EngineKind::Wasm
  }

  async fn load(&self, package: &ExtensionPackage) -> Result<Box<dyn ExtensionInstance>> {
    Ok(Box::new(self.load_instance(package).await?))
  }
}

impl WasmEngine {
  /// Load a package into a runnable instance: verify the content hash, the
  /// package engine kind and the ABI shape, then instantiate. Split from the
  /// trait method so tests can reach instance internals (the http egress
  /// policy) without a downcast.
  async fn load_instance(&self, package: &ExtensionPackage) -> Result<WasmInstance> {
    package.verify()?;
    if package.engine != EngineKind::Wasm {
      return Err(ExtensionError::Engine(format!(
        "extension {} targets {:?}, not the wasm engine",
        package.id, package.engine
      )));
    }

    // The http host import is linked only when the manifest declares the
    // capability (llm-provider design section 4).
    let http_capability = package.manifest.capabilities.iter().any(|c| c == "http");

    let module = Module::new(&self.engine, &package.artifact).map_err(|err| {
      ExtensionError::Engine(format!("failed to compile the wasm module: {err:#}"))
    })?;
    check_imports(&module, http_capability)?;
    check_exports(&module)?;

    let http_allow_hosts = Arc::new(RwLock::new(None));
    let state = WasmState {
      limits: StoreLimitsBuilder::new()
        .memory_size(self.limits.wasm_max_memory_bytes)
        .build(),
      extension_id: package.id.clone(),
      http_allow_hosts: Arc::clone(&http_allow_hosts),
    };
    let mut store = Store::new(&self.engine, state);
    store.limiter(|state| &mut state.limits);

    let instance = self
      .linker(http_capability)?
      .instantiate_async(&mut store, &module)
      .await
      .map_err(|err| {
        ExtensionError::GuestTrap(format!("failed to instantiate the guest: {err}"))
      })?;

    // Precise signature checks; the name/kind precheck above keeps the
    // common failure messages readable.
    let memory = instance
      .get_memory(&mut store, MEMORY_EXPORT)
      .ok_or_else(|| ExtensionError::Engine("guest exports no memory".to_string()))?;
    let alloc = instance
      .get_typed_func::<i32, i32>(&mut store, ALLOC_EXPORT)
      .map_err(|err| ExtensionError::Engine(format!("invalid lycoris_alloc signature: {err}")))?;
    let invoke = instance
      .get_typed_func::<(i32, i32, i32, i32), i64>(&mut store, INVOKE_EXPORT)
      .map_err(|err| ExtensionError::Engine(format!("invalid lycoris_invoke signature: {err}")))?;

    Ok(WasmInstance {
      store: Mutex::new(store),
      memory,
      alloc,
      invoke,
      fuel: self.limits.wasm_fuel_per_call,
      deadline: self.limits.invoke_timeout,
      http_allow_hosts,
    })
  }
}

/// A loaded WASM extension: one store holding one instantiated guest.
struct WasmInstance {
  store: Mutex<Store<WasmState>>,
  memory: Memory,
  alloc: TypedFunc<i32, i32>,
  invoke: TypedFunc<(i32, i32, i32, i32), i64>,
  fuel: u64,
  deadline: Duration,
  /// Egress policy shared with the store state; `None` allows every host.
  /// Read by the configure path (settings injection) and by engine tests.
  #[allow(dead_code)]
  http_allow_hosts: Arc<RwLock<Option<Vec<String>>>>,
}

#[async_trait]
impl ExtensionInstance for WasmInstance {
  async fn invoke(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>> {
    // The payload contract is JSON bytes end-to-end; reject junk early.
    serde_json::from_slice::<serde_json::Value>(payload).map_err(|err| {
      ExtensionError::InvalidPayload(format!("request payload is not valid JSON: {err}"))
    })?;

    let result = tokio::time::timeout(self.deadline, self.invoke_inner(method, payload)).await;
    match result {
      Ok(output) => output,
      Err(_) => Err(ExtensionError::Timeout(self.deadline)),
    }
  }
}

impl WasmInstance {
  /// Run one guest invocation: alloc, write request, call, read response.
  async fn invoke_inner(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>> {
    let method_len = i32::try_from(method.len()).map_err(|_| {
      ExtensionError::InvalidPayload("method name exceeds the guest addressable size".to_string())
    })?;
    let payload_len = i32::try_from(payload.len()).map_err(|_| {
      ExtensionError::InvalidPayload("payload exceeds the guest addressable size".to_string())
    })?;

    let mut store = self.store.lock().await;
    store
      .set_fuel(self.fuel)
      .map_err(|err| ExtensionError::Engine(format!("failed to set fuel: {err}")))?;

    let method_ptr = self
      .alloc
      .call_async(&mut *store, method_len)
      .await
      .map_err(guest_trap("allocating the method buffer"))?;
    self
      .memory
      .write(&mut *store, method_ptr as usize, method.as_bytes())
      .map_err(guest_trap("writing the method buffer"))?;

    let payload_ptr = self
      .alloc
      .call_async(&mut *store, payload_len)
      .await
      .map_err(guest_trap("allocating the payload buffer"))?;
    self
      .memory
      .write(&mut *store, payload_ptr as usize, payload)
      .map_err(guest_trap("writing the payload buffer"))?;

    let packed = self
      .invoke
      .call_async(
        &mut *store,
        (method_ptr, method_len, payload_ptr, payload_len),
      )
      .await
      .map_err(guest_trap("running the guest"))?;

    // The response buffer address packs as (ptr << 32) | len.
    let packed = packed as u64;
    let response_ptr = usize::try_from(packed >> 32)
      .map_err(|err| ExtensionError::Engine(format!("invalid response pointer: {err}")))?;
    let response_len = usize::try_from(packed & 0xFFFF_FFFF)
      .map_err(|err| ExtensionError::Engine(format!("invalid response length: {err}")))?;
    let data = self.memory.data(&*store);
    let response = data
      .get(response_ptr..response_ptr.saturating_add(response_len))
      .ok_or_else(|| {
        ExtensionError::Engine(
          "guest returned an out-of-bounds response buffer pointer".to_string(),
        )
      })?
      .to_vec();

    // Engines validate that guest output is well-formed JSON.
    serde_json::from_slice::<serde_json::Value>(&response).map_err(|err| {
      ExtensionError::InvalidPayload(format!("guest output is not valid JSON: {err}"))
    })?;
    Ok(response)
  }
}

/// Read a guest string for the `log` host import; `None` when the guest
/// hands the host an out-of-bounds slice.
fn read_guest_string(caller: &mut Caller<'_, WasmState>, ptr: i32, len: i32) -> Option<String> {
  let bytes = read_guest_bytes(caller, ptr, len)?;
  Some(String::from_utf8_lossy(&bytes).into_owned())
}

/// Read a slice out of guest linear memory; `None` when the guest hands the
/// host an out-of-bounds slice.
fn read_guest_bytes(caller: &mut Caller<'_, WasmState>, ptr: i32, len: i32) -> Option<Vec<u8>> {
  let memory = caller.get_export(MEMORY_EXPORT)?.into_memory()?;
  let data = memory.data(&caller);
  let start = usize::try_from(ptr).ok()?;
  let len = usize::try_from(len).ok()?;
  Some(data.get(start..start.checked_add(len)?)?.to_vec())
}

/// The `lycoris.http` host import: read the request document out of guest
/// memory, execute it (blocking, on `spawn_blocking`), and hand the response
/// document back as a guest-allocated buffer packed as `(ptr << 32) | len`.
///
/// Protocol-level failures (bad documents, disallowed hosts, transport
/// errors, over-limit responses) are encoded as structured error documents
/// by [`http::execute`]; only failures of the ABI machinery itself — the
/// guest allocator or an out-of-bounds response write — trap.
async fn run_http_import(
  caller: &mut Caller<'_, WasmState>, agent: &ureq::Agent, ptr: i32, len: i32,
) -> wasmtime::Result<i64> {
  let request = read_guest_bytes(caller, ptr, len)
    .unwrap_or_else(|| b"{\"error\":\"out-of-bounds request buffer\"}".to_vec());
  let allow_hosts = caller
    .data()
    .http_allow_hosts
    .read()
    .map(|guard| guard.clone())
    .unwrap_or(None);
  let agent = agent.clone();
  let response = tokio::task::spawn_blocking(move || http::execute(&agent, &request, &allow_hosts))
    .await
    .unwrap_or_else(|err| {
      // A panicked blocking task is a host failure, but the guest still gets
      // a document rather than a trap.
      serde_json::to_vec(&serde_json::json!({
        "error": { "type": "internal", "message": format!("http task failed: {err}") }
      }))
      .unwrap_or_default()
    });

  // Place the response in guest memory through the guest allocator, the same
  // packing convention as `lycoris_invoke` returns.
  let alloc = caller
    .get_export(ALLOC_EXPORT)
    .and_then(wasmtime::Extern::into_func)
    .ok_or_else(|| wasmtime::Error::msg("guest exports no lycoris_alloc"))?
    .typed::<i32, i32>(&caller)
    .map_err(|err| wasmtime::Error::msg(format!("invalid lycoris_alloc signature: {err}")))?;
  let response_len = i32::try_from(response.len())
    .map_err(|_| wasmtime::Error::msg("http response exceeds the guest addressable size"))?;
  let response_ptr = alloc.call_async(&mut *caller, response_len).await?;
  let memory = caller
    .get_export(MEMORY_EXPORT)
    .and_then(wasmtime::Extern::into_memory)
    .ok_or_else(|| wasmtime::Error::msg("guest exports no memory"))?;
  memory.write(&mut *caller, response_ptr as usize, &response)?;

  let packed = (u64::from(response_ptr as u32) << 32) | u64::from(response_len as u32);
  #[allow(clippy::cast_possible_wrap)]
  Ok(packed as i64)
}

/// Validate that the module imports nothing beyond the `lycoris` host module
/// — WASI is disabled, so any other import is unsupported. `lycoris.http`
/// requires the manifest-declared http capability and the ABI signature
/// `(i32, i32) -> i64`.
fn check_imports(module: &Module, http_capability: bool) -> Result<()> {
  for import in module.imports() {
    if import.module() != HOST_MODULE {
      return Err(ExtensionError::Engine(format!(
        "unsupported import {}.{}: only the lycoris host module is provided",
        import.module(),
        import.name()
      )));
    }
    match import.name() {
      name if name == LOG_IMPORT => match import.ty() {
        ExternType::Func(ty)
          if signature_matches(&ty, &[ValType::I32, ValType::I32, ValType::I32], &[]) => {}
        _ => {
          return Err(ExtensionError::Engine(
            "lycoris.log must have signature (i32, i32, i32) -> ()".to_string(),
          ));
        }
      },
      name if name == HTTP_IMPORT && http_capability => match import.ty() {
        ExternType::Func(ty)
          if signature_matches(&ty, &[ValType::I32, ValType::I32], &[ValType::I64]) => {}
        _ => {
          return Err(ExtensionError::Engine(
            "lycoris.http must have signature (i32, i32) -> i64".to_string(),
          ));
        }
      },
      name if name == HTTP_IMPORT => {
        return Err(ExtensionError::Engine(
          "guest imports lycoris.http but the manifest does not declare the http capability"
            .to_string(),
        ));
      }
      name => {
        return Err(ExtensionError::Engine(format!(
          "unsupported import {HOST_MODULE}.{name}: only {HOST_MODULE}.{LOG_IMPORT} and \
           {HOST_MODULE}.{HTTP_IMPORT} are provided"
        )));
      }
    }
  }
  Ok(())
}

/// Validate the ABI exports by name and kind before instantiation.
fn check_exports(module: &Module) -> Result<()> {
  if !matches!(
    module.get_export(MEMORY_EXPORT),
    Some(ExternType::Memory(_))
  ) {
    return Err(ExtensionError::Engine(
      "guest does not export a memory named \"memory\"".to_string(),
    ));
  }
  check_func_export(module, ALLOC_EXPORT, &[ValType::I32], &[ValType::I32])?;
  check_func_export(
    module,
    INVOKE_EXPORT,
    &[ValType::I32, ValType::I32, ValType::I32, ValType::I32],
    &[ValType::I64],
  )?;
  Ok(())
}

/// Validate one exported function signature.
fn check_func_export(
  module: &Module, name: &str, params: &[ValType], results: &[ValType],
) -> Result<()> {
  match module.get_export(name) {
    Some(ExternType::Func(ty)) if signature_matches(&ty, params, results) => Ok(()),
    _ => Err(ExtensionError::Engine(format!(
      "guest does not export {name} with the lycoris-abi-v1 signature"
    ))),
  }
}

/// Compare a wasmtime function type against an expected signature. `ValType`
/// has no `PartialEq` (reference types follow a subtyping relation), so
/// equality goes through `ValType::eq`.
fn signature_matches(ty: &FuncType, params: &[ValType], results: &[ValType]) -> bool {
  let params_match = ty.params().len() == params.len()
    && ty
      .params()
      .zip(params.iter())
      .all(|(a, b)| ValType::eq(&a, b));
  let results_match = ty.results().len() == results.len()
    && ty
      .results()
      .zip(results.iter())
      .all(|(a, b)| ValType::eq(&a, b));
  params_match && results_match
}

/// Build a closure mapping guest failures to [`ExtensionError::GuestTrap`]. The
/// alternate Display keeps the whole anyhow chain so the trap reason (fuel
/// exhaustion, `unreachable`, ...) survives in the message.
fn guest_trap<E: std::fmt::Display>(context: &'static str) -> impl Fn(E) -> ExtensionError {
  move |err| ExtensionError::GuestTrap(format!("{context}: {err:#}"))
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use super::*;
  use crate::manifest::ExtensionManifest;

  /// Bump-allocator echo guest: returns the payload bytes unchanged and
  /// exercises the `lycoris.log` host import on every call.
  const ECHO_WAT: &str = r#"
    (module
      (import "lycoris" "log" (func $lycoris_log (param i32 i32 i32)))
      (memory (export "memory") 1)
      (data (i32.const 16) "wasm guest invoked")
      (global $heap (mut i32) (i32.const 1024))
      (func $alloc (export "lycoris_alloc") (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $heap))
        (global.set $heap (i32.add (local.get $ptr) (local.get $len)))
        (local.get $ptr))
      (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
        (call $lycoris_log (i32.const 2) (i32.const 16) (i32.const 18))
        (i64.or
          (i64.shl (i64.extend_i32_u (local.get $pp)) (i64.const 32))
          (i64.extend_i32_u (local.get $pl)))))
  "#;

  /// Guest answering with the JSON array `["<method>"]`.
  const METHOD_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $heap (mut i32) (i32.const 1024))
      (func $alloc (export "lycoris_alloc") (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $heap))
        (global.set $heap (i32.add (local.get $ptr) (local.get $len)))
        (local.get $ptr))
      (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
        (local $out i32)
        (local $total i32)
        (local.set $total (i32.add (local.get $ml) (i32.const 4)))
        (local.set $out (call $alloc (local.get $total)))
        (i32.store8 (local.get $out) (i32.const 91))
        (i32.store8 (i32.add (local.get $out) (i32.const 1)) (i32.const 34))
        (memory.copy (i32.add (local.get $out) (i32.const 2)) (local.get $mp) (local.get $ml))
        (i32.store8 (i32.add (i32.add (local.get $out) (i32.const 2)) (local.get $ml)) (i32.const 34))
        (i32.store8 (i32.add (i32.add (local.get $out) (i32.const 3)) (local.get $ml)) (i32.const 93))
        (i64.or
          (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
          (i64.extend_i32_u (local.get $total)))))
  "#;

  /// Guest that traps on invocation.
  const TRAP_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (func (export "lycoris_alloc") (param $len i32) (result i32)
        (i32.const 1024))
      (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
        (unreachable)))
  "#;

  /// Guest that spins forever, burning fuel.
  const LOOP_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (func (export "lycoris_alloc") (param $len i32) (result i32)
        (i32.const 1024))
      (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
        (loop (br 0))
        (unreachable)))
  "#;

  /// Guest whose invoke hands back a non-JSON response buffer.
  const NON_JSON_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (data (i32.const 16) "not json")
      (func (export "lycoris_alloc") (param $len i32) (result i32)
        (i32.const 1024))
      (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
        (i64.or (i64.shl (i64.const 16) (i64.const 32)) (i64.const 8))))
  "#;

  fn manifest() -> ExtensionManifest {
    ExtensionManifest::from_map(&BTreeMap::from([(
      "semver".to_string(),
      "0.1.0".to_string(),
    )]))
    .unwrap()
  }

  fn package_wat(wat_source: &str) -> ExtensionPackage {
    ExtensionPackage::new(
      "test".to_string(),
      "test-extension".to_string(),
      1,
      EngineKind::Wasm,
      String::new(),
      manifest(),
      wat::parse_str(wat_source).unwrap(),
    )
  }

  async fn load(wat_source: &str) -> Box<dyn ExtensionInstance> {
    load_with_limits(wat_source, EngineLimits::default()).await
  }

  async fn load_with_limits(wat_source: &str, limits: EngineLimits) -> Box<dyn ExtensionInstance> {
    WasmEngine::new(limits)
      .unwrap()
      .load(&package_wat(wat_source))
      .await
      .unwrap()
  }

  fn parse_output(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap()
  }

  #[tokio::test]
  async fn echo_round_trip_preserves_structured_json() {
    let instance = load(ECHO_WAT).await;
    let input = br#"{"nested":{"a":[1,2,3],"b":null},"s":"x","n":1.5}"#;
    let output = instance.invoke("anything", input).await.unwrap();
    assert_eq!(
      parse_output(&output),
      serde_json::from_slice::<serde_json::Value>(input).unwrap()
    );
  }

  #[tokio::test]
  async fn method_name_reaches_the_guest() {
    let instance = load(METHOD_WAT).await;
    let output = instance.invoke("skills.run", b"{}").await.unwrap();
    assert_eq!(parse_output(&output), serde_json::json!(["skills.run"]));
  }

  #[tokio::test]
  async fn guest_traps_map_to_the_guest_trap_variant() {
    let instance = load(TRAP_WAT).await;
    let result = instance.invoke("m", b"{}").await;
    assert!(matches!(result, Err(ExtensionError::GuestTrap(_))));
  }

  #[tokio::test]
  async fn infinite_loops_exhaust_fuel_and_trap() {
    let limits = EngineLimits {
      wasm_fuel_per_call: 100_000,
      ..EngineLimits::default()
    };
    let instance = load_with_limits(LOOP_WAT, limits).await;
    let result = instance.invoke("m", b"{}").await;
    let Err(ExtensionError::GuestTrap(message)) = result else {
      panic!("expected a guest trap, got {result:?}");
    };
    assert!(
      message.contains("fuel"),
      "unexpected trap message: {message}"
    );
  }

  #[tokio::test]
  async fn memory_beyond_the_limit_fails_instantiation() {
    let balloon = r#"
      (module
        (memory (export "memory") 100)
        (func (export "lycoris_alloc") (param $len i32) (result i32)
          (i32.const 1024))
        (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
          (i64.const 0)))
    "#;
    let limits = EngineLimits {
      wasm_max_memory_bytes: 256 * 1024,
      ..EngineLimits::default()
    };
    let result = WasmEngine::new(limits)
      .unwrap()
      .load(&package_wat(balloon))
      .await;
    assert!(matches!(result, Err(ExtensionError::GuestTrap(_))));
  }

  #[tokio::test]
  async fn missing_alloc_export_is_rejected_at_load() {
    let missing_alloc = r#"
      (module
        (memory (export "memory") 1)
        (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
          (i64.const 0)))
    "#;
    let result = WasmEngine::new(EngineLimits::default())
      .unwrap()
      .load(&package_wat(missing_alloc))
      .await;
    assert!(matches!(result, Err(ExtensionError::Engine(err)) if err.contains("lycoris_alloc")));
  }

  #[tokio::test]
  async fn missing_memory_export_is_rejected_at_load() {
    let missing_memory = r#"
      (module
        (func (export "lycoris_alloc") (param $len i32) (result i32)
          (i32.const 1024))
        (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
          (i64.const 0)))
    "#;
    let result = WasmEngine::new(EngineLimits::default())
      .unwrap()
      .load(&package_wat(missing_memory))
      .await;
    assert!(matches!(result, Err(ExtensionError::Engine(err)) if err.contains("memory")));
  }

  #[tokio::test]
  async fn wrong_invoke_signature_is_rejected_at_load() {
    let wrong_signature = r#"
      (module
        (memory (export "memory") 1)
        (func (export "lycoris_alloc") (param $len i32) (result i32)
          (i32.const 1024))
        (func (export "lycoris_invoke") (param $p i32) (result i32)
          (i32.const 0)))
    "#;
    let result = WasmEngine::new(EngineLimits::default())
      .unwrap()
      .load(&package_wat(wrong_signature))
      .await;
    assert!(matches!(result, Err(ExtensionError::Engine(_))));
  }

  #[tokio::test]
  async fn wasi_imports_are_rejected_at_load() {
    let wasi_module = r#"
      (module
        (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 1)
        (func (export "lycoris_alloc") (param $len i32) (result i32)
          (i32.const 1024))
        (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
          (i64.const 0)))
    "#;
    let result = WasmEngine::new(EngineLimits::default())
      .unwrap()
      .load(&package_wat(wasi_module))
      .await;
    assert!(
      matches!(result, Err(ExtensionError::Engine(err)) if err.contains("unsupported import"))
    );
  }

  #[tokio::test]
  async fn non_json_guest_output_is_an_invalid_payload() {
    let instance = load(NON_JSON_WAT).await;
    let result = instance.invoke("m", b"{}").await;
    assert!(matches!(result, Err(ExtensionError::InvalidPayload(_))));
  }

  #[tokio::test]
  async fn non_json_input_is_an_invalid_payload() {
    let instance = load(ECHO_WAT).await;
    let result = instance.invoke("m", b"not json").await;
    assert!(matches!(result, Err(ExtensionError::InvalidPayload(_))));
  }

  #[tokio::test]
  async fn load_rejects_a_wrong_engine_kind() {
    let mut package = package_wat(ECHO_WAT);
    package.engine = EngineKind::Lua;
    let result = WasmEngine::new(EngineLimits::default())
      .unwrap()
      .load(&package)
      .await;
    assert!(matches!(result, Err(ExtensionError::Engine(_))));
  }

  #[tokio::test]
  async fn load_rejects_a_content_hash_mismatch() {
    let mut package = package_wat(ECHO_WAT);
    package.content_hash = "0".repeat(64);
    let result = WasmEngine::new(EngineLimits::default())
      .unwrap()
      .load(&package)
      .await;
    assert!(matches!(
      result,
      Err(ExtensionError::ContentHashMismatch { .. })
    ));
  }

  /// HTTP forwarder guest: hands the invoke payload to `lycoris.http`
  /// verbatim and returns the host response document unchanged (pure memory
  /// plumbing, llm-provider design section 4).
  const HTTP_FORWARD_WAT: &str = r#"
    (module
      (import "lycoris" "http" (func $lycoris_http (param i32 i32) (result i64)))
      (memory (export "memory") 1)
      (global $heap (mut i32) (i32.const 1024))
      (func $alloc (export "lycoris_alloc") (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $heap))
        (global.set $heap (i32.add (local.get $ptr) (local.get $len)))
        (local.get $ptr))
      (func (export "lycoris_invoke") (param $mp i32) (param $ml i32) (param $pp i32) (param $pl i32) (result i64)
        (call $lycoris_http (local.get $pp) (local.get $pl))))
  "#;

  /// Package the HTTP forwarder with a manifest declaring the http
  /// capability.
  fn http_package() -> ExtensionPackage {
    let manifest = ExtensionManifest::from_map(&BTreeMap::from([
      ("semver".to_string(), "0.1.0".to_string()),
      ("capabilities".to_string(), r#"["http"]"#.to_string()),
    ]))
    .unwrap();
    ExtensionPackage::new(
      "test".to_string(),
      "test-extension".to_string(),
      1,
      EngineKind::Wasm,
      String::new(),
      manifest,
      wat::parse_str(HTTP_FORWARD_WAT).unwrap(),
    )
  }

  /// A minimal mock HTTP server: one request per connection, routed canned
  /// responses. Returns the base URL; the task is aborted on drop.
  struct MockHttp {
    base_url: String,
    task: tokio::task::JoinHandle<()>,
  }

  impl MockHttp {
    async fn start() -> Self {
      use tokio::io::{AsyncReadExt, AsyncWriteExt};

      let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
      let base_url = format!("http://{}", listener.local_addr().unwrap());
      let task = tokio::spawn(async move {
        loop {
          let Ok((mut stream, _)) = listener.accept().await else {
            break;
          };
          tokio::spawn(async move {
            // Byte-wise header read avoids over-reading into the body.
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
              if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                return;
              }
              head.push(byte[0]);
              if head.len() > 64 * 1024 {
                return;
              }
            }
            let head = String::from_utf8_lossy(&head).into_owned();
            let content_length: usize = head
              .lines()
              .find_map(|line| {
                line
                  .to_ascii_lowercase()
                  .strip_prefix("content-length:")
                  .and_then(|value| value.trim().parse().ok())
              })
              .unwrap_or(0);
            let mut body = vec![0u8; content_length];
            if stream.read_exact(&mut body).await.is_err() {
              return;
            }
            let request_line = head.lines().next().unwrap_or_default().to_string();
            let path = request_line
              .split_whitespace()
              .nth(1)
              .unwrap_or("/")
              .to_string();
            let (status, reason, response_body): (u16, &str, Vec<u8>) = match path.as_str() {
              "/teapot" => (418, "I'm a teapot", b"short and stout".to_vec()),
              "/huge" => (200, "OK", vec![b'x'; 9 * 1024 * 1024]),
              _ => (
                200,
                "OK",
                serde_json::to_vec(&serde_json::json!({
                  "request": request_line,
                  "body": String::from_utf8_lossy(&body),
                }))
                .unwrap(),
              ),
            };
            let response = format!(
              "HTTP/1.1 {status} {reason}\r\ncontent-length: {}\r\nx-mock: yes\r\nconnection: close\r\n\r\n",
              response_body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.write_all(&response_body).await;
          });
        }
      });
      Self { base_url, task }
    }
  }

  impl Drop for MockHttp {
    fn drop(&mut self) {
      self.task.abort();
    }
  }

  fn request_document(method: &str, url: &str, body: Option<&str>) -> Vec<u8> {
    let mut document = serde_json::json!({
      "method": method,
      "url": url,
      "headers": {"content-type": "application/json"},
    });
    if let Some(body) = body {
      document["body"] = serde_json::Value::String(body.to_string());
    }
    serde_json::to_vec(&document).unwrap()
  }

  #[tokio::test]
  async fn http_import_without_the_capability_fails_to_load() {
    let result = WasmEngine::new(EngineLimits::default())
      .unwrap()
      .load(&package_wat(HTTP_FORWARD_WAT))
      .await;
    match result {
      Err(ExtensionError::Engine(err)) => {
        assert!(err.contains("http capability"), "unexpected error: {err}");
      }
      Err(other) => panic!("expected an engine error, got {other}"),
      Ok(_) => panic!("expected a capability error, but the load succeeded"),
    }
  }

  #[tokio::test]
  async fn http_round_trip_against_a_mock_server() {
    let server = MockHttp::start().await;
    let engine = WasmEngine::new(EngineLimits::default()).unwrap();
    let instance = engine.load_instance(&http_package()).await.unwrap();

    let document = request_document(
      "POST",
      &format!("{}/echo", server.base_url),
      Some(r#"{"a":1}"#),
    );
    let output = instance.invoke("http", &document).await.unwrap();
    let response = parse_output(&output);

    assert_eq!(response["status"], 200);
    assert_eq!(response["headers"]["x-mock"], "yes");
    // The response body is text (design section 4); the mock's echo document
    // rides inside it.
    let echoed: serde_json::Value =
      serde_json::from_str(response["body"].as_str().unwrap()).unwrap();
    assert_eq!(echoed["request"], "POST /echo HTTP/1.1");
    assert_eq!(echoed["body"], r#"{"a":1}"#);
  }

  #[tokio::test]
  async fn http_status_codes_pass_through_to_the_guest() {
    let server = MockHttp::start().await;
    let engine = WasmEngine::new(EngineLimits::default()).unwrap();
    let instance = engine.load_instance(&http_package()).await.unwrap();

    let document = request_document("GET", &format!("{}/teapot", server.base_url), None);
    let output = instance.invoke("http", &document).await.unwrap();
    let response = parse_output(&output);

    // A 418 is not an error: the guest receives the status and decides.
    assert_eq!(response["status"], 418);
    assert_eq!(response["body"], "short and stout");
    assert!(response.get("error").is_none());
  }

  #[tokio::test]
  async fn http_response_bodies_hit_the_8_mib_cap() {
    let server = MockHttp::start().await;
    let engine = WasmEngine::new(EngineLimits::default()).unwrap();
    let instance = engine.load_instance(&http_package()).await.unwrap();

    let document = request_document("GET", &format!("{}/huge", server.base_url), None);
    let output = instance.invoke("http", &document).await.unwrap();
    let response = parse_output(&output);

    assert_eq!(response["error"]["type"], "response_too_large");
  }

  #[tokio::test]
  async fn http_allow_hosts_rejects_unlisted_hosts_without_trapping() {
    let server = MockHttp::start().await;
    let engine = WasmEngine::new(EngineLimits::default()).unwrap();
    let instance = engine.load_instance(&http_package()).await.unwrap();
    *instance.http_allow_hosts.write().unwrap() = Some(vec!["api.openai.com".to_string()]);

    let document = request_document("GET", &format!("{}/echo", server.base_url), None);
    let output = instance.invoke("http", &document).await.unwrap();
    let response = parse_output(&output);

    assert_eq!(response["error"]["type"], "host_not_allowed");
  }

  #[tokio::test]
  async fn http_allow_hosts_passes_listed_hosts() {
    let server = MockHttp::start().await;
    let engine = WasmEngine::new(EngineLimits::default()).unwrap();
    let instance = engine.load_instance(&http_package()).await.unwrap();
    *instance.http_allow_hosts.write().unwrap() = Some(vec!["127.0.0.1".to_string()]);

    let document = request_document("GET", &format!("{}/echo", server.base_url), None);
    let output = instance.invoke("http", &document).await.unwrap();
    let response = parse_output(&output);

    assert_eq!(response["status"], 200);
  }
}
