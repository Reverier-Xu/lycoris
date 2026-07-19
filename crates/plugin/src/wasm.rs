//! WASM execution engine: ABI `lycoris-abi-v1` (design document section
//! 5.2).
//!
//! Core wasm on wasmtime with WASI disabled: the linker exposes only the
//! `lycoris` host module with a single `log(level, ptr, len)` import
//! forwarded to `tracing`. Guests must export `memory`,
//! `lycoris_alloc(len: i32) -> i32` and
//! `lycoris_invoke(method_ptr, method_len, payload_ptr, payload_len) -> i64`;
//! the invoke return value packs the response buffer as `(ptr << 32) | len`
//! in guest memory.
//!
//! Enforcement: fuel per call gives a deterministic execution bound,
//! [`StoreLimits`] caps linear memory, and every invocation is wrapped in a
//! wall-clock deadline. A guest exceeding any limit traps, and the trap
//! surfaces as [`PluginError::GuestTrap`], never as a host panic.

use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use wasmtime::{
  Caller, Config, Engine, ExternType, FuncType, Linker, Memory, Module, Store, StoreLimits,
  StoreLimitsBuilder, TypedFunc, ValType,
};

use crate::{
  engine::{EngineKind, EngineLimits, PluginEngine, PluginInstance},
  error::{PluginError, Result},
  package::PluginPackage,
};

/// Host module name exposed to guests.
const HOST_MODULE: &str = "lycoris";
/// Name of the exported guest allocator.
const ALLOC_EXPORT: &str = "lycoris_alloc";
/// Name of the exported guest entry point.
const INVOKE_EXPORT: &str = "lycoris_invoke";
/// Name of the exported guest linear memory.
const MEMORY_EXPORT: &str = "memory";

/// Per-instance store state: the resource limits plus the plugin id for
/// log attribution.
struct WasmState {
  limits: StoreLimits,
  plugin_id: String,
}

/// The WASM execution engine.
pub struct WasmEngine {
  engine: Engine,
  limits: EngineLimits,
}

impl WasmEngine {
  /// Create an engine enforcing `limits` on every instance it loads.
  pub fn new(limits: EngineLimits) -> Result<Self> {
    let mut config = Config::new();
    // Async support is built in since wasmtime 43 (`Config::async_support` is
    // a deprecated no-op); fuel metering still needs opting into.
    config.consume_fuel(true);
    let engine = Engine::new(&config)
      .map_err(|err| PluginError::Engine(format!("failed to create wasmtime engine: {err}")))?;
    Ok(Self { engine, limits })
  }

  /// Build the linker exposing exactly the `lycoris` host module.
  fn linker(&self) -> Result<Linker<WasmState>> {
    let mut linker = Linker::new(&self.engine);
    linker
      .func_wrap(
        HOST_MODULE,
        "log",
        |mut caller: Caller<'_, WasmState>, level: i32, ptr: i32, len: i32| {
          let plugin_id = caller.data().plugin_id.clone();
          let message = read_guest_string(&mut caller, ptr, len)
            .unwrap_or_else(|| "<out-of-bounds log message>".to_string());
          match level {
            0 => tracing::trace!(plugin = plugin_id, "{message}"),
            1 => tracing::debug!(plugin = plugin_id, "{message}"),
            3 => tracing::warn!(plugin = plugin_id, "{message}"),
            4 => tracing::error!(plugin = plugin_id, "{message}"),
            // 2 is info; out-of-range levels fall back to info as well.
            _ => tracing::info!(plugin = plugin_id, "{message}"),
          }
        },
      )
      .map_err(|err| {
        PluginError::Engine(format!("failed to register the lycoris host module: {err}"))
      })?;
    Ok(linker)
  }
}

#[async_trait]
impl PluginEngine for WasmEngine {
  fn kind(&self) -> EngineKind {
    EngineKind::Wasm
  }

  async fn load(&self, package: &PluginPackage) -> Result<Box<dyn PluginInstance>> {
    package.verify()?;
    if package.engine != EngineKind::Wasm {
      return Err(PluginError::Engine(format!(
        "plugin {} targets {:?}, not the wasm engine",
        package.id, package.engine
      )));
    }

    let module = Module::new(&self.engine, &package.artifact)
      .map_err(|err| PluginError::Engine(format!("failed to compile the wasm module: {err:#}")))?;
    check_imports(&module)?;
    check_exports(&module)?;

    let state = WasmState {
      limits: StoreLimitsBuilder::new()
        .memory_size(self.limits.wasm_max_memory_bytes)
        .build(),
      plugin_id: package.id.clone(),
    };
    let mut store = Store::new(&self.engine, state);
    store.limiter(|state| &mut state.limits);

    let instance = self
      .linker()?
      .instantiate_async(&mut store, &module)
      .await
      .map_err(|err| PluginError::GuestTrap(format!("failed to instantiate the guest: {err}")))?;

    // Precise signature checks; the name/kind precheck above keeps the
    // common failure messages readable.
    let memory = instance
      .get_memory(&mut store, MEMORY_EXPORT)
      .ok_or_else(|| PluginError::Engine("guest exports no memory".to_string()))?;
    let alloc = instance
      .get_typed_func::<i32, i32>(&mut store, ALLOC_EXPORT)
      .map_err(|err| PluginError::Engine(format!("invalid lycoris_alloc signature: {err}")))?;
    let invoke = instance
      .get_typed_func::<(i32, i32, i32, i32), i64>(&mut store, INVOKE_EXPORT)
      .map_err(|err| PluginError::Engine(format!("invalid lycoris_invoke signature: {err}")))?;

    Ok(Box::new(WasmInstance {
      store: Mutex::new(store),
      memory,
      alloc,
      invoke,
      fuel: self.limits.wasm_fuel_per_call,
      deadline: self.limits.invoke_timeout,
    }))
  }
}

/// A loaded WASM plugin: one store holding one instantiated guest.
struct WasmInstance {
  store: Mutex<Store<WasmState>>,
  memory: Memory,
  alloc: TypedFunc<i32, i32>,
  invoke: TypedFunc<(i32, i32, i32, i32), i64>,
  fuel: u64,
  deadline: Duration,
}

#[async_trait]
impl PluginInstance for WasmInstance {
  async fn invoke(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>> {
    // The payload contract is JSON bytes end-to-end; reject junk early.
    serde_json::from_slice::<serde_json::Value>(payload).map_err(|err| {
      PluginError::InvalidPayload(format!("request payload is not valid JSON: {err}"))
    })?;

    let result = tokio::time::timeout(self.deadline, self.invoke_inner(method, payload)).await;
    match result {
      Ok(output) => output,
      Err(_) => Err(PluginError::Timeout(self.deadline)),
    }
  }
}

impl WasmInstance {
  /// Run one guest invocation: alloc, write request, call, read response.
  async fn invoke_inner(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>> {
    let method_len = i32::try_from(method.len()).map_err(|_| {
      PluginError::InvalidPayload("method name exceeds the guest addressable size".to_string())
    })?;
    let payload_len = i32::try_from(payload.len()).map_err(|_| {
      PluginError::InvalidPayload("payload exceeds the guest addressable size".to_string())
    })?;

    let mut store = self.store.lock().await;
    store
      .set_fuel(self.fuel)
      .map_err(|err| PluginError::Engine(format!("failed to set fuel: {err}")))?;

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
      .map_err(|err| PluginError::Engine(format!("invalid response pointer: {err}")))?;
    let response_len = usize::try_from(packed & 0xFFFF_FFFF)
      .map_err(|err| PluginError::Engine(format!("invalid response length: {err}")))?;
    let data = self.memory.data(&*store);
    let response = data
      .get(response_ptr..response_ptr.saturating_add(response_len))
      .ok_or_else(|| {
        PluginError::Engine("guest returned an out-of-bounds response buffer pointer".to_string())
      })?
      .to_vec();

    // Engines validate that guest output is well-formed JSON.
    serde_json::from_slice::<serde_json::Value>(&response).map_err(|err| {
      PluginError::InvalidPayload(format!("guest output is not valid JSON: {err}"))
    })?;
    Ok(response)
  }
}

/// Read a guest string for the `log` host import; `None` when the guest
/// hands the host an out-of-bounds slice.
fn read_guest_string(caller: &mut Caller<'_, WasmState>, ptr: i32, len: i32) -> Option<String> {
  let memory = caller.get_export(MEMORY_EXPORT)?.into_memory()?;
  let data = memory.data(&caller);
  let start = usize::try_from(ptr).ok()?;
  let len = usize::try_from(len).ok()?;
  let bytes = data.get(start..start.checked_add(len)?)?;
  Some(String::from_utf8_lossy(bytes).into_owned())
}

/// Validate that the module imports nothing beyond `lycoris.log` — WASI is
/// disabled, so any other import is unsupported.
fn check_imports(module: &Module) -> Result<()> {
  for import in module.imports() {
    if import.module() != HOST_MODULE || import.name() != "log" {
      return Err(PluginError::Engine(format!(
        "unsupported import {}.{}: only lycoris.log is provided",
        import.module(),
        import.name()
      )));
    }
    match import.ty() {
      ExternType::Func(ty)
        if signature_matches(&ty, &[ValType::I32, ValType::I32, ValType::I32], &[]) => {}
      _ => {
        return Err(PluginError::Engine(
          "lycoris.log must have signature (i32, i32, i32) -> ()".to_string(),
        ));
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
    return Err(PluginError::Engine(
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
    _ => Err(PluginError::Engine(format!(
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

/// Build a closure mapping guest failures to [`PluginError::GuestTrap`]. The
/// alternate Display keeps the whole anyhow chain so the trap reason (fuel
/// exhaustion, `unreachable`, ...) survives in the message.
fn guest_trap<E: std::fmt::Display>(context: &'static str) -> impl Fn(E) -> PluginError {
  move |err| PluginError::GuestTrap(format!("{context}: {err:#}"))
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use super::*;
  use crate::manifest::PluginManifest;

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

  fn manifest() -> PluginManifest {
    PluginManifest::from_map(&BTreeMap::from([(
      "semver".to_string(),
      "0.1.0".to_string(),
    )]))
    .unwrap()
  }

  fn package_wat(wat_source: &str) -> PluginPackage {
    PluginPackage::new(
      "test".to_string(),
      "test-plugin".to_string(),
      1,
      EngineKind::Wasm,
      String::new(),
      manifest(),
      wat::parse_str(wat_source).unwrap(),
    )
  }

  async fn load(wat_source: &str) -> Box<dyn PluginInstance> {
    load_with_limits(wat_source, EngineLimits::default()).await
  }

  async fn load_with_limits(wat_source: &str, limits: EngineLimits) -> Box<dyn PluginInstance> {
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
    assert!(matches!(result, Err(PluginError::GuestTrap(_))));
  }

  #[tokio::test]
  async fn infinite_loops_exhaust_fuel_and_trap() {
    let limits = EngineLimits {
      wasm_fuel_per_call: 100_000,
      ..EngineLimits::default()
    };
    let instance = load_with_limits(LOOP_WAT, limits).await;
    let result = instance.invoke("m", b"{}").await;
    let Err(PluginError::GuestTrap(message)) = result else {
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
    assert!(matches!(result, Err(PluginError::GuestTrap(_))));
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
    assert!(matches!(result, Err(PluginError::Engine(err)) if err.contains("lycoris_alloc")));
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
    assert!(matches!(result, Err(PluginError::Engine(err)) if err.contains("memory")));
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
    assert!(matches!(result, Err(PluginError::Engine(_))));
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
    assert!(matches!(result, Err(PluginError::Engine(err)) if err.contains("unsupported import")));
  }

  #[tokio::test]
  async fn non_json_guest_output_is_an_invalid_payload() {
    let instance = load(NON_JSON_WAT).await;
    let result = instance.invoke("m", b"{}").await;
    assert!(matches!(result, Err(PluginError::InvalidPayload(_))));
  }

  #[tokio::test]
  async fn non_json_input_is_an_invalid_payload() {
    let instance = load(ECHO_WAT).await;
    let result = instance.invoke("m", b"not json").await;
    assert!(matches!(result, Err(PluginError::InvalidPayload(_))));
  }

  #[tokio::test]
  async fn load_rejects_a_wrong_engine_kind() {
    let mut package = package_wat(ECHO_WAT);
    package.engine = EngineKind::Lua;
    let result = WasmEngine::new(EngineLimits::default())
      .unwrap()
      .load(&package)
      .await;
    assert!(matches!(result, Err(PluginError::Engine(_))));
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
      Err(PluginError::ContentHashMismatch { .. })
    ));
  }
}
