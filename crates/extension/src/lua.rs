//! Lua execution engine (design document section 5.3).
//!
//! Each instance gets a fresh, isolated [`Lua`] state built from `Lua::new()`
//! and then stripped down further: `io` removed, `os` restricted to
//! `time`/`clock`, `package` search paths cleared, and the chunk-loading
//! globals (`load`, `loadfile`, `dofile`) removed. A script either defines a
//! global entry function (default name `invoke`) or returns a table holding
//! it; payloads cross the boundary as Lua values via mlua's serde bridge.
//!
//! Enforcement: an instruction-count hook aborts scripts that exceed the
//! per-call instruction budget, [`Lua::set_memory_limit`] caps allocations,
//! and every invocation is wrapped in a wall-clock deadline. Misbehaving
//! scripts surface as [`ExtensionError::Script`] /
//! [`ExtensionError::BudgetExceeded`] and never poison the host task.

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use async_trait::async_trait;
use mlua::{Function, HookTriggers, Lua, LuaSerdeExt, Table, Value, VmState};

use crate::{
  engine::{EngineKind, EngineLimits, ExtensionEngine, ExtensionInstance},
  error::{ExtensionError, Result},
  package::ExtensionPackage,
};

/// Instruction-hook granularity: the budget is checked every this many VM
/// instructions, so enforcement overshoots by at most this amount.
const HOOK_GRANULARITY: u32 = 1024;

/// The Lua execution engine.
pub struct LuaEngine {
  limits: EngineLimits,
}

impl LuaEngine {
  /// Create an engine enforcing `limits` on every instance it loads.
  pub fn new(limits: EngineLimits) -> Self {
    Self { limits }
  }
}

#[async_trait]
impl ExtensionEngine for LuaEngine {
  fn kind(&self) -> EngineKind {
    EngineKind::Lua
  }

  async fn load(&self, package: &ExtensionPackage) -> Result<Box<dyn ExtensionInstance>> {
    package.verify()?;
    if package.engine != EngineKind::Lua {
      return Err(ExtensionError::Engine(format!(
        "extension {} targets {:?}, not the lua engine",
        package.id, package.engine
      )));
    }
    let source = String::from_utf8(package.artifact.clone())
      .map_err(|err| ExtensionError::Engine(format!("lua artifact is not utf-8: {err}")))?;
    // The Lua engine has no http host capability in v1 (llm-provider design
    // section 4 is a WASM-only surface); fail the load instead of letting the
    // extension discover the gap at call time.
    if package.manifest.capabilities.iter().any(|c| c == "http") {
      return Err(ExtensionError::Engine(format!(
        "extension {} declares the http capability, which the lua engine does not support",
        package.id
      )));
    }
    let entry = package.entry.clone();
    let limits = self.limits;

    // Chunk evaluation runs arbitrary top-level code, so it happens on a
    // blocking thread with the instruction budget armed.
    let (lua, entry_fn) =
      tokio::task::spawn_blocking(move || build_state(&source, &entry, &limits))
        .await
        .map_err(|err| ExtensionError::Engine(format!("lua load task failed: {err}")))??;

    Ok(Box::new(LuaInstance {
      lua,
      entry_fn,
      instruction_budget: limits.lua_instructions_per_call,
      deadline: limits.invoke_timeout,
    }))
  }
}

/// A loaded Lua extension: one sandboxed `Lua` state plus its entry function.
struct LuaInstance {
  lua: Lua,
  entry_fn: Function,
  instruction_budget: u64,
  deadline: Duration,
}

#[async_trait]
impl ExtensionInstance for LuaInstance {
  async fn invoke(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>> {
    let input: serde_json::Value = serde_json::from_slice(payload).map_err(|err| {
      ExtensionError::InvalidPayload(format!("request payload is not valid JSON: {err}"))
    })?;

    let lua = self.lua.clone();
    let entry_fn = self.entry_fn.clone();
    let method = method.to_string();
    let budget = self.instruction_budget;

    let result = tokio::time::timeout(self.deadline, async move {
      tokio::task::spawn_blocking(move || call_entry(&lua, &entry_fn, &method, &input, budget))
        .await
        .map_err(|err| ExtensionError::Engine(format!("lua invoke task failed: {err}")))?
    })
    .await;

    match result {
      Ok(output) => output,
      Err(_) => Err(ExtensionError::Timeout(self.deadline)),
    }
  }
}

/// Build a fresh sandboxed state, evaluate the chunk under the instruction
/// budget, and resolve the entry function.
fn build_state(source: &str, entry: &str, limits: &EngineLimits) -> Result<(Lua, Function)> {
  let lua = Lua::new();
  sandbox(&lua)?;
  lua
    .set_memory_limit(limits.lua_max_memory_bytes)
    .map_err(|err| ExtensionError::Engine(format!("failed to set lua memory limit: {err}")))?;

  let fired = arm_instruction_hook(&lua, limits.lua_instructions_per_call)?;
  let returned = lua.load(source).eval::<Value>();
  lua.remove_hook();

  let returned = match returned {
    Ok(value) => value,
    Err(err) => {
      return Err(classify_script_error(
        err,
        &fired,
        limits.lua_instructions_per_call,
      ));
    }
  };

  // Prefer a global entry function; fall back to a table returned by the
  // chunk holding it (module style).
  if let Value::Function(entry_fn) = lua.globals().get::<Value>(entry).map_err(engine_err)? {
    return Ok((lua, entry_fn));
  }
  if let Value::Table(module) = returned
    && let Value::Function(entry_fn) = module.get::<Value>(entry).map_err(engine_err)?
  {
    return Ok((lua, entry_fn));
  }
  Err(ExtensionError::Engine(format!(
    "lua chunk defines no entry function {entry:?} (global or returned module)"
  )))
}

/// Execute one invocation under the instruction budget.
fn call_entry(
  lua: &Lua, entry_fn: &Function, method: &str, input: &serde_json::Value, budget: u64,
) -> Result<Vec<u8>> {
  let payload = lua.to_value(input).map_err(|err| {
    ExtensionError::InvalidPayload(format!("failed to bridge payload to lua: {err}"))
  })?;

  let fired = arm_instruction_hook(lua, budget)?;
  let result = entry_fn.call::<Value>((method.to_string(), payload));
  lua.remove_hook();

  let output = match result {
    Ok(value) => value,
    Err(err) => return Err(classify_script_error(err, &fired, budget)),
  };

  let json: serde_json::Value = lua.from_value(output).map_err(|err| {
    ExtensionError::InvalidPayload(format!("guest returned a non-JSON value: {err}"))
  })?;
  serde_json::to_vec(&json)
    .map_err(|err| ExtensionError::InvalidPayload(format!("failed to encode guest output: {err}")))
}

/// Strip the freshly created state down to the extension sandbox (design
/// document section 5.3). `Lua::new()` loads mlua's "safe" stdlib subset,
/// which still includes `io` and the full `os` library, so the hardening
/// below is mandatory, not defensive.
fn sandbox(lua: &Lua) -> Result<()> {
  let globals = lua.globals();
  globals.set("io", Value::Nil).map_err(engine_err)?;
  globals.set("load", Value::Nil).map_err(engine_err)?;
  globals.set("loadfile", Value::Nil).map_err(engine_err)?;
  globals.set("dofile", Value::Nil).map_err(engine_err)?;

  // `os` keeps only time and clock.
  let os: Table = globals.get("os").map_err(engine_err)?;
  let restricted = lua.create_table().map_err(engine_err)?;
  for name in ["time", "clock"] {
    let function: Function = os.get(name).map_err(engine_err)?;
    restricted.set(name, function).map_err(engine_err)?;
  }
  globals.set("os", restricted).map_err(engine_err)?;

  // `package` keeps no search path and no dynamic loader.
  let package: Table = globals.get("package").map_err(engine_err)?;
  package.set("path", "").map_err(engine_err)?;
  package.set("cpath", "").map_err(engine_err)?;
  package.set("loadlib", Value::Nil).map_err(engine_err)?;
  Ok(())
}

/// Arm the per-call instruction-count hook and return the shared fire
/// counter used afterwards to classify errors.
fn arm_instruction_hook(lua: &Lua, budget: u64) -> Result<Arc<AtomicU64>> {
  let fired = Arc::new(AtomicU64::new(0));
  let fired_in_hook = Arc::clone(&fired);
  let allowed = allowed_hook_fires(budget);
  lua
    .set_hook(
      HookTriggers::new().every_nth_instruction(HOOK_GRANULARITY),
      move |_, _| {
        if fired_in_hook.fetch_add(1, Ordering::Relaxed) >= allowed {
          Err(mlua::Error::external("lua instruction budget exceeded"))
        } else {
          Ok(VmState::Continue)
        }
      },
    )
    .map_err(|err| ExtensionError::Engine(format!("failed to arm instruction hook: {err}")))?;
  Ok(fired)
}

/// Hook fires allowed before the budget is considered exceeded.
fn allowed_hook_fires(budget: u64) -> u64 {
  budget.div_ceil(u64::from(HOOK_GRANULARITY))
}

/// Map a Lua failure onto the extension error taxonomy: budget exhaustion beats
/// script errors when the hook tripped, allocation failures are budget
/// failures, everything else is a plain script error.
fn classify_script_error(err: mlua::Error, fired: &AtomicU64, budget: u64) -> ExtensionError {
  if fired.load(Ordering::Relaxed) > allowed_hook_fires(budget) {
    return ExtensionError::BudgetExceeded(format!(
      "lua instruction budget of {budget} instructions exceeded"
    ));
  }
  match err {
    mlua::Error::MemoryError(message) => {
      ExtensionError::BudgetExceeded(format!("lua memory budget exceeded: {message}"))
    }
    other => ExtensionError::Script(other.to_string()),
  }
}

/// Map a sandbox/setup failure (host-side, not guest code).
fn engine_err(err: mlua::Error) -> ExtensionError {
  ExtensionError::Engine(format!("failed to prepare lua state: {err}"))
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;

  use super::*;
  use crate::manifest::ExtensionManifest;

  fn manifest() -> ExtensionManifest {
    ExtensionManifest::from_map(&BTreeMap::from([(
      "semver".to_string(),
      "0.1.0".to_string(),
    )]))
    .unwrap()
  }

  fn package(source: &str) -> ExtensionPackage {
    ExtensionPackage::new(
      "test".to_string(),
      "test-extension".to_string(),
      1,
      EngineKind::Lua,
      String::new(),
      manifest(),
      source.as_bytes().to_vec(),
    )
  }

  async fn load(source: &str) -> Box<dyn ExtensionInstance> {
    LuaEngine::new(EngineLimits::default())
      .load(&package(source))
      .await
      .unwrap()
  }

  fn parse_output(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap()
  }

  #[tokio::test]
  async fn echo_round_trip_preserves_structured_json() {
    let instance = load("function invoke(method, payload) return payload end").await;
    let input = br#"{"nested":{"a":[1,2,3],"b":null},"s":"x","n":1.5}"#;
    let output = instance.invoke("anything", input).await.unwrap();
    assert_eq!(
      parse_output(&output),
      serde_json::from_slice::<serde_json::Value>(input).unwrap()
    );
  }

  #[tokio::test]
  async fn method_name_is_passed_through() {
    let instance =
      load("function invoke(method, payload) return {method = method, payload = payload} end")
        .await;
    let output = instance.invoke("skills.run", b"[1]").await.unwrap();
    assert_eq!(
      parse_output(&output),
      serde_json::json!({"method": "skills.run", "payload": [1]})
    );
  }

  #[tokio::test]
  async fn module_style_chunk_returning_a_table_works() {
    let instance =
      load("local M = {}\nfunction M.invoke(method, payload) return {ok = true} end\nreturn M")
        .await;
    let output = instance.invoke("m", b"{}").await.unwrap();
    assert_eq!(parse_output(&output), serde_json::json!({"ok": true}));
  }

  #[tokio::test]
  async fn custom_entry_name_is_respected() {
    let package = ExtensionPackage::new(
      "test".to_string(),
      "test-extension".to_string(),
      1,
      EngineKind::Lua,
      "handle".to_string(),
      manifest(),
      b"function handle(method, payload) return payload end".to_vec(),
    );
    let instance = LuaEngine::new(EngineLimits::default())
      .load(&package)
      .await
      .unwrap();
    let output = instance.invoke("m", b"42").await.unwrap();
    assert_eq!(parse_output(&output), serde_json::json!(42));
  }

  #[tokio::test]
  async fn script_errors_surface_as_the_script_variant() {
    let instance = load("function invoke() error('boom') end").await;
    let result = instance.invoke("m", b"{}").await;
    assert!(matches!(result, Err(ExtensionError::Script(err)) if err.contains("boom")));
  }

  #[tokio::test]
  async fn syntax_errors_fail_load_with_the_script_variant() {
    let result = LuaEngine::new(EngineLimits::default())
      .load(&package("this is not lua"))
      .await;
    assert!(matches!(result, Err(ExtensionError::Script(_))));
  }

  #[tokio::test]
  async fn infinite_loops_hit_the_instruction_budget() {
    let limits = EngineLimits {
      lua_instructions_per_call: 10_000,
      ..EngineLimits::default()
    };
    let package = package("function invoke() while true do end end");
    let instance = LuaEngine::new(limits).load(&package).await.unwrap();
    let result = instance.invoke("m", b"{}").await;
    assert!(matches!(result, Err(ExtensionError::BudgetExceeded(_))));
  }

  #[tokio::test]
  async fn top_level_loops_hit_the_instruction_budget_at_load() {
    let limits = EngineLimits {
      lua_instructions_per_call: 10_000,
      ..EngineLimits::default()
    };
    let package = package("while true do end");
    let result = LuaEngine::new(limits).load(&package).await;
    assert!(matches!(result, Err(ExtensionError::BudgetExceeded(_))));
  }

  #[tokio::test]
  async fn balloon_allocations_hit_the_memory_limit() {
    let limits = EngineLimits {
      lua_max_memory_bytes: 1024 * 1024,
      lua_instructions_per_call: 1_000_000_000,
      ..EngineLimits::default()
    };
    let package = package("function invoke() return string.rep('x', 10 * 1024 * 1024) end");
    let instance = LuaEngine::new(limits).load(&package).await.unwrap();
    let result = instance.invoke("m", b"{}").await;
    assert!(matches!(result, Err(ExtensionError::BudgetExceeded(_))));
  }

  #[tokio::test]
  async fn sandbox_removes_dangerous_globals() {
    let instance = load(
      r#"function invoke()
        return {
          io_absent = io == nil,
          os_execute_absent = os == nil or os.execute == nil,
          os_date_absent = os == nil or os.date == nil,
          os_time_present = os ~= nil and os.time ~= nil,
          os_clock_present = os ~= nil and os.clock ~= nil,
          debug_absent = debug == nil,
          load_absent = load == nil,
          dofile_absent = dofile == nil,
          package_path = package ~= nil and package.path or "missing",
          package_cpath = package ~= nil and package.cpath or "missing"
        }
      end"#,
    )
    .await;
    let output = instance.invoke("m", b"{}").await.unwrap();
    assert_eq!(
      parse_output(&output),
      serde_json::json!({
        "io_absent": true,
        "os_execute_absent": true,
        "os_date_absent": true,
        "os_time_present": true,
        "os_clock_present": true,
        "debug_absent": true,
        "load_absent": true,
        "dofile_absent": true,
        "package_path": "",
        "package_cpath": ""
      })
    );
  }

  #[tokio::test]
  async fn non_json_input_is_an_invalid_payload() {
    let instance = load("function invoke(method, payload) return payload end").await;
    let result = instance.invoke("m", b"not json").await;
    assert!(matches!(result, Err(ExtensionError::InvalidPayload(_))));
  }

  #[tokio::test]
  async fn non_json_guest_output_is_an_invalid_payload() {
    let instance = load("function invoke() return function() end end").await;
    let result = instance.invoke("m", b"{}").await;
    assert!(matches!(result, Err(ExtensionError::InvalidPayload(_))));
  }

  #[tokio::test]
  async fn instances_do_not_share_state() {
    let source =
      "counter = counter or 0\nfunction invoke() counter = counter + 1 return counter end";
    let first = load(source).await;
    let second = load(source).await;
    assert_eq!(
      parse_output(&first.invoke("m", b"{}").await.unwrap()),
      serde_json::json!(1)
    );
    assert_eq!(
      parse_output(&first.invoke("m", b"{}").await.unwrap()),
      serde_json::json!(2)
    );
    assert_eq!(
      parse_output(&second.invoke("m", b"{}").await.unwrap()),
      serde_json::json!(1)
    );
  }

  #[tokio::test]
  async fn load_rejects_a_content_hash_mismatch() {
    let mut package = package("function invoke() end");
    package.artifact = b"tampered".to_vec();
    let result = LuaEngine::new(EngineLimits::default()).load(&package).await;
    assert!(matches!(
      result,
      Err(ExtensionError::ContentHashMismatch { .. })
    ));
  }

  #[tokio::test]
  async fn load_rejects_a_missing_entry_function() {
    let result = LuaEngine::new(EngineLimits::default())
      .load(&package("local x = 1"))
      .await;
    assert!(matches!(result, Err(ExtensionError::Engine(_))));
  }

  #[tokio::test]
  async fn load_rejects_a_wrong_engine_kind() {
    let mut package = package("function invoke() end");
    package.engine = EngineKind::Wasm;
    let result = LuaEngine::new(EngineLimits::default()).load(&package).await;
    assert!(matches!(result, Err(ExtensionError::Engine(_))));
  }

  #[tokio::test]
  async fn load_rejects_the_http_capability() {
    // The Lua engine has no http host capability in v1 (llm-provider design
    // section 4 is a WASM-only surface).
    let mut package = package("function invoke() end");
    package.manifest = ExtensionManifest::from_map(&BTreeMap::from([
      ("semver".to_string(), "0.1.0".to_string()),
      ("capabilities".to_string(), r#"["http"]"#.to_string()),
    ]))
    .unwrap();
    let result = LuaEngine::new(EngineLimits::default()).load(&package).await;
    match result {
      Err(ExtensionError::Engine(err)) => {
        assert!(err.contains("http"), "unexpected error: {err}");
      }
      Err(other) => panic!("expected an engine error, got {other}"),
      Ok(_) => panic!("expected an http capability error, but the load succeeded"),
    }
  }
}
