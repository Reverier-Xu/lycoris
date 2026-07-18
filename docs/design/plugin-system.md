# Plugin System Design

Status: architecture v1 (this document is the design contract for the initial
implementation; the implementation ships no production plugins).

## 1. Goals and non-goals

Plugins extend the agent workflow: modular skill execution, agent hooks,
capability providers (e.g. an LLM API provider). The cluster must treat plugin
code and plugin configuration as shared state: both are synchronized to every
node, and every node independently decides — via label selectors — whether and
how a plugin runs locally.

Goals:

- Two execution engines with one common invocation contract:
  - **WASM** for complex, performance- and stability-critical, properly
    versioned packages.
  - **Embedded script (Lua)** for small, fast-iterating plugins.
- Registered plugins are callable through a proto API on the unified node API
  server.
- Hook awareness is driven by configuration (the plugin manifest), not by
  code changes on the host.
- Cluster-wide sync of plugin packages and configs, reusing the existing
  resource anti-entropy pipeline.
- Label selectors in plugin config decide per-node activation.
- Automatic routing: a call for a plugin that does not run locally is
  forwarded to the best node that advertises it.

Non-goals (v1):

- No production plugins (an LLM provider is the motivating example only).
- No plugin marketplace/distribution channel; packages enter the cluster
  through the resource API like skills/rules.
- No host-service matrix beyond logging (HTTP and friends are declared as
  capabilities in the manifest but not implemented yet).

## 2. Technology selection

| Concern | Choice | Rationale |
| --- | --- | --- |
| WASM runtime | `wasmtime` 46 | Bytecode Alliance, the most actively maintained Rust WASM engine; capability-based sandboxing (WASI off by default); fuel metering for deterministic timeouts; pure-Rust build works on our musl static target. |
| Script engine | `mlua` 0.12 (Lua 5.4, `vendored`, `serialize`) | Actively maintained; tiny transitive footprint (vendored C Lua via `cc`, no system dependency); safe-by-default stdlib (`Lua::new()` excludes `io`, `os.execute`, `debug`, `loadlib`); proven embedding ecosystem; serde bridge for JSON payloads. |
| Payload format | JSON via `serde_json` | Language-neutral for both engines and for the wire. |
| Manifest versions | `semver` 1.0 | Human-facing SemVer strings validated at ingest; cluster convergence keeps using the monotonic `u64` version (see §8). |

Alternatives considered and rejected:

- **PyO3/CPython**: requires linking `libpython`, breaks static cross-platform
  distribution; operationally heavy.
- **RustPython**: pure Rust but an incomplete stdlib and a much larger
  dependency tree than Lua; embedding story still immature.
- **rquickjs (QuickJS)**: reasonable, but its maintenance cadence and Rust
  ecosystem are weaker than mlua's; Lua is the more conservative choice for a
  sandboxed guest.
- **Wasmer**: comparable to wasmtime, but wasmtime's WASI capability model and
  fuel API are the better documented fit; one engine keeps the audit surface
  small.
- **WASM component model / wit-bindgen**: the right long-term ABI, but the
  tooling chain is still churning; v1 defines a minimal core-wasm ABI (§5.2)
  that can later be bridged to components.

## 3. Architecture overview

```
                 ┌────────────────────────── node ───────────────────────────┐
                 │                                                           │
 CLI / other ───►│  PluginService (proto, cluster-key interceptor)           │
 cluster nodes   │        │                                                  │
                 │        ▼                                                  │
                 │  PluginManager ──reconcile──► PluginRegistry (storage)    │
                 │        │  label selector            ▲                     │
                 │        ▼                             │ anti-entropy       │
                 │  ┌─────────────┐   ┌─────────────┐  │ (ResourceKind::    │
                 │  │ WasmEngine  │   │ LuaEngine   │  │  PLUGIN, §4)       │
                 │  └─────────────┘   └─────────────┘  │                    │
                 │        │                 │          │                    │
                 │  capability announcement ▼          │                    │
                 │  MembershipService: local register annotations           │
                 │        │                                                │
                 └────────┼────────────────────────────────────────────────┘
                          │ gossip (plugin.<id>=<version>)
                          ▼
                    other nodes → routing table for forwarding (§7)
```

Data plane and control plane are deliberately separate:

- **Control plane** (what exists, what should run): plugin records and configs
  are cluster-shared resources; every node converges on the same set.
- **Data plane** (execution): each node runs its own engine instances; calls
  are served locally or forwarded one hop to a capable peer.

## 4. Package model, storage and sync

A plugin package is one resource:

- `ResourceKind::PLUGIN` with a `PluginBody`:
  - `version: u64` — monotonic, used by anti-entropy convergence.
  - `content_hash: string` — blake3 of `artifact`.
  - `engine: string` — `"wasm" | "lua"`.
  - `entry: string` — exported entry point, default `invoke`.
  - `artifact: bytes` — wasm module or Lua source.
  - `manifest: map<string, string>` — everything else: `semver`, `capabilities`
    (JSON array), `hooks` (JSON array of hook points), `selector` (JSON map),
    `settings` (opaque JSON passed to the plugin).
- Generic metadata (`id`, `name`, `labels`, `scope`, `source_node_id`,
  timestamps) rides in `ResourceMetadata`, exactly like skills/rules.

Storage (new `plugin` domain in `lycoris-storage`):

- `PluginRecord` (redb): id, name, version, engine, entry, content_hash,
  scope, source_node_id, created/updated, manifest (BTreeMap — deterministic
  postcard encoding, same lesson as `WorkspaceRecord`). Implements
  `VersionedRecord`, so the existing apply pipeline (`should_apply_versioned`,
  per-domain mutex, content-before-metadata ordering) is reused unchanged.
- `PluginBlobStore`: artifact bytes under `data_dir/plugins/blobs/<id>`,
  written *before* the metadata record (same failure atomicity as workspace),
  id whitelist identical to the content-store validation. Not git: artifacts
  are immutable, content-addressed bytes; history lives in the version
  sequence, not in a VCS.

Sync: no new protocol. `ResourceMapper` learns the PLUGIN kind (list/get/
apply/local_shared_resources), so plugin packages replicate through the
existing 5-second resource anti-entropy and the `SyncResources` RPC.

## 5. Execution engines

### 5.1 Common contract (`lycoris-plugin` crate)

```rust
pub enum EngineKind { Wasm, Lua }

pub struct PluginPackage { /* record + artifact bytes */ }

#[async_trait]
pub trait PluginEngine: Send + Sync {
    fn kind(&self) -> EngineKind;
    async fn load(&self, package: &PluginPackage) -> Result<Box<dyn PluginInstance>>;
}

#[async_trait]
pub trait PluginInstance: Send + Sync {
    /// `payload` is JSON; the return value is JSON.
    async fn invoke(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>>;
}
```

Invocation payload is JSON bytes end-to-end: proto bytes → engine → guest →
engine → proto bytes. Engines validate that guest output is well-formed JSON.

### 5.2 WASM engine — ABI `lycoris-abi-v1`

Core wasm (no component model yet), WASI **disabled**: the linker exposes only
the `lycoris` host module.

Guest exports:

- `memory`
- `lycoris_alloc(len: i32) -> i32` — guest allocator the host uses to place
  the request bytes.
- `lycoris_invoke(method_ptr: i32, method_len: i32, payload_ptr: i32, payload_len: i32) -> i64`
  — returns `(ptr << 32) | len` of the response buffer in guest memory.

Host imports (`lycoris` module):

- `log(level: i32, ptr: i32, len: i32)` — forwarded to `tracing`.

Limits (from daemon config, §9): fuel per call (deterministic timeout),
max linear memory, per-call wall-clock deadline enforced via fuel + async
preemption. A guest that exceeds any limit traps; the trap surfaces as a
structured `PluginError::GuestTrap`, never as a host panic.

### 5.3 Lua engine

`mlua` with Lua 5.4 vendored:

- Sandbox: `Lua::new()` stdlib minus `io`/`debug`/`loadlib`; `os` restricted
  to `time`/`clock`; `package` path cleared. Each instance gets a fresh,
  isolated `Lua` state — no shared globals between plugins.
- Shape: the chunk returns a table (or defines a global) with
  `invoke(method, payload) -> payload`; payloads cross the boundary as Lua
  values via mlua's serde bridge (JSON `Value` ⇄ Lua value).
- Limits: instruction-count hook (`Lua::set_hook`) aborting after a configured
  instruction budget; `Lua::set_memory_limit`; per-call deadline.
- A misbehaving script raises a Lua error, caught at the boundary and
  returned as `PluginError::Script`; the host task is never poisoned.

## 6. Selector-driven activation (`PluginManager`)

The manager reconciles the desired set (all synced plugin records) with the
running set:

1. Read the plugin's `selector` from its manifest.
2. Evaluate it with the existing `matches_selector` against the **node's own
   labels** (the same labels the node registers into membership).
3. Outcomes: match → load & serve; no match → ensure unloaded (the node
   neither serves nor advertises the plugin).
4. Engine-level config (`settings` in the manifest) is passed to the instance
   at load time.

Reconcile triggers: a `tokio::sync::Notify` fired by the resource-apply path
whenever a PLUGIN resource changes, plus a periodic 30 s safety-net pass.
Loads are lazy-safe: a failed load is logged and retried on the next trigger;
it never blocks reconcile.

## 7. Capability announcement and routing

Announcement: after each reconcile, the manager computes the set
`{plugin.<id> = <semver>}` for locally running plugins and pushes it into the
local member register's annotations via a new
`MembershipService::update_local_metadata` (labels stay untouched; the
heartbeat bump gossips the change through the existing Alive path). Capability
annotations are runtime-derived; they are *not* persisted into the node's
configured annotations.

Routing for `PluginService.Invoke`:

1. If the plugin runs locally → execute and return.
2. Else collect candidates from membership registers: state `Active` and an
   annotation `plugin.<id>` present. Order candidates by the existing peer
   policy (`peers::targets`: primary first, most-recently-seen first, failure
   backoff) — v1's definition of "nearest".
3. Forward via `PeerPool` with `origin_node_id` set. The receiving node
   executes locally and **never re-forwards** (hop limit 1): a request with
   `origin_node_id` set that still finds no local instance fails with
   `FAILED_PRECONDITION` instead of looping.
4. No candidates → `NOT_FOUND` ("no node currently serves plugin X").

`Sync`/`Membership` stay mTLS-only; `PluginService` sits behind the
cluster-key interceptor like `Cluster` — invoking plugins is an
admission-level operation, and forwarded calls reuse the daemon's cluster key.

## 8. Version management policy

- Convergence version: monotonic `u64` per plugin id (same model as
  skills/rules) — this is what anti-entropy orders by.
- Human version: `semver` string in the manifest, validated at ingest;
  announced in capability annotations so callers can make compatibility
  decisions.
- Upgrade = a new record with a higher `u64` version; nodes converge to it
  and reload. Old artifacts are replaced in the blob store (immutable
  content, single live version per id in v1; multi-version coexistence is a
  deliberate future extension keyed `id@semver`).
- Rollback = publishing the previous artifact as a new, higher version.

## 9. Configuration surface

Daemon TOML (node-local engine limits only):

```toml
[plugins]
wasm_fuel_per_call = 5_000_000
wasm_max_memory_bytes = 67_108_864   # 64 MiB
lua_instructions_per_call = 1_000_000
lua_max_memory_bytes = 33_554_432    # 32 MiB
invoke_timeout_ms = 10_000
```

Everything per-plugin (selector, hooks, capabilities, settings) lives in the
cluster-synced manifest, not in node config — nodes differ only through
labels, which is what makes selector-based activation meaningful.

Hook dispatch: the manifest's `hooks` array names hook points
(`"skill.invoke.pre"`, `"llm.call.post"`, …). The daemon owns a
`HookDispatcher`: workflow code emits `(point, context_json)`; the dispatcher
resolves subscribers from the synced registry (selector-filtered), invokes
them in manifest order through the same `PluginManager::invoke` path (so a
hook may run remotely), and applies each hook's `on_error` policy
(`"abort"` | `"ignore"`). v1 ships the dispatcher and its tests; concrete
hook points are declared by future workflow code.

## 10. Security notes

- WASM guests get no WASI capabilities; Lua guests get no io/os-execute.
  Declared `capabilities` in the manifest are the extension points for future
  host services and are validated on load (unknown capability → load error).
- Artifact integrity: `content_hash` (blake3) is verified at ingest (apply
  pipeline) and again at load time; a mismatch quarantines the plugin
  (not loaded, warning logged).
- Both engines impose hard instruction/fuel and memory budgets; a runaway
  guest cannot block the tokio runtime (execution happens on
  `tokio::task::spawn_blocking` for sync engine calls).
- Cluster-key admission + mTLS are unchanged; forwarding carries the
  forwarding node's identity, never the caller's credentials.

## 11. Testing strategy

- `lycoris-plugin`: Lua fixture (echo/transform script), WAT fixture compiled
  with the `wat` crate implementing `lycoris-abi-v1` (bump allocator + echo),
  sandbox escape attempts (io/os/debug absent), budget enforcement
  (infinite loop → budget error; balloon allocation → memory error).
- Storage: plugin apply pipeline reuses the versioned tests' shape; blob
  store id validation and content-before-metadata ordering.
- Daemon: selector activation (labels match / mismatch), capability
  annotation gossip, routing (local hit, forwarded hit with hop-limit-1,
  no-candidate `NOT_FOUND`), manifest validation failures.
- Integration: two-node test — plugin runs on node B only; invoke on node A
  returns B's result; registry convergence via existing resource sync.
- E2E fixture: a tiny Lua plugin registered through the CLI path
  (`cluster get plugins` lists it) — fixtures are test assets, not
  production plugins.

## 12. Rollout plan (this task)

1. `crates/plugin`: manifest model, engine trait, Lua engine, WASM engine,
   limits, unit tests.
2. proto (`PLUGIN` kind + `PluginService`) + storage plugin domain + blob
   store + mapper wiring.
3. daemon: `PluginManager`, capability announcement, routing/forwarding,
   `HookDispatcher`, config section, integration tests.
4. shell (`plugins` kind for `cluster get`), docs (README, crate READMEs,
   AGENTS.md), e2e fixture.
