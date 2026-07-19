# LLM Provider Design

Status: v1. Builds on `extension-system.md`; read that first. Short form of
extension: ext. This milestone ships the provider abstraction, the extension
system upgrades real providers need (outbound HTTP, per-node settings), and
one production-grade guest: an OpenAI-compatible provider as a WASM ext.

## 1. Layering

```
typed callers (future agent workflow, tests)
        │  ChatRequest / ChatResponse (Rust types)
        ▼
LlmProvider trait ── implemented by ──► ExtensionLlmProvider
(host facade, lycoris-extension)         (extension id + ExtensionManager)
        │  invoke("chat", json) — local or routed (hop-limit-1)
        ▼
guest ext (wasm)  ── lycoris_http host fn ──► provider HTTPS API
```

Three contracts, each independently testable:

1. **Typed trait** (host): what Rust callers see.
2. **Method/JSON convention** (wire): what any ext must implement to be an
   LLM provider — engine-agnostic, so a Lua ext could implement it too.
3. **Provider mapping** (guest): OpenAI request/response translation.

## 2. Typed trait (`lycoris-extension::llm`)

Types are OpenAI-flavored, engine-neutral, serde-serializable both ways:

- `ChatMessage { role: Role, content: String }`, `Role = System|User|Assistant|Tool`
- `ChatRequest { model, messages, temperature?, max_tokens? }`
- `ChatResponse { model, choices: Vec<Choice>, usage: Option<Usage> }`,
  `Choice { index, message, finish_reason }`,
  `Usage { prompt_tokens, completion_tokens, total_tokens }`
- `EmbedRequest { model, input: Vec<String> }`,
  `EmbedResponse { data: Vec<Embedding> }`
- `LlmError`: `Provider { status, message }` (upstream said no),
  `Unavailable` (no provider reachable), `Extension(ExtensionManagerError)`
  passthrough, `InvalidResponse` (guest returned something off-contract).

```rust
#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, LlmError>;
    async fn embed(&self, request: EmbedRequest) -> Result<EmbedResponse, LlmError>;
    async fn models(&self) -> Result<Vec<String>, LlmError>;
}
```

`ExtensionLlmProvider { manager, extension_id }` implements the trait by
serializing to the wire convention and calling `ExtensionManager::invoke` —
so locality, routing, and hop-limit semantics are inherited unchanged.

`LlmRouter` (daemon): resolves which extension serves LLM calls — an
explicitly named ext, or the single registered provider (ambiguous →
`Unavailable`). v1 keeps selection static and explicit.

## 3. Wire convention (what an ext implements)

An ext is an LLM provider when its manifest carries `provides = ["llm"]`
(new manifest key, JSON array of contract names; discovery for `LlmRouter`).
Methods on the instance:

- `chat` — request: the `ChatRequest` JSON, plus `stream: false` (streaming
  is out of scope for invoke semantics). Response: the `ChatResponse` JSON.
- `embed` — `EmbedRequest` → `EmbedResponse` JSON.
- `models` — `{}` → `{ data: [{id}] }` (OpenAI shape; facade flattens).
- `configure` — see §5.

Error convention: the payload is `{ "error": { "message", "type", "code"? } }`
when the upstream provider failed; the facade maps that to
`LlmError::Provider`. Transport/engine failures are engine errors, never
synthesized error payloads.

## 4. Outbound HTTP capability (`lycoris-abi-v1` upgrade)

Real providers need egress. The `http` capability is added to the manifest's
known set and gates a new host function in the `lycoris` module:

```wat
(func $lycoris_http (param i32 i32) (result i64)) ;; ptr,len -> packed ptr,len
```

- Request JSON: `{ "method", "url", "headers": {...}, "body": string? }`.
  Bodies are text (provider APIs are JSON); binary is base64 with
  `"body_b64"` in a later revision.
- Response JSON: `{ "status": u16, "headers": {...}, "body": string }`.
- Host enforcement: only `http:`/`https:` URLs; response body capped at
  8 MiB; requests run inside the engine's invoke deadline; the host fn is
  **not linked** unless the manifest declares `"http"`, so a guest importing
  it without the capability fails to instantiate.
- Optional `http_allow_hosts` (list of host names) in settings: when present,
  the host rejects requests to other hosts with a structured error. Egress
  policy stays declarative and per-node (settings merge, §5).
- Client: `ureq` 3 with rustls/ring (small transitive footprint, matches the
  existing rustls choice; blocking calls run on `spawn_blocking` inside the
  async host fn).

## 5. Per-node settings and secrets

Manifest `settings` are cluster-synced — so API keys must never live there.
Providers need per-node, non-replicated configuration (keys, base URL,
region). New daemon config section:

```toml
[extensions.local.openai]
api_key = "sk-..."
base_url = "https://api.openai.com/v1"
# local values are strings, so lists ride JSON-encoded
http_allow_hosts = "[\"api.openai.com\"]"
```

Merge semantics at load time: `resolved = manifest.settings` overlaid by
`[extensions.local.<id>]` (local wins, key by key; local values are strings,
so they land in the merged document as JSON strings). Nothing in
`[extensions.local]` ever leaves the node — it is not in any synced record.

Delivery contract: the engine itself invokes `configure` with the resolved
settings JSON inside `load`, after instantiation and before the instance is
handed back to the manager and registered as servable. A `configure`
failure fails the load; a guest predating the convention (an
unknown-method class error) is tolerated and simply runs without settings.
`configure` is idempotent and re-issued on reload. Guests treat every
method as stateless until `configure` has run (the OpenAI guest answers
`Provider { status: 0, message: "not configured" }` before that).

## 6. Guest support crate and the OpenAI ext

- `crates/extension-guest` (`lycoris-extension-guest`): the guest-side half
  of `lycoris-abi-v1` — `export_extension!` macro emitting `lycoris_alloc` /
  `lycoris_invoke` shims that dispatch to a guest
  `fn invoke(method, payload) -> Result<Vec<u8>, String>`; safe `host::log`
  and `host::http` wrappers over the extern imports (extern block only
  compiled for `wasm32`). Deps: serde/serde_json only.
- `extensions/openai` (`lycoris-ext-openai`, workspace member, `cdylib` +
  `lib`): pure, host-testable core —
  `chat_request_to_openai(ChatRequest, &Settings) -> HttpRequestSpec`,
  `openai_to_chat_response(status, body) -> Result<ChatResponse, LlmError>`,
  and the same split for embed/models. The `wasm32` glue binds these to the
  exported `invoke`; host tests drive the pure functions plus a mock
  transport, so no wasm toolchain is needed for unit tests.
- Settings: `api_key` (required), `base_url` (default
  `https://api.openai.com/v1`), optional `organization`.
- Behavior: POST `{base}/chat/completions`, `{base}/embeddings`,
  GET `{base}/models`; non-2xx → the §3 error payload; response mapped into
  the §2 types (unknown upstream fields ignored).

## 7. Build and test strategy

- `extensions/build.sh`: `cargo build --release --target
  wasm32-unknown-unknown -p lycoris-ext-openai`; the artifact
  (`lycoris_ext_openai.wasm`) is the registrable ext. `rustup target add
  wasm32-unknown-unknown` is documented and added to CI.
- Unit: pure transform tests (host), settings merge, capability gating
  (import without declaration → instantiate error), HTTP host fn against a
  local mock (status passthrough, size cap, host allowlist).
- Daemon integration: build the real `.wasm` (test invokes
  `extensions/build.sh`; fails loudly if the target is missing), register it
  on a node whose label matches, invoke `chat` from a *second* node against a
  mock OpenAI server (tiny tokio HTTP server in the test) — asserts sync,
  capability announcement, routing, HTTP egress, and response mapping.
- E2E (`e2e/shell-test.sh`): a mock `openai` container (nginx returning a
  canned chat completion), node-1 labelled `role=runner` with
  `[extensions.local.openai] base_url=http://openai-mock/v1`, register via
  `ext load` on node-0, `ext invoke openai chat ...` on node-0 — assert
  routed execution and canned content. No real API key, ever.

## 8. Explicit non-goals

- Streaming chat (needs a streaming invoke primitive — separate design).
- Token counting/rate limiting/cost accounting.
- Provider failover policies beyond the existing ext routing order.
- A typed `Llm` proto service: the generic `Extension.Invoke` path is the
  call surface for this milestone; the typed trait serves in-process callers.
