# lycoris-ext-openai

OpenAI-compatible LLM provider extension: a WASM guest implementing the
`provides = ["llm"]` wire contract (`configure` / `chat` / `embed` /
`models`) of `docs/design/llm-provider.md` on top of the `lycoris.http`
host import. Any OpenAI-compatible HTTP API works — point `base_url` at it.

The crate is split so everything but the ABI glue is host-testable:
`src/provider.rs` is the pure core (settings parsing, request construction,
upstream response mapping), `src/lib.rs` is the wire dispatch, and the
`wasm32`-only glue exports `lycoris_alloc` / `lycoris_invoke` through
`lycoris_extguest::export_extension!`.

## Build

```sh
rustup target add wasm32-unknown-unknown
cargo build --locked --release --target wasm32-unknown-unknown -p lycoris-ext-openai
# artifact: target/wasm32-unknown-unknown/release/lycoris_ext_openai.wasm
```

## Package file

A package file is the TOML document `lycoris cluster ext load` registers
(see `e2e/fixtures/openai.pkg.toml` for a working example):

```toml
id = "openai"
name = "OpenAI"
version = 1                          # monotonic, must strictly increase per id
engine = "wasm"
artifact = "./lycoris_ext_openai.wasm" # relative to the package file
semver = "0.1.0"
provides = ["llm"]                   # makes the ext discoverable as an LLM provider
capabilities = ["http"]              # required: links the lycoris.http host import
selector = { role = "runner" }       # nodes carrying these labels activate the ext
```

Register it on any node; the package converges cluster-wide and only nodes
whose labels match `selector` load it:

```sh
lycoris cluster ext load openai.pkg.toml
```

## Per-node settings

Manifest settings are cluster-synced, so the API key must never live there.
Every node whose labels match the selector carries the secrets in its
node-local daemon config (`[extensions.local.<id>]`), merged over the
manifest settings at load time and delivered to the guest via `configure`.
Nothing in this section ever leaves the node.

```toml
[extensions.local.openai]
api_key = "sk-..."
base_url = "https://api.openai.com/v1"        # optional; this is the default
organization = "org-..."                       # optional; sent as the openai-organization header
http_allow_hosts = "[\"api.openai.com\"]"      # optional egress allowlist (JSON-encoded list,
                                               # enforced host-side; absent means any host)
```

| key                | required | meaning                                             |
| ------------------ | -------- | --------------------------------------------------- |
| `api_key`          | yes      | provider API key, sent as the bearer token          |
| `base_url`         | no       | API base; defaults to `https://api.openai.com/v1`   |
| `organization`     | no       | OpenAI organization id header                       |
| `http_allow_hosts` | no       | host-side egress allowlist (JSON list of hostnames) |

## Invoke

From any node (the call routes one hop to a node whose labels match the
selector; the payload prints to stdout, the routing decision to stderr):

```sh
lycoris cluster ext invoke openai chat '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'
lycoris cluster ext invoke openai embed '{"model":"text-embedding-3-small","input":["hello"]}'
lycoris cluster ext invoke openai models '{}'
```

Provider-side failures (upstream non-2xx, transport, not configured) come
back as the wire error document `{"error": {message, type, code?, status?}}`,
not as RPC failures.

## Tests

```sh
# host-side unit tests (no wasm toolchain needed)
cargo test -p lycoris-ext-openai

# end-to-end against the real wasm artifact and a mock server
# (requires the wasm32-unknown-unknown target)
cargo test -p lycoris-ext-openai --all-features --locked -- --ignored
```
