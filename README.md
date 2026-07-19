English | [中文](README.cn.md)

# Lycoris

Lycoris is a decentralized cluster system for LLM agents. Every node is both a service provider and a cluster member, and the entire cluster is accessible through any node's API.

## Core Design

- Decentralized: the cluster is an undirected, cyclic, sparse graph; nodes probe each other and propagate state through a SWIM-style membership protocol.
- Partition tolerant: during a network partition, each partition keeps operating independently; once connectivity is restored, shared state is merged through anti-entropy synchronization.
- Shared and isolated: each node stores shared cluster base information, shared skills/rules, and a shared workspace metadata index, while owning its private memory and private workspace.
- Extensible: extension packages are cluster-shared resources synchronized to every node through the same anti-entropy pipeline; two sandboxed engines (WASM via wasmtime, Lua via mlua) share one JSON invocation contract, label selectors decide per-node activation, capability announcements (`ext.<id>`) route invocations to a capable node (one hop), and packages enter the cluster through `lycoris cluster ext load`. See `docs/design/extension-system.md`.
- LLM providers as extensions: an OpenAI-compatible provider ships as a WASM ext (`extensions/openai`) implementing the `provides = ["llm"]` wire contract (`configure`/`chat`/`embed`/`models`); the API key lives in node-local config (`[extensions.local.openai]`), never in the synced manifest, and calls route one hop to the node whose labels match the selector. Minimal flow: `rustup target add wasm32-unknown-unknown`, `cargo build --locked --release --target wasm32-unknown-unknown -p lycoris-ext-openai`, then `lycoris cluster ext load openai.pkg.toml` and `lycoris cluster ext invoke openai chat '{"model":"...","messages":[{"role":"user","content":"hi"}]}'` from any node. See `docs/design/llm-provider.md` and `extensions/openai/README.md`.

## Code Organization

```
crates/
  client      gRPC client handle: unified connection assembly and cluster key injection, used for node-to-node and CLI-to-node communication
  config      Daemon and client configuration parsing, validation, defaults, and fallback loading strategy
  core        Shared core primitives: cluster key, ResourceScope, time, and path conventions
  daemon      Cluster node runtime: transport connection pool, sync/ (SWIM dispatch, gossip, Merkle anti-entropy orchestration, peer selection),
              membership bridging (domain type boundary), resource facade, rpc/ (tonic boundary and cluster-key interceptor),
              extension/ (selector-driven activation, capability announcement, invocation routing, hook dispatch)
  extension   Extension engine layer: package and manifest model, sandboxed WASM (wasmtime) and Lua (mlua) engines
              behind one JSON invocation contract
  extguest    Guest-side half of the extension ABI: the export_extension! macro and safe host::log / host::http
              wrappers for WASM guests
  membership  Membership CRDT (deterministic total-order merge), SWIM state machine, Merkle tree and anti-entropy diff (independent of tonic, no transport)
  proto       protobuf/gRPC definitions, with protocol constants and NodeInfo construction helpers
  shell       Unified binary entry point `lycoris`, providing subcommands such as daemon and cluster
  storage     Persistence layer: redb generic table storage for node metadata/workspace/skill/rule, LanceDB for agent memory;
              unified version model and anti-entropy apply pipeline
  tls         TLS certificate generation, loading, and automatic renewal (SAN includes the node advertise address)

extensions/
  openai      OpenAI-compatible LLM provider extension: WASM guest (cdylib) plus a host-testable pure core
```

## Build

```bash
cargo build --release -p lycoris
```

## Run a Node

```bash
lycoris daemon --config /path/to/lycoris.toml
```

## Test

```bash
cargo test --workspace --all-features
./e2e/run.sh
```

End-to-end suites under `e2e/`:

- `e2e/run.sh` — compose-based cluster test (docker compose or podman-compose); runs in CI.
- `e2e/shell-test.sh` — CLI-focused test; requires a local podman installation.
- `e2e/partition-test.sh` — network-partition test; requires local podman and `iptables` inside containers (`NET_ADMIN`).

The two podman suites are not part of CI and must be run locally.
