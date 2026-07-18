English | [中文](README.cn.md)

# Lycoris

Lycoris is a decentralized cluster system for LLM agents. Every node is both a service provider and a cluster member, and the entire cluster is accessible through any node's API.

## Core Design

- Decentralized: the cluster is an undirected, cyclic, sparse graph; nodes probe each other and propagate state through a SWIM-style membership protocol.
- Partition tolerant: during a network partition, each partition keeps operating independently; once connectivity is restored, shared state is merged through anti-entropy synchronization.
- Shared and isolated: each node stores shared cluster base information, shared skills/rules, and a shared workspace metadata index, while owning its private memory and private workspace.

## Code Organization

```
crates/
  client      gRPC client handle: unified connection assembly and cluster key injection, used for node-to-node and CLI-to-node communication
  config      Daemon and client configuration parsing, validation, defaults, and fallback loading strategy
  core        Shared core primitives: cluster key, ResourceScope, time, and path conventions
  daemon      Cluster node runtime: transport connection pool, sync/ (SWIM dispatch, gossip, Merkle anti-entropy orchestration, peer selection),
              membership bridging (domain type boundary), resource facade, rpc/ (tonic boundary and cluster-key interceptor)
  membership  Membership CRDT (deterministic total-order merge), SWIM state machine, Merkle tree and anti-entropy diff (independent of tonic, no transport)
  proto       protobuf/gRPC definitions, with protocol constants and NodeInfo construction helpers
  shell       Unified binary entry point `lycoris`, providing subcommands such as daemon and cluster
  storage     Persistence layer: redb generic table storage for node metadata/workspace/skill/rule, LanceDB for agent memory;
              unified version model and anti-entropy apply pipeline
  tls         TLS certificate generation, loading, and automatic renewal (SAN includes the node advertise address)
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
