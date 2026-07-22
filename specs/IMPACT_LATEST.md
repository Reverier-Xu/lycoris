---
type: impact-analysis
context: rust-libp2p node overlay rewrite
status: approved
---

# Overlay Rewrite Impact

## Target

Replace `crates/daemon/src/transport.rs::PeerPool` and node-to-node tonic RPCs with a typed rust-libp2p overlay while preserving client gRPC and resource behavior.

## Dependents

- `crates/daemon/src/runtime.rs`: constructs peer transport, starts synchronization, and registers all tonic services.
- `crates/daemon/src/sync/mod.rs`: owns `PeerPool` and synchronization task lifecycle.
- `crates/daemon/src/sync/antientropy.rs`: membership Merkle and full-set exchanges.
- `crates/daemon/src/sync/gossip.rs`: membership register/state fanout and deduplication.
- `crates/daemon/src/sync/swim.rs`: probes and state transitions.
- `crates/daemon/src/sync/resource.rs`: shared-resource exchange around `ResourceMapper`.
- `crates/daemon/src/extension/mod.rs`: remote capability invocation.
- `crates/daemon/src/rpc/cluster.rs`: inbound node tonic adapters.
- `crates/client/src/client.rs`: aggregates public and peer-only tonic handles.
- `crates/config/src/daemon.rs`: conflates public API and peer addresses.
- `crates/config/src/client.rs`: derives the public API address from daemon config.
- `crates/storage/src/node/peers.rs`: persists URL-keyed peer health and primary endpoint.
- `crates/membership/src/register.rs`: stores one address string in each member record.
- `crates/proto/proto/node.proto`: combines retained public services and removable peer services.
- `crates/daemon/tests/integration.rs`: tests convergence, failover, resource replication, and extension routing through the old transport.
- `e2e/*.sh`: creates address-bound daemon configurations and tests CLI flows.

## Unchanged modules

- Storage resource merge ordering and integrity validation.
- Resource scope and local/shared visibility.
- Membership CRDT, Merkle tree, and Merkle diff algorithms where they are transport independent.
- Client-facing Cluster and Extension behavior.
- Extension package and engine semantics.

## Test coverage

Existing strong coverage:

- `crates/storage/src/versioned.rs`: version, timestamp, hash ordering, idempotence, and scope guards.
- `crates/storage/src/agent/mod.rs`: memory merge, integrity, scope, and concurrent convergence.
- `crates/storage/src/workspace/mod.rs`: skill, rule, and workspace application behavior.
- `crates/storage/src/extension/mod.rs`: extension integrity and convergence.
- `crates/daemon/src/resource/mapper.rs`: mapper validation and selected apply/export paths.
- `crates/daemon/tests/integration.rs`: positive multi-node resource convergence and partition recovery.

Coverage gaps to close before transport cutover:

- `ResourceSync::merge_and_list_shared` does not directly prove union, invalid-record isolation, or idempotence.
- `ResourceMapper` does not characterize all synchronized kinds through the Proto-to-storage boundary.
- No test proves the exported synchronization set contains full payloads for every shared kind while excluding all local records.
- No authenticated `NodeId` to `PeerId` binding exists.
- No bounded overlay codec, backpressure, cancellation, or admission replay tests exist.
- Existing E2E does not cover independent-cluster merge, mDNS healing, or recovery through a previously unrelated node.

## Risk: High

The transport is a shared dependency of membership, resources, extensions, configuration, persistence, and daemon startup. Identity and address are currently coupled, and the old peer store cannot be losslessly converted to authenticated PeerIds. The project explicitly permits an incompatible cutover, which removes rolling-upgrade and data-migration obligations but not state and security correctness obligations.

## Recommended action

Proceed incrementally:

1. lock resource behavior with transport-independent characterization tests;
2. add identity and authorization persistence before accepting network traffic;
3. start an inert libp2p actor alongside the old transport and validate platform builds;
4. implement admission, discovery, relay, and routing behind a typed handle;
5. migrate membership, resources, and extension forwarding separately;
6. remove peer gRPC only after all consumers have moved;
7. replace address-bound integration and E2E harnesses with topology-driven tests;
8. enforce a 10-second convergence SLO in integration and E2E tests.

## Residual risks

- QUIC and relay behavior varies across real NATs; local namespace tests cannot fully model carrier-grade NAT.
- A 10-second convergence target requires explicit timer budgets and may expose slow CI hosts.
- Concurrent identity recovery in disconnected partitions must fail closed as a conflict.
- Resource snapshots can approach the old 64 MiB unary limit; bounded paging must preserve the current final-state semantics.
