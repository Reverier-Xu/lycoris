---
type: implementation-plan
context: approved node overlay rewrite
status: active
---

# Libp2p Overlay Implementation Plan

## Quality gate for every code commit

```sh
cargo +nightly fmt --all
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Workflow changes additionally require `act`. The final dependency and release gates include the supported Linux musl, macOS, and Windows build paths.

## Atomic deliveries

- [x] Define the architecture, impact, invariants, and E2E acceptance contract.
- [x] Lock resource convergence behavior at the mapper and synchronization boundary.
- [x] Add overlay protocol identifiers, bounded wire types, and deterministic codecs.
- [ ] Persist node identities and the signed authorization registry.
- [ ] Add the single-owner libp2p link actor with QUIC and TCP/Noise/Yamux.
- [ ] Enforce known-peer authorization and deterministic duplicate-link arbitration.
- [ ] Add quarantined join-key and operator-approved enrollment.
- [ ] Add LAN discovery, address expiry, and reconnect behavior.
- [ ] Add relay reservations and DCUtR upgrade behavior.
- [ ] Add signed link-state and bounded sparse-graph request routing.
- [ ] Move membership traffic and routed probes onto the overlay.
- [ ] Move shared-resource synchronization onto `ResourceCarrier`.
- [ ] Move extension forwarding onto `ExtensionRouter`.
- [ ] Remove `PeerPool` and node-facing gRPC while retaining client control gRPC.
- [ ] Replace E2E with CLI-driven single-node, merge, partition, LAN-heal, and unrelated-peer recovery scenarios using a 10-second convergence deadline.

## Commit titles

1. `:memo: define the node overlay architecture`
2. `:white_check_mark: lock resource convergence behavior`
3. `:sparkles: add overlay protocol types`
4. `:sparkles: persist node identities and authorization records`
5. `:sparkles: add the libp2p link actor`
6. `:sparkles: enforce known peer authorization`
7. `:sparkles: add join key enrollment`
8. `:sparkles: add lan discovery and connection arbitration`
9. `:sparkles: add relay reservations and hole punching`
10. `:sparkles: add sparse overlay routing`
11. `:recycle: move membership traffic onto the overlay`
12. `:recycle: move resource sync onto the overlay`
13. `:recycle: route extension calls across the overlay`
14. `:recycle: remove peer grpc transport`
15. `:white_check_mark: cover overlay recovery topologies`

Commit bodies use imperative list items and document the focused tests and architectural boundary changed by that commit.

## Final release gate

- Every authorized node stores the same authorization registry after convergence.
- Every membership and shared-resource view converges within 10 seconds of path restoration.
- Known identity mismatch, wrong join key, replay, and foreign cluster admission fail closed.
- The final daemon exposes only client-facing gRPC services.
- The worktree is clean and the complete commit sequence is reviewed before one final push.
