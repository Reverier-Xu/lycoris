---
type: execution-roadmap
status: active
supersedes: specs/plans/libp2p-overlay.md
based_on:
  - specs/tech-architecture/node-overlay.md
  - .pi-subagents/audits/current-*.md
last_reviewed_commit: 9b972cf
---

# Node Overlay Hardening and Completion Plan

## Reason for existence

The first overlay cutover moved production node traffic to libp2p, but the audit
at `9b972cf` proved that the system is not yet safe to extend with cluster merge.
This plan defines the only supported execution order from the current state to a
single, testable architecture.

The plan is complete when the release gate at the end passes. Passing unit tests
or compiling without warnings does not complete a phase whose process-level
acceptance remains open.

## Fixed objective

Lycoris is a fully trusted, single-operator, multi-device Agent cluster. Every
authorized node may control the cluster; identities without valid node
authorization or client configuration cannot operate cluster state.

The completed system supports unstable addresses, NAT, sparse undirected
connectivity, LAN discovery, relay, DCUtR, partitions, restart recovery, and
explicit foreign-cluster merge. It uses one address-independent cryptographic
`NodeId`, libp2p as the only daemon transport, and mTLS gRPC only for client
`Cluster`/`Extension` operations. Resource semantics remain unchanged and state
converges within 10 seconds after a usable path exists.

## Never rules

1. Never acknowledge authorization before its complete checkpoint is durable.
2. Never expose relay or cluster protocols to an unauthorized peer.
3. Never maintain two production sources of peer identity, reachability, or
   routing truth.
4. Never use `lycoris-client` or `tonic::Status` as a daemon-internal transport
   error model.
5. Never mutate membership from an unauthenticated caller-supplied node ID.
6. Never replace a populated foreign registry as if it were a solo-node join.
7. Never claim topology support from unit-level envelopes or test-only legacy
   adapters; acceptance uses real CLI and daemon processes.
8. Never wait longer than 10 seconds for a convergence assertion.
9. Never combine cleanup, behavioral repair, and boundary extraction in one
   commit unless they are inseparable for compilation.

## Current baseline

Working foundations to preserve are persisted key-derived identities,
known-peer fail-closed authorization, transcript-bound quarantined admission,
QUIC and TCP links, mDNS/relay/DCUtR wiring, routed membership/resource/extension
messages, two-daemon convergence tests, and a release daemon exposing only
`Cluster` and `Extension` gRPC services.

Blocking findings are one-hop link-state propagation, unauthorized relay access,
fail-open counters, old gRPC/URL peer surfaces, incomplete process acceptance,
and the absence of a populated foreign-cluster merge transaction.

## Delivery protocol

Each numbered delivery is one atomic commit. Before every code commit run:

```sh
cargo +nightly fmt --all
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --release --target x86_64-unknown-linux-musl -p lycoris --locked
cargo xwin clippy --workspace --all-targets --all-features --locked \
  --target x86_64-pc-windows-msvc -- -D warnings
cargo xwin test --workspace --lib --tests --all-features --locked \
  --target x86_64-pc-windows-msvc --no-run
```

The `cargo xwin` checks are mandatory when `cargo-xwin` is available and verify
that the Windows MSVC target remains warning-free and that every test binary
cross-links. They do not replace the native Windows test run. Apple targets
require the Apple SDK, so source review on non-Apple hosts is not release
evidence; the native `macos-latest` quality job and `aarch64-apple-darwin`
release build must pass before the final push is accepted.

Workflow changes additionally require `act`. Commit messages use gitmoji,
lowercase imperative summaries, and list-form bodies. `.pi-subagents/` remains
untracked.

## Phase 1: restore authorization and routing invariants

Merge work is prohibited until this phase passes.

- [x] **1.1 Commit authorization checkpoints atomically.**
  - Build and validate a prospective registry without mutating the live one.
  - Replace the complete redb table in one transaction.
  - Publish the committed registry to the link actor, then adopt it in
    enrollment.
  - Reject admission or checkpoint application on any failure.
  - Cover storage failure, actor failure, restart, and no-partial-checkpoint
    behavior.
- [x] **1.2 Make admission resumable and idempotent.**
  - Permit an exact active identity/key match to prove the join key again and
    retrieve its existing record plus current checkpoint.
  - Reject every NodeId, initial-key, current-key, or peer-id mismatch; resume
    uses the sponsor's current record epoch rather than caller-supplied state.
  - Scope request IDs with a secure random 128-bit boot identifier and a
    monotonic sequence that fails closed before wrap. Cross-boot collisions are
    bounded by the 128-bit wire identifier rather than claimed impossible.
  - Bind every admission challenge and outcome to the dialed sponsor PeerId,
    exact candidate identity, and a uniquely authorized checkpoint record.
  - Cover a discarded committed outcome, sponsor restart, joiner restart, wrong
    key, response mismatch, sequence exhaustion, and replay.
- [x] **1.3 bind persisted registry to local identity at startup.**
  - Never generate a new identity when authorization state already exists.
  - Require the local NodeId, PeerId, and current key to match one active record.
  - Publish identity files through synced atomic replacement and no-clobber first
    publication.
  - Native Linux, macOS, and Windows filesystem behavior passed for implementation
    commit `5d38217` in quality run `30655272596`; universal power-loss and
    nonconforming filesystem claims remain excluded.
- [ ] **1.4 authorize relay and bound quarantine.**
  - Reject relay reservation and circuit use by unknown or inactive PeerIds.
  - Add quarantine TTL, per-peer request limits, bounded pending responses, and
    cleanup on malformed requests or inbound failure.
  - Cover unknown relay denial, authorized relay success, and capacity recovery.
- [ ] **1.5 flood signed link-state records.**
  - Forward each newer `(origin, sequence)` record to authorized neighbors except
    ingress.
  - Bound deduplication, expiry, and storage.
  - Cover four- and five-node chains, stale records, partition, and healing.
- [ ] **1.6 make monotonic state fail closed.**
  - Distinguish missing counters from corrupt or unreadable counters.
  - Persist boot incarnation and gossip sequence before publication.
  - Fail startup or allocation when durability cannot be established.

**Phase gate:** injected failures never produce split registry state; unauthorized
relay use fails; a five-node chain routes end to end; restart never reuses a
membership incarnation or gossip sequence.

## Phase 2: remove the old peer architecture

- [ ] **2.1 remove obsolete live Cluster mutations.**
  - Delete `Cluster.Join` and `Cluster.SetPrimaryEndpoint` from proto, daemon,
    client, shell, examples, and tests.
  - Delete raw `Cluster.Register`, or replace it with an authorization-backed
    operator command that cannot inject an arbitrary membership register.
  - Retain resource reads, extension operations, and an authorization-aware
    leave/revoke operation.
- [ ] **2.2 migrate legacy transport fixtures.**
  - Replace test `PeerPool`, tonic Membership/Sync servers, and legacy pool
    variants with an overlay harness or narrow in-process protocol fake.
  - Preserve anti-entropy, resource merge, and extension error-path coverage.
- [ ] **2.3 delete node-facing gRPC contracts.**
  - Remove Membership and Sync service declarations while retaining the Prost
    message types used by overlay envelopes.
  - Delete `PeerClient`, node RPC server source, legacy adapters, and dead
    comment-only integration targets.
- [ ] **2.4 delete URL peer and primary state.**
  - Remove `bootstrap_peers`, `PeerStorage`, primary endpoint, URL backoff/ranking,
    runtime seeding, and gossip endpoint seeding.
  - Keep `node.address` only as a clearly named control-plane address.
- [ ] **2.5 update public documentation.**
  - Describe libp2p as the only node transport and gRPC as client control only.
  - Remove `PeerPool`, HTTPS node bootstrap, and old join instructions.

**Phase gate:** repository search finds no node gRPC service/client, `PeerPool`,
primary endpoint, or URL bootstrap implementation; release behavior remains green.

## Phase 3: correct crate ownership before merge

- [ ] **3.1 introduce a transport-neutral peer error boundary.**
  - Sync, resource, membership, and extension routing return daemon/domain errors.
  - Overlay and control-plane adapters translate only at their outer boundaries.
  - Remove the daemon production dependency on `lycoris-client`.
- [ ] **3.2 extract identity and authorization as a pure domain crate.**
  - Own NodeId, identities, signed records, registry validation, enrollment, and
    future merge records without libp2p runtime or filesystem I/O.
  - Overlay adapts authorized peer material to `PeerId`; storage implements the
    repository; shell uses an identity repository.
- [ ] **3.3 centralize authorization application.**
  - One service owns validation, durable checkpoint replacement, live publication,
    and enrollment adoption.
  - Daemon composition supplies repository and link ports; it does not duplicate
    the workflow in admission and gossip paths.
- [ ] **3.4 establish a transport-independent resource domain.**
  - Own resource IDs, scope, version ordering, integrity, and typed aggregates.
  - Keep redb, LanceDB, Git paths, and Prost DTOs in adapters.
  - Remove duplicate metadata-map authority and stop replicating bare local
    workspace paths as cluster-shared meaning.
- [ ] **3.5 unify extension aggregate and selector truth.**
  - Extension owns validated package metadata and activation predicates.
  - Storage persists that aggregate; daemon coordinates execution and routing.
  - CLI renders daemon-reported activation instead of re-evaluating config labels.
- [ ] **3.6 remove proven abstraction entropy.**
  - Replace single-implementation, non-injectable storage trait objects with
    concrete repositories or make injection real.
  - Remove pass-through helpers such as `peer_timeout`.

**Phase gate:** Cargo dependency direction is domain -> ports -> adapters;
daemon internals expose no tonic/client types; each business invariant has one
owner and one implementation.

## Phase 4: build the real process acceptance harness

- [ ] **4.1 replace stale shell/container E2E.**
  - Start real `lycoris daemon` processes with isolated config/data directories.
  - Drive setup, identity, join, resources, extensions, and status through the
    compiled CLI.
  - Capture stable `/p2p/<peer-id>` sponsor descriptors.
- [ ] **4.2 implement one hard-deadline poller.**
  - Bound each CLI probe and the total assertion to 10 seconds using monotonic
    time.
  - Include a harness self-test that deliberately times out.
- [ ] **4.3 expose authorization attestation.**
  - Add a read-only CLI/control response for cluster ID and deterministic registry
    digest so tests can distinguish membership convergence from authorization
    convergence.
- [ ] **4.4 cover the pre-merge matrix.**
  - Single-node CLI operations.
  - Same-cluster two-node enrollment and restart.
  - Four-node sparse routing for membership, resources, and extension invocation.
  - Wrong key, replay, identity mismatch, foreign cluster, and no-resource-access
    failures.

**Phase gate:** CI executes the new harness; no process assertion waits beyond 10
seconds; old scripts and topology assumptions are deleted.

## Phase 5: implement explicit foreign-cluster merge

- [ ] **5.1 add target-cluster import authorization records.**
  - Re-anchor each active source identity, including rotated identities, while
    preserving initial-key NodeId binding and target sponsor action ordering.
- [ ] **5.2 define deterministic merge wire types and proofs.**
  - Bind merge ID and HMAC transcript to both cluster IDs, complete source
    checkpoint digest, bridge/sponsor identities, nonces, and protocol version.
  - Choose one deterministic target cluster to prevent opposite-direction merges.
- [ ] **5.3 persist merge plans and phases atomically.**
  - Store prepared, staged, promoting, promoted, and cleaned state beside the
    authorization checkpoint in crash-consistent transactions.
  - Every retry returns or advances the same plan.
- [ ] **5.4 add one bounded transition plane to the link actor.**
  - Authenticate physical peers against primary plus one transition registry.
  - Route merge control by envelope cluster ID on either plane.
  - Run application protocols only on the primary plane.
  - Promote without dropping physical connections; clean the old plane only after
    all reachable participants attest promotion.
- [ ] **5.5 implement prepare, validate, promote, recover, and cleanup.**
  - Every source node verifies that the target checkpoint imports all active source
    identities before staging.
  - Offline nodes resume the persisted plan after restart.
  - Partial promotion leaves old-plane merge routing available.
- [ ] **5.6 expose the operator merge command and status.**
  - CLI drives an explicit bridge operation; ordinary `join` remains solo-node
    enrollment only.

**Phase gate:** two independent two-node clusters merge into one authorization,
membership, resource, and extension view; crash/restart at every phase completes
idempotently without stranding an identity.

## Phase 6: complete topology acceptance

- [ ] Bridge two divergent same-cluster partitions and verify anti-entropy.
- [ ] Heal components through mDNS with no configured cross-component address.
- [ ] Run the ignored live mDNS smoke and the process-level healing scenario on
  a multicast-capable host; hosted CI without multicast does not count as
  evidence.
- [ ] Recover a node through an authorized peer with no historical logical edge.
- [ ] Establish service through relay, observe DCUtR direct upgrade, remove the
  relay, and preserve operation.
- [ ] Remove bridge nodes and verify sparse rerouting or bounded unreachability.
- [ ] Assert membership, resource, extension capability, invocation, and registry
  convergence through CLI within 10 seconds.

## Phase 7: finish crash consistency and repository hygiene

- [ ] Publish TLS certificate/key bundles as validated atomic generations.
- [ ] Store extension and workspace content immutably by hash/version so metadata
  never points to torn or mismatched bytes.
- [ ] Track and join registry broadcast tasks during shutdown.
- [ ] Propagate `ResourceSync::sync_with_peer` failures instead of returning false
  success.
- [ ] Reject unknown wire membership states instead of promoting them to Active.
- [ ] Remove orphaned docs, examples, fixtures, and duplicate generated contracts.
- [ ] Run an independent final security, boundary, redundancy, and user-flow audit.

## Final release gate

All conditions are mandatory:

- [ ] Every authorized node has one durable registry and the same registry digest.
- [ ] Unknown identities cannot invoke cluster protocols or relay traffic.
- [ ] Five-node sparse routing and every required recovery topology work.
- [ ] Foreign merge is explicit, atomic, restart-safe, and idempotent.
- [ ] Daemon-to-daemon gRPC, URL peer state, and test-only compatibility adapters
  are absent.
- [ ] Daemon business code does not depend on client/tonic transport errors.
- [ ] Real CLI E2E enforces the 10-second convergence deadline.
- [ ] Fmt, Clippy, workspace tests, Linux musl, and applicable `act` runs pass.
- [ ] Native macOS and Windows GitHub CI and release builds pass for the final
  commit on `main`.
- [ ] Worktree contains no untracked output except the intentionally retained
  `.pi-subagents/` audit artifacts.
- [ ] The complete atomic commit sequence receives an independent review before
  release.

## Immediate next step

After native final-candidate CI completes **1.3**, implement **1.4 Authorize relay
and bound quarantine**. Do not begin merge wire types or dual-plane routing
before every Phase 1 gate passes.

Verify this document remains actionable:

```sh
test -f docs/design/node-overlay-hardening-plan.md
rg -n '^## Phase [1-7]|^## Final release gate|^## Immediate next step' \
  docs/design/node-overlay-hardening-plan.md
```
