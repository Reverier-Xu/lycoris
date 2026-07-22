---
type: architecture
context: replace the node-to-node transport without changing resource semantics
status: approved
---

# Node Overlay Architecture

## Objective

Replace address-bound node-to-node gRPC with a trusted rust-libp2p overlay that supports unstable addresses, NAT, LAN discovery, relays, sparse topologies, partitions, and recovery. Keep client-to-daemon gRPC as the operator control plane.

## Non-negotiable invariants

1. Every authorized node is fully trusted and may administer the cluster.
2. Unauthorized identities cannot invoke cluster protocols or relay traffic.
3. Node identity is independent of IP, DNS, connection direction, TLS SANs, and dial addresses.
4. A known `NodeId` presented with a different key is rejected. Join credentials never override an identity mismatch.
5. A join key is optional and is proved with a transcript-bound MAC; it is never sent over the protocol.
6. Every node persists the complete authorization registry, including rotations, recoveries, conflicts, and revocations.
7. An established link is full duplex and represented as one logical undirected edge regardless of its dial direction.
8. Resource scope, version ordering, content-hash validation, conflict resolution, storage records, and apply semantics remain unchanged.
9. Membership and shared-resource state must converge within 10 seconds after a usable path exists.
10. No compatibility layer is required for the old peer protocol or old clusters.

## Layering

```text
operator client
  -> retained mTLS gRPC Cluster/Extension API

daemon domain services
  -> MembershipProtocol / ResourceCarrier / ExtensionRouter
  -> OverlayHandle (typed commands and events)
  -> OverlayActor (single owner of libp2p Swarm)
  -> QUIC or TCP + Noise + Yamux
  -> direct socket or circuit relay
```

Libp2p types and events stay inside the overlay module. Domain code addresses peers by `NodeId`, not `PeerId`, `Multiaddr`, URLs, or connection IDs.

## Transport profile

Use libp2p 0.56 with default features disabled.

Initial features:

- Tokio runtime
- QUIC as the preferred direct transport
- TCP + Noise + Yamux as the fallback transport
- Identify for signed capability and observed-address exchange, never authorization
- Ping for link health, never direct node-offline decisions
- mDNS for LAN address candidates
- circuit relay v2 for one-relay connectivity
- DCUtR for best-effort direct connection upgrade
- request-response for bounded typed protocols
- macros for the composed network behavior

Do not initially enable WebSocket, Kademlia, gossipsub, or AutoNAT. WebSocket is an optional later transport only if real deployments require HTTP proxy traversal. Persistent convergence uses the existing anti-entropy model rather than pub-sub delivery.

## Identity and authorization

`NodeId` is derived from the node's initial Ed25519 public key and remains the stable logical identity. The current libp2p identity key derives `PeerId`. Rotation or recovery changes `PeerId` while preserving `NodeId` through an authorized registry operation.

```text
AuthorizationRecord {
  cluster_id,
  node_id,
  peer_id,
  identity_public_key,
  epoch,
  state: active | revoked | conflicted,
  predecessor,
  authorized_by,
  signature
}
```

The registry is an append-only set of signed operations. Replication merges operations by hash. A node's effective record follows a single increasing epoch chain. Concurrent operations for the same predecessor and epoch produce `conflicted`; neither candidate is authorized until another authorized node resolves the fork.

Normal rotation is signed by the old identity. Lost-key recovery is signed by any active authorized node. If no authorized node or offline recovery material remains, the old identity cannot be recovered cryptographically and the cluster must be reinitialized.

## Admission state machine

Known peer:

```text
transport authenticated
  -> map PeerId to AuthorizationRecord
  -> exact active key and epoch match
  -> Authorized

missing record or mismatch
  -> RejectIdentity
```

A mismatch for an existing `NodeId` never enters the join path.

Unknown peer with a configured join key:

```text
transport authenticated
  -> Quarantined
  -> exchange nonces and admission metadata
  -> HMAC(join_key, protocol_version || cluster_id || both PeerIds ||
          both nonces || requested NodeId || sponsor PeerId)
  -> sponsor persists and signs Admit
  -> registry checkpoint returned
  -> disconnect and reconnect as a known peer
```

Unknown peer without a join key remains quarantined until an operator approves its fingerprint through an authorized client connection. Quarantined peers may use only the bounded admission protocol.

## Links, addresses, and routing

Addresses are expiring candidates with provenance, not identity. The peer directory stores `NodeId`, current authorized `PeerId`, multiple `Multiaddr` values, source, last success, and expiry.

Duplicate connections are resolved deterministically by path preference, then the canonical initiator (the smaller `PeerId`), then a connection nonce. Preference is direct QUIC, direct TCP, relayed QUIC/TCP. Closing a duplicate never changes the logical edge.

Circuit relay creates an end-to-end libp2p connection through one relay. Arbitrary sparse-graph paths use application forwarding:

```text
RouteEnvelope {
  request_id,
  source,
  destination,
  protocol,
  hop_limit,
  deadline,
  body
}
```

Each node advertises signed link state for its own confirmed edges. Nodes compute shortest routes locally. Forwarders enforce deadline, hop limit, bounded body size, duplicate suppression, and backpressure. All forwarders are trusted cluster members.

## Membership and recovery

Link failure and node failure are separate. Libp2p ping updates link health. Existing membership transitions are driven by routed end-to-end probes and anti-entropy, so a failed direct edge does not mark a node offline while another route works.

When partitions reconnect, synchronization order is:

1. authorization registry;
2. address and link-state records;
3. membership anti-entropy;
4. shared-resource anti-entropy;
5. extension capability refresh.

Only partitions with the same `ClusterId` merge automatically. Independently initialized clusters require an explicit authorized join between one node from each side; after admission, registry and domain state converge over the new bridge.

## Resource boundary

The following remain behaviorally unchanged:

- `ResourceScope` and synchronization eligibility;
- version, timestamp, and content-hash ordering;
- content-hash integrity checks;
- local-resource exclusion;
- `ResourceMapper::apply_resource`;
- `ResourceMapper::local_shared_resources`;
- storage record formats and conflict behavior.

`ResourceCarrier` replaces only the request/response transport around the current mapper. Resource payloads remain Prost values during the cutover. Pagination or streaming may bound memory, but it must reconstruct the same set before applying it.

## gRPC boundary

Retain the client-facing `Cluster` and `Extension` services, their mTLS listener, and client configuration. Remove the node-facing `Membership` and `Sync` services after the overlay cutover. `Extension.Invoke` remains public gRPC, while daemon-to-daemon forwarding moves behind `ExtensionRouter`.

## End-to-end acceptance

The final E2E suite must exercise the real CLI and daemon lifecycle with a hard 10-second convergence deadline:

1. initialize one node and run every supported cluster/resource/extension command applicable to one node;
2. initialize multiple nodes as two independent clusters;
3. join one node from each cluster and verify the two clusters merge into one authorization, membership, and resource view;
4. remove the bridge node or bridge link so the sparse graph partitions into two components;
5. verify LAN mDNS discovery establishes a new edge and both components reconverge without configured direct addresses;
6. disconnect and restart a node, then recover it through an authorized node with which it had no previous logical edge;
7. assert wrong join key, replayed proof, known identity mismatch, and foreign `ClusterId` fail without resource access;
8. assert every convergence wait fails at 10 seconds rather than sleeping for a fixed duration.

The test harness must expose topology operations and poll observable CLI state. Tests must not depend on implementation-only connection internals for correctness assertions.

## Delivery and commit policy

Each logical step is one gitmoji commit with a lowercase summary and list-form body. Every code commit includes focused tests and passes nightly formatting, workspace Clippy with warnings denied, and the full workspace test suite. No commit is pushed until all overlay and E2E gates pass.
