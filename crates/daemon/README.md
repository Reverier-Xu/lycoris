# lycoris-daemon

`lycoris-daemon` implements the runtime of a Lycoris cluster node.

## Responsibilities

- Starts the gRPC server and handles RPC requests from clients and other nodes; the cluster key is uniformly verified through a tonic interceptor.
- Maintains SWIM-style membership: probing neighbors, propagating suspected failures, and handling Join/Leave/Ping/PingReq.
- Performs anti-entropy synchronization of shared resources via Merkle trees and version vectors.
- Manages the node lifecycle: registration, startup, shutdown, and graceful cluster departure.
- Runs the extension subsystem (see `docs/design/extension-system.md`): the `ExtensionManager` reconciles synced extension records with locally running engine instances — label selectors in each manifest decide per-node activation against the node's configured `[node] labels` — republishes the running set as `ext.<id>` capability annotations on the local register, and routes invocations locally or one hop to a capable peer. `Extension.RegisterExtension` is the admission-side write path that brings packages into the cluster; the `HookDispatcher` invokes manifest-declared hook points through the same routing path.

## Main Modules

- `runtime`: node lifecycle and task scheduling.
- `transport`: peer connection pool, health tracking, and target selection.
- `membership`: membership state machine, SWIM probing, and Merkle sync service.
- `resource_sync`: shared-resource anti-entropy engine.
- `resource`: resource facade mapping between stored records and wire resources.
- `extension`: selector-driven extension manager, capability announcement, invocation routing, hook dispatch.
- `rpc`: gRPC server, resource and extension handlers, and the cluster-key interceptor.
- `cluster_sync`: cluster-wide shared-state synchronization logic.
