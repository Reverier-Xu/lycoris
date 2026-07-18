# lycoris-daemon

`lycoris-daemon` implements the runtime of a Lycoris cluster node.

## Responsibilities

- Starts the gRPC server and handles RPC requests from clients and other nodes; the cluster key is uniformly verified through a tonic interceptor.
- Maintains SWIM-style membership: probing neighbors, propagating suspected failures, and handling Join/Leave/Ping/PingReq.
- Performs anti-entropy synchronization of shared resources via Merkle trees and version vectors.
- Manages the node lifecycle: registration, startup, shutdown, and graceful cluster departure.

## Main Modules

- `runtime`: node lifecycle and task scheduling.
- `transport`: peer connection pool, health tracking, and target selection.
- `membership`: membership state machine, SWIM probing, and Merkle sync service.
- `resource_sync`: shared-resource anti-entropy engine.
- `rpc`: gRPC server, resource handlers, and the cluster-key interceptor.
- `cluster_sync`: cluster-wide shared-state synchronization logic.
