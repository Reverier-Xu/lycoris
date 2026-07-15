# lycoris-daemon

`lycoris-daemon` 实现 Lycoris 集群节点的运行时。

## 职责

- 启动 gRPC server，处理来自客户端与其他节点的 RPC 请求。
- 维护 SWIM 风格的成员关系：探测邻居、传播疑似失效、处理 Join/Leave/Ping/PingReq。
- 通过 Merkle tree 与版本向量对共享资源进行反熵同步。
- 管理节点生命周期：注册、启动、关闭、优雅离开集群。

## 主要模块

- `runtime`：节点生命周期与任务调度。
- `membership`：成员状态机、SWIM 探测、Merkle 同步服务。
- `rpc`：gRPC server 与资源同步 handler。
- `cluster_sync`：集群级共享状态同步逻辑。
