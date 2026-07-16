# Lycoris

Lycoris 是一个面向 LLM Agent 的去中心化集群系统。每个节点既是服务提供者也是集群成员，从任意节点的 API 都可以访问整个集群。

## 核心设计

- 去中心化：集群是无向有环稀疏图，节点通过 SWIM 风格的成员协议相互探测、传播状态。
- 分区容忍：网络分区期间，各分区可继续独立运行；重新连通后通过反熵同步合并共享状态。
- 共享与隔离：每个节点保存共享的集群基础信息、共享 skill/rule、共享工作区元数据索引，同时拥有专属的 memory 与专属工作区。

## 代码组织

```
crates/
  client      gRPC 客户端，用于节点间以及 CLI 与节点通信
  config      守护进程与客户端配置解析、校验与默认值
  core        共享核心原语：节点标识、membership 状态、cluster key、ResourceScope、selector、time、validation、路径约定
  daemon      集群节点运行时：transport 连接池、membership 同步、resource_sync 反熵、gRPC server 与 cluster-key 拦截器
  membership  成员 CRDT、SWIM 状态机、Merkle 树与反熵（独立于 tonic）
  proto       protobuf/gRPC 定义
  shell       统一二进制入口 `lycoris`，提供 daemon、cluster 等子命令
  storage     持久化层：节点元数据、workspace、skill、rule、agent memory；统一版本模型与反熵辅助函数
  tls         TLS 证书生成与加载
```

## 构建

```bash
cargo build --release -p lycoris
```

## 运行节点

```bash
lycoris daemon --config /path/to/lycoris.toml
```

## 测试

```bash
cargo test --workspace --all-features
./e2e/run.sh
```
