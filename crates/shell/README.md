# lycoris (shell)

`lycoris` 是 Lycoris 的统一二进制入口，通过子命令区分不同执行模式。

## 子命令

- `lycoris daemon`：启动集群节点守护进程。
- `lycoris cluster`：查看与操作集群成员状态。
- `lycoris setup`：初始化节点配置、TLS 证书与 cluster key。

## 设计说明

Shell 本身只负责命令解析、配置加载与调用 `lycoris-daemon` 或 `lycoris-client`。真正用于客户端通信的逻辑由 `lycoris-client` 提供。`lycoris` 与 `lycoris-daemon` 共享同一工作目录与数据目录约定。
