[English](README.md) | 中文

# Lycoris

Lycoris 是一个面向 LLM Agent 的去中心化集群系统。每个节点既是服务提供者也是集群成员，从任意节点的 API 都可以访问整个集群。

## 核心设计

- 去中心化：集群是无向有环稀疏图，节点通过 SWIM 风格的成员协议相互探测、传播状态。
- 分区容忍：网络分区期间，各分区可继续独立运行；重新连通后通过反熵同步合并共享状态。
- 共享与隔离：每个节点保存共享的集群基础信息、共享 skill/rule、共享工作区元数据索引，同时拥有专属的 memory 与专属工作区。
- 可扩展：扩展包是集群共享资源，经同一条反熵管线同步到每个节点；两个沙箱引擎（wasmtime 承载 WASM、mlua 承载 Lua）共用一套 JSON 调用契约，label selector 决定扩展在哪些节点激活，能力通告（`ext.<id>`）把调用路由到有能力执行的节点（单跳），扩展包通过 `lycoris cluster ext load` 进入集群。详见 `docs/design/extension-system.md`。
- LLM provider 即扩展：OpenAI 兼容 provider 以 WASM 扩展形式提供（`extensions/openai`），实现 `provides = ["llm"]` 线约定（`configure`/`chat`/`embed`/`models`）；API key 放在节点本地配置（`[extensions.local.openai]`），不进同步的 manifest；调用经单跳路由到 label 匹配 selector 的节点执行。最小上手：`rustup target add wasm32-unknown-unknown`，`cargo build --locked --release --target wasm32-unknown-unknown -p lycoris-ext-openai`，然后 `lycoris cluster ext load openai.pkg.toml`，即可在任意节点 `lycoris cluster ext invoke openai chat '{"model":"...","messages":[{"role":"user","content":"hi"}]}'`。详见 `docs/design/llm-provider.md` 与 `extensions/openai/README.md`。

## 代码组织

```
crates/
  client      gRPC 客户端句柄：统一连接装配、cluster key 注入，用于节点间以及 CLI 与节点通信
  config      守护进程与客户端配置解析、校验、默认值与回退加载策略
  core        共享核心原语：cluster key、ResourceScope、time、路径约定
  daemon      集群节点运行时：transport 连接池、sync/（SWIM 派发、gossip、merkle 反熵编排、peer 选择）、
              membership 桥接（领域类型边界）、resource 资源外观、rpc/（tonic 边界与 cluster-key 拦截器）、
              extension/（selector 驱动激活、能力通告、调用路由、hook 派发）
  extension   扩展引擎层：扩展包与 manifest 模型，沙箱化的 WASM（wasmtime）与 Lua（mlua）引擎，
              共用一套 JSON 调用契约
  extguest    扩展 ABI 的 guest 侧：export_extension! 宏与安全的 host::log / host::http 封装（面向 WASM guest）
  membership  成员 CRDT（确定性全序 merge）、SWIM 状态机、Merkle 树与反熵 diff（独立于 tonic，无 transport）
  proto       protobuf/gRPC 定义，附协议常量与 NodeInfo 构造辅助
  shell       统一二进制入口 `lycoris`，提供 daemon、cluster 等子命令
  storage     持久化层：redb 泛型表存储承载节点元数据/workspace/skill/rule，LanceDB 承载 agent memory；
              统一版本模型与反熵 apply 管线
  tls         TLS 证书生成、加载与自动续期（SAN 含节点 advertise 地址）

extensions/
  openai      OpenAI 兼容 LLM provider 扩展：WASM guest（cdylib）+ 可在宿主机单测的纯核心
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

`e2e/` 下的端到端测试套件:

- `e2e/run.sh` — 基于 compose 的集群测试(docker compose 或 podman-compose);在 CI 中运行。
- `e2e/shell-test.sh` — 面向 CLI 的测试;需要本地安装 podman。
- `e2e/partition-test.sh` — 网络分区测试;需要本地 podman,且容器内需要 `iptables`(`NET_ADMIN`)。

后两套 podman 测试不在 CI 中运行,需在本地执行。
