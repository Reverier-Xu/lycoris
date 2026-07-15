# lycoris-storage

`lycoris-storage` 提供 Lycoris 节点的持久化层。

## 职责

- 存储节点本地元数据、peer 信息、workspace 索引。
- 管理共享 skill 与 rule：加载、版本化、内容校验与同步。
- 提供向量存储能力，用于长期 memory 检索。

## 主要模块

- `node`：节点级元数据与 peer 状态存储。
- `workspace`：workspace、skill、rule 的存储与版本管理。
- `agent`：Agent memory 与相关持久化结构。

## 依赖后端

- `redb`：本地键值/表结构元数据存储。
- `lancedb`：向量数据存储。
