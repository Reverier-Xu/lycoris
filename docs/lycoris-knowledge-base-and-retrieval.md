# Lycoris 个人知识库与检索设计

更新日期：2026-05-01
状态：设计草案 v1

## 1. 文档范围

本文只回答以下问题：

- 用户如何主动添加信息来源；
- Lycoris 如何获取、本地化、解析和索引这些来源；
- 个人知识库如何与长期记忆区分；
- 临时会话如何判断是否需要搜索个人知识库；
- 检索引擎如何选型和抽象。

长期记忆和技能树，见 [lycoris-memory-and-skill-tree.md](lycoris-memory-and-skill-tree.md)。
协议、事件和磁盘布局，见 [lycoris-protocol-and-storage.md](lycoris-protocol-and-storage.md)。

## 2. 基本定位

个人知识库是用户主动添加的信息来源集合，功能形态接近 RAG，但必须本地优先。

它不同于长期记忆：

- 长期记忆保存 Lycoris 从历史交互中提炼出的稳定结论。
- 个人知识库保存用户主动提供或授权获取的外部资料。
- 长期记忆通常短、小、结构化。
- 个人知识库可以大、杂、原文保留，并通过索引检索。

它也不同于 session history：

- session history 是交互事实流水。
- knowledge source 是用户声明的可复用资料源。
- index 是 source 的衍生检索结构，不是事实来源。

## 3. 信息来源

用户可以主动添加以下 source：

- 本地文件；
- 本地目录；
- PDF、Markdown、HTML、纯文本、代码文件；
- URL；
- git repository；
- 文档站点；
- 用户导出的聊天记录或笔记；
- 以后可扩展到邮件、issue、wiki、云盘等 connector。

每个 source 必须记录：

- `source_id`
- `owner_actor_id`
- `scope`：`global` 或 `project`
- `project_id`：仅 project scope 需要
- `source_kind`
- `original_ref`
- `local_ref`
- `fetch_policy`
- `index_policy`
- `created_at`
- `last_indexed_at`

## 4. 本地化流程

添加 source 后，Lycoris 必须先本地化，再解析和索引。

推荐流程：

1. 用户提交 source。
2. daemon 创建 `knowledge.source_created` 事件。
3. fetcher 将内容获取到本地 staging 区。
4. canonicalizer 生成稳定本地副本或快照。
5. parser 提取文本、metadata、链接和 chunk。
6. indexer 建立或更新检索索引。
7. daemon 记录 `knowledge.indexed` 事件。

本地化约束：

- 远程 source 不应在每次检索时实时访问网络。
- local copy 是检索事实来源，index 是衍生层。
- source 更新必须产生新版本或可审计的 refresh 事件。
- 删除 source 时必须删除本地副本和对应索引条目。

## 5. 索引模型

Lycoris 应把索引引擎做成可替换 backend。

候选 backend：

- `tantivy`：适合作为嵌入式本地全文索引，部署简单，符合本地优先。
- `meilisearch`：适合作为独立服务，便于多前端共享和更复杂的排序，但需要 daemon 管理服务生命周期或连接配置。
- `sqlite_fts`：适合作为 Phase 1 / Phase 2 的低依赖 fallback。

建议抽象：

- `KnowledgeIndex` trait 负责 add/update/delete/search。
- `KnowledgeStore` 负责 source、document、chunk、metadata 的事实存储。
- `SearchBackend` 只负责检索，不持有唯一事实。

索引至少支持：

- keyword search；
- path / project / source filter；
- recency filter；
- chunk metadata；
- source citation；
- 未来可扩展 vector embedding 和 hybrid search。

## 6. 临时会话中的检索判断

general session 和临时对话默认可以使用个人知识库，但必须遵守用户授权和 scope。

判断是否搜索知识库的信号：

- 用户显式要求“查我的资料”、“从知识库找”、“参考我添加的文档”；
- prompt 中出现个人项目、笔记、文档名、source 名称；
- 当前问题依赖用户私有上下文，而长期记忆不足；
- 当前 general session 无 repo，但需要跨会话资料；
- runtime 认为需要检索时，必须生成可审计的 `knowledge.search_requested` 事件。

默认行为：

- global knowledge 可以服务 general session。
- project knowledge 只在 project root 匹配或用户显式选择时检索。
- 检索结果必须带 source 引用和 chunk id。
- 不应把大量 chunk 全量注入 prompt。
- 如果检索置信度低，应说明没有足够知识库证据，而不是强行回答。

## 7. 用户控制面

必须提供非 AI 命令：

- `knowledge source add`
- `knowledge source list`
- `knowledge source refresh`
- `knowledge source remove`
- `knowledge source inspect`
- `knowledge index rebuild`
- `knowledge search`

Web / Messenger 也必须提供等价控制面，不能只依赖自然语言。

## 8. 权限与隐私

个人知识库默认本地优先：

- source 获取需要显式用户动作或已保存授权。
- 远程 refresh 需要遵守 fetch policy。
- 检索个人知识库必须记录事件。
- source 原文、chunk 和 index 默认不上传。
- 如果未来接入远程 embedding 或外部 reranker，必须单独授权。

## 9. 与 Unix 哲学的关系

知识库 ingestion 和 indexing 可以是长任务，但不能要求 shell 前端长期驻留。

具体要求：

- source add / refresh / index rebuild 是一次性命令。
- 长任务由 daemon 持有，前端可 attach。
- CLI 完成提交或任务终态后退出。
- daemon 空闲时不持续抓取或自发扩展知识库。
- 自动 refresh 必须来自显式配置，不是默认行为。

## 10. 测试重点

需要覆盖：

- source add 后是否产生本地副本；
- index 是否可从本地副本重建；
- 删除 source 是否清理 chunk 和索引；
- general session 是否能按需检索 global knowledge；
- project knowledge 是否不会在不匹配 project root 时泄漏；
- 检索结果是否包含 source citation 和 chunk id；
- 低置信度检索是否不会强行注入错误上下文；
- tantivy / meilisearch / sqlite_fts backend 是否遵守同一接口语义。
