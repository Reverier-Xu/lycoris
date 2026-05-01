# Lycoris 协议、数据模型与存储设计

更新日期：2026-05-01
状态：设计草案 v1

## 1. 文档范围

本文只回答四类问题：

- daemon 对外暴露什么控制协议；
- Lycoris 内部有哪些核心对象；
- session / run / history / approval 如何持久化；
- engineering run 内 sub-agent 如何建模和审计；
- 通用长期记忆、项目路径长期记忆和 skill tree 如何持久化；
- 个人知识库 source、本地副本、chunk 和索引如何持久化；
- engineering session 和 general session 的能力边界如何在协议层落地。

Shell 集成、git 检测、worktree 生命周期、merge-back 生命周期，见 [lycoris-shell-worktree-lifecycle.md](./lycoris-shell-worktree-lifecycle.md)。

## 2. 控制面原则

Lycoris 的控制面必须满足以下原则：

- `lycoris-daemon` 是唯一真相来源。
- 所有前端都只通过 daemon API 交互。
- 任何可恢复的交互都必须先落盘，再对外流式展示。
- engineering session 与 general session 共用一套 session / run / history / attach 协议骨架。
- 两类 session 的差异主要由 capability policy 决定，而不是由前端分叉实现。
- sub-agent 是 engineering parent run 的 child run，不是独立 session，也不是外部前端可以直接驱动的常驻 agent。
- 长期记忆和 skill tree 都必须由持久化事件和候选对象驱动，不能只存在于运行时 prompt。
- skill 安装必须可审计，并且必须有用户审核事件。
- 个人知识库必须 source-first，先本地化再索引，搜索索引不能成为唯一事实来源。

## 3. 对外协议

### 3.1 传输层

推荐使用：

- `HTTP`
  - 提交请求
  - 查询 session
  - 查询历史
  - 查询 run 状态
  - 执行显式 session 管理命令
- `WebSocket`
  - attach
  - live stream
  - buffered input
  - approval / question 交互

推荐原因：

- shell、web、messenger 都容易接入；
- `attach = replay + live tail` 用 WebSocket 最自然；
- 前后端都可以基于同一事件流模型实现。

### 3.2 建议 API 轮廓

#### 3.2.1 任务入口

- `POST /v1/invoke`
  - 统一入口。
  - daemon 根据 `cwd` 与 shell context 决定走 engineering 或 general 路径。

建议请求体：

```json
{
  "frontend": "shell",
  "cwd": "/path/to/current/dir",
  "prompt": "fix the failing tests",
  "shell_context": {
    "shell": "zsh",
    "pid": 12345,
    "ppid": 12000,
    "process_group": 12345,
    "tty": "/dev/pts/3",
    "host": "workstation-a",
    "ssh": false
  }
}
```

建议响应体：

```json
{
  "session_type": "engineering",
  "session_id": "sess_123",
  "run_id": "run_123",
  "attach_url": "/v1/attach?run_id=run_123"
}
```

#### 3.2.2 session 管理

- `POST /v1/sessions`
  - 显式新建 session。
- `GET /v1/sessions`
  - 列出 session。
- `GET /v1/sessions/{session_id}`
  - 查看 session 元数据。
- `POST /v1/sessions/{session_id}/select`
  - 显式切换或声明后续交互使用某个 session。
- `GET /v1/sessions/{session_id}/history`
  - 查询历史。

这些 API 对 Web / Messenger 特别重要，因为它们缺少 shell 上下文。

#### 3.2.3 run / attach

- `GET /v1/runs/{run_id}`
  - 查询 run 状态。
- `GET /v1/runs/{run_id}/history`
  - 查询 run 历史。
- `GET /v1/runs/{run_id}/children`
  - 查询 engineering parent run 派生的 sub-agent / child run。
- `GET /v1/attach?run_id=...`
  - WebSocket attach。
- `POST /v1/runs/{run_id}/buffered-inputs`
  - 追加 durable 用户输入。
- `POST /v1/runs/{run_id}/interrupt`
  - 请求中断点插入提示。

#### 3.2.4 审批与问题

- `GET /v1/approvals`
- `POST /v1/approvals/{approval_id}/resolve`
- `GET /v1/questions`
- `POST /v1/questions/{question_id}/answer`

#### 3.2.5 长期记忆

- `GET /v1/memory/global`
  - 查询 actor 级通用长期记忆。
- `GET /v1/memory/projects/{project_id}`
  - 查询项目路径长期记忆。
- `GET /v1/memory/candidates`
  - 查询待审核或已处理的 memory candidate。
- `POST /v1/memory/candidates/{candidate_id}/resolve`
  - 接受、修改后接受、拒绝或忽略 memory candidate。

#### 3.2.6 技能树

- `GET /v1/skill-tree`
  - 查询 observed pattern、skill candidate 和 installed skill 的树状索引。
- `GET /v1/skill-candidates`
  - 查询待审核 skill candidate。
- `POST /v1/skill-candidates/{candidate_id}/resolve`
  - 接受、修改后接受、拒绝或禁止同类建议。
- `GET /v1/skills`
  - 查询已安装 skill。
- `POST /v1/skills`
  - 写入用户审核后的 skill。
- `POST /v1/skills/import`
  - 从本地路径或压缩包导入 skill。
- `POST /v1/skills/download`
  - 从 registry、URL 或 git repository 下载 skill 到 staging，并等待确认安装。

#### 3.2.7 个人知识库

- `POST /v1/knowledge/sources`
  - 添加用户授权的信息来源。
- `GET /v1/knowledge/sources`
  - 列出 knowledge source。
- `GET /v1/knowledge/sources/{source_id}`
  - 查看 source 元数据、本地化状态和索引状态。
- `POST /v1/knowledge/sources/{source_id}/refresh`
  - 按 fetch policy 刷新 source。
- `DELETE /v1/knowledge/sources/{source_id}`
  - 删除 source、本地副本、chunk 和索引条目。
- `POST /v1/knowledge/index/rebuild`
  - 重建索引。
- `POST /v1/knowledge/search`
  - 显式搜索个人知识库。

### 3.3 前端与协议的职责划分

- shell
  - 负责上传 `cwd` 和 shell context。
- web
  - 负责显式 session 管理与 attach。
- messenger
  - 负责会话命令映射和消息转发。

前端不负责：

- session 路由决策；
- run 真相维护；
- 权限状态维护；
- worktree 生命周期。

## 4. 核心数据模型

### 4.1 ProjectRoot

project root 是项目路径长期记忆的 key。对 git 工程，project root 默认等于 repo root；对非 git 项目，必须由用户或显式规则声明。

建议模型：

```json
{
  "project_id": "proj_123",
  "canonical_root": "/path/to/repo",
  "root_kind": "git",
  "memory_ref": "projects/proj_123/memory/long-term.md",
  "skill_scope_ref": "skills/projects/proj_123/"
}
```

### 4.2 Repository

只存在于 engineering 路径：

```json
{
  "repo_id": "repo_123",
  "project_id": "proj_123",
  "repo_root": "/path/to/repo",
  "git_remote": "origin",
  "default_branch": "main",
  "merge_policy": "merge",
  "memory_ref": "projects/proj_123/memory/long-term.md"
}
```

语义：

- repo 默认映射到一个 project root，project root 持有项目路径长期记忆；
- repo 不是单个 session；
- 一个 repo 可以有多个 engineering session。

### 4.3 Session

共享抽象：

```json
{
  "session_id": "sess_123",
  "session_type": "engineering",
  "actor_id": "local:1000",
  "frontend": "shell",
  "shell_identity": {
    "host": "workstation-a",
    "tty": "/dev/pts/3",
    "process_group": 12345
  },
  "status": "idle",
  "last_active_at": "2026-04-22T02:00:00Z"
}
```

#### engineering session 扩展字段

```json
{
  "repo_id": "repo_123",
  "session_root": "/path/to/repo",
  "worktree_id": "wt_123",
  "path_cluster": "crates/daemon",
  "capability_profile": "engineering-full"
}
```

#### general session 扩展字段

```json
{
  "capability_profile": "general-restricted"
}
```

### 4.4 Run

```json
{
  "run_id": "run_123",
  "session_id": "sess_123",
  "run_kind": "parent",
  "parent_run_id": null,
  "state": "running",
  "created_at": "2026-04-22T02:00:00Z",
  "entry_prompt": "fix the failing tests",
  "active_frontends": ["shell"]
}
```

推荐状态机：

- `queued`
- `preparing`
- `running`
- `awaiting_approval`
- `awaiting_user`
- `merging`
- `completed`
- `failed`
- `blocked`
- `cancelled`

其中：

- `merging` 只对 engineering session 有意义；
- `blocked` 主要用于 merge 冲突或不可自动恢复的策略阻塞。

### 4.5 SubAgentRun

sub-agent 是 run 的受限特化形态，只允许存在于 engineering session 的 parent run 内。

建议模型：

```json
{
  "run_id": "run_child_123",
  "session_id": "sess_123",
  "run_kind": "subagent",
  "parent_run_id": "run_123",
  "state": "running",
  "role": "explorer",
  "scope": {
    "path_globs": ["crates/daemon/**"],
    "tool_policy": "engineering-full",
    "write_policy": "read-only"
  },
  "budget": {
    "max_duration_ms": 600000,
    "max_tool_calls": 80
  }
}
```

强语义：

- sub-agent 必须复用父 session 的 `session_id`。
- sub-agent 必须有 `parent_run_id`，不能作为顶层 run 被前端直接创建。
- sub-agent 不能创建 session，不能触发 merge-back，不能在父 run 终态后继续运行。
- writable sub-agent 必须有显式 path scope 或锁策略，最终 patch 集成由 parent run 完成。
- sub-agent 的输出必须落入父 session 的事件流和 artifact 目录。

### 4.6 BufferedInput

```json
{
  "buffered_input_id": "buf_123",
  "run_id": "run_123",
  "session_id": "sess_123",
  "kind": "append",
  "content": "also check the daemon crate",
  "submitted_at": "2026-04-22T02:01:00Z",
  "consumed": false
}
```

### 4.7 Approval

```json
{
  "approval_id": "apr_123",
  "run_id": "run_123",
  "session_id": "sess_123",
  "scope": "filesystem-read",
  "target": "/etc/hosts",
  "status": "pending"
}
```

### 4.8 Question

```json
{
  "question_id": "q_123",
  "run_id": "run_123",
  "session_id": "sess_123",
  "prompt": "which branch should I merge into?",
  "status": "pending"
}
```

### 4.9 Artifact

用于保存可审计产物：

- patch
- diff
- test report
- build output
- summary
- merge result

### 4.10 LongTermMemoryEntry

长期记忆条目只保存稳定、可复用、可追溯的结论。

```json
{
  "memory_id": "mem_123",
  "scope": "project",
  "actor_id": "local:1000",
  "project_id": "proj_123",
  "summary": "this project uses cargo +nightly fmt --all before commits",
  "source_run_id": "run_123",
  "confidence": 0.92,
  "created_at": "2026-05-01T02:00:00Z",
  "updated_at": "2026-05-01T02:00:00Z"
}
```

`scope` 只能是：

- `global`
- `project`

语义：

- `global` 记忆绑定 actor，服务于跨项目偏好和通用工作模式。
- `project` 记忆绑定 project root，服务于某个项目的长期上下文。
- 记忆条目必须短而稳定，不能保存大段 transcript、diff、日志或一次性实现细节。

### 4.11 MemoryCandidate

run 结束后可以生成 memory candidate，但不能无条件写入长期记忆。

```json
{
  "candidate_id": "memcand_123",
  "scope": "project",
  "actor_id": "local:1000",
  "project_id": "proj_123",
  "summary": "run tests with cargo test --workspace --all-features",
  "rationale": "the same validation command appeared in multiple accepted project workflows",
  "source_run_id": "run_123",
  "status": "pending_review",
  "created_at": "2026-05-01T02:00:00Z"
}
```

推荐状态：

- `pending_review`
- `accepted`
- `accepted_with_edits`
- `rejected`
- `ignored`

### 4.12 SkillPattern

skill tree 的观测节点用于记录重复操作模式。

```json
{
  "pattern_id": "pat_123",
  "scope": "project",
  "project_id": "proj_123",
  "label": "run rust quality gates before finishing docs or code changes",
  "observed_count": 7,
  "last_observed_at": "2026-05-01T02:00:00Z",
  "related_memory_ids": ["mem_123"]
}
```

### 4.13 SkillCandidate

skill candidate 是可以建议用户沉淀为 skill 的候选。

```json
{
  "candidate_id": "skillcand_123",
  "scope": "project",
  "project_id": "proj_123",
  "name": "rust-quality-gates",
  "trigger": "before completing rust code or docs tasks in this repo",
  "rationale": "quality gate commands were repeatedly required by repo instructions",
  "proposed_steps": [
    "run cargo +nightly fmt --all -- --check",
    "run cargo +nightly clippy --workspace --all-targets --all-features -- -D warnings",
    "run cargo test --workspace --all-features"
  ],
  "status": "pending_review"
}
```

skill candidate 不能被 runtime 当作 installed skill 使用。

### 4.14 InstalledSkill

installed skill 是用户审核后写入的技能资产。

```json
{
  "skill_id": "skill_123",
  "scope": "project",
  "project_id": "proj_123",
  "name": "rust-quality-gates",
  "path": "skills/projects/proj_123/rust-quality-gates/SKILL.md",
  "origin": "generated",
  "source_ref": "skillcand_123",
  "installed_from_candidate_id": "skillcand_123",
  "approved_by": "local:1000",
  "installed_at": "2026-05-01T02:00:00Z"
}
```

写入规则：

- `global` skill 写入用户级 skill 目录。
- `project` skill 写入项目级或 repo-local skill 目录。
- skill 内容必须避免携带临时对话、大段日志、敏感信息和单次 patch 细节。
- `origin` 可以是 `generated`、`local_import` 或 `remote_download`。

### 4.15 KnowledgeSource

用户主动添加的信息来源。

```json
{
  "source_id": "ksrc_123",
  "owner_actor_id": "local:1000",
  "scope": "project",
  "project_id": "proj_123",
  "source_kind": "url",
  "original_ref": "https://example.com/docs",
  "local_ref": "knowledge/sources/ksrc_123/snapshot",
  "fetch_policy": "manual",
  "index_policy": "full_text",
  "status": "indexed",
  "created_at": "2026-05-01T02:00:00Z",
  "last_indexed_at": "2026-05-01T02:05:00Z"
}
```

### 4.16 KnowledgeDocument

source 本地化后的文档单位。

```json
{
  "document_id": "kdoc_123",
  "source_id": "ksrc_123",
  "local_path": "knowledge/sources/ksrc_123/documents/page.md",
  "content_hash": "sha256:...",
  "mime_type": "text/markdown",
  "title": "project deployment notes",
  "created_at": "2026-05-01T02:03:00Z"
}
```

### 4.17 KnowledgeChunk

索引和检索使用的 chunk。

```json
{
  "chunk_id": "kchunk_123",
  "document_id": "kdoc_123",
  "source_id": "ksrc_123",
  "ordinal": 12,
  "text_ref": "knowledge/chunks/kchunk_123.txt",
  "metadata": {
    "heading": "deployment",
    "path": "docs/deploy.md"
  }
}
```

### 4.18 KnowledgeIndex

索引 backend 的元数据。

```json
{
  "index_id": "kidx_123",
  "backend": "tantivy",
  "scope": "project",
  "project_id": "proj_123",
  "path": "knowledge/indexes/kidx_123",
  "status": "ready",
  "last_built_at": "2026-05-01T02:05:00Z"
}
```

backend 候选：

- `sqlite_fts`
- `tantivy`
- `meilisearch`

### 4.19 KnowledgeSearchResult

检索结果必须保留来源引用。

```json
{
  "chunk_id": "kchunk_123",
  "source_id": "ksrc_123",
  "document_id": "kdoc_123",
  "score": 0.84,
  "snippet": "deployment uses the internal release checklist",
  "citation": {
    "title": "project deployment notes",
    "local_ref": "knowledge/sources/ksrc_123/documents/page.md"
  }
}
```

## 5. 事件流模型

Lycoris 必须把 attach 和历史查询统一建立在事件流之上。

### 5.1 建议事件类型

- `session.selected`
- `session.created`
- `run.created`
- `run.preparing`
- `run.started`
- `assistant.delta`
- `tool.called`
- `tool.stdout`
- `tool.stderr`
- `tool.completed`
- `buffered_input.accepted`
- `approval.requested`
- `approval.resolved`
- `question.requested`
- `question.answered`
- `subagent.started`
- `subagent.delta`
- `subagent.completed`
- `subagent.failed`
- `subagent.cancelled`
- `memory.candidate_created`
- `memory.candidate_resolved`
- `memory.written`
- `skill.pattern_observed`
- `skill.candidate_created`
- `skill.candidate_resolved`
- `skill.imported`
- `skill.downloaded`
- `skill.installed`
- `knowledge.source_created`
- `knowledge.source_localized`
- `knowledge.index_started`
- `knowledge.indexed`
- `knowledge.search_requested`
- `knowledge.search_completed`
- `knowledge.source_removed`
- `merge.started`
- `merge.completed`
- `merge.blocked`
- `run.completed`
- `run.failed`
- `run.cancelled`

### 5.2 attach 语义

attach 必须严格等于：

1. 回放该 run 或 session 已存在的历史事件；
2. 再继续推送 live events。

因此 attach 不是“连接到某个内存中的 UI 实例”，而是“订阅某个持久化事件流”。

当 parent run 使用 sub-agent 时，attach 默认应展示 parent run 的有序事件流，并用 `parent_run_id` / `child_run_id` 标识嵌套事件。需要排查细节时，前端可以再查询 child run 历史，但 child run 不能被当成独立 session 操作。

## 6. 会话与历史查询语义

所有 session 都必须支持以下查询维度：

- 按 session 查询
- 按 run 查询
- 按时间范围查询
- 按工具调用类型查询
- 按审批状态查询

engineering session 额外支持：

- 按 repo 查询
- 按 path cluster 查询
- 按 merge 结果查询
- 按 parent run 查询 child run / sub-agent 结果

长期记忆额外支持：

- 按 actor 查询通用长期记忆
- 按 project root 查询项目路径长期记忆
- 按 source run 查询记忆来源
- 按 candidate status 查询待审核记忆

skill tree 额外支持：

- 按 scope 查询 observed pattern
- 按 project root 查询项目级 skill candidate
- 按审核状态查询 skill candidate
- 按 installed skill 查询来源 candidate 和审核事件

个人知识库额外支持：

- 按 actor 查询 global knowledge source
- 按 project root 查询 project knowledge source
- 按 source kind 查询 source
- 按 source / document / chunk 查询索引状态
- 按 session / run 查询知识库检索事件

## 7. 存储布局

```text
$XDG_STATE_HOME/lycoris/
  daemon/
    config.effective.toml
    auth/
  actors/
    <actor-id>/
      memory/
        long-term.md
        candidates.jsonl
        summaries.jsonl
      skill-tree/
        patterns.jsonl
        candidates.jsonl
        installed.jsonl
      skills/
        <skill-name>/
          SKILL.md
      knowledge/
        sources/
          <source-id>/
            source.json
            documents/
            chunks/
        indexes/
          <index-id>/
  projects/
    <project-id>/
      project.json
      memory/
        long-term.md
        candidates.jsonl
        summaries.jsonl
      skill-tree/
        patterns.jsonl
        candidates.jsonl
        installed.jsonl
      skills/
        <skill-name>/
          SKILL.md
      knowledge/
        sources/
          <source-id>/
            source.json
            documents/
            chunks/
        indexes/
          <index-id>/
  repos/
    <repo-id>/
      repo.json
      sessions/
        <session-id>/
          session.json
          worktree.json
          events.jsonl
          runs/
            <run-id>.json
            <run-id>.stream.jsonl
            <run-id>.children.jsonl
          buffered-inputs.jsonl
          approvals.jsonl
          questions.jsonl
          artifacts/
      merges/
        history.jsonl
  general-sessions/
    <session-id>/
      session.json
      events.jsonl
      runs/
        <run-id>.json
        <run-id>.stream.jsonl
      buffered-inputs.jsonl
      approvals.jsonl
      questions.jsonl
      artifacts/
  worktrees/
    <repo-id>/
      <session-id>/
  indexes/
    repos.sqlite
    projects.sqlite
    sessions.sqlite
    runs.sqlite
    memory.sqlite
    skills.sqlite
    knowledge.sqlite
```

### 7.1 哪些是事实来源

事实来源：

- `session.json`
- `events.jsonl`
- `runs/*.json`
- `runs/*.stream.jsonl`
- `runs/*.children.jsonl`
- `buffered-inputs.jsonl`
- `approvals.jsonl`
- `questions.jsonl`
- `merges/history.jsonl`
- `actors/*/memory/candidates.jsonl`
- `projects/*/memory/candidates.jsonl`
- `actors/*/skill-tree/candidates.jsonl`
- `projects/*/skill-tree/candidates.jsonl`
- `actors/*/skill-tree/installed.jsonl`
- `projects/*/skill-tree/installed.jsonl`
- `actors/*/knowledge/sources/*/source.json`
- `projects/*/knowledge/sources/*/source.json`
- `actors/*/knowledge/sources/*/documents/*`
- `projects/*/knowledge/sources/*/documents/*`
- `actors/*/knowledge/sources/*/chunks/*`
- `projects/*/knowledge/sources/*/chunks/*`

衍生层：

- `actors/*/memory/long-term.md`
- `projects/*/memory/long-term.md`
- `actors/*/memory/summaries.jsonl`
- `projects/*/memory/summaries.jsonl`
- `actors/*/skill-tree/patterns.jsonl`
- `projects/*/skill-tree/patterns.jsonl`
- `actors/*/knowledge/indexes/*`
- `projects/*/knowledge/indexes/*`
- `indexes/*.sqlite`

### 7.2 为什么不是纯数据库

- 用户要求 session 持久化在磁盘上；
- 事件流天然适合 replay、attach、审计；
- 文件树更适合人工检查和故障恢复；
- SQLite 可以做索引，但不应该替代原始事件流。

## 8. 权限与能力策略

### 8.1 capability profile

建议把能力边界做成 profile，而不是散落的条件判断：

- `engineering-full`
- `general-restricted`

### 8.2 默认能力矩阵

| 能力           | engineering-full | general-restricted |
| -------------- | ---------------- | ------------------ |
| 读当前目录     | 允许             | 允许               |
| 读当前 repo    | 允许             | 按上下文决定       |
| 读超出范围路径 | 审批             | 审批               |
| 写文件         | 仅限 worktree    | 拒绝               |
| shell 命令     | 完整策略         | 受限策略           |
| toolcalls      | 完整             | 子集               |
| sandbox        | 完整             | 子集               |
| skills         | 完整             | 子集               |
| workflows      | 完整             | 子集               |
| sub-agents     | 完整但有界       | 关闭               |
| 通用长期记忆   | 读取 / 候选写入  | 读取 / 候选写入    |
| 项目路径记忆   | 读取 / 候选写入  | 显式绑定后读取     |
| skill tree     | 观察 / 建议      | 观察 / 建议        |
| skill 安装     | 用户审核后       | 用户审核后         |
| skill 导入/下载 | 显式确认         | 显式确认           |
| global knowledge | 读取 / 检索      | 读取 / 检索        |
| project knowledge | 匹配项目后检索  | 显式绑定后检索     |

### 8.3 强约束

- `general-restricted` 永远不能比 `engineering-full` 更强。
- engineering session 的写路径必须是 session worktree。
- sub-agent 只能在 engineering parent run 内创建，不能拥有独立 session 或常驻生命周期。
- sub-agent 不能绕过父 run 的 capability profile、审批事件、worktree 限制和 merge-back 收口。
- 通用长期记忆不能写入大量临时对话信息。
- 项目路径长期记忆不能写入太具体的单次工程实现。
- skill candidate 未经用户审核不能进入 installed skill。
- 手动导入和远程下载的 skill 未经显式确认不能启用。
- knowledge source 必须由用户主动添加或授权。
- knowledge search 必须遵守 global / project scope，并记录检索事件。
- 超出默认作用域的读取和高风险操作必须可审批、可审计。

## 9. 协议层测试建议

### 9.1 一致性

- `attach` 回放结果与 live stream 是否一致；
- 事件顺序是否稳定；
- 中途重连后是否能接上正确位置。

### 9.2 数据正确性

- `session.json` 与 `events.jsonl` 是否一致；
- `run.state` 与终态事件是否一致；
- blocked / merging / completed 是否互斥。

### 9.3 能力策略

- general session 是否绝对无写权限；
- engineering session 是否只写 worktree；
- 越界读取是否总能生成审批事件。

### 9.4 sub-agent 边界

- general session 是否完全禁止创建 sub-agent；
- sub-agent 是否总是有 `parent_run_id`；
- parent run 进入终态前是否取消或收口所有 child run；
- child run 是否不能触发独立 commit、merge-back 或 session attach。

### 9.5 长期记忆与技能树

- 通用长期记忆是否避免吸收大量临时对话信息；
- 项目路径长期记忆是否避免吸收过细的单次工程实现；
- memory candidate 是否能追溯到 source run；
- rejected memory candidate 是否不会写入 long-term memory；
- skill candidate 是否只来自频率信号或用户显式要求；
- local import / remote download skill 是否记录 origin 和 source ref；
- installed skill 是否总有审核事件；
- 未审核 skill candidate 是否不会被 runtime 发现为可用 skill。

### 9.6 个人知识库

- knowledge source 是否先本地化再 index；
- index 是否能从 source local copy 重建；
- 删除 source 是否清理 document、chunk 和 index；
- general session 是否只默认检索 global knowledge；
- project knowledge 是否只在 project root 匹配或显式绑定时检索；
- search result 是否总带 source、document 和 chunk 引用。

## 10. 与其他文档的关系

- 产品总览与阶段计划：见 [lycoris-architecture-plan.md](lycoris-architecture-plan.md)
- shell 集成、git 检测、worktree、merge 生命周期：见 [lycoris-shell-worktree-lifecycle.md](lycoris-shell-worktree-lifecycle.md)
- 长期记忆分层与技能树流程：见 [lycoris-memory-and-skill-tree.md](lycoris-memory-and-skill-tree.md)
- 个人知识库与检索：见 [lycoris-knowledge-base-and-retrieval.md](lycoris-knowledge-base-and-retrieval.md)
