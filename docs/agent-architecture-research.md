# AI Agent 架构调研：Codex、OpenCode、OpenClaw

更新日期：2026-05-01

## 1. 结论先行

- Codex 代表的是“本地交互 + 云端任务隔离 + 强治理能力”的路线。
- OpenCode 代表的是“本地优先 + client/server + 多界面 attach”的路线。
- OpenClaw 代表的是“常驻 gateway + 事件驱动 + 多渠道/主动型 agent”的路线。
- Lycoris 最接近 OpenCode 的控制面拆分思路，但应当比 OpenCode 更偏 Unix、比 Codex 更偏本地单控制面、比 OpenClaw 更少常驻自治行为。

## 2. 总览对比

| 项目             | 控制面形态                                | 执行面形态                                            | Session 模型                                                                        | attach / 恢复                                 | 是否默认主动运行                     | Multi-agent 倾向                 | 对 Lycoris 的主要启发                                                            |
| ---------------- | ----------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------- | ------------------------------------ | -------------------------------- | -------------------------------------------------------------------------------- |
| Codex            | 本地 CLI + Codex Cloud / App / IDE 多入口 | CLI 本地执行，Cloud 任务在隔离环境中独立运行          | 既有本地交互 session，也有独立的云任务                                              | Cloud 天然解耦；CLI 也支持更多自动化/远程能力 | 否                                   | 有 subagents，但不是产品核心叙事 | 强审批、强规则、强隔离、强可验证性                                               |
| OpenCode         | 明确的 client/server                      | `serve` / `web` 启动后端，TUI / Web / IDE 作为前端    | 持久化 session，可 list / export / import / share                                   | `attach` 是一等能力                           | 否                                   | 内置 primary agents + subagents  | 前后端分离、attach、统一控制面                                                   |
| OpenClaw         | 单个长生命周期 gateway                    | gateway 内部事件循环 + agent runtime                  | 渠道驱动、队列驱动、长期存在                                                        | 强调长期在线而非短前端 attach                 | 是，heartbeat / webhook / timer 常在 | 明显偏多代理和长期编排           | 多渠道统一入口、事件队列、持久状态                                               |
| Lycoris 目标形态 | 中心 daemon + 多个轻前端                  | daemon 只在有任务时调度 run，且可并发承载多个 session | 磁盘直存、智能自动选 session；repo 映射项目路径记忆，session 作为 worktree 工作单元 | shell crash 不影响 run，`attach` 可恢复       | 否，空闲时不跑 AI 任务               | engineering run 内支持有界 sub-agent，不做常驻 agent team | 统一控制面、文件优先、repo-session 走 git/worktree，非工程任务允许 shell-session |

## 3. Codex

### 3.1 观察到的架构特征

- Codex CLI 是一个本地运行、用 Rust 实现、开源的终端 coding agent。
- Codex CLI 可以直接在选定目录下读文件、改文件、跑命令，并提供规则、skills、MCP、subagents、approval modes、local code review、Codex Cloud tasks 等能力。
- OpenAI 还提供 Codex Cloud 形态，任务在独立云沙箱中执行，每个任务与仓库环境隔离，并行处理多个任务。
- Codex Cloud 通过 `AGENTS.md`、测试命令、环境配置来约束 agent 行为，并强调可验证证据，例如终端日志与测试输出。

### 3.2 值得借鉴的点

- 把“任务”视为隔离的执行单元，而不是把整个 agent 当成一团持续运行的黑盒。
- 把 subagent 当成任务内部的受控分解能力，而不是新的顶层会话形态。
- 把审批、规则、技能、MCP 接入当成控制面的正式能力，而不是后补的插件系统。
- 把“前端交互界面”和“实际执行环境”解耦，这让恢复、复查、审计都更自然。

### 3.3 不适合照搬的点

- Codex 的产品形态横跨 CLI、IDE、App、Cloud，本质上是多运行面并存。Lycoris 不需要同时维护本地执行和云端执行两套主路径。
- Codex 更强调“任务隔离”和“多任务并行”。Lycoris 更适合强调“daemon 可同时承载多个 session，但单个 session 默认单 active run”与“前端快速退出”。
- Codex CLI 的默认交互仍然是一个持续停留在终端里的 agent 会话；Lycoris 的 shell 应该更像一次性提交器和流式查看器。

## 4. OpenCode

### 4.1 观察到的架构特征

- OpenCode 是开源 coding agent，入口包括 terminal、desktop、IDE。
- OpenCode 明确写出自己采用 client/server architecture，TUI 只是其中一个 client。
- OpenCode 提供 `serve`、`web`、`attach`、`session list`、`export`、`import` 等能力，说明后端 session 与前端界面天然解耦。
- OpenCode 有 built-in primary agents 和 subagents，也有权限模型、rules、skills、MCP、自定义 commands。
- OpenCode 会把 session 和项目相关数据直接落盘，日志和 storage 目录结构清晰可见。

### 4.2 值得借鉴的点

- “先有 backend，再有 attach”这条路径和 Lycoris 的 shell crash 恢复需求高度一致。
- 前端可以是 TUI、Web、IDE，但 session 与任务控制仍然只走一套核心协议。
- 权限、rules、skills 都是配置化能力，前端只负责展示和交互，不持有业务真相。
- built-in subagents 证明工程 agent 需要分工能力，但这类能力应该在 Lycoris 中收束为 parent run 的 child run。

### 4.3 不适合照搬的点

- OpenCode 的体验中心还是一个持续停留的 TUI/桌面交互界面，Lycoris 需要更激进地强调“任务完成即退出”。
- OpenCode 明显接受多 session 并行与较重的交互状态；Lycoris 应该把“单次 shell 调用只跟随一个活跃任务流，但 daemon 可同时托管多个 session”做成默认心智。
- OpenCode 的 share/import/export 协作能力很强，但对 Lycoris v1 来说，这类分享功能应低于 durable attach 和自动 session 选择。

## 5. OpenClaw

### 5.1 观察到的架构特征

- OpenClaw 的官方架构文档把自己描述为一个单一、长生命周期的 gateway。
- control-plane clients、nodes、web chat 都连接到这个 gateway；gateway 统一拥有多个消息渠道和 WebSocket API。
- OpenClaw 的解释文档把核心循环抽象为 `Time -> Events -> Queue -> Agent -> State -> Loop`。
- 其输入天然包括 messages、heartbeats、webhooks、agent-to-agent communication，因此系统默认是“活着”的。
- 它更像一个长期在线的 agent runtime / gateway，而不是一个只在用户触发时工作的任务控制面。

### 5.2 值得借鉴的点

- 统一控制面处理不同前端和不同入口，这点对 CLI / Web / Messenger 共用同一 session 核心非常重要。
- 把所有外部输入都规约成有序事件并经过队列处理，这对消息顺序、权限请求、恢复 replay 都很有价值。
- 渠道适配层与 agent 会话层有明确边界。

### 5.3 不适合照搬的点

- OpenClaw 的主动性来自 heartbeat、timer、webhook 等持续输入，这与 Lycoris “默认不主动运行任务”的原则相冲突。
- OpenClaw 把多渠道接入与长期 agent runtime 强耦合在 gateway 中；Lycoris 更适合让 messenger/web 作为 frontends 或 adapters，经由 daemon API 接入，而不是把所有外部协议都塞进核心。
- OpenClaw 的常驻多代理、agent-to-agent society 设计不是 Lycoris 的产品方向。Lycoris 可以支持 engineering run 内有界 sub-agent，但仍应维持单 session / 单用户可见 active parent run 的简单心智。

## 6. 对 Lycoris 的归纳结论

### 6.1 应该明确吸收的能力

- 参考 OpenCode：采用“中心后端 + attachable 前端”的控制面设计。
- 参考 Codex：把 rules、skills、审批、工具权限、审计证据做成一等能力。
- 参考 OpenClaw：把所有输入统一成可持久化事件，并通过顺序队列驱动状态机。
- 参考长期 agent 与 coding agent 的共同经验：把 memory、summary、skills 从 transcript 中拆出来，作为可审计、可维护的长期资产。

### 6.2 应该明确拒绝的方向

- 不做 OpenClaw 那种 heartbeat / cron 驱动的默认主动 agent。
- 不做 OpenClaw / AutoGen 风格的 agent team 作为核心架构。
- 不把 sub-agent 提升为第三类 session、后台自治 worker 或外部用户入口。
- 不做 Codex 那种本地执行面与云执行面长期并行维护的复杂产品面。
- 不把 shell 做成一个必须长期驻留的重 TUI；shell 应当像 `git commit` 或 `ssh` 一样，完成事就退出。

### 6.3 Lycoris 的目标定位

- daemon 是唯一真相来源，前端只是连接器和呈现层。
- daemon 整体上应当能同时承载多个 session；串行约束只应施加在单个 session 内。
- 对工程型工作，repo root 应是 session 的根边界；同一 repo 下允许多个 session，通过独立 worktree 隔离。
- engineering session 需要支持 parent run 内的有界 sub-agent，以覆盖代码探索、分片实现、验证和复核等真实工程需求。
- sub-agent 必须继承 parent run 的 worktree、权限、事件流和终止条件，不能拥有独立 session 生命周期。
- 长期记忆应分为 actor 级通用记忆和 project root 级项目路径记忆，分别服务跨项目偏好和项目上下文。
- skill 可以来自长期沉淀、手动导入或下载；skill tree 只负责从长期记忆与重复操作频率中发现 candidate。
- 个人知识库应作为用户主动添加 source 的本地化检索层，使用可替换索引 backend 支撑临时会话搜索。
- session 要直接存盘，最好是可检查、可 replay、可 rebuild index 的文件优先结构。
- run 必须独立于 shell 进程生命周期。
- 自动 session 选择必须可解释、可覆盖、可回退，并且能够把同一工作线程上的多次交互长期归并到同一 session。
- 对非工程任务，允许不绑定路径、只绑定当前 shell 实例的轻量 shell-session。
- 系统空闲时不应存在持续迭代的 AI worker。

## 7. 参考资料

- Codex CLI docs: `https://developers.openai.com/codex/cli`
- Codex product announcement: `https://openai.com/index/introducing-codex/`
- Codex open-source repository: `https://github.com/openai/codex`
- OpenCode docs: `https://opencode.ai/docs/`
- OpenCode CLI docs: `https://opencode.ai/docs/cli/`
- OpenCode agents docs: `https://opencode.ai/docs/agents/`
- OpenClaw gateway architecture: `https://github.com/openclaw/openclaw/blob/main/docs/concepts/architecture.md`
- OpenClaw event-loop explainer: `https://openclaw.design/learn/how-openclaw-works`
