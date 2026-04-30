# AI Agent 代表性论文调研

更新日期：2026-04-30

本文只挑对 Lycoris 架构决策有直接帮助的论文，不追求“全收录”，重点关注以下四类问题：

- agent 如何把“推理”与“执行”串起来；
- 长会话、长任务如何做 memory / compaction / skills；
- multi-agent 到底是不是必须；
- 如何评测真实 agent，而不是只评测静态问答能力。

## 1. 推理与行动闭环

| 论文                                                          | 年份 | 关键贡献                                                        | 对 Lycoris 的启发                                                                      |
| ------------------------------------------------------------- | ---- | --------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| ReAct: Synergizing Reasoning and Acting in Language Models    | 2022 | 把 reasoning trace 与 action 交错执行，形成“思考-行动-观察”闭环 | Lycoris 的 run 状态机不应只有聊天消息，还应显式记录 tool call、observation、checkpoint |
| Toolformer: Language Models Can Teach Themselves to Use Tools | 2023 | 证明工具调用不是外挂，而是模型能力扩展的一部分                  | 工具 API 必须细粒度、类型化、低副作用，不能只靠自由文本命令                            |
| Reflexion: Language Agents with Verbal Reinforcement Learning | 2023 | 用语言化反思替代参数更新，把失败经验写入 episodic memory        | session 持久化不能只有 transcript，还应保留 compacted lessons / reflections            |

## 2. 记忆、长期行为与技能

| 论文                                                             | 年份 | 关键贡献                                                  | 对 Lycoris 的启发                                                         |
| ---------------------------------------------------------------- | ---- | --------------------------------------------------------- | ------------------------------------------------------------------------- |
| Generative Agents: Interactive Simulacra of Human Behavior       | 2023 | 提出 observation、memory、reflection、planning 的分层结构 | Lycoris 应把原始事件、提炼摘要、当前计划拆开存，而不是混成一份上下文      |
| Voyager: An Open-Ended Embodied Agent with Large Language Models | 2023 | 技能库、自动课程、长期积累能力                            | skills 应该由重复任务沉淀而来，但 Lycoris 必须加入用户审核门槛           |
| MemGPT: Towards LLMs as Operating Systems                        | 2023 | 用分层 memory 管理突破上下文窗口限制                      | Lycoris 的 session 存储应是“磁盘为真相，内存为窗口”，而不是反过来         |

## 3. 多代理与编排

| 论文                                                                               | 年份        | 关键贡献                               | 对 Lycoris 的启发                                                                   |
| ---------------------------------------------------------------------------------- | ----------- | -------------------------------------- | ----------------------------------------------------------------------------------- |
| CAMEL: Communicative Agents for "Mind" Exploration of Large Language Model Society | 2023        | 用 role-playing 做多代理协作与数据生成 | 可以作为未来扩展参考，但不应成为 Lycoris 的顶层 session 模型                         |
| AutoGen: Enabling Next-Gen LLM Applications via Multi-Agent Conversation           | 2023 / 2024 | 把 agent conversation 做成通用框架     | orchestration 可以启发 engineering run 内 sub-agent，但不能污染 session 核心        |

## 4. 软件工程 agent 与真实世界 benchmark

| 论文                                                                                       | 年份        | 关键贡献                                                 | 对 Lycoris 的启发                                                 |
| ------------------------------------------------------------------------------------------ | ----------- | -------------------------------------------------------- | ----------------------------------------------------------------- |
| SWE-agent: Agent-Computer Interfaces Enable Automated Software Engineering                 | 2024        | 说明 agent-computer interface 会显著影响软件工程任务表现 | shell、tool API、文件与命令接口是产品核心，不只是 UI 细节         |
| AgentBench: Evaluating LLMs as Agents                                                      | 2023        | 把 agent 评测从静态问答推进到交互环境                    | Lycoris 必须做交互式系统评测，而不是只看一次 prompt 的输出质量    |
| WebArena: A Realistic Web Environment for Building Autonomous Agents                       | 2023 / 2024 | 提供真实网页环境和执行式评测                             | 如果后续要做 web-side agent 行为，需要环境级 benchmark，而非 mock |
| GAIA: a benchmark for General AI Assistants                                                | 2023 / 2024 | 强调真实工具使用、多模态、开放世界问题                   | 可作为 Lycoris 通用问答/检索任务的外部参考，而不局限于 coding     |
| OSWorld: Benchmarking Multimodal Agents for Open-Ended Tasks in Real Computer Environments | 2024        | 提供真实计算机环境 benchmark，覆盖 GUI 与 CLI 任务       | attach、恢复、工具执行、computer-use 扩展都应该在真实环境里评测   |

## 5. 阅读优先级建议

### 5.1 必读

- ReAct
- Reflexion
- MemGPT
- SWE-agent
- AgentBench

### 5.2 与 Lycoris v2 更相关

- Voyager
- WebArena
- OSWorld
- GAIA

### 5.3 理解边界用

- CAMEL
- AutoGen
- Generative Agents

## 6. 对 Lycoris 的直接方法论启发

### 6.1 Agent 运行时

- 运行时应该显式区分 message、tool action、observation、reflection、summary，而不是只存聊天记录。
- 长 session 不能只靠“把历史全文再塞回模型”解决，必须做 compaction 与 memory hierarchy。
- 长期记忆应该分成 actor 级通用记忆和 project root 级项目记忆。
- 通用长期记忆只吸收跨场景稳定偏好，不能把临时对话大量写进去。
- 项目路径长期记忆只吸收长期项目上下文，不能把某次工程实现的具体 patch 当成项目事实。
- skills 更像可发现的能力目录，而不是神秘系统 prompt 的附录。
- skill tree 应从长期记忆和重复操作频率中产生候选，并在用户审核后才写入 skill。

### 6.2 产品边界

- multi-agent 很重要，但不是所有 agent 系统的起点。
- 对 Lycoris 这种“中心 daemon + 多前端”的产品，先把单 session、可恢复、可审计做好，收益远高于早期上 team orchestration。
- engineering session 仍然需要 sub-agent 来支持工程分工；它应该是 parent run 内的有界 child run，而不是新的长期 agent 社会。
- Unix 哲学要求 sub-agent 有明确输入、输出、scope 和终止条件；不能靠常驻自治循环来维持产品能力。

### 6.3 评测方式

- 静态 benchmark 不够，必须做 execution-based 评测。
- 要把“任务完成率”和“系统可靠性”分开看。
- 对 Lycoris 来说，shell crash 恢复、权限问询、session 自动选择准确率，本身就是一类 benchmark。

## 7. 推荐引用到后续设计文档的核心观点

- ReAct 告诉我们，agent 不是“先想完再干”，而是交替推理与执行。
- Reflexion 告诉我们，失败记录应该成为会话资产。
- MemGPT 告诉我们，磁盘与摘要层才是长任务系统的基础设施。
- SWE-agent 告诉我们，tool interface 与 terminal interface 会直接改变 agent 表现。
- AgentBench / WebArena / OSWorld 告诉我们，agent 必须在可执行环境里被测，而不是在聊天框里被测。

## 8. 参考论文与页面

- ReAct: `https://react-lm.github.io/`
- Toolformer: `https://arxiv.org/abs/2302.04761`
- Reflexion: `https://arxiv.org/abs/2303.11366`
- Generative Agents: `https://research.google/pubs/generative-agents-interactive-simulacra-of-human-behavior/`
- Voyager: `https://arxiv.org/abs/2305.16291`
- MemGPT: `https://arxiv.org/abs/2310.08560`
- CAMEL: `https://arxiv.org/abs/2303.17760`
- AutoGen: `https://www.microsoft.com/en-us/research/publication/autogen-enabling-next-gen-llm-applications-via-multi-agent-conversation-framework/`
- SWE-agent: `https://arxiv.org/abs/2405.15793`
- AgentBench: `https://arxiv.org/abs/2308.03688`
- WebArena: `https://arxiv.org/abs/2307.13854`
- GAIA: `https://arxiv.org/abs/2311.12983`
- OSWorld: `https://arxiv.org/abs/2404.07972`
