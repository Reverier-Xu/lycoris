# Lycoris 长期记忆与技能树设计

更新日期：2026-05-01
状态：设计草案 v1

## 1. 文档范围

本文只回答以下问题：

- 长期记忆分成哪些层；
- 通用长期记忆和项目路径长期记忆分别允许保存什么；
- run 结束后如何生成、审核和写入记忆候选；
- skill tree 如何从长期记忆和操作频率中产生建议；
- 用户如何审核并安装 skill。

协议、事件和磁盘布局细节，见 [lycoris-protocol-and-storage.md](lycoris-protocol-and-storage.md)。

## 2. 基本原则

长期记忆和 skill tree 都是 session 之上的长期资产，不是某次对话的完整副本。

核心约束：

- 原始 transcript、tool output、临时思路默认属于 run history，不直接进入长期记忆。
- 长期记忆只保存可复用、可解释、长期有效的信息。
- 通用长期记忆避免吸收大量临时对话细节。
- 项目路径长期记忆避免吸收太具体的某次工程实现。
- skill 只能在用户审核后写入，不能由 daemon 自动静默安装。
- skill tree 记录的是操作频率、触发条件和技能候选，不是另一个隐藏 prompt。

## 3. 长期记忆分层

### 3.1 通用长期记忆

通用长期记忆绑定 actor，而不是绑定 repo 或 shell。

适合保存：

- 用户稳定偏好；
- 常用工具选择；
- 跨项目都成立的工作习惯；
- 用户明确要求长期记住的约束；
- 反复出现的通用任务模式。

不适合保存：

- 某次临时问答的大段内容；
- 一次性排错细节；
- 没有跨场景价值的文件路径；
- 未经用户确认的私人信息；
- 只在某个项目里成立的实现细节。

### 3.2 项目路径长期记忆

项目路径长期记忆绑定 canonical project root。对 git 工程，默认使用 repo root；对非 git 项目，必须由用户或显式规则声明项目根。

适合保存：

- 项目结构和主要模块职责；
- 构建、测试、格式化、发布命令；
- 本项目稳定的架构约束；
- 本项目常见任务入口；
- 本项目长期有效的编码约定；
- 项目内常用工作流和审核要求。

不适合保存：

- 某次 bugfix 的具体 patch 细节；
- 某次实现中的临时取舍；
- 已经被代码本身表达清楚的局部实现；
- 过期分支、临时文件、短期实验；
- 大段 diff、日志或测试输出。

项目记忆的目标是给未来任务提供更全面的上下文，而不是复述过去某次工程执行过程。

### 3.3 session history 不是长期记忆

session history 保存事实流水，长期记忆保存经过筛选的稳定结论。

因此：

- history 可以完整、细粒度、可审计；
- memory 必须短、稳、可复用；
- memory entry 应能追溯到来源 run，但不应依赖完整 transcript 才能理解。

## 4. 记忆写入流程

run 结束后，daemon 可以生成 memory candidate，但不能无条件写入长期记忆。

推荐流程：

1. 从 run history 提取候选结论。
2. 判断候选属于通用记忆还是项目路径记忆。
3. 过滤临时、过细、低置信度或敏感内容。
4. 生成短句化 memory candidate。
5. 持久化候选及来源引用。
6. 按策略自动忽略、等待用户确认，或在前端显示建议。
7. 通过审核后写入对应长期记忆。

候选内容必须包含：

- `scope`：`global` 或 `project`
- `project_root`：仅项目记忆需要
- `summary`
- `rationale`
- `source_run_id`
- `confidence`
- `status`

## 5. 记忆检索流程

每次 run 开始前，daemon 应按 scope 组装记忆上下文：

- 总是可以读取 actor 的通用长期记忆；
- 如果当前调用能确定 project root，则读取对应项目路径长期记忆；
- engineering session 优先加载 repo/project 记忆；
- general session 默认只加载通用长期记忆，除非用户显式绑定项目路径。

检索结果必须有预算限制：

- 不把整份长期记忆直接塞进 prompt；
- 优先选择与当前 prompt、路径、工具和最近任务相关的条目；
- 项目记忆优先于通用记忆中的冲突条目；
- 冲突条目应被标记并等待用户或后续整理处理。

## 6. 技能树

skill tree 是从长期记忆和历史操作频率中提炼出来的技能候选系统。

它不直接代表“已经安装的技能”，而是包含三类节点：

- observed pattern：系统观察到的重复操作模式；
- skill candidate：值得向用户建议沉淀的技能候选；
- installed skill：用户审核后写入并可被运行时发现的技能。

### 6.1 频率判断

daemon 可以根据以下信号判断某个操作是否频繁：

- 多次 run 中出现相似工具调用序列；
- 用户多次要求相似任务；
- 同一项目中反复执行相同验证流程；
- 长期记忆中多次出现同类偏好或约束；
- 人工明确标记“以后遇到这种情况就这样做”。

频率判断必须保留 scope：

- 通用频繁操作应进入 global skill candidate；
- 只在某项目路径内频繁出现的操作应进入 project skill candidate。

### 6.2 skill 建议流程

某次对话或工程 run 结束后，如果系统发现高价值重复模式，可以向用户建议创建 skill。

建议必须包含：

- skill 名称；
- 触发条件；
- 适用 scope；
- 预期步骤；
- 安全边界；
- 验证方式；
- 为什么建议沉淀为 skill；
- 关联的历史 pattern 或 memory entry。

用户可以：

- 接受并让 Lycoris 起草 skill；
- 修改后接受；
- 拒绝本次建议；
- 禁止某类建议再次出现。

未通过用户审核的 skill 不能进入 installed skill。

### 6.3 skill 写入规则

通过审核后，Lycoris 才能写入 skill。

写入目标：

- global skill：用户级 skills 目录；
- project skill：项目路径或 repo-local skills 目录；
- temporary skill：默认不支持长期安装，只能作为 run 内计划或 workflow。

skill 内容必须避免：

- 复制大段临时对话；
- 写入敏感信息；
- 写入某次具体实现的 patch；
- 绑定过窄、很快失效的文件行号；
- 绕过审批或权限策略。

## 7. 与 Unix 哲学的关系

长期记忆和 skill tree 不能让 Lycoris 变成常驻自驱系统。

具体要求：

- 记忆提取发生在 run 收口阶段或显式维护命令中。
- skill 建议作为事件和持久化候选存在，不要求 CLI 长期停留。
- 用户下一次 attach、打开 Web 或查询建议时可以处理候选。
- daemon 空闲时不为了“自我训练”持续运行 AI worker。
- skill 安装后也只是可发现能力，不是自动启动任务的触发器。

## 8. 测试重点

需要覆盖：

- 通用记忆不会吸收大量临时对话内容；
- 项目记忆不会保存过细的单次实现细节；
- 同一 actor 的通用记忆能跨 session 检索；
- 项目路径记忆只在匹配 project root 时检索；
- skill candidate 只能由频率信号或用户明确指令产生；
- installed skill 必须经过用户审核；
- 拒绝的 skill candidate 不会被自动安装；
- memory / skill 事件都能通过 history 和 attach 审计。
