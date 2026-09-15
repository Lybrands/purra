# PurrTypos 接入 PurrA 1.0.0：统一 Run 执行

本地候选使用 1.0.0 版本号，尚未发布。`707e16b` 是历史基线，不能代表
工作区中的新协议。宿主以 `backend/purra-candidate.json` 和
`backend/requirements-purra.txt` 的精确 wheel 哈希识别候选。

## 普通操作

普通工具和 Recipe Unit 属于拥有任务的当前 Run，不自动创建 Agent 或新 Run。
Python `AgentCore.execute_operation(request, run_id=..., operation_id=..., options=...)`
提供私有的 reactive 模型与工具循环，要求当前 Run 正在执行，且 Core 不启用 Agent 树。
调用方负责操作准入、租约和完成状态；此入口不改变主 Run 的计划、检查点或最终状态。
模型用量与工具事件记入拥有者 Run，工具结果带 `executionScopeId`，正文保持私有。

PurrTypos 使用 `run_durable_operation` 校验 Unit 租约及任务拥有者，并继承拥有者 Agent
的能力授权。小说分析、写作技法、剧本及剧本最终结构化响应走此唯一入口。
新的 Unit 不绑定独立 Run；产物和读证据按操作作用域隔离，避免并行结果相互覆盖。

## 模型决定 Agent 职责

需要独立上下文和持续职责时，由模型调用 `delegateToAgents` 创建 Agent，
用 `listAgents`、`getAgent` 查找已有 Agent，并通过 `continueAgent` 派发后续任务。
职责说明与本轮目标分开；不内置业务角色，也不把 Unit 与 Agent 固定映射。
宿主工具白名单必须开放这些能力和 `receiveAgentResults`。

PurrTypos 显式配置 `AgentTreePolicy.result_presentation_instruction`，并将交付接入
活动主 Agent 的 `report_agent_results`。真实子 Agent 的每次结果触发主 Agent
串行公开说明；失败同样说明。该产品策略由宿主提供，框架默认只接收结果。
普通操作不产生子 Agent 反馈阶段，公开工具过程与最终正文统一属于主 Run。

## 持久化与验证

宿主使用 `ai_agent_tree_commands_v3` 作为唯一活动树日志，校验参考状态机摘要。
旧日志原样保留用于诊断，不参与新执行。旧未完成任务的独立 Unit Run 绑定不能
自动迁移或重跑，需要独立对账。不要修改真实数据库来执行测试。

构建 core wheel 时隔离生成目录，核对包内全部 Python 文件与当前源码完全一致。
宿主按本地 file URL 与 sha256 安装，检查 `direct_url.json` 与候选清单。
适配器 wheel 未改动时保留原哈希。

协议见 [统一输出与结果接收](../../conformance/parent-result-streaming.md)。
宿主说明见 PurrTypos 的 `docs/migrations/purra-1.0-integration.zh-CN.md`。
确定性测试、包一致性、真实 Provider 和 Web/Electron 验收是不同边界，不能互相替代。
