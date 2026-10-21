# PurrA

[English](README.md) | 简体中文

PurrA 是面向 Python 和 TypeScript 的 Agent 运行时，负责在应用中协调模型调用、
工具执行、规划、上下文和可恢复的 Run。应用提供模型、工具、数据源与访问规则。

## 1.0 发布线

当前源码版本为 **1.0.3**。请安装版本一致的 Core 与集成包。本次更新将 Python 的
执行结果与公开回复的呈现结果分开记录；TypeScript 运行时行为不变。具体改动及覆盖范围见
[更新日志](CHANGELOG.md)。

双端已实现持久化 Reactive、Planned、Auto Root 的审批等待与恢复、宿主授权 MCP 写入、
持久回执及只读恢复诊断。存储与效果恢复已在支持的 Root 单写边界内实现；Child 写审批、
混合／多调用审批批次及通用分布式效果恢复不在该边界内。详见
[审批契约](conformance/durable-approval.zh-CN.md)和
[实现与验收边界](conformance/runtime-extension-acceptance.zh-CN.md)。

确定性测试与已选服务组合的验证，不能代替真实业务 MCP、下游应用、跨平台迁移或生产负载验收。
应用仍需验证实际使用的模型、协议、工具及宿主配置。升级前请阅读
[升级说明](docs/migrations/1.0.zh-CN.md)与[公共兼容承诺](ARCHITECTURE.md#public-compatibility)。

## 安装

Python 3.11+：

```sh
pip install purra
```

Node.js 22+（ESM）：

```sh
npm install purra
```

Core 没有运行时依赖。模型 SDK、存储和记忆集成通过[可选包](integrations/README.zh-CN.md)提供。

## Python 快速开始

提供模型网关、包含所选模型能力信息的 `ModelRequest`，以及模型的上下文窗口大小。
网关可以使用可选适配包，也可以自行实现 PurrA 的 `ModelGateway` 接口。

```python
from purra.api import AgentCore, AgentPreset, InMemoryAgentAdapters
from purra.contracts import (
    AgentMessage, AgentRunRequest, DomainContext, ModelRequest, RuntimeLimits,
)
from purra.tools import InMemoryToolCatalog

async def ask(gateway, model: ModelRequest, context_window: int):
    storage = InMemoryAgentAdapters()
    core = AgentCore(
        model_gateway=gateway,
        preset=AgentPreset(
            id="assistant",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            runtime_limits=RuntimeLimits(max_run_generation_tokens=8192),
        ),
        run_repository=storage.runs,
        output_repository=storage.outputs,
        output_publisher=storage.publisher,
    )
    try:
        run = await core.submit(AgentRunRequest(
            messages=(AgentMessage(role="user", content="Hello"),),
            model=model,
            domain_context=DomainContext(namespace="example"),
            context_window=context_window,
        ))
        result = await run.wait()
        return result.final_response
    finally:
        await core.close()
```

示例使用内存保存 Run；需要在进程重启后保留数据时，请配置持久化仓储。
无需 API 凭据即可运行的完整示例见[本地示例](examples/README.zh-CN.md)。
JavaScript 和 TypeScript 用法见 [TypeScript 指南](typescript/README.zh-CN.md)。

## 运行时能力

- **Run 管理**：提交任务、订阅已提交事件、取消执行和重放输出。
- **工具执行**：校验参数、执行访问与副作用策略、记录结果。
- **规划**：配置 Planner 后，可选择 Auto、Reactive 或 Planned 执行模式。
- **结构化输出**：执行带 Schema 的对象任务，采用本地校验或明确支持的服务端原生约束。
- **上下文**：分配输入预算、检索外部数据和压缩对话历史。
- **Agent 树与长任务**：委派工作，通过仓储接口保存执行进度。
- **模型选择**：在创建 Run 前选择已授权且能力兼容的绑定，恢复时保留原绑定。
- **记忆**：通过可选 Mem0 集成提供限定作用域的记忆、授权捕获、显式关系与宿主选择的上下文。
- **静态图片**：通过能力兼容且显式启用的模型传输，输入内联 PNG、JPEG 或 WebP 并获取文本回复。

应用负责选择模型及其限制、授权数据访问和工具副作用，以及管理持久化存储。
检索内容和工具结果作为数据处理，不作为指令。

## 预算与恢复

`ModelRequest.max_generation_tokens` 是用户对单次 Provider 总生成量的可选上限；
当模型档案声明思考 Token 包含在生成量中时，这个上限也包含思考。
`RuntimeLimits.max_run_generation_tokens` 限制整个 Run 的累计总生成量；`None`
表示不设有限上限。使用有限预算时，模型服务必须报告实际用量。工作流可选的
`result_capacity_target_tokens` 仅用于上下文容量规划和诊断，既不会缩小 Provider
上限，也不保证实际可见正文数量。

恢复从已提交的检查点继续。应用需要还原相同的模型和工具配置，并在重试前确认
中断的外部写入是否已经执行。仅配置持久化存储，不能保证这类写入可以安全重放。

可选 SQLite 集成保存 Run、输出、检查点、lease 与回执。宿主驱动的恢复 Worker 可发现和
诊断持久化 Run，再调用公共恢复 API；发现 Run 不代表获得执行授权，也不会清除未知效果。
SQLite v4→v5 审批激活需要显式离线执行，Python 与 TypeScript 数据库不能互换。详见
[恢复 Worker](conformance/recovery-worker.md)与[SQLite 升级和回滚](conformance/sqlite-upgrade.md)。

## 执行结果与公开回复

Python 使用 `validated_result` 模式并启用 `model_live` 呈现时，
`AgentRunResult.presentation` 可单独记录 `completed` 或 `failed` 回执，与执行状态分开。
如果执行成功但公开回复呈现失败，已校验的结果仍然提交，Run 为 `done`，
`final_response` 为空。宿主应分别检查两种结果，避免仅为重新生成回复而重复业务执行。
取消与执行截止时间仍保留原有执行语义；历史 Run 和其他呈现模式不会被推断补上回执。

宿主也可以省略公开呈现。Python 与 TypeScript 使用不同的结果 API；仅供宿主使用的结果
不能直接转发到公开聊天界面。[宿主策略指南](docs/host-policies.md)说明组合方式、默认行为与输出可见性。

## 文档

- 开始使用：[TypeScript 指南](typescript/README.zh-CN.md)、[本地示例](examples/README.zh-CN.md)、[可选包](integrations/README.zh-CN.md)。
- 执行：[宿主策略与输出](docs/host-policies.md)、[计划输出](docs/planning-output.md)、[结构化输出](docs/structured-output.md)、[工具并发](docs/tool-concurrency.md)。
- 恢复：[持久化审批](conformance/durable-approval.zh-CN.md)、[恢复 Worker](conformance/recovery-worker.md)、[接入与恢复诊断](docs/integration-inspection.md)、[验收边界](conformance/runtime-extension-acceptance.zh-CN.md)。
- 模型与输入：[模型路由](conformance/model-routing.md)、[静态图片](docs/static-images.md)。
- 记忆：[捕获与关系契约](conformance/memory-capture-relations.md)、[上下文选择](integrations/mem0/selection.md)、[关系提案](integrations/mem0/relations.md)。
- 升级：[0.x 到 1.0](docs/migrations/1.0.zh-CN.md)、[SQLite v4 到 v5](conformance/sqlite-upgrade.md)。
- 参考：[架构与兼容承诺](ARCHITECTURE.md)、[跨语言一致性检查](conformance/README.zh-CN.md)、[更新日志](CHANGELOG.md)。

## 许可证

[MIT](LICENSE)
