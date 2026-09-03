# PurrA

[English](README.md) | 简体中文

PurrA 是面向 Python 和 TypeScript 的 Agent 运行时，负责在应用中协调模型调用、
工具执行、规划、上下文和可恢复的 Run。应用提供模型、工具、数据源与访问规则。

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
            runtime_limits=RuntimeLimits(max_run_output_tokens=8192),
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
- **上下文**：分配输入预算、检索外部数据和压缩对话历史。
- **Agent 树与长任务**：委派工作，通过仓储接口保存执行进度。

应用负责选择模型及其限制、授权数据访问和工具副作用，以及管理持久化存储。
检索内容和工具结果作为数据处理，不作为指令。

## 预算与恢复

`max_call_output_tokens` 限制单次模型调用的输出，`max_run_output_tokens` 限制整个 Run
累计的模型输出。累计预算必须显式设置；`None` 表示不设有限 Token 上限。
使用有限预算时，模型服务必须报告实际用量。

恢复从已提交的检查点继续。应用需要还原相同的模型和工具配置，并在重试前确认
中断的外部写入是否已经执行。仅配置持久化存储，不能保证这类写入可以安全重放。

## 文档

- [TypeScript 指南](typescript/README.zh-CN.md)
- [示例](examples/README.zh-CN.md)
- [可选包](integrations/README.zh-CN.md)
- [架构](ARCHITECTURE.md)
- [跨语言一致性检查](conformance/README.zh-CN.md)
- [更新日志](CHANGELOG.md)

## 许可证

[MIT](LICENSE)
