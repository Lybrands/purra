# PurrA for JavaScript and TypeScript

[English](README.md) | 简体中文

适用于 Node.js 22+ 的 ESM Agent 运行时。应用提供 `ModelGateway`，通过 `purra` 的公开导出
组合工具、上下文、规划和持久化能力。


## 1.0.0 候选状态

当前源码为尚未发布的 1.0.0 候选。请按候选清单安装准确本地产物；注册表安装命令不保证获取本候选。
首发结构化任务、MCP 只读工具、安全读批次并行及只读诊断的支持范围，
以及 0.x 升级和 1.x 兼容承诺，见[候选说明](../conformance/release-1.0.zh-CN.md)。

## 安装

```sh
npm install purra
```

## 运行 Agent

将配置好的模型网关传入 `ask`。[OpenAI 和 Anthropic 适配包](../integrations/README.zh-CN.md)
提供各自原生 API 的网关。自定义网关需要声明模型能力，并确认每次请求实际应用的精确总生成上限。

```ts
import { Agent, type ModelGateway } from "purra";

async function ask(model: ModelGateway) {
  const agent = new Agent({ model });
  const run = await agent.submit({
    messages: [{ role: "user", content: "Hello" }],
  }, {
    budgets: { maxRunGenerationTokens: 8192 },
  });

  for await (const event of run.events()) {
    console.log(event.kind, event.payload);
  }
  return (await run.result).output;
}
```

`submit()` 返回 Run 句柄，提供已提交事件、取消和执行结果。调用 `run.cancel()` 请求取消。
只需要进程内结果时使用 `invoke()`；需要临时事件流、不需要持久化 Run 时使用 `stream()`。
本地网关与工具的完整代码见[快速开始示例](examples/quickstart.ts)。

## 工具与检索

通过 `new Agent({ model, tools })` 传入工具定义。每个工具声明输入 Schema 和副作用策略。
有副作用的工具需要幂等保障；`confirm` 工具还需要应用批准。

`RetrieverTool` 将应用实现的 `Retriever` 接入同一套工具系统。应用选择数据源和作用域，
并在读取前校验访问权限；模型只提供查询文本。注册方式见[快速开始示例](examples/quickstart.ts)。

带工具的普通 Run 会额外调用一次不带工具的模型来生成公开回答。
设置调用次数、Token 和截止时间预算时，需要为这次调用预留额度。

## 规划

已有模型网关 `model` 时，可这样启用内置 Planner：

```ts
import { Agent, ModelWorkPlanner } from "purra";

const agent = new Agent({
  model,
  planning: { plannerFactory: tasks => new ModelWorkPlanner(tasks) },
});
```

Run 请求通过 `planningMode` 选择模式：

| 模式 | 行为 |
| --- | --- |
| `auto` | 默认模式。正常开始，在模型请求规划或工具要求规划时进入规划流程。 |
| `reactive` | 通过模型与工具循环执行，不激活规划。 |
| `planned` | 执行任务前先规划。 |

Planner 要求网关支持流式输出。订阅 Run 事件可接收公开的 `planning.progress`，
其中不包含私有计划和推理。订阅与重放用法见[规划示例](examples/planner-streaming.ts)。

## 预算与持久化

`RunRequest.maxGenerationTokens` 是用户对单次 Provider 调用全部生成 Token 的上限；当模型把
推理计入 generation 时，其中也包括推理 Token。`maxRunGenerationTokens` 限制 Run 累计生成量，
并且必须显式设置；`null` 表示不设有限 Token 上限。有限预算要求模型服务报告实际用量。

`NewRunOptions.resultCapacityTargetTokens` 是正式结果内容的可选工作流容量目标，不会降低
Provider 的生成额度；只有选定上下文窗口的物理容量可以收紧该额度。模型未报告推理用量时，
Core 会保留“未知”状态，而不会把它记成 0。

内置仓储使用内存。需要持久化时，配置 [SQLite 适配器](../integrations/sqlite/typescript/README.zh-CN.md)
或实现仓储接口。恢复使用已提交的检查点，并要求还原原有模型与工具配置。
结果不确定的外部写入，需要先确认执行结果再重试。

使用子 Agent 时，通过 `runRepository` 和 `agentTree.repository` 传入同一组 Run 与 Run 树仓储。
子 Agent 的能力和预算受父级约束，详见[架构说明](ARCHITECTURE.md)。

## 更多

- [项目指南](../README.zh-CN.md)
- [示例](../examples/README.zh-CN.md)
- [可选包](../integrations/README.zh-CN.md)
- [MIT 许可证](LICENSE)
