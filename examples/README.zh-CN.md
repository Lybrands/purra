# 示例

[English](README.md) | 简体中文

这些示例使用本地模型网关和内存存储，无需 API Key，也不会调用网络模型服务。

| 示例 | Python | TypeScript |
| --- | --- | --- |
| 调用工具并返回回答 | [quickstart.py](python/quickstart.py) | [quickstart.ts](../typescript/examples/quickstart.ts) |
| 订阅规划进度、取消与重放事件 | [planner_streaming.py](python/planner_streaming.py) | [planner-streaming.ts](../typescript/examples/planner-streaming.ts) |

## Python

使用 Python 3.11+，在仓库根目录执行：

```sh
python -m pip install -e .
python examples/python/quickstart.py
python examples/python/planner_streaming.py
```

## TypeScript

使用 Node.js 22+，在仓库根目录执行：

```sh
npm --prefix typescript ci
npm --prefix typescript run example
```

该命令会编译并运行两个 TypeScript 示例。

接入模型服务或持久化存储时，可将本地适配器替换为[可选包](../integrations/README.zh-CN.md)
或应用自己的实现。快速开始中的 Retriever 读取固定的公开数据；应用的 Retriever 需要校验其数据源访问权限。


## 结构化任务、只读并行与诊断

- [Python 结构化任务](python/structured_task.py) / [TS 结构化任务](../typescript/examples/structured-task.ts)：本地固定响应，一次显式修复；低层 runner 回执明确没有持久化或 Root 预算绑定，宿主应传入当前 Run 的 runner/authority。
- [Python 并行读取](python/parallel_tools.py) / [TS 并行读取](../typescript/examples/parallel-tools.ts)：宿主声明安全的只读批次，限额 2。
- [Python 接入报告](python/integration_check.py) / [TS 接入报告](../typescript/examples/integration-check.ts)：复用已有 conformance 检查，报告已执行/未执行覆盖，只读查询缺少的恢复证据保留 unknown。
- 安装 Core 和可选 MCP 包后，可运行 [Python stdio 消费者](../integrations/mcp/python/scripts/check_installed.py) / [TS stdio 消费者](../integrations/mcp/typescript/scripts/check-installed.mjs)。使用官方 SDK 启动并关闭本地 fixture server，不代表第三方 MCP 服务验证。

所有 Core TS 示例均纳入 `npm --prefix typescript run example`。Python 示例可直接用安装了 Core 的解释器运行。诊断的边界见[公开契约](../ARCHITECTURE.md#integration-reports-and-recovery-inspection)。
