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
