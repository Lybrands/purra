# PurrA

[English](https://github.com/Lybrands/purra/blob/main/README.md) | 简体中文

PurrA 是一个与产品无关的 Agent 运行时，适合需要明确控制模型调用、工具、状态与恢复流程的应用。

Python 与 JavaScript/TypeScript 是相同安全契约的独立实现。

## 安装

Python 3.11+：

```bash
pip install purra
```

Node.js 22+：

```bash
npm install purra
```

npm 包使用原生 ESM。

## 开始使用

Python 完整 Run 从 `purra.api.AgentCore.submit()` 进入。参见可运行的
[Python Quickstart](https://github.com/Lybrands/purra/blob/main/examples/python/quickstart.py)。

JavaScript 和 TypeScript 参见
[包指南](https://github.com/Lybrands/purra/blob/main/typescript/README.md)与
[Quickstart](https://github.com/Lybrands/purra/blob/main/typescript/examples/quickstart.ts)。

Reactive 是默认执行方式，Planned 和 Durable 按需启用。宿主负责
Provider 适配器、工具、业务授权与生产持久化；运行保证和所有权边界见
[架构说明](https://github.com/Lybrands/purra/blob/main/ARCHITECTURE.md)。

## 输出 Token 契约

PurrA 0.5.0 明确区分单次模型调用上限与整个 Run 的累计预算。Python
分别使用 `max_call_output_tokens` 和
`max_run_output_tokens`；TypeScript 分别使用
`maxCallOutputTokens` 和 `maxRunOutputTokens`。

创建 Run 时必须显式给出累计预算；只有宿主明确选择“不设置有限累计上限”时，
才使用 Python 的 `None` 或 TypeScript 的 `null`。Provider 网关还必须回报
实际应用的单次上限；缺失或不匹配会在模型输出提交前触发契约错误。0.5.0
不为旧输出 Token 字段提供别名或持久化迁移。

## 兼容性

PurrA 目前处于 1.0 之前，破坏公共契约的变更必须升级 Minor 版本。
Python 与 npm 包使用同一 Git Tag 编码的版本号；相同版本不表示两端自动具备相同能力。

## 链接

- [可运行示例](https://github.com/Lybrands/purra/blob/main/examples/README.md)
- [问题反馈](https://github.com/Lybrands/purra/issues)

## 许可证

[MIT](https://github.com/Lybrands/purra/blob/main/LICENSE)
