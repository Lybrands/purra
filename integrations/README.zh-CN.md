# 可选包

[English](README.md) | 简体中文

这些包将 PurrA 连接到模型服务、存储和记忆系统。按应用需要选用，并与相同版本的 Core 一起安装。

| 包 | 用途 | 指南 |
| --- | --- | --- |
| `purra-openai` | OpenAI Responses 与 Chat Completions | [Python](openai/python/README.zh-CN.md) · [TypeScript](openai/typescript/README.zh-CN.md) |
| `purra-anthropic` | Anthropic Messages | [Python](anthropic/python/README.zh-CN.md) · [TypeScript](anthropic/typescript/README.zh-CN.md) |
| `purra-sqlite` | 持久化 Run、事件、检查点和工具回执 | [Python](sqlite/python/README.zh-CN.md) · [TypeScript](sqlite/typescript/README.zh-CN.md) |
| `purra-interaction` | 结构化提问与等待输入后的恢复 | [Python](interaction/python/README.zh-CN.md) · [TypeScript](interaction/typescript/README.zh-CN.md) |
| `purra-compaction` | 使用模型压缩对话历史 | [Python](compaction/python/README.zh-CN.md) · [TypeScript](compaction/typescript/README.zh-CN.md) |
| `purra-mcp` | 宿主连接上的只读 MCP 工具 | [Python](mcp/python/README.md) · [TypeScript](mcp/typescript/README.md) |
| `purra-mem0` | 按作用域隔离的长期记忆与检索 | [Python](mem0/python/README.zh-CN.md) · [TypeScript](mem0/typescript/README.zh-CN.md) |

模型适配包支持 OpenAI 和 Anthropic 原生 API。其他厂商的协议差异、模型配置和凭据由应用处理。

## 从源码安装

在仓库根目录执行。以下以 OpenAI 为例，可替换为所需组件的目录。

Python 3.11+：

```sh
python -m pip install . ./integrations/openai/python
```

Node.js 22+（SQLite、用户交互和 Mem0 组件要求 22.13+）：

```sh
npm --prefix typescript ci
npm --prefix typescript run build
npm --prefix integrations/openai/typescript ci
npm --prefix integrations/openai/typescript run build
```

然后在业务应用中安装构建好的 TypeScript 包：

```sh
npm install /path/to/purra/typescript /path/to/purra/integrations/openai/typescript
```

需要在同一 Run 内等待输入并恢复时，先安装并构建 `purra-sqlite`，再安装 `purra-interaction`。
Mem0 受控模型调用所需的附加依赖见对应语言指南。

## 应用配置

应用提供已认证身份、已授权作用域、模型能力和存储路径，并负责客户端生命周期、备份与数据保留。
组件使用 Core 的执行与预算接口；安装包后仍需在 Agent 中配置才能启用。

[升级说明](../CHANGELOG.md#从-0x-升级)。适配器安装不证明真实服务支持；应分别验证实际协议、能力与服务／模型组合。
