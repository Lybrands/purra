# purra-openai · TypeScript

[English](README.md) | 简体中文

使用官方 SDK 将 OpenAI 接入 PurrA，支持文本、函数工具、流式输出、取消和 Token 用量。
要求 Node.js 22+（ESM）。

## 安装

按照[源码安装说明](../../README.zh-CN.md#从源码安装)操作，选择
`integrations/openai/typescript`。

## 配置

传入所选模型 ID 和已核实的模型能力快照：

```ts
import { Agent, type ModelCapabilitySnapshot } from "purra";
import { OpenAIResponsesGateway } from "purra-openai";

function createAgent(model: string, capabilities: ModelCapabilitySnapshot) {
  const gateway = new OpenAIResponsesGateway({
    model,
    capabilities,
    timeoutMs: 60_000,
  });
  return new Agent({ model: gateway });
}
```

通过 `client` 传入 SDK 客户端，可自定义凭据与传输配置。
省略时，网关使用 SDK 的环境配置创建客户端。

| 网关 | API | 模型参数 |
| --- | --- | --- |
| `OpenAIResponsesGateway` | Responses | `reasoning` |
| `OpenAIChatCompletionsGateway` | Chat Completions | `reasoningEffort`、`temperature`、`topP` |

两个网关均接受 `model`、`capabilities`、`client` 和 `timeoutMs`。
使用 Chat Completions 时，导入 `OpenAIChatCompletionsGateway` 并使用这些参数构造。
模型参数应按所选模型支持的能力设置。

## 行为

Core 计算输出上限，网关将其应用到对应 API。SDK 自动重试已关闭，重试和预算核算由 Core 控制。
服务端响应存储已关闭。

Responses 的推理续接信息保存在私有消息数据中，恢复时应与对话一同保留。
有工具可用时采用自动工具选择。不支持图片、音频及服务端托管工具，其他厂商由应用提供适配。
