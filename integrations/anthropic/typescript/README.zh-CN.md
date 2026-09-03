# purra-anthropic · TypeScript

[English](README.md) | 简体中文

使用官方 SDK 将 Anthropic Messages 接入 PurrA，支持文本、函数工具、流式输出、取消和 Token 用量。
要求 Node.js 22+（ESM）。

## 安装

按照[源码安装说明](../../README.zh-CN.md#从源码安装)操作，选择
`integrations/anthropic/typescript`。

## 配置

传入所选模型 ID 和已核实的模型能力快照：

```ts
import { Agent, type ModelCapabilitySnapshot } from "purra";
import { AnthropicMessagesGateway } from "purra-anthropic";

function createAgent(model: string, capabilities: ModelCapabilitySnapshot) {
  const gateway = new AnthropicMessagesGateway({
    model,
    capabilities,
    timeoutMs: 60_000,
  });
  return new Agent({ model: gateway });
}
```

通过 `client` 传入 SDK 客户端，可自定义凭据与传输配置。
省略时，网关使用 SDK 的环境配置创建客户端。

## 模型参数

构造函数接受 `thinking`、`outputConfig`、`temperature`、`topP` 和 `topK`。
根据所选模型支持的能力设置参数。例如，`thinking: { type: "adaptive" }`
要求模型支持自适应 thinking。手动推理预算必须是至少 1024 的整数，且小于 Core 确定的输出上限。

## 行为

SDK 自动重试已关闭，重试与预算核算由 Core 控制。网关使用 Core 确定的输出上限，
将 system/developer 指令合并到 system 字段，有工具可用时采用自动工具选择。

带签名的 thinking 和 redacted 块保留为私有续接数据。使用同一模型继续对话时，
应与未改动的 assistant 消息一同保留；它们不会进入公开输出。输入用量包括缓存写入和读取。

不支持多模态内容、服务端托管工具和 beta API。其他厂商由应用提供适配。
