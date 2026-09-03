# purra-anthropic · Python

[English](README.md) | 简体中文

使用官方 Python SDK 将 Anthropic Messages 接入 PurrA，支持文本、函数工具、流式输出、
取消和 Token 用量。要求 Python 3.11+。

## 安装

在仓库根目录执行：

```sh
python -m pip install . ./integrations/anthropic/python
```

## 配置

```python
from anthropic import AsyncAnthropic
from purra_anthropic import AnthropicMessagesGateway

client = AsyncAnthropic()
gateway = AnthropicMessagesGateway(client, timeout_seconds=60)
```

将 `gateway` 传给 `AgentCore(model_gateway=gateway, ...)`。
在 Run 的 `ModelRequest` 中设置 `provider="anthropic"`、模型 ID 和模型能力快照。
凭据与已核实的模型限制由应用提供。

## 模型参数

`ModelRequest.options` 接受 `max_tokens`、`temperature`、`top_p`、`top_k`、`thinking`
和 `output_config`。根据所选模型支持的能力设置参数。

启用推理时，需要显式配置 `thinking`，例如在支持的模型上使用 `{"type": "adaptive"}`。
手动设置的 `budget_tokens` 至少为 1024，且必须小于已确定的输出上限。
启用 thinking 时不能强制工具选择。

## 行为

SDK 自动重试已关闭，重试与预算核算由 Core 控制。网关使用 Core 确定的输出上限，
并将 system/developer 指令合并到 Anthropic 的 system 字段。

带签名的 thinking 和 redacted 块保留为私有续接数据。使用同一模型继续对话时，
应与未改动的 assistant 消息一同保留；它们不会进入公开输出。输入用量包括缓存写入和读取。

不支持多模态内容、服务端托管工具和 beta API。其他厂商由应用提供适配。
传入的 SDK 客户端由应用关闭；`await gateway.close()` 只关闭网关自行创建的客户端。
