# purra-openai · Python

[English](README.md) | 简体中文

使用官方 Python SDK 将 OpenAI 接入 PurrA，支持文本、函数工具、流式输出、取消和 Token 用量。
要求 Python 3.11+。

## 安装

在仓库根目录执行：

```sh
python -m pip install . ./integrations/openai/python
```

## 选择 API

| 网关 | API |
| --- | --- |
| `OpenAIResponsesGateway` | Responses |
| `OpenAIChatCompletionsGateway` | Chat Completions |

```python
from openai import AsyncOpenAI
from purra_openai import OpenAIResponsesGateway

client = AsyncOpenAI()
gateway = OpenAIResponsesGateway(client, timeout_seconds=60)
```

使用 Chat Completions 时替换网关：

```python
from purra_openai import OpenAIChatCompletionsGateway

gateway = OpenAIChatCompletionsGateway(client, timeout_seconds=60)
```

将 `gateway` 传给 `AgentCore(model_gateway=gateway, ...)`。
在 Run 的 `ModelRequest` 中设置 `provider="openai"`、模型 ID 和模型能力快照。
凭据与已核实的模型限制由应用提供。

## 参数与行为

`ModelRequest.options` 接受 `max_tokens`、`temperature`、`top_p` 和 `reasoning_effort`。
根据所选模型支持的能力设置参数。Core 计算输出上限，网关将其应用到对应 API。

SDK 自动重试已关闭，重试和预算核算由 Core 控制。服务端响应存储已关闭。
Responses 的推理续接信息保存在私有消息数据中，恢复时应与对话一同保留。

网关支持 OpenAI 原生文本和函数工具 API，不支持图片、音频及服务端托管工具。
其他厂商由应用提供适配。

传入的 SDK 客户端由应用关闭。`await gateway.close()` 只关闭网关自行创建的客户端。
