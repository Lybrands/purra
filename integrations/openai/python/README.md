# purra-openai · Python

English | [简体中文](README.zh-CN.md)

OpenAI model gateways for PurrA using the official Python SDK. Supports text,
function tools, streaming, cancellation, and token usage. Requires Python 3.11+.

## Install

From the repository root:

```sh
python -m pip install . ./integrations/openai/python
```

## Choose an API

| Gateway | API |
| --- | --- |
| `OpenAIResponsesGateway` | Responses |
| `OpenAIChatCompletionsGateway` | Chat Completions |

```python
from openai import AsyncOpenAI
from purra_openai import OpenAIResponsesGateway

client = AsyncOpenAI()
gateway = OpenAIResponsesGateway(client, timeout_seconds=60)
```

For Chat Completions, replace the gateway:

```python
from purra_openai import OpenAIChatCompletionsGateway

gateway = OpenAIChatCompletionsGateway(client, timeout_seconds=60)
```

Pass `gateway` to `AgentCore(model_gateway=gateway, ...)`. In the Run's
`ModelRequest`, set `provider="openai"`, the model ID, and its capability snapshot.
The application supplies credentials and verified model limits.

## Options and behavior

`ModelRequest.options` accepts `max_tokens`, `temperature`, `top_p`, and
`reasoning_effort`. Choose options supported by the selected model.
Core resolves the output limit; the gateway applies it to the selected API.

SDK retries are disabled so Core controls retry and budget accounting.
Server-side response storage is disabled. Responses reasoning continuation is
kept in private message data; preserve it with the conversation when resuming.

These gateways support native OpenAI text and function-tool APIs. Images, audio,
and hosted tools are not supported. Other vendors require application adapters.

Close a supplied SDK client in the application. `await gateway.close()` closes
only a client created by the gateway itself.
