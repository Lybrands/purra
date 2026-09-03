# purra-anthropic · Python

English | [简体中文](README.zh-CN.md)

Anthropic Messages gateway for PurrA using the official Python SDK. Supports text,
function tools, streaming, cancellation, and token usage. Requires Python 3.11+.

## Install

From the repository root:

```sh
python -m pip install . ./integrations/anthropic/python
```

## Configure

```python
from anthropic import AsyncAnthropic
from purra_anthropic import AnthropicMessagesGateway

client = AsyncAnthropic()
gateway = AnthropicMessagesGateway(client, timeout_seconds=60)
```

Pass `gateway` to `AgentCore(model_gateway=gateway, ...)`. In the Run's
`ModelRequest`, set `provider="anthropic"`, the model ID, and its capability snapshot.
The application supplies credentials and verified model limits.

## Model options

`ModelRequest.options` accepts `max_tokens`, `temperature`, `top_p`, `top_k`,
`thinking`, and `output_config`. Choose options supported by the selected model.

Enabled reasoning requires an explicit `thinking` configuration, such as
`{"type": "adaptive"}` on a supporting model. A manual `budget_tokens` must be
at least 1024 and below the resolved output limit. Forced tool choice cannot be
combined with enabled thinking.

## Behavior

SDK retries are disabled; Core controls retry and budget accounting. The gateway
applies Core's resolved output limit and combines system/developer instructions
into Anthropic's system field.

Signed thinking and redacted blocks are retained as private continuation data.
Keep them with the unchanged assistant message when continuing on the same model;
they are excluded from public output. Input usage includes cache writes and reads.

Multimodal content, hosted tools, and beta APIs are not supported.
Other vendors require application adapters. Close a supplied SDK client in the
application; `await gateway.close()` only closes a client created by the gateway.
