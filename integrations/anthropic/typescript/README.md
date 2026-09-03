# purra-anthropic · TypeScript

English | [简体中文](README.zh-CN.md)

Anthropic Messages gateway for PurrA using the official SDK. Supports text,
function tools, streaming, cancellation, and token usage. Requires Node.js 22+ (ESM).

## Install

Follow [source installation](../../README.md#source-installation), selecting
`integrations/anthropic/typescript`.

## Configure

Supply the selected model ID and its verified capability snapshot:

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

Pass an SDK client through `client` to configure credentials or transport.
Without one, the gateway creates a client using the SDK's environment configuration.

## Model options

The constructor accepts `thinking`, `outputConfig`, `temperature`, `topP`, and
`topK`. Choose options supported by the selected model. For example,
`thinking: { type: "adaptive" }` requires a model that supports adaptive thinking.
Manual thinking budgets must be integers, at least 1024, and below Core's resolved
output limit.

## Behavior

SDK retries are disabled; Core controls retry and budget accounting. The gateway
applies Core's output limit and combines system/developer instructions into the
system field. Tool choice is automatic when tools are available.

Signed thinking and redacted blocks are retained as private continuation data.
Keep them with the unchanged assistant message when continuing on the same model;
they are excluded from public output. Input usage includes cache writes and reads.

Multimodal content, hosted tools, and beta APIs are not supported.
Other vendors require application adapters.
