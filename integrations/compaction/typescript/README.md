# purra-compaction · TypeScript

English | [简体中文](README.zh-CN.md)

Compress older conversation turns under context pressure using PurrA's managed
model task runner. Requires Node.js 22+ and a matching `purra` package.

## Install

Follow [source installation](../../README.md#source-installation), selecting
`integrations/compaction/typescript`.

## Configure

```ts
import { Agent, type ModelGateway } from "purra";
import { SemanticCompaction } from "purra-compaction";

function createAgent(model: ModelGateway) {
  return new Agent({
    model,
    context: {
      compressionFactory: tasks => new SemanticCompaction(tasks, {
        maxSummaryTokens: 1024,
        maxInputTokens: 16000,
        keepRecentMessages: 8,
      }),
    },
  });
}
```

| Option | Default | Purpose |
| --- | --- | --- |
| `maxSummaryTokens` | 1024 | Output limit for the summary call |
| `maxInputTokens` | 16000 | Maximum estimated input tokens for the summarizer |
| `keepRecentMessages` | 8 | Recent-message retention target, adjusted to complete turns |

## Behavior

Each compression uses one managed call and consumes the Run's budget. It retains
instructions and recent complete turns, and summarizes older goals, constraints,
decisions, completed work, open questions, and evidence references.

The summary is an untrusted context block. Private reasoning and provider
continuation data are excluded from summarization. Invalid or oversized summaries
fail without replacing the input. Core persists accepted summaries in prepared
context checkpoints; the application retains the original conversation.
