# purra-mem0 · TypeScript

English | [简体中文](README.zh-CN.md)

Scoped memory, retrieval, and lifecycle management using `Memory` from
`mem0ai/oss`. Requires Node.js 22.13+; hosted Mem0 Platform clients are not supported.

## Install

Follow [source installation](../../README.md#source-installation), selecting
`integrations/mem0/typescript`. Mem0's local store needs the `better-sqlite3`
native installation scripts.

For managed LLM/Embedding callbacks, install the optional peer in your application:

```sh
npm install @langchain/core@1.1.47
```

Before importing Mem0, set `MEM0_TELEMETRY=false` and an application-owned
`MEM0_DIR`. Explicitly configure the LLM, embedder, `vectorStore.config.dbPath`,
and `historyDbPath`.

## Save and read

The application supplies `mem0Config`, authenticated `userId`, authorized
`projectId`, and a persistent `journalPath`:

```ts
import { Memory } from "mem0ai/oss";
import { Mem0Memory } from "purra-mem0";

const sdk = new Memory(mem0Config);
const memory = new Mem0Memory({
  client: sdk,
  scope: { user: userId, project: projectId },
  journalPath,
});

async function savePreference() {
  const saved = await memory.add("Reply in Chinese.", {
    source: { id: "preference:language", revision: "1" },
    key: "preference:language:1",
  });
  return memory.get(saved.ids[0]!);
}
```

`add` creates an active record by default; use `state: "pending"` for review first.
`get` returns `undefined` for unavailable records. Updates, state changes, and
deletions require the current `version` and a stable operation `key`.

## Connect to an Agent

Supply `memoryQuery` as a function that selects query text from a Core `ContextRequest`:

```ts
import { RetrieverTool } from "purra";
import { MemoryContext } from "purra-mem0";

const recall = new RetrieverTool({
  retriever: memory,
  name: "recallMemory",
  description: "Recall saved preferences and facts.",
});
const context = new MemoryContext({ memory, query: memoryQuery });
```

Add `recall.definition` to the Agent's tools, or compose `context` with its context
providers. The memory instance already binds the scope. For explicit record
selection, use `assembleMemoryContext(memory, ids, allowance)`.

## Managed model calls

Use the following configuration instead of a raw SDK client when calls need
persistent budgets. Storage paths, embedding dimensions, and the budget key come
from the application; the numeric limits below illustrate a budget configuration.

`modelTasks` is the runner supplied by a PurrA extension factory.
The async `embed(texts, signal)` callback must return `EmbeddingResult` with
vectors and any reported input-token usage.

```ts
import { createManagedClient, runModel } from "purra-mem0";

const client = await createManagedClient({
  embeddingDims: embeddingDimensions,
  config: { vectorStore: vectorStoreConfig, historyDbPath: historyPath },
});
const memory = new Mem0Memory({
  client,
  scope: { user: userId, project: projectId },
  journalPath,
  allowInference: true,
  providers: {
    budget: {
      key: budgetKey,
      maxLlmCalls: 4,
      maxEmbeddingCalls: 64,
      maxInputChars: 100_000,
      maxOutputTokens: 8192,
      resultCapacityTargetTokens: 2048,
    },
    complete: runModel(modelTasks),
    embed,
  },
});
```

Use `memory.budgetUsage()` to inspect admitted and reported usage. Raw SDK mode
reports internal usage as unknown.

`resultCapacityTargetTokens` reserves the expected per-call memory result size.
It is not the Provider generation limit; `runModel()` preserves the Run's user
generation ceiling and records the memory value as a workflow capacity target.

## Extract and review

With managed providers and `allowInference: true`:

```ts
import { MemoryWorkflow, type MemorySource } from "purra-mem0";

const workflow = new MemoryWorkflow(memory);

async function capture(
  messages: readonly { role: "user" | "assistant"; content: string }[],
  source: MemorySource,
  operationKey: string,
) {
  return workflow.capture(messages, { source, key: operationKey });
}
```

This configuration reviews candidates and leaves them pending. To apply decisions,
pass `policy(candidate, review)` and a stable `policyRevision` to `MemoryWorkflow`.
Return an authorized `MemoryResolution` that retains the review key, or `undefined`
to leave the candidate pending. The policy may be async.

## Lifecycle

Before shutdown, call `await memory.drain()` and `memory.close()`, then close SDK
and provider resources. For paging, withdrawal, evidence validation, and uncertain
writes, see the [memory lifecycle guide](../README.md).
