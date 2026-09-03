# purra-mem0 · TypeScript

[English](README.md) | 简体中文

使用 `mem0ai/oss` 的 `Memory` 提供作用域记忆、检索和生命周期管理。
要求 Node.js 22.13+，不支持托管的 Mem0 Platform 客户端。

## 安装

按照[源码安装说明](../../README.zh-CN.md#从源码安装)操作，选择
`integrations/mem0/typescript`。Mem0 的本地存储需要执行 `better-sqlite3` 的原生安装脚本。

需要受控的 LLM/Embedding 回调时，在应用中安装可选 peer 依赖：

```sh
npm install @langchain/core@1.1.47
```

导入 Mem0 前，设置 `MEM0_TELEMETRY=false`，并将 `MEM0_DIR` 指向应用管理的数据目录。
显式配置 LLM、embedder、`vectorStore.config.dbPath` 和 `historyDbPath`。

## 保存与读取

应用提供 `mem0Config`、已认证的 `userId`、已授权的 `projectId`，以及持久化 `journalPath`：

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

`add` 默认创建已启用记录；需要先审核时设置 `state: "pending"`。
记录不可用时，`get` 返回 `undefined`。更新、状态切换和删除需要当前 `version` 与稳定的操作 `key`。

## 接入 Agent

提供 `memoryQuery` 函数，从 Core 的 `ContextRequest` 中选取查询文本：

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

将 `recall.definition` 加入 Agent 工具，或将 `context` 与其他上下文提供器组合。
记忆实例已经绑定作用域。需要显式选择记录时，使用 `assembleMemoryContext(memory, ids, allowance)`。

## 受控模型调用

调用需要持久化预算时，使用以下配置替代原生 SDK 客户端。
存储路径、向量维度和预算键由应用提供，数值仅用于演示预算配置。

`modelTasks` 是 PurrA 扩展工厂提供的执行器。
异步回调 `embed(texts, signal)` 需要返回 `EmbeddingResult`，包含向量和服务报告的输入 Token 用量。

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
      maxCallOutputTokens: 2048,
    },
    complete: runModel(modelTasks),
    embed,
  },
});
```

通过 `memory.budgetUsage()` 查看已准入调用和报告用量。原生 SDK 模式的内部用量记为未知。

## 提取与审查

配置受控模型调用并设置 `allowInference: true` 后：

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

此配置审查候选并让其保持待处理。需要应用决定时，为 `MemoryWorkflow` 传入
`policy(candidate, review)` 和稳定的 `policyRevision`。
策略返回已授权且保留审查键的 `MemoryResolution`，或返回 `undefined` 保持待处理；策略可以是异步函数。

## 生命周期

关闭前调用 `await memory.drain()` 和 `memory.close()`，再关闭 SDK 与模型服务资源。
分页、来源撤回、证据校验和不确定写入的处理见[记忆生命周期指南](../README.zh-CN.md)。
