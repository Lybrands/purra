# purra-interaction · TypeScript

[English](README.md) | 简体中文

持久化结构化问题，等待用户输入后继续执行。要求 Node.js 22.13+。

| API | 用途 |
| --- | --- |
| `SqliteClarification` | 通过 `purra-sqlite` 检查点暂停并恢复同一 Run |
| `ClarificationStore` + `ClarificationWorkflow` | 在执行前收集输入，再通过应用回调创建 Run |

## 安装

按照[源码安装说明](../../README.zh-CN.md#从源码安装)操作。
使用 `SqliteClarification` 时，先构建并安装 `purra-sqlite`，再处理
`integrations/interaction/typescript`。

## 配置 Run

已有模型网关 `model` 时：

```ts
import { Agent } from "purra";
import { SqliteAgentAdapters } from "purra-sqlite";
import { SqliteClarification } from "purra-interaction";

const storage = new SqliteAgentAdapters("agent.db", { scope: "user-1/project-1" });
const interaction = new SqliteClarification(storage);
const agent = new Agent({
  model,
  preset: { id: "assistant", revision: "1" },
  tools: [interaction.tool],
  checkpointHandler: interaction.checkpointHandler,
  runRepository: storage.runs,
  outputPublisher: storage.publisher,
});
```

通过 `agent.submit(request, { budgets: { maxRunOutputTokens: 8192 } })` 提交。
Agent 提问后，句柄的 `result` 会以 `purra` 导出的 `UserInputRequired` 拒绝。
使用异常中的 `error.requestId` 调用 `await interaction.get(error.requestId)`，获取可展示的问题数据。

Run 保持等待状态，不占用持续运行的执行任务或模型调用。收到用户回答后，传入问题修订号、
稳定的命令键，以及按问题 ID 组织的回答：

```ts
async function answerAndResume(
  requestId: string,
  revision: number,
  answerKey: string,
  answers: Record<string, string>,
) {
  const ready = await interaction.answer(requestId, {
    revision, key: answerKey, answers,
  });
  return interaction.resume(agent, ready.id);
}
```

返回的句柄继续执行原 Run，保留已消耗预算和截止时间，也可能再次请求输入。
回答只提供任务数据，不授予工具权限。

## 重新加载与取消

进程重启后，还原相同的存储、Agent preset、网关和交互对象。
`listPending()` 包含等待中和已回答的请求，可恢复其中处于 `ready` 状态的记录。
等待期间使用 `cancel(requestId)`；恢复执行后使用 Run 句柄取消。

使用 Agent 树时，绑定同一 Run 树仓储，回答 Root 下的全部待答问题后再恢复。
支持 Auto、Reactive 和 Planned。暂停发生在完整工具轮次结束后。
结果不确定的外部写入需要先确认；等待活动执行结束后再关闭存储。

## 执行前收集输入

`ClarificationStore.ask(...)` 保存问题和应用检查点。
向界面提供 `ClarificationStore.publicView(saved)`，再通过 `store.answer(...)` 保存回答。
`ClarificationWorkflow.resume(id, { revision, submit })` 调用
`submit(snapshot, continuationKey)` 创建新 Run 并返回其 ID。

将续接键保存在对应 Run 上。提交中断时，先根据已有 Run 确认结果再重试。
应用负责还原凭据、权限和任务剩余预算，并在关闭时关闭 store。
