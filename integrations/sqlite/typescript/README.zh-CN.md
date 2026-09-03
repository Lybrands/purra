# purra-sqlite · TypeScript

[English](README.md) | 简体中文

为 PurrA 的 Run、事件、操作、预算、检查点和工具回执提供 SQLite 持久化。
依赖 `node:sqlite`，要求 Node.js 22.13+。

## 安装

按照[源码安装说明](../../README.zh-CN.md#从源码安装)操作，选择
`integrations/sqlite/typescript`。

## 配置

已有模型网关 `model` 时：

```ts
import { Agent } from "purra";
import { SqliteAgentAdapters } from "purra-sqlite";

const storage = new SqliteAgentAdapters("agent.db", {
  scope: "user-1/project-1",
});
const agent = new Agent({
  model,
  runRepository: storage.runs,
  outputPublisher: storage.publisher,
  idempotency: storage.idempotency,
});
```

`scope` 应由应用根据已认证的用户与项目绑定确定。
该组件还提供 `runTree`、`delegations`、`artifacts` 和 `longTasks`。

## 恢复

`agent.resume(runId, originalRequest)` 恢复满足条件的检查点，并保留原有预算和截止时间。
恢复时还原原有模型与工具配置。仓储提供执行所有权控制，阻止并发恢复。

外部工具写入结果不确定时，先确认实际执行情况，再调用
`storage.reconcileTool(key, { result })` 或
`storage.reconcileTool(key, { notExecuted: true })`。
需要保存提问和回答时，使用 [SqliteClarification](../../interaction/typescript/README.zh-CN.md)。

## 存储与关闭

每个作用域保存为一个事务快照，加载和序列化成本随历史数据增长，适合数据量受控的本地场景。
Python 与 TypeScript 的执行快照不能互换。

应用负责数据库访问、备份和数据保留。检查点包含私有模型数据。
等待活动执行结束后再调用 `storage.close()`。
