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
该组件还提供 `runTree`、`artifacts` 和 `longTasks`。

## 恢复

`agent.resume(runId, originalRequest)` 恢复满足条件的检查点，并保留原有预算和截止时间。
恢复时还原原有模型与工具配置。仓储提供执行所有权控制，阻止并发恢复。

外部工具写入结果不确定时，先确认实际执行情况，再调用
`storage.reconcileTool(key, { result })` 或
`storage.reconcileTool(key, { notExecuted: true })`。
需要保存提问和回答时，使用 [SqliteClarification](../../interaction/typescript/README.zh-CN.md)。

## 存储与关闭

规范输出事件按行增量保存，通过 Run 序号和 Root 序号索引分页读取。事件追加与执行状态
快照在同一个事务中提交；`runs.listEvents()`、`runs.listRootEvents()` 和订阅轮询不加载
执行快照，也不申请写锁。`runs.get()` 保持只读事务。工具回执、租约续期与释放跳过
Run 状态和输出历史的还原；Agent 树、产物与长任务操作只还原对应仓储。
指向已有 Run 的写入先通过 SQL 校验序号和事件数量，按索引查询幂等键，并缓冲新增事件。
规划、公开进度和终态收尾所需的原始证据仍由 Core 按需读取。已持久化的事件数量和
待提交事件共同决定 Run 与 Root 的序号，兄弟任务仍共享预算。Run 读取完整还原所属
Root 日志；未加载的 Root 不能读取或写入，其检查点与事件计数保持不变。
租约认领、公开 `transaction()` 和创建 Run 等无法定位既有 Root 的操作仍校验完整作用域。
执行快照仍按作用域加载和保存，包含检查点和回执，适合数据量受控的本地场景。
本次存储格式为 v3，明确拒绝 v1/v2 数据；没有自动迁移，已有数据库不能直接恢复。
Python 与 TypeScript 的执行快照不能互换。
事件幂等键的索引范围是单个 Root（Python 则是整个作用域）。打开已有 v3 数据库时，
通过 SQLite `json_extract` 建立该索引。序号校验使用选定 Root 的覆盖索引，并按 Run
精确匹配子任务，无需读取事件正文所在的数据行。Root 与 Run 的映射也有覆盖索引。
已有 v3 数据库在打开时补建索引，需要耗时和额外磁盘，新增事件也需要维护索引。
元数据仍按作用域保存，
因此写入成本并非常数。事件正文在读取时校验；租约认领和公开事务仍还原完整日志。

构建 Core 和本包后，在仓库根目录运行空轮询与末尾分页基准：

```sh
node integrations/sqlite/typescript/scripts/benchmark-reads.mjs
```

基准使用临时数据库，报告预热后的读取中位耗时，不代表并发吞吐量或真实模型端到端性能。

工具回执写入基准计入认领和结果提交两个事务，使用本地无外部副作用的工具函数：

```sh
node integrations/sqlite/typescript/scripts/benchmark-writes.mjs
```

它不计真实业务工具或模型调用耗时。

另一个 Root 的历史数据增长时，活动 Run 的事件写入成本可用以下基准测量：

```sh
node integrations/sqlite/typescript/scripts/benchmark-run-writes.mjs
```

它测量无关 Root 的隔离效果，不代表活动 Root 自身历史不断增长时的性能。

同一 Root 内的事件追加、调用登记和检查点提交基准，每种操作预热两次后测量十次：

```sh
node integrations/sqlite/typescript/scripts/benchmark-execution-writes.mjs
```

历史数据为私有诊断事件；结果不包含规划证据重放、并发吞吐量或真实 Provider 耗时。
与 Python 的调用登记和检查点接口不同，TypeScript 的这两类操作都会追加输出事件，
因此应分别与各 SDK 自身的基线比较。
追加 `--profile` 可分别查看日志准备、Run 状态导入导出和事务其余部分的耗时。
其余部分包含外层快照处理、其他仓储和 SQL 写入。各分段的中位数独立计算，
不能直接相加得到总耗时中位数。

应用负责数据库访问、备份和数据保留。检查点包含私有模型数据。
等待活动执行结束后再调用 `storage.close()`。
