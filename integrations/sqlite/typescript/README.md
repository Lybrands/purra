# purra-sqlite · TypeScript

English | [简体中文](README.zh-CN.md)

SQLite persistence for PurrA Runs, events, operations, budgets, checkpoints, and
tool receipts. Requires Node.js 22.13+ for `node:sqlite`.

## Install

Follow [source installation](../../README.md#source-installation), selecting
`integrations/sqlite/typescript`.

## Configure

Given a configured model gateway `model`:

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

Select `scope` from the application's authenticated user/project binding.
The bundle also exposes `runTree`, `delegations`, `artifacts`, and `longTasks`.

## Recovery

`agent.resume(runId, originalRequest)` restores an eligible checkpoint with its
existing budget and deadline. Restore the original model and tool configuration.
The repository supplies execution ownership to prevent concurrent resumes.

For uncertain external tool writes, call
`storage.reconcileTool(key, { result })` or
`storage.reconcileTool(key, { notExecuted: true })` only after confirming what
happened. For persisted questions and answers, use
[SqliteClarification](../../interaction/typescript/README.md).

## Storage and shutdown

Each scope is stored as one transactional snapshot. Loading and serialization
cost grow with its history, so this adapter suits bounded local workloads.
Python and TypeScript execution snapshots are not interchangeable.

The application owns database access, backups, and retention. Checkpoints contain
private model data. Wait for active executions to settle before `storage.close()`.
