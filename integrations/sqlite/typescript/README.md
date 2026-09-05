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
The bundle also exposes `runTree`, `artifacts`, and `longTasks`.

## Recovery

`agent.resume(runId, originalRequest)` restores an eligible checkpoint with its
existing budget and deadline. Restore the original model and tool configuration.
The repository supplies execution ownership to prevent concurrent resumes.

For uncertain external tool writes, call
`storage.reconcileTool(key, { result })` or
`storage.reconcileTool(key, { notExecuted: true })` only after confirming what
happened. For persisted questions and answers, use
[SqliteClarification](../../interaction/typescript/README.md).

## Adapter state boundary

Core owns the versioned `StorageSession` contract and the repository state schemas.
SQLite owns transactions, indexes, leases and tool-effect reconciliation. The state
bridge is version-pinned between packages, not a portable business interchange
format. Install matching Core and integration builds together.

`StorageSession` is exported by `purra`. It combines versioned repository state
with detached journal rows. `storage.transaction(async (stores, extra) => ...)`
keeps transaction-local ports and extension metadata. Sessions and their lazy
history callbacks must not escape the transaction; the snapshot body alone is
not a complete backup. Repository methods are exposed from an explicit port list.

## Storage and shutdown

Canonical output events are appended as rows with Run and Root sequence indexes.
Event additions and the execution snapshot commit in one transaction.
`runs.listEvents()`, `runs.listRootEvents()` and subscription polling neither load
the execution snapshot nor acquire a writer lock. `runs.get()` remains read-only.
Tool receipts and lease renewal/release skip Run state and journal hydration;
Agent tree, Artifact and Long Task operations restore only their own repository.
Writes targeting an existing Run validate sequence counts in SQL, use indexed
source-key lookups and buffer only new events. Core planning, public-progress
and terminal-settlement rules read original Run evidence on demand. Persisted
counts and pending events jointly determine Run and Root sequences; sibling
budgets remain shared. Run reads restore the complete Root journal. Unloaded Roots reject access;
their checkpoints and event counts are preserved. Lease acquisition, public
`transaction()` and operations such as creating a Run still validate the full
scope. Execution snapshots retain checkpoints and receipts and
are still loaded and saved at scope granularity. This adapter therefore
still suits bounded local workloads.
Storage v4 rejects every other storage version (including v1/v2/v3) during
construction, before changing database pragmas, tables or indexes. There is no
automatic migration or old-format resume path; rejected databases remain unchanged.
Python and TypeScript execution snapshots are not interchangeable.
The source-key index is local to each Root (Python keys are scope-wide).
Opening existing v4 data creates the index using SQLite `json_extract`.
Sequence checks scan a covering index for the selected Root and match each child
by Run id, without fetching event body rows. Root headers have a covering index
as well. Existing v4 databases build these indexes on opening, consuming time and
disk space; inserts maintain the extra indexes. Metadata remains scope-sized;
writes are not constant-cost. Event bodies are validated when read. Lease
acquisition and public transactions retain full journal hydration.

After building Core and this package, run the empty-poll and tail-pagination
benchmark from the repository root:

```sh
node integrations/sqlite/typescript/scripts/benchmark-reads.mjs
```

This temporary-database benchmark reports warm median read latency, not
concurrent throughput or real-model end-to-end performance.

Measure tool receipt writes, including claim and result-commit transactions:

```sh
node integrations/sqlite/typescript/scripts/benchmark-writes.mjs
```

The tool callback is local and has no external side effect; this excludes real
business-tool and model latency.

Measure active Run event writes beside a growing unrelated Root:

```sh
node integrations/sqlite/typescript/scripts/benchmark-run-writes.mjs
```

This measures isolation from other Roots, not scaling within a single growing Root.

Measure appends, invocation registration and checkpoint commits within the same
growing Root (two warmups and ten measured writes per operation):

```sh
node integrations/sqlite/typescript/scripts/benchmark-execution-writes.mjs
```

History consists of private diagnostic events. Results exclude planning evidence
replay, concurrent throughput and real Provider latency. Unlike Python's
reservation and checkpoint APIs, TypeScript emits an output event for each of
these operations, so compare each SDK against its own baseline.
Add `--profile` to report journal preparation, Run state import/export and the
remaining transaction time. The remainder includes outer snapshot handling,
other repositories and SQL writes. Phase medians are calculated independently
and need not sum to the total median.

The application owns database access, backups, and retention. Checkpoints contain
private model data. Wait for active executions to settle before `storage.close()`.
