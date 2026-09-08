# purra-sqlite · Python

English | [简体中文](README.zh-CN.md)

SQLite persistence for PurrA Runs, events, operations, budgets, checkpoints, and
tool receipts. Uses Python's standard library and requires Python 3.11+.

## Install

From the repository root:

```sh
python -m pip install . ./integrations/sqlite/python
```

## Configure

Given your model `gateway` and Agent `preset`:

```python
from purra.api import AgentCore
from purra_sqlite import SqliteAgentAdapters

storage = SqliteAgentAdapters("agent.db", scope="user-1/project-1")
core = AgentCore(
    model_gateway=gateway,
    preset=preset,
    run_repository=storage.runs,
    output_repository=storage.outputs,
    output_publisher=storage.publisher,
    execution_lease_store=storage.leases,
)
```

Select `scope` from the application's authenticated user/project binding.
The bundle also exposes `idempotency`, `run_tree`, `artifacts`,
and `long_tasks` for the corresponding Core ports.

## Recovery

Use `storage.list_running()` to find interrupted Runs and
`core.resume(run_id, request, options=...)` to resume an eligible checkpoint.
Restore the original Agent configuration. Execution leases prevent concurrent owners.

An interrupted external tool call may already have taken effect. Use
`storage.reconcile_tool(...)` with its result or evidence that it did not execute
before retrying. For persisted questions and answers, use
[SqliteClarification](../../interaction/python/README.md).

## Adapter state boundary

Core owns the versioned `StorageSession` contract and the repository state schemas.
SQLite owns transactions, indexes, leases and tool-effect reconciliation. The state
bridge is version-pinned between packages, not a portable business interchange
format. Install matching Core and integration builds together.

`from purra.storage import StorageSession` replaces the SQLite-owned reflective
codec. Records have explicit identifiers and fields independent of Python module
paths. `storage.transaction()` yields a session with `runs`, `outputs`, `run_tree`,
`artifacts`, `artifact_claims`, `artifact_maintenance` and `long_tasks` ports.
For transaction-local recovery metadata use `get_run_info()` and `find_tree_run()`;
never access an in-memory repository's private fields. Event history is stored
separately; `export_snapshot()` alone is not a complete backup. Session ports and
lazy history callbacks must not escape the transaction.

## Storage and shutdown

Canonical output events are appended as rows with Run and Root sequence indexes.
Event additions and the execution snapshot commit in one transaction. Output
pagination and subscription polling neither load the execution snapshot nor
acquire a writer lock. Run queries, lease lookup and `list_running()` remain
read-only. Tool receipts, lease renewal/release, cancellation requests, Agent
tree, Artifact and Long Task repository operations skip journal hydration and
flushing. Writes with an identifiable Run or output stream validate the Root
tree's sequence counts in SQL and buffer new events without decoding its history.
Core rules that inspect history (including planning projections and terminal
operation settlement) load the required Run's original events on demand.
Shared budgets still use all sibling Run counters; event-key replay uses indexed
lookups. Run reads retain complete Root journal hydration.
Cross-Root event keys use an index; Python SQLite requires `json_extract`, and
opening an existing v4 database creates this index on first use. Lease acquisition,
public `transaction()` and operations without an identifiable Run still validate
the full scope. Execution snapshots retain Run history,
checkpoints and receipts and are still loaded and saved at scope granularity.
This adapter therefore still suits bounded local workloads.
Storage v4 rejects every other storage version (including v1/v2/v3) during
construction, before changing database pragmas, tables or indexes. There is no
automatic migration or old-format resume path; rejected databases remain unchanged.
Python and TypeScript execution snapshots are not interchangeable.
Both SDKs defer history loading for Run-scoped writes. SQL sequence checks scan the
selected Root's covering index without fetching event body rows or sorting by
Run. Root headers also have a covering index. Existing v4 databases build these
indexes on opening; this takes time and disk space, and inserts maintain them.
Metadata snapshots remain scope-sized, so these writes are
not constant-cost. Event bodies are validated when read; lease acquisition and
public transactions continue to decode the full journal.
Body Run/Root ids and sequence values must match their SQL columns on every
event read, including indexed replay and pagination. Inconsistent rows raise
`ValueError` and roll back the current transaction; they are not automatically
repaired. Unread event bodies remain deferred.

From the repository root, measure empty output polling and tail pagination with
100, 1,000 and 5,000 historical events:

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_reads.py
```

This temporary-database benchmark reports warm median read latency, not
concurrent throughput or real-model end-to-end performance.

Measure tool receipt writes at the same journal sizes, including both claim and
result-commit transactions:

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_writes.py
```

The tool callback is local and has no external side effect; this excludes real
business-tool and model latency.

Measure active Run event writes beside a growing unrelated Root:

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_run_writes.py
```

This measures isolation from other Roots, not scaling within a single growing Root.

Measure appends, model-attempt reservations and checkpoint commits within the same
growing Root (two warmups and ten measured writes per operation):

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_execution_writes.py
```

The history consists of private domain events. This excludes planning evidence
replay, concurrent throughput and real Provider latency.
Add `--profile` to report journal preparation, execution-state encoding/decoding
and the remaining transaction time separately. Phase medians are calculated
independently and need not sum to the total median.

The application owns database access, backups, and retention. Checkpoints contain
private model data. Call `await core.close()` before `storage.close()`.

## Read-only recovery inspection

`await storage.inspect_recovery(run_id, expected_preset=effective_preset_snapshot)`
reads one committed snapshot without claiming a lease, resuming, reconciling or
calling a model/tool. Omit `expected_preset` when unavailable: configuration stays
unknown. The report contains only enums/counts/fixed codes, never checkpoint
messages or raw tool receipts. Run-bound pending claims and post-checkpoint model
attempts are separate blockers; reconciling a tool does not clear the attempt.
Permissions, complete usage, effects outside this adapter and Agent Tree ownership
remain unknown. `authority` is always `diagnosis_only`; execution must revalidate.
See the Core [inspection contract](../../../ARCHITECTURE.md#integration-reports-and-recovery-inspection).

## 1.1 approval storage foundation (unreleased)

Explicit offline v5 activation and host-authorized decision storage are available.
The opt-in tool-ready runtime supports Reactive, Planned and Auto Root recovery
with atomic claim/receipt association. Host-authorized MCP writes have deterministic
and controlled independent-service coverage. Child writes are unsupported; real
Provider, business MCP and downstream acceptance remain separate.
Keep production databases on their current format until the runtime integration
and acceptance are complete. See the [durable approval contract](../../../conformance/durable-approval.md#storage-foundation-api)
for activation, API methods, replay behavior and SDK format boundaries.

### Approval observations (1.1 development)

On explicitly activated v5 storage, recovery inspection adds current tool-ready
approval state, record count, checkpoint intent match, matching completed receipt
and Run-associated unknown approval claims. Reads do not expire or alter decisions.
Current host binding/permission checks remain unknown and required at execution.
Reports retain `diagnosis_only` authority and omit private approval data. Legacy v4
reports retain their existing shape. See the [approval inspection contract](../../../conformance/durable-approval.md#read-only-approval-inspection-implemented).

## Offline approval upgrade preflight

`await storage.inspect_approval_upgrade()` reads a consistent database snapshot and returns
`schemaVersion: 1`, `authority: diagnosis_only`, `storageVersion`, `targetVersion: 5`,
`status` (`ready`, `blocked`, `already_enabled`) and `blockers`. It does not create
schema objects, migrate rows, refresh decisions, or acquire a writer transaction.
As with other inspections, initial adapter construction is separate and can initialize
its normal schema. Use preflight on an already open adapter, not as a raw-file validator.

For v4, preflight validates same-SDK metadata and journals across every scope and
reports the first encountered foreign-SDK or unsettled-execution blocker. Corrupt
state and unsupported formats raise instead of reporting readiness. Results omit
scope names, Run IDs and stored content. `already_enabled` reports the format only,
not the health of all v5 execution state.

A `ready` result can become stale immediately. Stop writers, back up and use the
existing explicit activation method; activation repeats the checks inside its write
transaction. Preflight is not an upgrade permit, backup, automatic migration or
v5-to-v4 rollback API. Never point a validation fixture at production data.

For WAL-safe backup and restoration to a new path, see the
[offline upgrade procedure](../../../conformance/sqlite-upgrade.md).

## Synthetic host lifecycle check

From the repository root, run:

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python \
  integrations/sqlite/python/scripts/check_host_lifecycle.py
```

The check persists host-owned Run configuration separately, closes the first host,
reconstructs the request profile, current binding/scope revisions and absolute
approval expiry, then starts `RecoveryWorker` with a durable cursor and schedule.
An authenticated synthetic decision wakes the worker. The check waits for canonical
completion, stops the service, reopens storage, and verifies one completed receipt
with no claim or active lease.

This repository check imports its deterministic test host; it is not a production
host library. Applications must own the authenticated principal, configuration
registry and current resource-scope resolver. Missing or mismatched configuration
must fail before public resume. No Provider or MCP service is called.
