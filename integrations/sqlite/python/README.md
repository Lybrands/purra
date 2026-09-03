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
The bundle also exposes `idempotency`, `run_tree`, `delegations`, `artifacts`,
and `long_tasks` for the corresponding Core ports.

## Recovery

Use `storage.list_running()` to find interrupted Runs and
`core.resume(run_id, request, options=...)` to resume an eligible checkpoint.
Restore the original Agent configuration. Execution leases prevent concurrent owners.

An interrupted external tool call may already have taken effect. Use
`storage.reconcile_tool(...)` with its result or evidence that it did not execute
before retrying. For persisted questions and answers, use
[SqliteClarification](../../interaction/python/README.md).

## Storage and shutdown

Each scope is stored as one transactional snapshot. Loading and serialization
cost grow with its history, so this adapter suits bounded local workloads.
Python and TypeScript execution snapshots are not interchangeable.

The application owns database access, backups, and retention. Checkpoints contain
private model data. Call `await core.close()` before `storage.close()`.
