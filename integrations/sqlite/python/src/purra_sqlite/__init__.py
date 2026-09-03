"""SQLite transactions around PurrA's canonical storage state machines."""
import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import replace
import sqlite3
import time
from uuid import uuid4

from purra.adapters.memory import InMemoryAgentAdapters
from purra.contracts import ToolHandlerResult, RunExecutionLease, RunStatus
from purra.execution.ownership import execution_owner, execution_claim
from purra.errors import ContractViolationError
from .codec import dumps, loads


class _Port:
    def __init__(self, store, name):
        self.store, self.name = store, name
        template = getattr(InMemoryAgentAdapters(), name)
        self._methods = {key for key in dir(template) if not key.startswith("_") and callable(getattr(template, key))}

    def __getattr__(self, method):
        if method not in self._methods: raise AttributeError(method)
        async def call(*args, **kwargs):
            async with self.store.transaction() as adapters:
                if self.name in ("runs", "outputs") and method not in ("get", "list_events", "list_root_events", "load_validated_result"):
                    first = args[0] if args else None
                    run_id = first if isinstance(first, str) else getattr(first, "run_id", None)
                    if run_id:
                        self.store._guard(run_id)
                return await getattr(getattr(adapters, self.name), method)(*args, **kwargs)
        return call


class _Publisher:
    def __init__(self, store): self.store = store

    async def publish_committed(self, event):
        rows = await self.store.outputs.list_events(event.run_id, after_sequence=event.sequence - 1, limit=1)
        if not rows or rows[0] != event:
            raise ValueError("only persisted output can be published")

    async def wait_for_sequence(self, run_id, *, after_sequence):
        while not await self.store.outputs.list_events(run_id, after_sequence=after_sequence, limit=1):
            await asyncio.sleep(0.05)


class _Idempotency:
    def __init__(self, store): self.store = store

    async def execute_once(self, run_id, tool_call, operation):
        key = (run_id, tool_call.id)
        async with self.store.transaction() as adapters:
            state = adapters.runs._state
            receipt = state.tool_receipts.get(key)
            if receipt is not None:
                if receipt[0] != tool_call: raise ValueError("tool_idempotency_conflict")
                return replace(receipt[1], from_cache=True)
            claimed = self.store._claims.get(key)
            if claimed is not None:
                raise ContractViolationError("Reconcile the previous tool attempt before retrying", code="tool_effect_unknown")
            self.store._claims[key] = tool_call
        # External work is never performed while holding a SQLite transaction.
        result = await operation()
        if not isinstance(result, ToolHandlerResult): raise TypeError("invalid tool result")
        async with self.store.transaction() as adapters:
            adapters.runs._state.tool_receipts[key] = (tool_call, result)
            del self.store._claims[key]
        return result


class SqliteAgentAdapters:
    """Scoped, restartable Run/output, tree, Artifact and Long Task adapters.

    All state-machine mutations commit atomically. Each namespace is intended for
    a bounded local project; use separate scopes for independent projects.
    """
    def __init__(self, path, *, scope, busy_timeout=5):
        if not isinstance(scope, str) or not scope.strip(): raise ValueError("scope is required")
        self.scope, self.busy_timeout = scope, busy_timeout
        if os.fspath(path) != ":memory:":
            try: os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
            except FileExistsError: pass
        self._lock = asyncio.Lock()
        self._db = sqlite3.connect(path, timeout=0, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("CREATE TABLE IF NOT EXISTS purra_state (scope TEXT NOT NULL, sdk TEXT NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(scope,sdk))")
        self._claims = {}
        self._leases = {}
        self.extra = {}
        for name in ("runs", "outputs", "delegations", "run_tree", "artifacts", "artifact_claims", "artifact_maintenance", "long_tasks"):
            setattr(self, name, _Port(self, name))
        self.publisher = _Publisher(self)
        self.idempotency = _Idempotency(self)
        self.leases = _Leases(self)

    def _guard(self, run_id):
        lease = self._leases.get(run_id)
        owner = execution_owner.get()
        claim = execution_claim.get()
        if lease is not None and owner is not None and (
            lease.owner_id != owner or (lease.expires_at_ms or 0) <= int(time.time() * 1000)
            or (claim is not None and claim[0] == run_id and claim[2] != lease.attempt)
        ):
            raise ContractViolationError("Execution lease was lost", code="run_lease_lost")

    @asynccontextmanager
    async def transaction(self):
        async with self._lock:
            deadline = time.monotonic() + self.busy_timeout
            while True:
                try:
                    self._db.execute("BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError as error:
                    if "locked" not in str(error) or time.monotonic() >= deadline: raise
                    await asyncio.sleep(0.01)
            try:
                adapters = InMemoryAgentAdapters()
                groups = {
                    "run": (adapters.runs._state, {"lock", "changed", "tool_inflight", "run_tree_authority"}),
                    "tree": (adapters.run_tree, {"_lock", "_clock_ms"}),
                    "artifact": (adapters.artifacts, {"_lock", "_clock_ms", "_run_is_available"}),
                    "task": (adapters.long_tasks, {"_lock", "_clock_ms"}),
                }
                row = self._db.execute("SELECT version,body FROM purra_state WHERE scope=? AND sdk='python'", (self.scope,)).fetchone()
                if row:
                    if row[0] != 1: raise ValueError("unsupported SQLite storage version")
                    saved = loads(row[1])
                    for name, (obj, excluded) in groups.items():
                        for key, value in saved[name].items():
                            if key in excluded or key not in vars(obj): raise ValueError("invalid storage field")
                            setattr(obj, key, value)
                    self._claims = saved["claims"]
                    self._leases = saved.get("leases", {})
                    self.extra = saved.get("extra", {})
                else:
                    self._claims = {}
                    self._leases = {}
                    self.extra = {}
                yield adapters
                saved = {name: {k: v for k, v in vars(obj).items() if k not in excluded}
                         for name, (obj, excluded) in groups.items()}
                saved["claims"] = self._claims
                saved["leases"] = self._leases
                saved["extra"] = self.extra
                # ponytail: O(project history) snapshots; use indexed row storage for large journals.
                body = dumps(saved)
                if row is None or row[1] != body:
                    self._db.execute("INSERT INTO purra_state VALUES(?, 'python', 1, ?) ON CONFLICT(scope,sdk) DO UPDATE SET body=excluded.body", (self.scope, body))
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    async def reconcile_tool(self, run_id, tool_call, *, result=None, not_executed=False):
        if (result is None) == (not_executed is False): raise ValueError("supply result or proof of non-execution")
        async with self.transaction() as adapters:
            key = (run_id, tool_call.id)
            if self._claims.get(key) != tool_call: raise ValueError("tool_claim_conflict")
            if result is not None:
                if not isinstance(result, ToolHandlerResult): raise TypeError("invalid tool result")
                adapters.runs._state.tool_receipts[key] = (tool_call, result)
            del self._claims[key]

    async def list_running(self):
        async with self.transaction() as adapters:
            return tuple(key for key, run in adapters.runs._state.runs.items() if run.status.value == "running")

    def close(self):
        if self._lock.locked(): raise RuntimeError("storage transaction is active")
        self._db.close()


__all__ = ["SqliteAgentAdapters"]


class _Leases:
    def __init__(self, store): self.store = store

    async def get(self, run_id):
        async with self.store.transaction() as adapters:
            run = adapters.runs._state.runs.get(run_id)
            if run is None: return None
            return replace(self.store._leases.get(run_id, RunExecutionLease(run_id, run.status)), status=run.status)

    async def claim(self, run_id, owner_id, *, lease_duration_ms):
        if not owner_id or lease_duration_ms <= 0: raise ValueError("invalid lease")
        async with self.store.transaction() as adapters:
            now = int(time.time() * 1000)
            run = adapters.runs._state.runs[run_id]
            old = self.store._leases.get(run_id, RunExecutionLease(run_id, run.status))
            if run.status is not RunStatus.RUNNING or old.cancellation_requested_at_ms is not None: return False
            if old.owner_id is not None and (old.expires_at_ms or 0) > now: return False
            if run.execution_checkpoint is not None and len(run.model_attempt_ids) != run.checkpoint_attempt_count:
                raise ContractViolationError("The last model/tool attempt needs reconciliation", code="run_recovery_requires_reconciliation")
            self.store._leases[run_id] = replace(old, owner_id=owner_id, expires_at_ms=now + lease_duration_ms, heartbeat_at_ms=now, attempt=old.attempt + 1)
            return True

    async def renew(self, run_id, owner_id, *, lease_duration_ms):
        async with self.store.transaction():
            now = int(time.time() * 1000)
            old = self.store._leases.get(run_id)
            if old is None or old.owner_id != owner_id or (old.expires_at_ms or 0) <= now: return False
            self.store._leases[run_id] = replace(old, expires_at_ms=now + lease_duration_ms, heartbeat_at_ms=now)
            return True

    async def release(self, run_id, owner_id):
        async with self.store.transaction():
            old = self.store._leases.get(run_id)
            if old is None or old.owner_id != owner_id: return False
            self.store._leases[run_id] = replace(old, owner_id=None, expires_at_ms=None)
            return True

    async def request_cancellation(self, run_id):
        async with self.store.transaction() as adapters:
            run = adapters.runs._state.runs.get(run_id)
            if run is None or run.status is not RunStatus.RUNNING: return False
            old = self.store._leases.get(run_id, RunExecutionLease(run_id, run.status))
            if old.cancellation_requested_at_ms is not None: return False
            self.store._leases[run_id] = replace(old, cancellation_requested_at_ms=int(time.time() * 1000))
            return True
