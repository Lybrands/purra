"""SQLite transactions around PurrA's canonical storage state machines."""
import asyncio
import os
from inspect import signature
from contextlib import asynccontextmanager
from dataclasses import replace
import sqlite3
import time
from uuid import uuid4

from purra.storage import StorageSession, STORAGE_PORT_METHODS
from purra.contracts import ToolHandlerResult, RunExecutionLease, RunStatus
from purra.execution.ownership import execution_owner, execution_claim
from purra.errors import ContractViolationError
from .journal import OutputJournal
from .approval_format import storage_version, enable_approvals, inspect_approval_upgrade


_READ_METHODS = {
    "runs": frozenset({"get"}),
    "outputs": frozenset({"list_events", "list_root_events", "load_validated_result"}),
}
_INDEPENDENT_PORTS = frozenset({"run_tree", "artifacts", "artifact_claims", "artifact_maintenance", "long_tasks"})


def _journal_run(arguments):
    for name in ("run_id", "root_run_id"):
        if isinstance(arguments.get(name), str):
            return arguments[name]
    for name in ("draft", "spec"):
        run_id = getattr(arguments.get(name), "run_id", None)
        if isinstance(run_id, str):
            return run_id
    drafts = arguments.get("drafts")
    if isinstance(drafts, (tuple, list)) and drafts:
        ids = {getattr(draft, "run_id", None) for draft in drafts}
        if len(ids) == 1 and isinstance(next(iter(ids)), str):
            return next(iter(ids))
    return None


class _Port:
    def __init__(self, store, name):
        self.store, self.name = store, name
        template = getattr(StorageSession(), name)
        self._methods = STORAGE_PORT_METHODS[name]
        self._signatures = {key: signature(getattr(template, key)) for key in self._methods} if name in ("runs", "outputs") else {}

    def __getattr__(self, method):
        if method not in self._methods: raise AttributeError(method)
        async def call(*args, **kwargs):
            if self.name == "outputs" and method in ("list_events", "list_root_events"):
                return await getattr(self.store, "_" + method)(*args, **kwargs)
            read_only = method in _READ_METHODS.get(self.name, ())
            arguments = self._signatures[method].bind(*args, **kwargs).arguments if method in self._signatures else {}
            journal_run_id = _journal_run(arguments)
            stream_id = arguments.get("output_stream_id")
            async with self.store._transaction(read_only=read_only, with_journal=self.name not in _INDEPENDENT_PORTS, journal_run_id=journal_run_id, journal_stream_id=stream_id, lazy_journal=not read_only) as adapters:
                if self.name in ("runs", "outputs") and method not in ("get", "list_events", "list_root_events", "load_validated_result"):
                    first = args[0] if args else None
                    run_id = journal_run_id or adapters.run_for_stream(stream_id)
                    if run_id is None:
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
        approval_record = None
        approval_claim = None
        async with self.store._transaction(with_journal=False) as adapters:
            if storage_version(self.store._db) == 5 and self.store._db.execute(
                "SELECT 1 FROM purra_approvals WHERE scope=? AND sdk='python' AND run_id=? AND call_id=?", (self.store.scope, run_id, tool_call.id)
            ).fetchone():
                from .approvals import SqliteApprovalStore
                gate = SqliteApprovalStore(self.store, authorize=lambda *_: False)
                approval_record = await gate._dispatch_record(adapters, run_id, tool_call)
            receipt = adapters.get_tool_receipt(key)
            if receipt is not None:
                if receipt[0] != tool_call: raise ValueError("tool_idempotency_conflict")
                return replace(receipt[1], from_cache=True)
            claimed = self.store._claims.get(key)
            if claimed is not None:
                raise ContractViolationError("Reconcile the previous tool attempt before retrying", code="tool_effect_unknown")
            self.store._claims[key] = tool_call
            if approval_record is not None:
                lease = adapters.leases[run_id]
                approval_claim = {"intentDigest": approval_record.intent.digest, "state": "claimed", "effectState": "unknown",
                    "approvalRevision": approval_record.decision_audit["revision"], "leaseOwnerId": lease.owner_id, "leaseEpoch": lease.attempt}
                adapters.extra.setdefault("approvalExecutions", {})[approval_record.approval_id] = dict(approval_claim)
        # External work is never performed while holding a SQLite transaction.
        result = await operation()
        if not isinstance(result, ToolHandlerResult): raise TypeError("invalid tool result")
        if approval_record is not None and result.effect_state.value == "unknown":
            raise ContractViolationError("The approved tool effect is unknown", code="tool_effect_unknown")
        async with self.store._transaction(with_journal=False) as adapters:
            if approval_record is not None:
                gate._require_owner(adapters, run_id)
                association = adapters.extra.get("approvalExecutions", {}).get(approval_record.approval_id)
                if association != approval_claim:
                    raise ContractViolationError("Approval claim conflicts", code="approval_claim_conflict")
                association.update(state="complete", effectState=result.effect_state.value)
            adapters.save_tool_receipt(key, tool_call, result)
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
        try:
            storage_version(self._db)
        except BaseException:
            self._db.close()
            raise
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.execute("CREATE TABLE IF NOT EXISTS purra_state (scope TEXT NOT NULL, sdk TEXT NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(scope,sdk))")
        self._journal = OutputJournal(self._db, scope)
        self._claims = {}
        self._leases = {}
        self.extra = {}
        for name in ("runs", "outputs", "run_tree", "artifacts", "artifact_claims", "artifact_maintenance", "long_tasks"):
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

    def transaction(self):
        return self._transaction()

    async def inspect_approval_upgrade(self):
        """Read-only v4-to-v5 readiness; activation rechecks under a writer lock."""
        async with self._connection(read_only=True):
            return inspect_approval_upgrade(self._db)

    async def enable_approvals(self):
        """Explicitly activate v5 while all scopes have no unsettled execution."""
        await enable_approvals(self)

    def approval_store(self, *, authorize, clock_ms=None):
        from .approvals import SqliteApprovalStore
        return SqliteApprovalStore(self, authorize=authorize, clock_ms=clock_ms)

    @asynccontextmanager
    async def _connection(self, *, read_only=False):
        async with self._lock:
            deadline = time.monotonic() + self.busy_timeout
            while True:
                try:
                    self._db.execute("BEGIN" if read_only else "BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError as error:
                    if "locked" not in str(error) or time.monotonic() >= deadline: raise
                    await asyncio.sleep(0.01)
            try:
                yield
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    @asynccontextmanager
    async def _transaction(self, *, read_only=False, with_journal=True, journal_run_id=None, journal_stream_id=None, lazy_journal=False):
        async with self._connection(read_only=read_only):
            version = storage_version(self._db)
            row = self._db.execute("SELECT version,body FROM purra_state WHERE scope=? AND sdk='python'", (self.scope,)).fetchone()
            if row and row[0] != version:
                raise ValueError("unsupported SQLite storage version")
            adapters = StorageSession(row[1] if row else None)
            self._claims, self._leases, self.extra = adapters.claims, adapters.leases, adapters.extra
            if with_journal:
                journal_run_id = journal_run_id or adapters.run_for_stream(journal_stream_id)
                self._journal.restore(adapters, root_run_id=adapters.root_for_run(journal_run_id), lazy=lazy_journal)
            yield adapters
            if not read_only:
                if version == 4 and adapters.has_tool_ready_checkpoint():
                    raise ValueError("approval_storage_not_enabled")
                body = adapters.export_snapshot()
                if row is None or row[1] != body:
                    self._db.execute("INSERT INTO purra_state VALUES(?, 'python', ?, ?) ON CONFLICT(scope,sdk) DO UPDATE SET version=excluded.version,body=excluded.body", (self.scope, version, body))
                if with_journal:
                    self._journal.append(adapters)

    async def _list_events(self, run_id, *, after_sequence, limit=200):
        async with self._connection(read_only=True):
            return self._journal.read(run_id, after_sequence, limit)

    async def _list_root_events(self, root_run_id, *, after_root_sequence, limit=200):
        async with self._connection(read_only=True):
            return self._journal.read(root_run_id, after_root_sequence, limit, root=True)

    async def reconcile_tool(self, run_id, tool_call, *, result=None, not_executed=False):
        if (result is None) == (not_executed is False): raise ValueError("supply result or proof of non-execution")
        async with self._transaction(with_journal=False) as adapters:
            key = (run_id, tool_call.id)
            if self._claims.get(key) != tool_call: raise ValueError("tool_claim_conflict")
            approval = None
            if storage_version(self._db) == 5:
                from .approvals import SqliteApprovalStore
                row = self._db.execute("SELECT approval_id FROM purra_approvals WHERE scope=? AND sdk='python' AND run_id=? AND call_id=?", (self.scope, run_id, tool_call.id)).fetchone()
                if row is not None:
                    approval = SqliteApprovalStore(self, authorize=lambda *_: False)._load(row[0])
                    lease = adapters.leases.get(run_id)
                    if lease is not None and lease.owner_id is not None and (lease.expires_at_ms or 0) > int(time.time() * 1000):
                        raise ValueError("approval_reconciliation_requires_idle_run")
                    if result is not None and (not isinstance(result, ToolHandlerResult) or result.effect_state.value == "unknown"):
                        raise ValueError("approval_reconciliation_requires_known_effect")
                    association = adapters.extra.get("approvalExecutions", {}).get(approval.approval_id)
                    if (not isinstance(association, dict) or association.get("intentDigest") != approval.intent.digest
                            or association.get("state") != "claimed" or association.get("effectState") != "unknown"
                            or association.get("approvalRevision") != approval.decision_audit.get("revision")):
                        raise ValueError("approval_claim_conflict")
            if result is not None:
                if not isinstance(result, ToolHandlerResult): raise TypeError("invalid tool result")
                adapters.save_tool_receipt(key, tool_call, result)
                if approval is not None:
                    association.update(state="complete", effectState=result.effect_state.value)
            elif approval is not None:
                del adapters.extra["approvalExecutions"][approval.approval_id]
            del self._claims[key]
            from .recovery_schedule import wake_recovery_schedule
            wake_recovery_schedule(adapters.extra, run_id, existing_only=True)

    async def prune_recovery_schedule(self, run_ids):
        """Remove hints only for supplied canonical terminal Runs."""
        from .recovery_schedule import remove_recovery_schedule
        removed = []
        async with self._transaction(with_journal=False) as session:
            for run_id in dict.fromkeys(run_ids):
                saved = await session.runs.get(run_id)
                if saved.status is not RunStatus.RUNNING and remove_recovery_schedule(session.extra, run_id):
                    removed.append(run_id)
        return tuple(removed)

    def recovery_schedule(self, **options):
        from .recovery_schedule import SqliteRecoverySchedule
        return SqliteRecoverySchedule(self, **options)

    async def list_run_candidates(self, *, after_run_id=None, limit=100):
        """Read a bounded index page; candidates may be terminal or Child Runs."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("candidate page limit must be between 1 and 1000")
        if after_run_id is not None and (not isinstance(after_run_id, str) or not after_run_id):
            raise ValueError("candidate cursor must be a nonempty Run id")
        async with self._connection(read_only=True):
            storage_version(self._db)
            rows = self._db.execute(
                "SELECT run_id FROM purra_journal_runs WHERE scope=? AND sdk='python'"
                + (" AND run_id>?" if after_run_id is not None else "")
                + " ORDER BY run_id LIMIT ?",
                (self.scope, after_run_id, limit + 1) if after_run_id is not None else (self.scope, limit + 1),
            ).fetchall()
            ids = tuple(row[0] for row in rows[:limit])
            if any(not isinstance(row[0], str) or not row[0] for row in rows):
                raise ValueError("invalid Run candidate identity")
            return {"authority": "candidate_only", "runIds": ids,
                    "nextAfterRunId": ids[-1] if len(rows) > limit else None}

    async def list_running(self):
        async with self._transaction(read_only=True, with_journal=False) as adapters:
            return adapters.running_run_ids()

    async def inspect_recovery(self, run_id, *, expected_preset=None):
        """Observe one committed snapshot without claiming, reconciling or resuming.

        expected_preset is a host-supplied *complete* effective preset snapshot.
        Permissions and complete usage/effect coverage outside this adapter remain unknown.
        """
        async with self._transaction(read_only=True, with_journal=False) as adapters:
            return await self._inspect_recovery_session(adapters, run_id, expected_preset)

    async def inspect_recovery_many(self, run_ids):
        """Diagnose up to 100 requested Runs in one committed snapshot."""
        if not isinstance(run_ids, (list, tuple)) or len(run_ids) > 100:
            raise ValueError("inspection batch must contain at most 100 Run ids")
        if any(not isinstance(key, str) or not key for key in run_ids):
            raise ValueError("inspection requires nonempty Run ids")
        if not run_ids:
            return {}
        async with self._transaction(read_only=True, with_journal=False) as adapters:
            return {key: await self._inspect_recovery_session(adapters, key, None)
                    for key in dict.fromkeys(run_ids)}

    async def _inspect_recovery_session(self, adapters, run_id, expected_preset):
        from purra.observability.inspection import build_recovery_inspection
        saved = await adapters.runs.get(run_id)
        info = adapters.get_run_info(run_id)
        lease = adapters.leases.get(run_id)
        now = int(time.time() * 1000)
        configuration = "unknown"
        if expected_preset and saved.agent_preset_snapshot:
            configuration = "matched" if saved.agent_preset_snapshot == expected_preset else "mismatch"
        from .approvals import inspect_approval_state
        return build_recovery_inspection({
            **inspect_approval_state(self, adapters, saved, now),
            "status": "running" if saved.status is RunStatus.RUNNING else "terminal",
            "checkpoint": "present" if info.has_checkpoint else "missing",
            "attemptsAfterCheckpoint": info.model_attempt_count - info.checkpoint_attempt_count if info.has_checkpoint else None,
            "unknownToolReceipts": sum(key[0] == run_id for key in adapters.claims),
            "receiptScope": "run",
            "lease": "active" if lease and lease.owner_id and (lease.expires_at_ms or 0) > now else "inactive",
            "configuration": configuration,
            "cancellation": "requested" if saved.status is RunStatus.CANCELED or (lease and lease.cancellation_requested_at_ms is not None) else "clear",
            "deadline": "expired" if saved.deadline_at_ms is not None and saved.deadline_at_ms <= now else "open",
        })

    def close(self):
        if self._lock.locked(): raise RuntimeError("storage transaction is active")
        self._db.close()


__all__ = ["SqliteAgentAdapters"]


class _Leases:
    def __init__(self, store): self.store = store

    async def get(self, run_id):
        async with self.store._transaction(read_only=True, with_journal=False) as adapters:
            run = adapters.get_run_info(run_id)
            if run is None: return None
            return replace(self.store._leases.get(run_id, RunExecutionLease(run_id, run.status)), status=run.status)

    async def claim(self, run_id, owner_id, *, lease_duration_ms):
        if not owner_id or lease_duration_ms <= 0: raise ValueError("invalid lease")
        async with self.store.transaction() as adapters:
            now = int(time.time() * 1000)
            run = adapters.get_run_info(run_id)
            old = self.store._leases.get(run_id, RunExecutionLease(run_id, run.status))
            if run.status is not RunStatus.RUNNING or old.cancellation_requested_at_ms is not None: return False
            if old.owner_id is not None and (old.expires_at_ms or 0) > now: return False
            from purra.agent_execution_checkpoint import AgentToolExecutionCheckpoint
            saved = await adapters.runs.get(run_id)
            if isinstance(saved.execution_checkpoint, AgentToolExecutionCheckpoint) and any(key[0] == run_id for key in adapters.claims):
                raise ContractViolationError("Reconcile the approved tool effect before recovery", code="tool_effect_unknown")
            if run.has_checkpoint and run.model_attempt_count != run.checkpoint_attempt_count:
                raise ContractViolationError("The last model/tool attempt needs reconciliation", code="run_recovery_requires_reconciliation")
            self.store._leases[run_id] = replace(old, owner_id=owner_id, expires_at_ms=now + lease_duration_ms, heartbeat_at_ms=now, attempt=old.attempt + 1)
            return True

    async def renew(self, run_id, owner_id, *, lease_duration_ms):
        async with self.store._transaction(with_journal=False):
            now = int(time.time() * 1000)
            old = self.store._leases.get(run_id)
            if old is None or old.owner_id != owner_id or (old.expires_at_ms or 0) <= now: return False
            claim = execution_claim.get()
            if claim is not None and claim[:2] == (run_id, owner_id) and claim[2] != old.attempt: return False
            self.store._leases[run_id] = replace(old, expires_at_ms=now + lease_duration_ms, heartbeat_at_ms=now)
            return True

    async def release(self, run_id, owner_id):
        async with self.store._transaction(with_journal=False):
            old = self.store._leases.get(run_id)
            if old is None or old.owner_id != owner_id: return False
            claim = execution_claim.get()
            if claim is not None and claim[:2] == (run_id, owner_id) and claim[2] != old.attempt: return False
            self.store._leases[run_id] = replace(old, owner_id=None, expires_at_ms=None)
            return True

    async def request_cancellation(self, run_id):
        async with self.store._transaction(with_journal=False) as adapters:
            run = adapters.get_run_info(run_id)
            if run is None or run.status is not RunStatus.RUNNING: return False
            old = self.store._leases.get(run_id, RunExecutionLease(run_id, run.status))
            if old.cancellation_requested_at_ms is not None: return False
            self.store._leases[run_id] = replace(old, cancellation_requested_at_ms=int(time.time() * 1000))
            return True
