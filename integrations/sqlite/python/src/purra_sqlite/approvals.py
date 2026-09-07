"""Transactional approval decisions; runtime suspension/dispatch is separate."""
from dataclasses import replace
import inspect
import json
import time

from purra.approvals import ApprovalIntent, ApprovalRecord, ApprovalDecisionCommand
from purra.errors import ContractViolationError
from purra.json_values import freeze_json_mapping, thaw_json_mapping
from purra.structured import json_identity_digest
from .approval_format import storage_version


def _fail(code):
    raise ContractViolationError("Approval operation was rejected", code=code)


def _text(value, name):
    if not isinstance(value, str) or not value or value.strip() != value or len(value) > 1024:
        raise ValueError(f"invalid approval {name}")


def _integer(value, name):
    if type(value) is not int or not 0 <= value <= 9007199254740991:
        raise ValueError(f"invalid approval {name}")
    return value


class SqliteApprovalStore:
    """Host-only intent/decision storage. Approved records are not execution permits.

    authorize(principal_id, record, command) must return literal True. Obtain
    principal_id from authenticated host context, never from model/tool data.
    """
    def __init__(self, storage, *, authorize, clock_ms=None):
        if not callable(authorize):
            raise TypeError("approval authorizer is required")
        self._storage, self._authorize = storage, authorize
        self._clock = clock_ms or (lambda: int(time.time() * 1000))

    def _now(self):
        return _integer(self._clock(), "clock")

    def _enabled(self):
        if storage_version(self._storage._db) != 5:
            _fail("approval_storage_not_enabled")

    def _load(self, approval_id):
        _text(approval_id, "id")
        self._enabled()
        row = self._storage._db.execute(
            "SELECT run_id,call_id,body FROM purra_approvals WHERE scope=? AND sdk='python' AND approval_id=?",
            (self._storage.scope, approval_id),
        ).fetchone()
        if row is None:
            _fail("approval_not_found")
        record = ApprovalRecord.from_mapping(json.loads(row[2]))
        if record.approval_id != approval_id or (record.intent.run_id, record.intent.tool_call_id) != row[:2]:
            _fail("approval_record_conflict")
        return record

    def _save(self, record):
        self._storage._db.execute(
            "INSERT INTO purra_approvals VALUES(?, 'python', ?, ?, ?, ?) "
            "ON CONFLICT(scope,sdk,approval_id) DO UPDATE SET body=excluded.body",
            (self._storage.scope, record.approval_id, record.intent.run_id, record.intent.tool_call_id,
             json.dumps(record.to_mapping(), ensure_ascii=False, separators=(",", ":"))),
        )

    async def _run_state(self, session, intent):
        info = session.get_run_info(intent.run_id)
        if info is None or info.root_run_id != intent.root_run_id:
            _fail("approval_run_conflict")
        run = await session.runs.get(intent.run_id)
        root = await session.runs.get(intent.root_run_id)
        canceled = any(row.status.value != "running" for row in (run, root)) or any(
            lease is not None and lease.cancellation_requested_at_ms is not None
            for lease in (session.leases.get(intent.run_id), session.leases.get(intent.root_run_id))
        )
        deadlines = [row.deadline_at_ms for row in (run, root) if row.deadline_at_ms is not None]
        return canceled, min(deadlines) if deadlines else None, json_identity_digest(run.agent_preset_snapshot)

    async def create(self, intent: ApprovalIntent, *, expires_at_ms: int):
        if not isinstance(intent, ApprovalIntent):
            raise TypeError("approval intent is required")
        _integer(expires_at_ms, "expiry")
        identifier = json_identity_digest({"profile": "purra.approval-key/v1", "runId": intent.run_id, "toolCallId": intent.tool_call_id})
        async with self._storage._transaction(with_journal=False) as session:
            self._enabled()
            canceled, deadline, fingerprint = await self._run_state(session, intent)
            if canceled:
                _fail("approval_run_terminal")
            if fingerprint != intent.preset_fingerprint:
                _fail("approval_configuration_mismatch")
            expiry = min(expires_at_ms, deadline) if deadline is not None else expires_at_ms
            existing = self._storage._db.execute(
                "SELECT 1 FROM purra_approvals WHERE scope=? AND sdk='python' AND approval_id=?",
                (self._storage.scope, identifier),
            ).fetchone()
            if existing:
                record = self._load(identifier)
                if record.intent.digest != intent.digest or record.expires_at_ms != expiry:
                    _fail("approval_intent_conflict")
                return record
            now = self._now()
            if now >= expiry:
                _fail("approval_expired")
            record = ApprovalRecord(identifier, intent, 1, "pending", now, expiry)
            self._save(record)
            return record

    async def get(self, approval_id):
        async with self._storage._connection(read_only=True):
            return self._load(approval_id)

    async def list_pending(self, *, run_id=None):
        if run_id is not None:
            _text(run_id, "Run id")
        async with self._storage._connection(read_only=True):
            self._enabled()
            rows = self._storage._db.execute(
                "SELECT approval_id FROM purra_approvals WHERE scope=? AND sdk='python'"
                + (" AND run_id=?" if run_id is not None else "") + " ORDER BY approval_id",
                (self._storage.scope, run_id) if run_id is not None else (self._storage.scope,),
            ).fetchall()
            return tuple(record for (identifier,) in rows if (record := self._load(identifier)).status in {"pending", "approved"})

    async def _refresh(self, session, record, now):
        canceled, deadline, fingerprint = await self._run_state(session, record.intent)
        status = "canceled" if canceled else "expired" if now >= min(record.expires_at_ms, deadline if deadline is not None else record.expires_at_ms) else None
        if status is not None and record.status in {"pending", "approved"}:
            record = replace(record, status=status, revision=record.revision + 1)
            self._save(record)
        return record, fingerprint

    async def refresh(self, approval_id):
        """Persist only expiry/canonical Run cancellation, never a human decision."""
        async with self._storage._transaction(with_journal=False) as session:
            record, _ = await self._refresh(session, self._load(approval_id), self._now())
            return record

    async def decide(self, command: ApprovalDecisionCommand, *, principal_id: str):
        if not isinstance(command, ApprovalDecisionCommand):
            raise TypeError("approval decision command is required")
        _text(principal_id, "principal")
        observed = await self.get(command.approval_id)
        try:
            allowed = self._authorize(principal_id, observed, command)
            if inspect.isawaitable(allowed):
                allowed = await allowed
        except Exception:
            _fail("approval_authorization_failed")
        if allowed is not True:
            _fail("approval_authorization_denied")
        error = None
        async with self._storage._transaction(with_journal=False) as session:
            record = self._load(command.approval_id)
            audit = record.decision_audit
            if audit and audit["command"]["commandKey"] == command.command_key:
                if audit["principalId"] != principal_id or thaw_json_mapping(audit["command"]) != command.to_mapping():
                    _fail("approval_command_conflict")
                return _receipt(record)
            if record.revision != command.expected_revision or record.intent.digest != command.intent_digest or record.status != "pending":
                _fail("approval_revision_conflict")
            now = self._now()
            record, fingerprint = await self._refresh(session, record, now)
            if record.status in {"expired", "canceled"}:
                error = "approval_" + record.status
            elif fingerprint != record.intent.preset_fingerprint:
                error = "approval_configuration_mismatch"
            else:
                record = replace(record, status="approved" if command.decision == "approve" else "rejected",
                    revision=record.revision + 1, decision_audit={"command": command.to_mapping(),
                        "principalId": principal_id, "decidedAtMs": now, "revision": record.revision + 1})
                self._save(record)
        if error:
            _fail(error)
        return _receipt(record)


def _receipt(record):
    audit = record.decision_audit
    return freeze_json_mapping({"approvalId": record.approval_id, "intentDigest": record.intent.digest,
        "commandKey": audit["command"]["commandKey"], "status": "approved" if audit["command"]["decision"] == "approve" else "rejected",
        "revision": audit["revision"], "decidedAtMs": audit["decidedAtMs"]})
