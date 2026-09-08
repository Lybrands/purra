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
        return self._decode(approval_id, row)

    @staticmethod
    def _decode(approval_id, row):
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
            return await self._create(session, intent, expires_at_ms, identifier)

    async def _create(self, session, intent, expires_at_ms, identifier):
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

    async def prepare(self, checkpoint, intent, *, expires_at_ms):
        """Commit the intent and real tool-ready cursor together, then suspend.

        Called only by a leased runtime boundary. Reconstruct intent from current
        host bindings on every resume; no record itself grants dispatch authority.
        """
        from purra.agent_execution_checkpoint import AgentToolExecutionCheckpoint
        from purra.approvals import ApprovalRequired
        from purra.ports import RunCommit
        from purra.events import AgentEvent
        if not isinstance(checkpoint, AgentToolExecutionCheckpoint) or not isinstance(intent, ApprovalIntent):
            raise TypeError("tool-ready checkpoint and intent required")
        _integer(expires_at_ms, "expiry")
        call = checkpoint.assistant.tool_calls[0]
        if (checkpoint.run_id != intent.run_id or call.id != intent.tool_call_id or call.name != intent.tool_name
                or json_identity_digest(json.loads(call.arguments_json)) != json_identity_digest(intent.arguments)):
            _fail("approval_checkpoint_conflict")
        identifier = json_identity_digest({"profile": "purra.approval-key/v1", "runId": intent.run_id, "toolCallId": intent.tool_call_id})
        async with self._storage._transaction() as session:
            self._require_owner(session, intent.run_id)
            if not session.has_settled_tool_invocation(intent.run_id, checkpoint.invocation_id, checkpoint.model_budget_key):
                _fail("approval_invocation_unsettled")
            record = await self._create(session, intent, expires_at_ms, identifier)
            current = (await session.runs.get(intent.run_id)).execution_checkpoint
            if current != checkpoint:
                await session.runs.commit(intent.run_id, RunCommit(execution_checkpoint=checkpoint,
                    events=(AgentEvent("agent.execution_checkpointed", {"schemaVersion": 3, "phase": "tool_ready", "nextRound": checkpoint.next_round}, intent.run_id),)))
                if record.status == "pending":
                    from datetime import datetime, timezone
                    from purra.output import AgentOutputEventDraft
                    await session.outputs.append_event(AgentOutputEventDraft(
                        run_id=intent.run_id, turn_id=None, output_stream_id=None, invocation_id=checkpoint.invocation_id,
                        source_event_key=f"approval:{record.approval_id}:required:{record.revision}",
                        source="runtime", kind="runtime.event", channel="lifecycle", visibility="private",
                        payload={"type": "approval.required", "status": "pending", "revision": record.revision},
                        occurred_at=datetime.now(timezone.utc)))
            record, _ = await self._refresh(session, record, self._now())
            completed = self._completed_receipt(session, record, call)
        if completed:
            return record
        if record.status == "pending":
            raise ApprovalRequired(record.intent.run_id, record.approval_id)
        if record.status != "approved":
            _fail("approval_" + record.status)
        return record

    def _require_owner(self, session, run_id):
        from purra.execution.ownership import execution_owner, execution_claim
        claim = execution_claim.get()
        lease = session.leases.get(run_id)
        if (claim is None or claim[0] != run_id or lease is None or execution_owner.get() is None
                or lease.owner_id != execution_owner.get() or claim[2] != lease.attempt
                or (lease.expires_at_ms or 0) <= int(time.time() * 1000)):
            _fail("agent_run_lease_lost")

    def gateway(self):
        """Bind only alongside prepare and this adapter's idempotency gateway."""
        return _DurableApprovalGateway(self)

    @staticmethod
    def _completed_receipt(session, record, call):
        receipt = session.get_tool_receipt((record.intent.run_id, call.id))
        if receipt is None:
            return False
        association = session.extra.get("approvalExecutions", {}).get(record.approval_id)
        if (receipt[0] != call or receipt[1].effect_state.value == "unknown" or not isinstance(association, dict)
                or set(association) != {"intentDigest", "state", "effectState", "approvalRevision", "leaseOwnerId", "leaseEpoch"}
                or association["intentDigest"] != record.intent.digest or association["state"] != "complete"
                or association["effectState"] != receipt[1].effect_state.value
                or association["approvalRevision"] != record.decision_audit.get("revision")
                or not isinstance(association["leaseOwnerId"], str) or not association["leaseOwnerId"]
                or type(association["leaseEpoch"]) is not int or association["leaseEpoch"] < 1):
            _fail("approval_claim_conflict")
        return True

    async def _dispatch_record(self, session, run_id, call):
        self._enabled()
        from purra.agent_execution_checkpoint import AgentToolExecutionCheckpoint
        self._require_owner(session, run_id)
        identifier = json_identity_digest({"profile": "purra.approval-key/v1", "runId": run_id, "toolCallId": call.id})
        record = self._load(identifier)
        intent = record.intent
        checkpoint = (await session.runs.get(run_id)).execution_checkpoint
        if (not isinstance(checkpoint, AgentToolExecutionCheckpoint) or checkpoint.assistant.tool_calls != (call,)
                or call.name != intent.tool_name
                or json_identity_digest(json.loads(call.arguments_json)) != json_identity_digest(intent.arguments)):
            _fail("approval_checkpoint_conflict")
        if not session.has_settled_tool_invocation(run_id, checkpoint.invocation_id, checkpoint.model_budget_key):
            _fail("approval_invocation_unsettled")
        if self._completed_receipt(session, record, call):
            return record
        canceled, deadline, fingerprint = await self._run_state(session, intent)
        if canceled:
            _fail("approval_canceled")
        if record.status != "approved":
            _fail("approval_" + record.status)
        if self._now() >= min(record.expires_at_ms, deadline if deadline is not None else record.expires_at_ms):
            _fail("approval_expired")
        if fingerprint != intent.preset_fingerprint:
            _fail("approval_configuration_mismatch")
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
                "SELECT approval_id,run_id,call_id,body FROM purra_approvals WHERE scope=? AND sdk='python'"
                + (" AND run_id=?" if run_id is not None else "") + " ORDER BY approval_id",
                (self._storage.scope, run_id) if run_id is not None else (self._storage.scope,),
            ).fetchall()
            return tuple(record for row in rows if (record := self._decode(row[0], row[1:])).status in {"pending", "approved"})

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


class _DurableApprovalGateway:
    requires_durable_idempotency = True

    @property
    def idempotency_gateway(self):
        return self._store._storage.idempotency

    def __init__(self, store):
        self._store = store

    async def request(self, run_id, approval, event_sink, signal=None):
        from purra.contracts import ApprovalResult, ApprovalStatus
        if signal is not None and signal.is_set():
            return ApprovalResult(None, ApprovalStatus.CANCELED)
        async with self._store._storage._transaction(with_journal=False) as session:
            record = await self._store._dispatch_record(session, run_id, approval.tool_call)
            if approval.binding is not None:
                intent = record.intent.to_mapping()
                if any(intent[key] != value for key, value in approval.binding.items()):
                    _fail("approval_intent_conflict")
        return ApprovalResult(record.approval_id, ApprovalStatus.APPROVED)

    async def resolve(self, run_id, approval_id, decision):
        return None

    async def cancel_pending(self, run_id):
        # Suspending a durable Run must not cancel its persisted intent.
        return 0


def inspect_approval_state(storage, session, saved, now):
    """Read committed approval evidence without refreshing or authorizing a record."""
    from purra.agent_execution_checkpoint import AgentToolExecutionCheckpoint
    if storage_version(storage._db) != 5:
        return {}
    rows = storage._db.execute("SELECT approval_id,call_id,body FROM purra_approvals WHERE scope=? AND sdk='python' AND run_id=?",
        (storage.scope, saved.run_id)).fetchall()
    records = []
    for identifier, call_id, body in rows:
        record = ApprovalRecord.from_mapping(json.loads(body))
        if record.approval_id != identifier or record.intent.run_id != saved.run_id or record.intent.tool_call_id != call_id:
            _fail("approval_record_conflict")
        records.append(record)
    observation = {"approvalState": "unknown" if records else "none", "approvalRecords": len(records),
        "approvalUnknownReceipts": sum((saved.run_id, record.intent.tool_call_id) in session.claims for record in records),
        "approvalCheckpointIntent": "unknown", "approvalReceipt": "unknown"}
    checkpoint = saved.execution_checkpoint
    if not isinstance(checkpoint, AgentToolExecutionCheckpoint):
        return observation
    call = checkpoint.assistant.tool_calls[0]
    record = next((record for record in records if record.intent.tool_call_id == call.id), None)
    if record is None:
        observation.update(approvalState="missing", approvalReceipt="absent")
        return observation
    matches = (record.intent.tool_name == call.name and json_identity_digest(record.intent.arguments) == json_identity_digest(json.loads(call.arguments_json))
        and record.intent.preset_fingerprint == json_identity_digest(saved.agent_preset_snapshot))
    complete = matches and SqliteApprovalStore._completed_receipt(session, record, call)
    state = record.status
    if state in {"pending", "approved"}:
        if saved.status.value != "running": state = "canceled"
        elif now >= min(record.expires_at_ms, saved.deadline_at_ms if saved.deadline_at_ms is not None else record.expires_at_ms): state = "expired"
    observation.update(approvalState=state, approvalCheckpointIntent="matched" if matches else "mismatch", approvalReceipt="complete" if complete else "absent")
    return observation
