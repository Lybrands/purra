"""Process-local reference stores for PurrA durable-state ports."""

from __future__ import annotations

from purra.adapter_records import StoredLongTask

from purra.adapter_state import ArtifactState, LongTaskState

import asyncio
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from graphlib import CycleError, TopologicalSorter
from uuid import uuid4

from purra.artifacts.contracts import (
    ArtifactAppendCommand,
    ArtifactBatch,
    ArtifactBatchReceipt,
    ArtifactCreateCommand,
    ArtifactFinalizeCommand,
    ArtifactMutationLease,
    ArtifactRecord,
    ArtifactStatus,
)
from purra.artifacts.continuity import (
    ArtifactClaimLeaseCommand,
    ArtifactWriteClaim,
    ArtifactWriteClaimCommand,
)
from purra.artifacts.errors import (
    ArtifactConflictError,
    ArtifactNotFoundError,
    ArtifactStateError,
)
from purra.artifacts.maintenance import (
    ArtifactMaintenancePolicy,
    ArtifactMaintenanceReport,
    ArtifactMaintenanceSnapshot,
)
from purra.artifacts.ownership import ArtifactOwnerRef
from purra.errors import ContractViolationError
from purra.json_values import thaw_json_mapping
from purra.long_tasks.contracts import (
    BudgetExhaustionDisposition,
    LongTaskBudgetLimits,
    LongTaskCreateCommand,
    LongTaskRecord,
    LongTaskRunBinding,
    LongTaskRunRelation,
    LongTaskSplitResult,
    LongTaskStatus,
    LongTaskUnitRecord,
    LongTaskUnitResult,
    LongTaskUnitSpec,
    LongTaskUnitStatus,
    LongTaskUsage,
)
from purra.long_tasks.ports import LongTaskRepository
from purra.recovery import (
    FailureDecision,
    FailureDisposition,
    FailureScope,
)


from ._long_task_mixins import LongTaskUnitSettlementMixin, LongTaskUnitSchedulingMixin
from ._store_kit import (
    canonical_digest as _digest,
    required_text_field as _required,
    utc_timestamp as _timestamp,
    wall_time_ms as _wall_time_ms,
)


class InMemoryArtifactStore:
    """One cancellation-linearizable reference adapter for Artifact ports."""

    def __init__(
        self,
        *,
        clock_ms: Callable[[], int] = _wall_time_ms,
        run_is_available: Callable[[str], bool] | None = None,
        state: ArtifactState | None = None,
    ) -> None:
        self._state = state if state is not None else ArtifactState()
        self._lock = asyncio.Lock()
        self._clock_ms = clock_ms
        self._run_is_available = run_is_available or (lambda _run_id: True)

    async def create(
        self,
        artifact_id: str,
        command: ArtifactCreateCommand,
    ) -> ArtifactRecord:
        normalized_id = _required(artifact_id, "artifact id")
        async with self._lock:
            existing = self._state.artifacts.get(normalized_id)
            candidate = ArtifactRecord(
                id=normalized_id,
                namespace=command.namespace,
                kind=command.kind,
                owner_id=command.owner_id,
                owner_ref=command.owner_ref,
                created_by_run_id=command.created_by_run_id,
                schema_version=command.schema_version,
                expected_item_count=command.expected_item_count,
                metadata=command.metadata,
            )
            if existing is not None:
                if existing == candidate:
                    return existing
                raise ArtifactConflictError(
                    "artifact id conflicts",
                    code="artifact_id_conflict",
                )
            owner_key = self._owner_key(command)
            owned_id = self._state.owners.get(owner_key)
            if owned_id is not None:
                owned = self._state.artifacts[owned_id]
                if self._matches_artifact_create(owned, command):
                    return owned
                raise ArtifactConflictError(
                    "artifact owner identity conflicts",
                    code="artifact_owner_conflict",
                )
            self._state.artifacts[normalized_id] = candidate
            self._state.owners[owner_key] = normalized_id
            self._state.batches[normalized_id] = []
            self._state.updated_at_ms[normalized_id] = self._clock_ms()
            return candidate

    async def load(self, artifact_id: str) -> ArtifactRecord | None:
        async with self._lock:
            return self._state.artifacts.get(str(artifact_id or "").strip())

    async def find_for_owner(
        self,
        *,
        namespace: str,
        kind: str,
        owner_id: str,
        owner_ref: ArtifactOwnerRef,
    ) -> ArtifactRecord | None:
        key = (
            _required(namespace, "artifact namespace"),
            _required(kind, "artifact kind"),
            _required(owner_id, "artifact owner id"),
            owner_ref.kind,
            owner_ref.id,
        )
        async with self._lock:
            artifact_id = self._state.owners.get(key)
            return self._state.artifacts.get(artifact_id) if artifact_id else None

    async def replay_receipt(
        self,
        command: ArtifactAppendCommand,
    ) -> ArtifactBatchReceipt | None:
        async with self._lock:
            return self._replay_receipt(command)

    async def append(
        self,
        command: ArtifactAppendCommand,
    ) -> ArtifactBatchReceipt:
        async with self._lock:
            replayed = self._replay_receipt(command)
            if replayed is not None:
                return replayed
            artifact = self._require_artifact(command.artifact_id)
            self._require_open(artifact)
            self._require_revision(artifact, command.expected_revision)
            if artifact.next_sequence != command.sequence:
                raise ArtifactConflictError(
                    "artifact sequence conflicts",
                    code="artifact_sequence_conflict",
                )
            claim = self._require_claim(artifact, command.write_lease)
            committed_revision = artifact.revision + 1
            batch = ArtifactBatch(
                artifact_id=artifact.id,
                batch_id=command.batch_id,
                idempotency_key=command.idempotency_key,
                sequence=command.sequence,
                committed_revision=committed_revision,
                items=command.items,
                coverage_keys=command.coverage_keys,
                content_digest=command.content_digest,
            )
            receipt = ArtifactBatchReceipt(
                artifact_id=artifact.id,
                batch_id=command.batch_id,
                sequence=command.sequence,
                committed_revision=committed_revision,
                next_sequence=command.sequence + 1,
                accepted_count=len(command.items),
            )
            self._state.batches[artifact.id].append(batch)
            receipt_key = (artifact.id, command.idempotency_key)
            self._state.receipts[receipt_key] = receipt
            self._state.receipt_digests[receipt_key] = command.content_digest
            self._state.artifacts[artifact.id] = replace(
                artifact,
                revision=committed_revision,
                next_sequence=command.sequence + 1,
                committed_item_count=(
                    artifact.committed_item_count + len(command.items)
                ),
            )
            self._state.claims[artifact.id] = replace(
                claim,
                acquired_revision=committed_revision,
                expires_at_ms=(
                    self._clock_ms() + command.write_lease.lease_duration_ms
                ),
            )
            self._state.updated_at_ms[artifact.id] = self._clock_ms()
            return receipt

    async def list_batches(self, artifact_id: str) -> Sequence[ArtifactBatch]:
        async with self._lock:
            self._require_artifact(artifact_id)
            return tuple(self._state.batches.get(artifact_id, ()))

    async def finalize(
        self,
        command: ArtifactFinalizeCommand,
        *,
        coverage_digest: str,
    ) -> ArtifactRecord:
        async with self._lock:
            artifact = self._require_artifact(command.artifact_id)
            self._require_open(artifact)
            self._require_revision(artifact, command.expected_revision)
            self._require_claim(artifact, command.write_lease)
            finalized = replace(
                artifact,
                status=ArtifactStatus.FINALIZED,
                revision=artifact.revision + 1,
                resource_ref=command.resource_ref,
                coverage_digest=coverage_digest,
            )
            self._state.artifacts[artifact.id] = finalized
            self._state.claims.pop(artifact.id, None)
            self._state.updated_at_ms[artifact.id] = self._clock_ms()
            return finalized

    async def abort(
        self,
        artifact_id: str,
        *,
        expected_revision: int,
        write_lease: ArtifactMutationLease,
    ) -> ArtifactRecord:
        async with self._lock:
            artifact = self._require_artifact(artifact_id)
            self._require_open(artifact)
            self._require_revision(artifact, expected_revision)
            self._require_claim(artifact, write_lease)
            aborted = replace(
                artifact,
                status=ArtifactStatus.ABORTED,
                revision=artifact.revision + 1,
            )
            self._state.artifacts[artifact.id] = aborted
            self._state.claims.pop(artifact.id, None)
            self._state.updated_at_ms[artifact.id] = self._clock_ms()
            return aborted

    async def acquire(
        self,
        command: ArtifactWriteClaimCommand,
    ) -> ArtifactWriteClaim:
        async with self._lock:
            artifact = self._require_artifact(command.artifact_id)
            self._require_open(artifact)
            self._require_revision(artifact, command.expected_revision)
            current = self._active_claim(artifact.id)
            if current is not None:
                if (
                    current.run_id == command.run_id
                    and current.acquired_revision == command.expected_revision
                ):
                    return current
                raise ArtifactConflictError(
                    "artifact already has an active writer",
                    code="artifact_write_claim_conflict",
                )
            claim = ArtifactWriteClaim(
                artifact_id=artifact.id,
                run_id=command.run_id,
                claim_token=uuid4().hex,
                acquired_revision=artifact.revision,
                expires_at_ms=self._clock_ms() + command.lease_duration_ms,
            )
            self._state.claims[artifact.id] = claim
            return claim

    async def load_active(
        self,
        artifact_id: str,
    ) -> ArtifactWriteClaim | None:
        async with self._lock:
            return self._active_claim(str(artifact_id or "").strip())

    async def renew(
        self,
        command: ArtifactClaimLeaseCommand,
    ) -> ArtifactWriteClaim:
        if command.lease_duration_ms is None:
            raise ValueError("claim renewal requires lease_duration_ms")
        async with self._lock:
            artifact = self._require_artifact(command.artifact_id)
            claim = self._require_claim_command(artifact, command)
            renewed = replace(
                claim,
                expires_at_ms=self._clock_ms() + command.lease_duration_ms,
            )
            self._state.claims[artifact.id] = renewed
            return renewed

    async def release(self, command: ArtifactClaimLeaseCommand) -> bool:
        if command.lease_duration_ms is not None:
            raise ValueError("claim release cannot include lease_duration_ms")
        async with self._lock:
            claim = self._state.claims.get(command.artifact_id)
            if claim is None or (
                claim.run_id != command.run_id
                or claim.claim_token != command.claim_token
            ):
                return False
            self._state.claims.pop(command.artifact_id, None)
            return True

    async def release_for_run(self, run_id: str) -> int:
        normalized = _required(run_id, "claim Run id")
        async with self._lock:
            targets = [
                artifact_id
                for artifact_id, claim in self._state.claims.items()
                if claim.run_id == normalized
            ]
            for artifact_id in targets:
                self._state.claims.pop(artifact_id, None)
            return len(targets)

    async def maintain(
        self,
        policy: ArtifactMaintenancePolicy,
        *,
        timestamp_ms: int | None = None,
    ) -> ArtifactMaintenanceReport:
        now = self._clock_ms() if timestamp_ms is None else int(timestamp_ms)
        async with self._lock:
            expired = unavailable = invalid = 0
            for artifact_id, claim in tuple(self._state.claims.items()):
                artifact = self._state.artifacts.get(artifact_id)
                if claim.expires_at_ms <= now:
                    expired += 1
                elif not self._run_is_available(claim.run_id):
                    unavailable += 1
                elif (
                    artifact is None
                    or artifact.status is not ArtifactStatus.OPEN
                    or artifact.revision != claim.acquired_revision
                ):
                    invalid += 1
                else:
                    continue
                self._state.claims.pop(artifact_id, None)
            purged = 0
            if policy.terminal_retention_ms is not None:
                cutoff = now - policy.terminal_retention_ms
                candidates = sorted(
                    (
                        updated,
                        artifact_id,
                    )
                    for artifact_id, updated in self._state.updated_at_ms.items()
                    if self._state.artifacts[artifact_id].status is not ArtifactStatus.OPEN
                    if updated <= cutoff
                )[:policy.max_purge_artifacts]
                for _, artifact_id in candidates:
                    self._purge_artifact(artifact_id)
                    purged += 1
            return ArtifactMaintenanceReport(
                expired_claims_released=expired,
                unavailable_run_claims_released=unavailable,
                invalid_target_claims_released=invalid,
                purged_artifacts=purged,
            )

    async def inspect(
        self,
        *,
        run_id: str | None = None,
        timestamp_ms: int | None = None,
    ) -> ArtifactMaintenanceSnapshot:
        now = self._clock_ms() if timestamp_ms is None else int(timestamp_ms)
        normalized_run = str(run_id or "").strip() or None
        async with self._lock:
            artifacts = tuple(
                artifact
                for artifact in self._state.artifacts.values()
                if normalized_run is None
                or artifact.created_by_run_id == normalized_run
                or (
                    (claim := self._state.claims.get(artifact.id)) is not None
                    and claim.run_id == normalized_run
                )
            )
            claims = tuple(
                claim
                for claim in self._state.claims.values()
                if normalized_run is None or claim.run_id == normalized_run
            )
            expired = sum(claim.expires_at_ms <= now for claim in claims)
            unavailable = sum(
                claim.expires_at_ms > now
                and not self._run_is_available(claim.run_id)
                for claim in claims
            )
            invalid = sum(
                claim.expires_at_ms > now
                and self._run_is_available(claim.run_id)
                and (
                    (artifact := self._state.artifacts.get(claim.artifact_id)) is None
                    or artifact.status is not ArtifactStatus.OPEN
                    or artifact.revision != claim.acquired_revision
                )
                for claim in claims
            )
            return ArtifactMaintenanceSnapshot(
                checked_at_ms=now,
                scope_run_id=normalized_run,
                open_artifacts=sum(
                    item.status is ArtifactStatus.OPEN for item in artifacts
                ),
                finalized_artifacts=sum(
                    item.status is ArtifactStatus.FINALIZED for item in artifacts
                ),
                aborted_artifacts=sum(
                    item.status is ArtifactStatus.ABORTED for item in artifacts
                ),
                active_claims=len(claims) - expired - unavailable - invalid,
                expired_claims=expired,
                unavailable_run_claims=unavailable,
                invalid_target_claims=invalid,
            )

    @staticmethod
    def _owner_key(command: ArtifactCreateCommand) -> tuple[str, str, str, str, str]:
        return (
            command.namespace,
            command.kind,
            command.owner_id,
            command.owner_ref.kind,
            command.owner_ref.id,
        )

    @staticmethod
    def _matches_artifact_create(
        artifact: ArtifactRecord,
        command: ArtifactCreateCommand,
    ) -> bool:
        return (
            artifact.namespace == command.namespace
            and artifact.kind == command.kind
            and artifact.owner_id == command.owner_id
            and artifact.owner_ref == command.owner_ref
            and artifact.created_by_run_id == command.created_by_run_id
            and artifact.schema_version == command.schema_version
            and artifact.expected_item_count == command.expected_item_count
            and thaw_json_mapping(artifact.metadata)
            == thaw_json_mapping(command.metadata)
        )

    def _require_artifact(self, artifact_id: str) -> ArtifactRecord:
        try:
            return self._state.artifacts[_required(artifact_id, "artifact id")]
        except KeyError as error:
            raise ArtifactNotFoundError(
                "artifact does not exist",
                code="artifact_not_found",
            ) from error

    @staticmethod
    def _require_open(artifact: ArtifactRecord) -> None:
        if artifact.status is not ArtifactStatus.OPEN:
            raise ArtifactStateError(
                "artifact is not open",
                code="artifact_not_open",
            )

    @staticmethod
    def _require_revision(artifact: ArtifactRecord, expected: int) -> None:
        if artifact.revision != int(expected):
            raise ArtifactConflictError(
                "artifact revision conflicts",
                code="artifact_revision_conflict",
            )

    def _active_claim(self, artifact_id: str) -> ArtifactWriteClaim | None:
        claim = self._state.claims.get(artifact_id)
        if claim is not None and claim.expires_at_ms <= self._clock_ms():
            return None
        return claim

    def _require_claim(
        self,
        artifact: ArtifactRecord,
        lease: ArtifactMutationLease,
    ) -> ArtifactWriteClaim:
        claim = self._active_claim(artifact.id)
        if claim is None or (
            claim.run_id != lease.run_id
            or claim.claim_token != lease.claim_token
            or claim.acquired_revision != artifact.revision
        ):
            raise ArtifactConflictError(
                "artifact write claim is invalid",
                code="artifact_write_claim_invalid",
            )
        return claim

    def _require_claim_command(
        self,
        artifact: ArtifactRecord,
        command: ArtifactClaimLeaseCommand,
    ) -> ArtifactWriteClaim:
        return self._require_claim(
            artifact,
            ArtifactMutationLease(
                run_id=command.run_id,
                claim_token=command.claim_token,
                lease_duration_ms=command.lease_duration_ms or 1,
            ),
        )

    def _replay_receipt(
        self,
        command: ArtifactAppendCommand,
    ) -> ArtifactBatchReceipt | None:
        key = (command.artifact_id, command.idempotency_key)
        receipt = self._state.receipts.get(key)
        if receipt is None:
            return None
        if self._state.receipt_digests[key] != command.content_digest:
            raise ArtifactConflictError(
                "artifact idempotency key conflicts",
                code="artifact_idempotency_conflict",
            )
        return replace(receipt, replayed=True)

    def _purge_artifact(self, artifact_id: str) -> None:
        artifact = self._state.artifacts.pop(artifact_id)
        self._state.owners.pop((
            artifact.namespace,
            artifact.kind,
            artifact.owner_id,
            artifact.owner_ref.kind,
            artifact.owner_ref.id,
        ), None)
        self._state.batches.pop(artifact_id, None)
        self._state.claims.pop(artifact_id, None)
        self._state.updated_at_ms.pop(artifact_id, None)
        for key in tuple(self._state.receipts):
            if key[0] == artifact_id:
                self._state.receipts.pop(key, None)
                self._state.receipt_digests.pop(key, None)


class InMemoryLongTaskRepository(
    LongTaskUnitSchedulingMixin,
    LongTaskUnitSettlementMixin,
):
    """Process-local executable specification of ``LongTaskRepository``."""

    def __init__(self, *, clock_ms: Callable[[], int] = _wall_time_ms, state: LongTaskState | None = None) -> None:
        self._state = state if state is not None else LongTaskState()
        self._lock = asyncio.Lock()
        self._clock_ms = clock_ms

    async def create(
        self,
        task_id: str,
        command: LongTaskCreateCommand,
    ) -> LongTaskRecord:
        normalized_id = _required(task_id, "long task id")
        async with self._lock:
            existing = self._state.tasks.get(normalized_id)
            if existing is not None:
                if self._matches_create(existing, command):
                    return existing.record
                raise ValueError("long task id conflicts")
            active = self._find_active_state(
                command.namespace,
                command.owner_id,
                command.kind,
                command.metadata.get("sessionId"),
                True,
            )
            if active is not None:
                return active.record
            timestamp = _timestamp()
            record = LongTaskRecord(
                id=normalized_id,
                namespace=command.namespace,
                kind=command.kind,
                owner_id=command.owner_id,
                created_by_run_id=command.created_by_run_id,
                status=LongTaskStatus.PENDING,
                revision=1,
                total_units=sum(unit.required for unit in command.units),
                completed_units=0,
                failed_units=0,
                max_parallelism=command.max_parallelism,
                deadline_at_ms=command.deadline_at_ms,
                budget_limits=command.budget_limits,
                budget_exhaustion_disposition=(
                    command.budget_exhaustion_disposition
                ),
                metadata=command.metadata,
                create_time=timestamp,
                update_time=timestamp,
            )
            units = {
                spec.id: self._unit_from_spec(record.id, spec, timestamp)
                for spec in command.units
            }
            binding = LongTaskRunBinding(
                task_id=record.id,
                run_id=command.created_by_run_id,
                relation=LongTaskRunRelation.CREATED,
            )
            self._state.tasks[record.id] = StoredLongTask(
                record=record,
                units=units,
                bindings={binding.run_id: binding},
                usage_by_run={},
            )
            return record

    async def load(self, task_id: str) -> LongTaskRecord | None:
        async with self._lock:
            state = self._state.tasks.get(str(task_id or "").strip())
            return state.record if state else None

    async def bind_run(
        self,
        task_id: str,
        run_id: str,
        *,
        relation: LongTaskRunRelation,
    ) -> LongTaskRunBinding:
        relation = LongTaskRunRelation(relation)
        if relation is LongTaskRunRelation.CREATED:
            raise ValueError("creator Run binding is immutable after creation")
        normalized_run = _required(run_id, "long task Run id")
        async with self._lock:
            state = self._require_state(task_id)
            existing = state.bindings.get(normalized_run)
            if existing is not None:
                if existing.relation is relation:
                    return existing
                raise ValueError("long task Run binding conflicts")
            binding = LongTaskRunBinding(state.record.id, normalized_run, relation)
            state.bindings[normalized_run] = binding
            return binding

    async def list_run_bindings(
        self,
        task_id: str,
    ) -> Sequence[LongTaskRunBinding]:
        async with self._lock:
            return tuple(self._require_state(task_id).bindings.values())

    async def list_for_owner(
        self,
        *,
        namespace: str,
        owner_id: str,
        kind: str | None = None,
        limit: int = 20,
    ) -> Sequence[LongTaskRecord]:
        normalized_kind = str(kind or "").strip() or None
        async with self._lock:
            records = [
                state.record
                for state in reversed(tuple(self._state.tasks.values()))
                if state.record.namespace == namespace
                and state.record.owner_id == owner_id
                and (normalized_kind is None or state.record.kind == normalized_kind)
            ]
            return tuple(records[:max(1, int(limit))])

    async def find_by_idempotency_key(
        self,
        namespace: str,
        idempotency_key: str,
    ) -> LongTaskRecord | None:
        normalized_namespace = _required(namespace, "long task namespace")
        normalized_key = _required(
            idempotency_key,
            "long task idempotency key",
        )
        async with self._lock:
            matches = tuple(
                state.record
                for state in self._state.tasks.values()
                if state.record.namespace == normalized_namespace
                and state.record.metadata.get("idempotencyKey") == normalized_key
            )
            if len(matches) > 1:
                raise ValueError("long task idempotency key conflicts")
            return matches[0] if matches else None

    async def find_active(
        self,
        *,
        namespace: str,
        owner_id: str,
        kind: str,
        session_id=None,
        match_session: bool = False,
    ) -> LongTaskRecord | None:
        async with self._lock:
            state = self._find_active_state(
                namespace,
                owner_id,
                kind,
                session_id,
                match_session,
            )
            return state.record if state else None

    async def list_units(self, task_id: str) -> Sequence[LongTaskUnitRecord]:
        async with self._lock:
            units = self._require_state(task_id).units.values()
            return tuple(sorted(units, key=lambda unit: unit.position))

    async def record_usage(
        self,
        task_id: str,
        *,
        run_id: str,
        usage: LongTaskUsage,
        expected_revision: int,
    ) -> LongTaskRecord:
        if not isinstance(usage, LongTaskUsage):
            raise TypeError("long task usage must be LongTaskUsage")
        normalized_run = _required(run_id, "long task usage Run id")
        async with self._lock:
            state = self._require_state(task_id)
            existing = state.usage_by_run.get(normalized_run)
            if existing is not None:
                if existing == usage:
                    return state.record
                raise ValueError("long task Run usage conflicts")
            self._require_task_revision(state, expected_revision)
            state.usage_by_run[normalized_run] = usage
            state.record = replace(
                state.record,
                usage=self._aggregate_usage(state.usage_by_run.values()),
            )
            budget_kind = self._task_budget_exhaustion(state, exceeded_only=True)
            if budget_kind is not None:
                self._fail_budget(state, budget_kind)
                return state.record
            self._touch(state)
            return state.record

    async def start(
        self,
        task_id: str,
        *,
        expected_revision: int,
    ) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            if self._deadline_elapsed(state):
                self._expire_deadline(state)
                return state.record
            if state.record.cancellation_requested_at_ms is not None:
                self._cancel(state)
                return state.record
            if state.record.status is LongTaskStatus.RUNNING:
                return state.record
            if state.record.status is not LongTaskStatus.PENDING:
                raise ValueError("only a pending long task can start")
            self._require_task_revision(state, expected_revision)
            self._set_status(state, LongTaskStatus.RUNNING)
            return state.record


    async def expire_deadline(self, task_id: str) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            if state.record.status.terminal:
                return state.record
            if not self._deadline_elapsed(state):
                raise ContractViolationError(
                    "long task deadline has not elapsed",
                    code="long_task_deadline_not_elapsed",
                )
            self._expire_deadline(state)
            return state.record


    async def pause(
        self,
        task_id: str,
        *,
        expected_revision: int | None = None,
        reason_code: str | None = None,
    ) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            if (
                expected_revision is not None
                and state.record.revision != int(expected_revision)
            ):
                raise ValueError("long task revision conflict")
            if state.record.status is LongTaskStatus.PAUSED:
                return state.record
            if state.record.status not in {
                LongTaskStatus.PENDING,
                LongTaskStatus.RUNNING,
            }:
                raise ValueError("long task cannot pause")
            self._release_active_units(
                state,
                str(reason_code or "").strip() or "execution_paused",
            )
            self._set_status(state, LongTaskStatus.PAUSED)
            return state.record

    async def resume(
        self,
        task_id: str,
        *,
        additional_attempts: int = 0,
    ) -> LongTaskRecord:
        extra = int(additional_attempts)
        if extra < 0:
            raise ValueError("additional attempts cannot be negative")
        async with self._lock:
            state = self._require_state(task_id)
            if state.record.status is LongTaskStatus.RUNNING:
                return state.record
            if state.record.status not in {
                LongTaskStatus.PAUSED,
                LongTaskStatus.FAILED,
            }:
                raise ValueError("long task cannot resume")
            recoverable = {
                LongTaskUnitStatus.BLOCKED,
                LongTaskUnitStatus.FAILED,
                LongTaskUnitStatus.CANCELED,
            }
            targets = [
                unit for unit in state.units.values() if unit.status in recoverable
            ]
            blocked_targets = [
                unit for unit in targets
                if unit.status is LongTaskUnitStatus.BLOCKED
            ]
            if blocked_targets and len(blocked_targets) == len(targets) and extra <= 0:
                # A paused transient failure resumes with one new attempt;
                # permanent failures still require an explicit retry decision.
                extra = 1
            if targets and extra <= 0:
                raise ValueError("durable retry requires additional attempts")
            if not targets and extra:
                raise ValueError("long task has no unit requiring attempts")
            for unit in targets:
                state.units[unit.id] = replace(
                    unit,
                    status=LongTaskUnitStatus.PENDING,
                    max_attempts=unit.max_attempts + extra,
                    worker_id=None,
                    lease_expires_at_ms=None,
                )
            state.record = replace(state.record, failed_units=0)
            self._set_status(state, LongTaskStatus.RUNNING)
            return state.record

    async def cancel(self, task_id: str) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            if state.record.status is LongTaskStatus.CANCELED:
                return state.record
            if state.record.status.terminal:
                raise ValueError("terminal long task cannot be canceled")
            self._cancel(state)
            return state.record

    async def request_cancel(
        self,
        task_id: str,
        *,
        requested_at_ms: int | None = None,
    ) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            if state.record.status.terminal:
                return state.record
            if state.record.cancellation_requested_at_ms is None:
                state.record = replace(
                    state.record,
                    cancellation_requested_at_ms=(
                        self._clock_ms()
                        if requested_at_ms is None
                        else int(requested_at_ms)
                    ),
                )
                self._touch(state)
            return state.record

    async def finalize_if_complete(self, task_id: str) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            if self._deadline_elapsed(state):
                self._expire_deadline(state)
                return state.record
            if state.record.cancellation_requested_at_ms is not None:
                self._cancel(state)
                return state.record
            if state.record.status is not LongTaskStatus.RUNNING:
                return state.record
            required = tuple(unit for unit in state.units.values() if unit.required)
            if any(unit.status is LongTaskUnitStatus.FAILED for unit in required):
                self._set_status(state, LongTaskStatus.FAILED)
            elif all(
                unit.status is LongTaskUnitStatus.COMPLETED for unit in required
            ):
                self._set_status(state, LongTaskStatus.COMPLETED)
            elif any(
                unit.status is LongTaskUnitStatus.BLOCKED for unit in required
            ) and not self._has_runnable_work(state):
                # A retryable exhausted Unit is a checkpoint.  Keep it
                # blocked so resume can continue from the same durable input.
                self._set_status(state, LongTaskStatus.PAUSED)
            elif not self._has_runnable_work(state):
                stranded = next(
                    (
                        unit
                        for unit in required
                        if unit.status is not LongTaskUnitStatus.COMPLETED
                    ),
                    None,
                )
                if stranded is not None:
                    state.units[stranded.id] = replace(
                        stranded,
                        status=LongTaskUnitStatus.FAILED,
                        worker_id=None,
                        lease_expires_at_ms=None,
                        error_code=(
                            stranded.error_code or "long_task_no_runnable_work"
                        ),
                    )
                    self._fail_task(state, stranded.id)
            return state.record

    async def recover_after_restart(
        self,
        *,
        reason_code: str = "execution_recovery_after_restart",
    ) -> Sequence[str]:
        normalized_reason = _required(reason_code, "restart recovery reason")
        async with self._lock:
            recovered: list[str] = []
            now_ms = int(time.time() * 1000)
            for state in self._state.tasks.values():
                if state.record.status is not LongTaskStatus.RUNNING:
                    continue
                if any(
                    unit.status in {
                        LongTaskUnitStatus.CLAIMED,
                        LongTaskUnitStatus.RUNNING,
                    }
                    and unit.lease_expires_at_ms is not None
                    and unit.lease_expires_at_ms > now_ms
                    for unit in state.units.values()
                ):
                    continue
                recovered.append(state.record.id)
                if state.record.cancellation_requested_at_ms is not None:
                    self._cancel(state)
                else:
                    self._release_active_units(state, normalized_reason)
                    self._set_status(state, LongTaskStatus.PAUSED)
            return tuple(recovered)

    def _require_state(self, task_id: str) -> StoredLongTask:
        try:
            return self._state.tasks[_required(task_id, "long task id")]
        except KeyError as error:
            raise LookupError("long task does not exist") from error

    @staticmethod
    def _require_unit(
        state: StoredLongTask,
        unit_id: str,
    ) -> LongTaskUnitRecord:
        try:
            return state.units[_required(unit_id, "long task unit id")]
        except KeyError as error:
            raise LookupError("long task unit does not exist") from error

    def _require_unit_claim(
        self,
        state: StoredLongTask,
        unit: LongTaskUnitRecord,
        worker_id: str,
        lease_epoch: int,
    ) -> None:
        if self._deadline_elapsed(state):
            self._expire_deadline(state)
            raise ContractViolationError(
                "long task deadline was exceeded",
                code="long_task_deadline_exceeded",
                details={"taskId": state.record.id},
            )
        worker = _required(worker_id, "long task worker id")
        if (
            state.record.status is not LongTaskStatus.RUNNING
            or unit.status not in {
                LongTaskUnitStatus.CLAIMED,
                LongTaskUnitStatus.RUNNING,
            }
            or unit.worker_id != worker
            or unit.lease_epoch != int(lease_epoch)
            or unit.lease_expires_at_ms is None
            or unit.lease_expires_at_ms <= self._clock_ms()
        ):
            self._raise_lease_lost(unit)

    @staticmethod
    def _raise_lease_lost(unit: LongTaskUnitRecord) -> None:
        raise ContractViolationError(
            "long task unit lease authority was lost",
            code="long_task_unit_lease_lost",
            details={"taskId": unit.task_id, "unitId": unit.id},
        )

    @staticmethod
    def _require_running(state: StoredLongTask) -> None:
        if state.record.status is not LongTaskStatus.RUNNING:
            raise ValueError("long task is not running")

    @staticmethod
    def _require_task_revision(state: StoredLongTask, expected: int) -> None:
        if state.record.revision != int(expected):
            raise ValueError("long task revision conflicts")

    @staticmethod
    def _unit_from_spec(
        task_id: str,
        spec: LongTaskUnitSpec,
        timestamp: str,
    ) -> LongTaskUnitRecord:
        return LongTaskUnitRecord(
            task_id=task_id,
            id=spec.id,
            position=spec.position,
            status=LongTaskUnitStatus.PENDING,
            semantic_key=spec.semantic_key,
            dependencies=spec.dependencies,
            parent_unit_id=spec.parent_unit_id,
            required=spec.required,
            max_attempts=spec.max_attempts,
            input_ref=spec.input_ref,
            metadata=spec.metadata,
            create_time=timestamp,
            update_time=timestamp,
        )

    def _touch(self, state: StoredLongTask) -> None:
        state.record = replace(
            state.record,
            revision=state.record.revision + 1,
            update_time=_timestamp(),
        )

    def _set_status(
        self,
        state: StoredLongTask,
        status: LongTaskStatus,
    ) -> None:
        if state.record.status is status:
            return
        state.record = replace(state.record, status=status)
        self._touch(state)

    def _refresh_totals(self, state: StoredLongTask) -> None:
        required = tuple(unit for unit in state.units.values() if unit.required)
        state.record = replace(
            state.record,
            total_units=len(required),
            completed_units=sum(
                unit.status is LongTaskUnitStatus.COMPLETED for unit in required
            ),
            failed_units=sum(
                unit.status is LongTaskUnitStatus.FAILED for unit in required
            ),
        )
        self._touch(state)

    def _cancel(self, state: StoredLongTask) -> None:
        for unit_id, unit in tuple(state.units.items()):
            if not unit.status.terminal:
                state.units[unit_id] = replace(
                    unit,
                    status=LongTaskUnitStatus.CANCELED,
                    worker_id=None,
                    lease_expires_at_ms=None,
                )
        self._set_status(state, LongTaskStatus.CANCELED)

    def _fail_task(self, state: StoredLongTask, failed_unit_id: str) -> None:
        for unit_id, unit in tuple(state.units.items()):
            if unit_id != failed_unit_id and not unit.status.terminal:
                state.units[unit_id] = replace(
                    unit,
                    status=LongTaskUnitStatus.CANCELED,
                    worker_id=None,
                    lease_expires_at_ms=None,
                    error_code=unit.error_code or "task_failed_dependency",
                )
        self._refresh_totals(state)
        self._set_status(state, LongTaskStatus.FAILED)

    def _release_active_units(
        self,
        state: StoredLongTask,
        reason_code: str,
    ) -> None:
        for unit_id, unit in tuple(state.units.items()):
            if unit.status in {
                LongTaskUnitStatus.CLAIMED,
                LongTaskUnitStatus.RUNNING,
            }:
                state.units[unit_id] = replace(
                    unit,
                    status=LongTaskUnitStatus.PENDING,
                    max_attempts=unit.max_attempts + 1,
                    worker_id=None,
                    lease_expires_at_ms=None,
                    error_code=reason_code,
                )

    def _has_runnable_work(self, state: StoredLongTask) -> bool:
        completed = {
            unit.id
            for unit in state.units.values()
            if unit.status is LongTaskUnitStatus.COMPLETED
        }
        now = self._clock_ms()
        return any(
            (
                unit.status in {
                    LongTaskUnitStatus.CLAIMED,
                    LongTaskUnitStatus.RUNNING,
                }
                and (unit.lease_expires_at_ms or 0) > now
            )
            or (
                unit.status in {
                    LongTaskUnitStatus.PENDING,
                    LongTaskUnitStatus.WAITING_RETRY,
                }
                and unit.attempt < unit.max_attempts
                and set(unit.dependencies).issubset(completed)
            )
            for unit in state.units.values()
        )

    def _find_active_state(
        self,
        namespace: str,
        owner_id: str,
        kind: str,
        session_id: object,
        match_session: bool,
    ) -> StoredLongTask | None:
        expected_session = "" if session_id is None else str(session_id)
        return next((
            state
            for state in reversed(tuple(self._state.tasks.values()))
            if state.record.namespace == namespace
            and state.record.owner_id == owner_id
            and state.record.kind == kind
            and state.record.status in {
                LongTaskStatus.PENDING,
                LongTaskStatus.RUNNING,
                LongTaskStatus.PAUSED,
            }
            and (
                not match_session
                or str(state.record.metadata.get("sessionId") or "")
                == expected_session
            )
        ), None)

    @staticmethod
    def _matches_create(
        state: StoredLongTask,
        command: LongTaskCreateCommand,
    ) -> bool:
        record = state.record
        units = tuple(sorted(state.units.values(), key=lambda unit: unit.position))
        specs = tuple(sorted(command.units, key=lambda unit: unit.position))
        return (
            record.namespace == command.namespace
            and record.kind == command.kind
            and record.owner_id == command.owner_id
            and record.created_by_run_id == command.created_by_run_id
            and record.max_parallelism == command.max_parallelism
            and record.deadline_at_ms == command.deadline_at_ms
            and record.budget_limits == command.budget_limits
            and thaw_json_mapping(record.metadata)
            == thaw_json_mapping(command.metadata)
            and len(units) == len(specs)
            and all(
                unit.id == spec.id
                and unit.position == spec.position
                and unit.semantic_key == spec.semantic_key
                and unit.dependencies == spec.dependencies
                and unit.required == spec.required
                and unit.input_ref == spec.input_ref
                and unit.max_attempts == spec.max_attempts
                and thaw_json_mapping(unit.metadata)
                == thaw_json_mapping(spec.metadata)
                for unit, spec in zip(units, specs)
            )
        )

    @staticmethod
    def _require_acyclic(units: Mapping[str, LongTaskUnitRecord]) -> None:
        try:
            tuple(TopologicalSorter({
                unit.id: unit.dependencies for unit in units.values()
            }).static_order())
        except CycleError as error:
            raise ValueError("long task dependencies contain a cycle") from error

    def _deadline_elapsed(self, state: StoredLongTask) -> bool:
        deadline = state.record.deadline_at_ms
        return deadline is not None and deadline <= self._clock_ms()

    def _expire_deadline(self, state: StoredLongTask) -> None:
        for unit_id, unit in tuple(state.units.items()):
            if not unit.status.terminal:
                state.units[unit_id] = replace(
                    unit,
                    status=LongTaskUnitStatus.FAILED,
                    worker_id=None,
                    lease_expires_at_ms=None,
                    error_code="long_task_deadline_exceeded",
                )
        self._refresh_totals(state)
        self._set_status(state, LongTaskStatus.FAILED)

    @staticmethod
    def _task_budget_exhaustion(
        state: StoredLongTask,
        *,
        exceeded_only: bool = False,
    ) -> str | None:
        usage = state.record.usage
        limits: LongTaskBudgetLimits = state.record.budget_limits
        if usage.unreported_usage_attempts and any(
            limit is not None
            for limit in (
                limits.max_input_tokens,
                limits.max_run_generation_tokens,
                limits.max_reasoning_tokens,
            )
        ):
            return "provider_usage_unreported"
        if limits.max_reasoning_tokens is not None and usage.reasoning_tokens is None:
            return "reasoning_tokens_unreported"
        for kind, value, limit in (
            ("model_attempts", usage.invocation_count, limits.max_invocation_attempts),
            ("input_tokens", usage.input_tokens, limits.max_input_tokens),
            (
                "generation_tokens",
                usage.generation_tokens,
                limits.max_run_generation_tokens,
            ),
            ("reasoning_tokens", usage.reasoning_tokens, limits.max_reasoning_tokens),
        ):
            if limit is not None and value is not None and (
                value > limit or (not exceeded_only and value >= limit)
            ):
                return kind
        return None

    def _fail_budget(self, state: StoredLongTask, budget_kind: str) -> None:
        if (
            state.record.budget_exhaustion_disposition
            is BudgetExhaustionDisposition.PAUSE_RECOVERABLE
        ):
            for unit_id, unit in tuple(state.units.items()):
                if not unit.status.terminal:
                    state.units[unit_id] = replace(
                        unit,
                        status=LongTaskUnitStatus.BLOCKED,
                        worker_id=None,
                        lease_expires_at_ms=None,
                        disposition=FailureDisposition.PAUSE_RECOVERABLE,
                        error_code="runtime_budget_exceeded",
                        metadata={
                            **thaw_json_mapping(unit.metadata),
                            "budgetKind": budget_kind,
                        },
                    )
            self._refresh_totals(state)
            self._set_status(state, LongTaskStatus.PAUSED)
            return
        for unit_id, unit in tuple(state.units.items()):
            if not unit.status.terminal:
                state.units[unit_id] = replace(
                    unit,
                    status=LongTaskUnitStatus.FAILED,
                    worker_id=None,
                    lease_expires_at_ms=None,
                    error_code="runtime_budget_exceeded",
                    metadata={
                        **thaw_json_mapping(unit.metadata),
                        "budgetKind": budget_kind,
                    },
                )
        self._refresh_totals(state)
        self._set_status(state, LongTaskStatus.FAILED)


class InMemoryDurableAdapters:
    """Ready-to-use process-local durable stores for examples and tests."""

    def __init__(
        self,
        *,
        clock_ms: Callable[[], int] = _wall_time_ms,
        run_is_available: Callable[[str], bool] | None = None,
        artifact_state: ArtifactState | None = None,
        task_state: LongTaskState | None = None,
    ) -> None:
        artifacts = InMemoryArtifactStore(
            clock_ms=clock_ms,
            state=artifact_state,
            run_is_available=run_is_available,
        )
        self.artifacts = artifacts
        self.artifact_claims = artifacts
        self.artifact_maintenance = artifacts
        self.long_tasks: LongTaskRepository = InMemoryLongTaskRepository(
            clock_ms=clock_ms, state=task_state
        )


__all__ = [
    "InMemoryArtifactStore",
    "InMemoryDurableAdapters",
    "InMemoryLongTaskRepository",
]
