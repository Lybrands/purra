"""Process-local reference stores for PurrA durable-state ports."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
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
from purra.json_values import thaw_json_mapping
from purra.long_tasks.contracts import (
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


def _wall_time_ms() -> int:
    return int(time.time() * 1000)


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required(value: object, name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{name} is required")
    return normalized


class InMemoryArtifactStore:
    """One cancellation-linearizable reference adapter for Artifact ports."""

    def __init__(
        self,
        *,
        clock_ms: Callable[[], int] = _wall_time_ms,
        run_is_available: Callable[[str], bool] | None = None,
    ) -> None:
        self._lock = asyncio.Lock()
        self._clock_ms = clock_ms
        self._run_is_available = run_is_available or (lambda _run_id: True)
        self._artifacts: dict[str, ArtifactRecord] = {}
        self._owners: dict[tuple[str, str, str, str, str], str] = {}
        self._batches: dict[str, list[ArtifactBatch]] = {}
        self._receipts: dict[tuple[str, str], ArtifactBatchReceipt] = {}
        self._receipt_digests: dict[tuple[str, str], str] = {}
        self._claims: dict[str, ArtifactWriteClaim] = {}
        self._updated_at_ms: dict[str, int] = {}

    async def create(
        self,
        artifact_id: str,
        command: ArtifactCreateCommand,
    ) -> ArtifactRecord:
        normalized_id = _required(artifact_id, "artifact id")
        async with self._lock:
            existing = self._artifacts.get(normalized_id)
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
            owned_id = self._owners.get(owner_key)
            if owned_id is not None:
                owned = self._artifacts[owned_id]
                if self._matches_artifact_create(owned, command):
                    return owned
                raise ArtifactConflictError(
                    "artifact owner identity conflicts",
                    code="artifact_owner_conflict",
                )
            self._artifacts[normalized_id] = candidate
            self._owners[owner_key] = normalized_id
            self._batches[normalized_id] = []
            self._updated_at_ms[normalized_id] = self._clock_ms()
            return candidate

    async def load(self, artifact_id: str) -> ArtifactRecord | None:
        async with self._lock:
            return self._artifacts.get(str(artifact_id or "").strip())

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
            artifact_id = self._owners.get(key)
            return self._artifacts.get(artifact_id) if artifact_id else None

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
            self._batches[artifact.id].append(batch)
            receipt_key = (artifact.id, command.idempotency_key)
            self._receipts[receipt_key] = receipt
            self._receipt_digests[receipt_key] = command.content_digest
            self._artifacts[artifact.id] = replace(
                artifact,
                revision=committed_revision,
                next_sequence=command.sequence + 1,
                committed_item_count=(
                    artifact.committed_item_count + len(command.items)
                ),
            )
            self._claims[artifact.id] = replace(
                claim,
                acquired_revision=committed_revision,
                expires_at_ms=(
                    self._clock_ms() + command.write_lease.lease_duration_ms
                ),
            )
            self._updated_at_ms[artifact.id] = self._clock_ms()
            return receipt

    async def list_batches(self, artifact_id: str) -> Sequence[ArtifactBatch]:
        async with self._lock:
            self._require_artifact(artifact_id)
            return tuple(self._batches.get(artifact_id, ()))

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
            self._artifacts[artifact.id] = finalized
            self._claims.pop(artifact.id, None)
            self._updated_at_ms[artifact.id] = self._clock_ms()
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
            self._artifacts[artifact.id] = aborted
            self._claims.pop(artifact.id, None)
            self._updated_at_ms[artifact.id] = self._clock_ms()
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
            self._claims[artifact.id] = claim
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
            self._claims[artifact.id] = renewed
            return renewed

    async def release(self, command: ArtifactClaimLeaseCommand) -> bool:
        if command.lease_duration_ms is not None:
            raise ValueError("claim release cannot include lease_duration_ms")
        async with self._lock:
            claim = self._claims.get(command.artifact_id)
            if claim is None or (
                claim.run_id != command.run_id
                or claim.claim_token != command.claim_token
            ):
                return False
            self._claims.pop(command.artifact_id, None)
            return True

    async def release_for_run(self, run_id: str) -> int:
        normalized = _required(run_id, "claim Run id")
        async with self._lock:
            targets = [
                artifact_id
                for artifact_id, claim in self._claims.items()
                if claim.run_id == normalized
            ]
            for artifact_id in targets:
                self._claims.pop(artifact_id, None)
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
            for artifact_id, claim in tuple(self._claims.items()):
                artifact = self._artifacts.get(artifact_id)
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
                self._claims.pop(artifact_id, None)
            purged = 0
            if policy.terminal_retention_ms is not None:
                cutoff = now - policy.terminal_retention_ms
                candidates = sorted(
                    (
                        updated,
                        artifact_id,
                    )
                    for artifact_id, updated in self._updated_at_ms.items()
                    if self._artifacts[artifact_id].status is not ArtifactStatus.OPEN
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
                for artifact in self._artifacts.values()
                if normalized_run is None
                or artifact.created_by_run_id == normalized_run
                or (
                    (claim := self._claims.get(artifact.id)) is not None
                    and claim.run_id == normalized_run
                )
            )
            claims = tuple(
                claim
                for claim in self._claims.values()
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
                    (artifact := self._artifacts.get(claim.artifact_id)) is None
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
            return self._artifacts[_required(artifact_id, "artifact id")]
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
        claim = self._claims.get(artifact_id)
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
        receipt = self._receipts.get(key)
        if receipt is None:
            return None
        if self._receipt_digests[key] != command.content_digest:
            raise ArtifactConflictError(
                "artifact idempotency key conflicts",
                code="artifact_idempotency_conflict",
            )
        return replace(receipt, replayed=True)

    def _purge_artifact(self, artifact_id: str) -> None:
        artifact = self._artifacts.pop(artifact_id)
        self._owners.pop((
            artifact.namespace,
            artifact.kind,
            artifact.owner_id,
            artifact.owner_ref.kind,
            artifact.owner_ref.id,
        ), None)
        self._batches.pop(artifact_id, None)
        self._claims.pop(artifact_id, None)
        self._updated_at_ms.pop(artifact_id, None)
        for key in tuple(self._receipts):
            if key[0] == artifact_id:
                self._receipts.pop(key, None)
                self._receipt_digests.pop(key, None)


@dataclass(slots=True)
class _LongTaskState:
    record: LongTaskRecord
    units: dict[str, LongTaskUnitRecord]
    bindings: dict[str, LongTaskRunBinding]
    usage_by_run: dict[str, LongTaskUsage]


class InMemoryLongTaskRepository:
    """Process-local executable specification of ``LongTaskRepository``."""

    def __init__(self, *, clock_ms: Callable[[], int] = _wall_time_ms) -> None:
        self._lock = asyncio.Lock()
        self._clock_ms = clock_ms
        self._tasks: dict[str, _LongTaskState] = {}

    async def create(
        self,
        task_id: str,
        command: LongTaskCreateCommand,
    ) -> LongTaskRecord:
        normalized_id = _required(task_id, "long task id")
        async with self._lock:
            existing = self._tasks.get(normalized_id)
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
            self._tasks[record.id] = _LongTaskState(
                record=record,
                units=units,
                bindings={binding.run_id: binding},
                usage_by_run={},
            )
            return record

    async def load(self, task_id: str) -> LongTaskRecord | None:
        async with self._lock:
            state = self._tasks.get(str(task_id or "").strip())
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
                for state in reversed(tuple(self._tasks.values()))
                if state.record.namespace == namespace
                and state.record.owner_id == owner_id
                and (normalized_kind is None or state.record.kind == normalized_kind)
            ]
            return tuple(records[:max(1, int(limit))])

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

    async def claim_ready_unit(
        self,
        task_id: str,
        *,
        worker_id: str,
        lease_duration_ms: int,
    ) -> LongTaskUnitRecord | None:
        worker = _required(worker_id, "long task worker id")
        duration = int(lease_duration_ms)
        if duration <= 0:
            raise ValueError("long task lease duration must be positive")
        async with self._lock:
            state = self._require_state(task_id)
            if (
                state.record.status is not LongTaskStatus.RUNNING
                or state.record.cancellation_requested_at_ms is not None
            ):
                return None
            now = self._clock_ms()
            normalized_expired = False
            for unit_id, unit in tuple(state.units.items()):
                if (
                    unit.status in {
                        LongTaskUnitStatus.CLAIMED,
                        LongTaskUnitStatus.RUNNING,
                    }
                    and (unit.lease_expires_at_ms or 0) <= now
                    and unit.attempt >= unit.max_attempts
                ):
                    state.units[unit_id] = replace(
                        unit,
                        status=LongTaskUnitStatus.BLOCKED,
                        worker_id=None,
                        lease_expires_at_ms=None,
                        error_code="lease_expired_attempts_exhausted",
                    )
                    normalized_expired = True
            if normalized_expired:
                self._touch(state)
            active = sum(
                unit.status in {
                    LongTaskUnitStatus.CLAIMED,
                    LongTaskUnitStatus.RUNNING,
                }
                and (unit.lease_expires_at_ms or 0) > now
                for unit in state.units.values()
            )
            if active >= state.record.max_parallelism:
                return None
            completed = {
                unit.id
                for unit in state.units.values()
                if unit.status is LongTaskUnitStatus.COMPLETED
            }
            candidates = sorted(state.units.values(), key=lambda unit: unit.position)
            for unit in candidates:
                eligible_status = unit.status in {
                    LongTaskUnitStatus.PENDING,
                    LongTaskUnitStatus.WAITING_RETRY,
                } or (
                    unit.status in {
                        LongTaskUnitStatus.CLAIMED,
                        LongTaskUnitStatus.RUNNING,
                    }
                    and (unit.lease_expires_at_ms or 0) <= now
                )
                if (
                    not eligible_status
                    or unit.attempt >= unit.max_attempts
                    or not set(unit.dependencies).issubset(completed)
                ):
                    continue
                claimed = replace(
                    unit,
                    status=LongTaskUnitStatus.CLAIMED,
                    attempt=unit.attempt + 1,
                    worker_id=worker,
                    lease_expires_at_ms=now + duration,
                    run_id=None,
                )
                state.units[unit.id] = claimed
                self._touch(state)
                return claimed
            return None

    async def bind_unit_run(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        run_id: str,
    ) -> LongTaskUnitRecord:
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            self._require_worker(unit, worker_id)
            self._require_active_unit(unit)
            normalized_run = _required(run_id, "long task unit Run id")
            history = list(thaw_json_mapping(unit.metadata).get("runHistory") or ())
            if not any(item.get("runId") == normalized_run for item in history):
                history.append({"attempt": unit.attempt, "runId": normalized_run})
            metadata = thaw_json_mapping(unit.metadata)
            metadata["runHistory"] = history[-8:]
            bound = replace(
                unit,
                status=LongTaskUnitStatus.RUNNING,
                run_id=normalized_run,
                metadata=metadata,
            )
            state.units[unit.id] = bound
            self._touch(state)
            return bound

    async def update_unit_progress(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        metadata: Mapping[str, object],
    ) -> LongTaskUnitRecord:
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            self._require_worker(unit, worker_id)
            self._require_active_unit(unit)
            updated = replace(
                unit,
                metadata={**thaw_json_mapping(unit.metadata), **dict(metadata)},
            )
            state.units[unit.id] = updated
            self._touch(state)
            return updated

    async def complete_unit(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        result: LongTaskUnitResult,
    ) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            if unit.status is LongTaskUnitStatus.COMPLETED:
                if self._matches_result(unit, result):
                    return state.record
                raise ValueError("long task unit completion conflicts")
            self._require_running(state)
            self._require_worker(unit, worker_id)
            self._require_active_unit(unit)
            state.units[unit.id] = replace(
                unit,
                status=LongTaskUnitStatus.COMPLETED,
                worker_id=None,
                lease_expires_at_ms=None,
                run_id=result.run_id or unit.run_id,
                output_ref=result.output_ref,
                artifact_digest=result.artifact_digest,
                validation_receipt=result.validation_receipt,
                failure={},
                disposition=None,
                error_code=None,
                metadata={
                    **thaw_json_mapping(unit.metadata),
                    **thaw_json_mapping(result.metadata),
                },
            )
            self._refresh_totals(state)
            return state.record

    async def settle_unit_failure(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        decision: FailureDecision,
    ) -> LongTaskRecord:
        if not isinstance(decision, FailureDecision):
            raise TypeError("failure settlement requires FailureDecision")
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            self._require_worker(unit, worker_id)
            self._require_active_unit(unit)
            targets = {
                FailureDisposition.RETRY_ATTEMPT: LongTaskUnitStatus.WAITING_RETRY,
                FailureDisposition.RESUME_CHECKPOINT: LongTaskUnitStatus.WAITING_RETRY,
                FailureDisposition.SPLIT_PART: LongTaskUnitStatus.NEEDS_SPLIT,
                FailureDisposition.PAUSE_RECOVERABLE: LongTaskUnitStatus.BLOCKED,
                FailureDisposition.CANCEL: LongTaskUnitStatus.CANCELED,
                FailureDisposition.FAIL_PERMANENT: LongTaskUnitStatus.FAILED,
            }
            target = targets[decision.disposition]
            state.units[unit.id] = replace(
                unit,
                status=target,
                worker_id=None,
                lease_expires_at_ms=None,
                error_code=decision.code,
                disposition=decision.disposition,
                failure=self._failure_payload(decision),
                max_attempts=(
                    unit.max_attempts + 1
                    if decision.disposition is FailureDisposition.RESUME_CHECKPOINT
                    and unit.attempt >= unit.max_attempts
                    else unit.max_attempts
                ),
            )
            if target is LongTaskUnitStatus.CANCELED:
                self._cancel(state)
            elif target is LongTaskUnitStatus.FAILED:
                self._fail_task(state, unit.id)
            elif target is LongTaskUnitStatus.BLOCKED and (
                decision.scope is FailureScope.SYSTEMIC
                or not self._has_runnable_work(state)
            ):
                self._set_status(state, LongTaskStatus.PAUSED)
            else:
                self._touch(state)
            return state.record

    async def expand_unit(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        split: LongTaskSplitResult,
        decision: FailureDecision,
    ) -> LongTaskRecord:
        if decision.disposition is not FailureDisposition.SPLIT_PART:
            raise ValueError("long task expansion requires split decision")
        if not split.children:
            raise ValueError("long task expansion requires children")
        async with self._lock:
            state = self._require_state(task_id)
            parent = self._require_unit(state, unit_id)
            if parent.status is LongTaskUnitStatus.EXPANDED:
                return state.record
            self._require_worker(parent, worker_id)
            self._require_active_unit(parent)
            ids = set(state.units)
            keys = {unit.semantic_key for unit in state.units.values()}
            positions = {unit.position for unit in state.units.values()}
            if ids.intersection(child.id for child in split.children):
                raise ValueError("split child id conflicts")
            if keys.intersection(child.semantic_key for child in split.children):
                raise ValueError("split child semantic key conflicts")
            if positions.intersection(child.position for child in split.children):
                raise ValueError("split child position conflicts")
            prospective = dict(state.units)
            timestamp = _timestamp()
            for child in split.children:
                dependencies = tuple(dict.fromkeys(
                    (*parent.dependencies, *child.dependencies)
                ))
                if parent.id in dependencies:
                    raise ValueError("split child cannot depend on parent")
                spec = replace(
                    child,
                    dependencies=dependencies,
                    parent_unit_id=child.parent_unit_id or parent.id,
                )
                prospective[child.id] = self._unit_from_spec(
                    state.record.id,
                    spec,
                    timestamp,
                )
            for downstream_id, downstream in tuple(prospective.items()):
                if downstream_id == parent.id or parent.id not in downstream.dependencies:
                    continue
                if not split.replacement_dependency_ids:
                    raise ValueError("split must replace downstream dependencies")
                dependencies = tuple(dict.fromkeys(
                    dependency
                    for current in downstream.dependencies
                    for dependency in (
                        split.replacement_dependency_ids
                        if current == parent.id
                        else (current,)
                    )
                ))
                prospective[downstream_id] = replace(
                    downstream,
                    dependencies=dependencies,
                )
            self._require_acyclic(prospective)
            prospective[parent.id] = replace(
                parent,
                status=LongTaskUnitStatus.EXPANDED,
                required=False,
                worker_id=None,
                lease_expires_at_ms=None,
                error_code=decision.code,
                disposition=decision.disposition,
                failure=self._failure_payload(decision),
            )
            state.units = prospective
            self._refresh_totals(state)
            return state.record

    async def interrupt_unit(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        reason_code: str,
    ) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            self._require_worker(unit, worker_id)
            self._require_active_unit(unit)
            state.units[unit.id] = replace(
                unit,
                status=LongTaskUnitStatus.PENDING,
                max_attempts=unit.max_attempts + 1,
                worker_id=None,
                lease_expires_at_ms=None,
                error_code=_required(reason_code, "interruption reason"),
            )
            self._set_status(state, LongTaskStatus.PAUSED)
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
            elif not self._has_runnable_work(state):
                self._set_status(state, LongTaskStatus.PAUSED)
            return state.record

    async def recover_after_restart(
        self,
        *,
        reason_code: str = "execution_recovery_after_restart",
    ) -> Sequence[str]:
        normalized_reason = _required(reason_code, "restart recovery reason")
        async with self._lock:
            recovered: list[str] = []
            for state in self._tasks.values():
                if state.record.status is not LongTaskStatus.RUNNING:
                    continue
                recovered.append(state.record.id)
                if state.record.cancellation_requested_at_ms is not None:
                    self._cancel(state)
                else:
                    self._release_active_units(state, normalized_reason)
                    self._set_status(state, LongTaskStatus.PAUSED)
            return tuple(recovered)

    def _require_state(self, task_id: str) -> _LongTaskState:
        try:
            return self._tasks[_required(task_id, "long task id")]
        except KeyError as error:
            raise LookupError("long task does not exist") from error

    @staticmethod
    def _require_unit(
        state: _LongTaskState,
        unit_id: str,
    ) -> LongTaskUnitRecord:
        try:
            return state.units[_required(unit_id, "long task unit id")]
        except KeyError as error:
            raise LookupError("long task unit does not exist") from error

    @staticmethod
    def _require_worker(unit: LongTaskUnitRecord, worker_id: str) -> None:
        if unit.worker_id != _required(worker_id, "long task worker id"):
            raise ValueError("long task unit worker conflicts")

    @staticmethod
    def _require_active_unit(unit: LongTaskUnitRecord) -> None:
        if unit.status not in {
            LongTaskUnitStatus.CLAIMED,
            LongTaskUnitStatus.RUNNING,
        }:
            raise ValueError("long task unit is not active")

    @staticmethod
    def _require_running(state: _LongTaskState) -> None:
        if state.record.status is not LongTaskStatus.RUNNING:
            raise ValueError("long task is not running")

    @staticmethod
    def _require_task_revision(state: _LongTaskState, expected: int) -> None:
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

    @staticmethod
    def _matches_result(
        unit: LongTaskUnitRecord,
        result: LongTaskUnitResult,
    ) -> bool:
        return (
            unit.output_ref == result.output_ref
            and unit.artifact_digest == result.artifact_digest
            and thaw_json_mapping(unit.validation_receipt)
            == thaw_json_mapping(result.validation_receipt)
        )

    @staticmethod
    def _failure_payload(decision: FailureDecision) -> dict[str, object]:
        return {
            "category": decision.category.value,
            "code": decision.code,
            "disposition": decision.disposition.value,
            "effectState": decision.effect_state.value,
            "scope": decision.scope.value,
        }

    @staticmethod
    def _aggregate_usage(values: Sequence[LongTaskUsage]) -> LongTaskUsage:
        values = tuple(values)
        return LongTaskUsage(
            invocation_count=sum(value.invocation_count for value in values),
            input_tokens=sum(value.input_tokens for value in values),
            output_tokens=sum(value.output_tokens for value in values),
            reasoning_tokens=(
                None
                if any(value.reasoning_tokens is None for value in values)
                else sum(int(value.reasoning_tokens or 0) for value in values)
            ),
        )

    def _touch(self, state: _LongTaskState) -> None:
        state.record = replace(
            state.record,
            revision=state.record.revision + 1,
            update_time=_timestamp(),
        )

    def _set_status(
        self,
        state: _LongTaskState,
        status: LongTaskStatus,
    ) -> None:
        if state.record.status is status:
            return
        state.record = replace(state.record, status=status)
        self._touch(state)

    def _refresh_totals(self, state: _LongTaskState) -> None:
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

    def _cancel(self, state: _LongTaskState) -> None:
        for unit_id, unit in tuple(state.units.items()):
            if not unit.status.terminal:
                state.units[unit_id] = replace(
                    unit,
                    status=LongTaskUnitStatus.CANCELED,
                    worker_id=None,
                    lease_expires_at_ms=None,
                )
        self._set_status(state, LongTaskStatus.CANCELED)

    def _fail_task(self, state: _LongTaskState, failed_unit_id: str) -> None:
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
        state: _LongTaskState,
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

    def _has_runnable_work(self, state: _LongTaskState) -> bool:
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
    ) -> _LongTaskState | None:
        expected_session = "" if session_id is None else str(session_id)
        return next((
            state
            for state in reversed(tuple(self._tasks.values()))
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
        state: _LongTaskState,
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


class InMemoryDurableAdapters:
    """Ready-to-use process-local durable stores for examples and tests."""

    def __init__(
        self,
        *,
        clock_ms: Callable[[], int] = _wall_time_ms,
        run_is_available: Callable[[str], bool] | None = None,
    ) -> None:
        artifacts = InMemoryArtifactStore(
            clock_ms=clock_ms,
            run_is_available=run_is_available,
        )
        self.artifacts = artifacts
        self.artifact_claims = artifacts
        self.artifact_maintenance = artifacts
        self.long_tasks: LongTaskRepository = InMemoryLongTaskRepository(
            clock_ms=clock_ms
        )


__all__ = [
    "InMemoryArtifactStore",
    "InMemoryDurableAdapters",
    "InMemoryLongTaskRepository",
]
