"""Unit scheduling and settlement mixins for the long-task store."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace

from purra.errors import ContractViolationError
from purra.json_values import thaw_json_mapping
from purra.long_tasks.contracts import (
    LongTaskRecord,
    LongTaskSplitResult,
    LongTaskStatus,
    LongTaskUnitRecord,
    LongTaskUnitResult,
    LongTaskUnitStatus,
    LongTaskUsage,
)
from purra.recovery import (
    FailureDecision,
    FailureDisposition,
    FailureScope,
)

from ._store_kit import (
    required_text_field as _required,
    utc_timestamp as _timestamp,
)


class LongTaskUnitSchedulingMixin:
    """Unit claim and lease-renewal methods (shares state with the store)."""

    async def claim_ready_unit(
        self,
        task_id: str,
        *,
        worker_id: str,
        lease_duration_ms: int,
    ) -> LongTaskUnitRecord | None:
        return await self._claim_unit(task_id, worker_id=worker_id,
                                      lease_duration_ms=lease_duration_ms)

    async def claim_unit(
        self, task_id: str, unit_id: str, *, worker_id: str,
        lease_duration_ms: int,
    ) -> LongTaskUnitRecord | None:
        return await self._claim_unit(
            task_id, unit_id=_required(unit_id, "long task unit id"),
            worker_id=worker_id, lease_duration_ms=lease_duration_ms,
        )

    async def _claim_unit(
        self, task_id: str, *, worker_id: str, lease_duration_ms: int,
        unit_id: str | None = None,
    ) -> LongTaskUnitRecord | None:
        worker = _required(worker_id, "long task worker id")
        duration = int(lease_duration_ms)
        if duration <= 0:
            raise ValueError("long task lease duration must be positive")
        async with self._lock:
            state = self._require_state(task_id)
            if self._deadline_elapsed(state):
                self._expire_deadline(state)
                return None
            budget_kind = self._task_budget_exhaustion(state)
            if budget_kind is not None:
                self._fail_budget(state, budget_kind)
                return None
            if (
                state.record.status is not LongTaskStatus.RUNNING
                or state.record.cancellation_requested_at_ms is not None
            ):
                return None
            now = self._clock_ms()
            normalized_expired = False
            failed_required_unit_id: str | None = None
            for expired_id, unit in tuple(state.units.items()):
                if (
                    unit.status in {
                        LongTaskUnitStatus.CLAIMED,
                        LongTaskUnitStatus.RUNNING,
                    }
                    and (unit.lease_expires_at_ms or 0) <= now
                    and unit.attempt >= unit.max_attempts
                ):
                    state.units[expired_id] = replace(
                        unit,
                        status=LongTaskUnitStatus.FAILED,
                        worker_id=None,
                        lease_expires_at_ms=None,
                        error_code="lease_expired_attempts_exhausted",
                    )
                    normalized_expired = True
                    if unit.required and failed_required_unit_id is None:
                        failed_required_unit_id = expired_id
            if normalized_expired:
                if failed_required_unit_id is not None:
                    self._fail_task(state, failed_required_unit_id)
                    return None
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
                if unit_id is not None and unit.id != unit_id:
                    continue
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
                    lease_epoch=unit.lease_epoch + 1,
                    lease_expires_at_ms=now + duration,
                    settled_by_worker_id=None,
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
        lease_epoch: int,
        run_id: str,
    ) -> LongTaskUnitRecord:
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            self._require_unit_claim(state, unit, worker_id, lease_epoch)
            normalized_run = _required(run_id, "long task unit Run id")
            if unit.run_id is not None:
                if unit.run_id != normalized_run:
                    raise ContractViolationError("A Unit attempt cannot change its Run", code="long_task_unit_run_conflict")
                return unit
            if any(other.id != unit.id and other.run_id == normalized_run for other in state.units.values()):
                raise ContractViolationError("A Run cannot belong to two task Units", code="long_task_unit_run_conflict")
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

    async def renew_unit_lease(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        lease_epoch: int,
        lease_duration_ms: int,
    ) -> LongTaskUnitRecord:
        duration = int(lease_duration_ms)
        if duration <= 0:
            raise ValueError("long task lease duration must be positive")
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            self._require_unit_claim(state, unit, worker_id, lease_epoch)
            renewed = replace(
                unit,
                lease_expires_at_ms=self._clock_ms() + duration,
            )
            state.units[unit.id] = renewed
            self._touch(state)
            return renewed


class LongTaskUnitSettlementMixin:
    """Unit progress, completion, and failure-settlement methods."""

    async def update_unit_progress(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        lease_epoch: int,
        metadata: Mapping[str, object],
    ) -> LongTaskUnitRecord:
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            self._require_unit_claim(state, unit, worker_id, lease_epoch)
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
        lease_epoch: int,
        result: LongTaskUnitResult,
    ) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            if unit.status is LongTaskUnitStatus.COMPLETED:
                if (
                    unit.lease_epoch == int(lease_epoch)
                    and unit.settled_by_worker_id
                    == _required(worker_id, "long task worker id")
                    and self._matches_result(unit, result)
                ):
                    return state.record
                self._raise_lease_lost(unit)
            self._require_unit_claim(state, unit, worker_id, lease_epoch)
            if unit.run_id is not None and result.run_id is not None and result.run_id != unit.run_id:
                raise ContractViolationError("Unit result belongs to a different Run", code="long_task_unit_run_conflict")
            selected_run = result.run_id or unit.run_id
            if selected_run is not None and any(other.id != unit.id and other.run_id == selected_run for other in state.units.values()):
                raise ContractViolationError("A Run cannot belong to two task Units", code="long_task_unit_run_conflict")
            state.units[unit.id] = replace(
                unit,
                status=LongTaskUnitStatus.COMPLETED,
                worker_id=None,
                lease_expires_at_ms=None,
                settled_by_worker_id=_required(worker_id, "long task worker id"),
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
        lease_epoch: int,
        decision: FailureDecision,
    ) -> LongTaskRecord:
        if not isinstance(decision, FailureDecision):
            raise TypeError("failure settlement requires FailureDecision")
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            self._require_unit_claim(state, unit, worker_id, lease_epoch)
            targets = {
                FailureDisposition.RETRY_ATTEMPT: LongTaskUnitStatus.WAITING_RETRY,
                FailureDisposition.RESUME_CHECKPOINT: LongTaskUnitStatus.WAITING_RETRY,
                FailureDisposition.PAUSE_RECOVERABLE: LongTaskUnitStatus.BLOCKED,
                FailureDisposition.SPLIT_PART: LongTaskUnitStatus.NEEDS_SPLIT,
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
                self._release_active_units(state, decision.code)
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
        lease_epoch: int,
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
                if (
                    parent.lease_epoch == int(lease_epoch)
                    and parent.settled_by_worker_id
                    == _required(worker_id, "long task worker id")
                ):
                    return state.record
                self._raise_lease_lost(parent)
            self._require_unit_claim(state, parent, worker_id, lease_epoch)
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
                settled_by_worker_id=_required(worker_id, "long task worker id"),
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
        lease_epoch: int,
        reason_code: str,
    ) -> LongTaskRecord:
        async with self._lock:
            state = self._require_state(task_id)
            unit = self._require_unit(state, unit_id)
            self._require_unit_claim(state, unit, worker_id, lease_epoch)
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

    @staticmethod
    def _matches_result(
        unit: LongTaskUnitRecord,
        result: LongTaskUnitResult,
    ) -> bool:
        return (
            unit.output_ref == result.output_ref
            and (result.run_id is None or unit.run_id == result.run_id)
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
            unreported_usage_attempts=sum(
                value.unreported_usage_attempts for value in values
            ),
            input_tokens=sum(value.input_tokens for value in values),
            generation_tokens=sum(value.generation_tokens for value in values),
            reasoning_tokens=(
                None
                if any(value.reasoning_tokens is None for value in values)
                else sum(int(value.reasoning_tokens or 0) for value in values)
            ),
        )
