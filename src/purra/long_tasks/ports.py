"""Persistence and execution ports for durable tasks."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from purra.contracts import SessionId
from purra.long_tasks.contracts import (
    LongTaskCreateCommand,
    LongTaskRecord,
    LongTaskRunBinding,
    LongTaskRunRelation,
    LongTaskSplitResult,
    LongTaskUnitRecord,
    LongTaskUnitResult,
    LongTaskUsage,
)
from purra.ports import CancellationSignal
from purra.recovery import FailureDecision, FailureSignal


@runtime_checkable
class LongTaskRepository(Protocol):
    """Durable task store.

    ``create`` atomically persists the task, its units and the CREATED binding
    for ``created_by_run_id``.
    """

    async def create(
        self,
        task_id: str,
        command: LongTaskCreateCommand,
    ) -> LongTaskRecord: ...

    async def load(self, task_id: str) -> LongTaskRecord | None: ...

    async def bind_run(
        self,
        task_id: str,
        run_id: str,
        *,
        relation: LongTaskRunRelation,
    ) -> LongTaskRunBinding:
        """Append idempotently; an existing relation cannot be rewritten."""
        ...

    async def list_run_bindings(
        self,
        task_id: str,
    ) -> Sequence[LongTaskRunBinding]: ...

    async def list_for_owner(
        self,
        *,
        namespace: str,
        owner_id: str,
        kind: str | None = None,
        limit: int = 20,
    ) -> Sequence[LongTaskRecord]: ...

    async def find_active(
        self,
        *,
        namespace: str,
        owner_id: str,
        kind: str,
        session_id: SessionId | None = None,
        match_session: bool = False,
    ) -> LongTaskRecord | None: ...

    async def list_units(self, task_id: str) -> Sequence[LongTaskUnitRecord]: ...

    async def record_usage(
        self,
        task_id: str,
        *,
        run_id: str,
        usage: LongTaskUsage,
        expected_revision: int,
    ) -> LongTaskRecord: ...

    async def start(self, task_id: str, *, expected_revision: int) -> LongTaskRecord: ...

    async def claim_ready_unit(
        self,
        task_id: str,
        *,
        worker_id: str,
        lease_duration_ms: int,
    ) -> LongTaskUnitRecord | None: ...

    async def expire_deadline(self, task_id: str) -> LongTaskRecord: ...

    async def bind_unit_run(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        lease_epoch: int,
        run_id: str,
    ) -> LongTaskUnitRecord: ...

    async def renew_unit_lease(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        lease_epoch: int,
        lease_duration_ms: int,
    ) -> LongTaskUnitRecord: ...

    async def update_unit_progress(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        lease_epoch: int,
        metadata: Mapping[str, Any],
    ) -> LongTaskUnitRecord: ...

    async def complete_unit(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        lease_epoch: int,
        result: LongTaskUnitResult,
    ) -> LongTaskRecord: ...

    async def settle_unit_failure(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        lease_epoch: int,
        decision: FailureDecision,
    ) -> LongTaskRecord: ...

    async def expand_unit(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        lease_epoch: int,
        split: LongTaskSplitResult,
        decision: FailureDecision,
    ) -> LongTaskRecord: ...

    async def interrupt_unit(
        self,
        task_id: str,
        unit_id: str,
        *,
        worker_id: str,
        lease_epoch: int,
        reason_code: str,
    ) -> LongTaskRecord: ...

    async def pause(
        self,
        task_id: str,
        *,
        expected_revision: int | None = None,
        reason_code: str | None = None,
    ) -> LongTaskRecord: ...

    async def resume(
        self,
        task_id: str,
        *,
        additional_attempts: int = 0,
    ) -> LongTaskRecord: ...

    async def cancel(self, task_id: str) -> LongTaskRecord: ...

    async def request_cancel(
        self,
        task_id: str,
        *,
        requested_at_ms: int | None = None,
    ) -> LongTaskRecord: ...

    async def finalize_if_complete(self, task_id: str) -> LongTaskRecord: ...

    async def recover_after_restart(
        self,
        *,
        reason_code: str = "execution_recovery_after_restart",
    ) -> Sequence[str]:
        """Pause running tasks and release process-owned unit leases."""
        ...


@runtime_checkable
class LongTaskUnitRunner(Protocol):
    async def run_unit(
        self,
        task: LongTaskRecord,
        unit: LongTaskUnitRecord,
        signal: CancellationSignal | None = None,
    ) -> LongTaskUnitResult: ...

    def classify_unit_failure(
        self,
        task: LongTaskRecord,
        unit: LongTaskUnitRecord,
        error: Exception,
    ) -> FailureSignal: ...

    def split_unit(
        self,
        task: LongTaskRecord,
        unit: LongTaskUnitRecord,
        error: Exception,
    ) -> LongTaskSplitResult: ...


__all__ = ["LongTaskRepository", "LongTaskUnitRunner"]
