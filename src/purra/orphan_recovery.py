"""Storage-neutral orchestration for abandoned Agent Runs."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import replace
from time import time

from purra.contracts import RunStatus
from purra.long_tasks.contracts import LongTaskStatus
from purra.long_tasks.ports import LongTaskRepository
from purra.ports.persistence import RunControlStore
from purra.run_control import (
    OrphanRunCandidate,
    OrphanRunDecision,
    OrphanRunDisposition,
    decide_orphan_run,
)


OrphanRunSettlement = Callable[[OrphanRunDecision, bool], Awaitable[None]]


class OrphanRecoveryCoordinator:
    """Claim abandoned Runs and preserve resumable durable work."""

    def __init__(
        self,
        *,
        control: RunControlStore,
        long_tasks: LongTaskRepository,
        owner_id: str,
        settle: OrphanRunSettlement,
        lease_duration_ms: int = 30_000,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._control = control
        self._long_tasks = long_tasks
        self._owner_id = str(owner_id or "").strip()
        if not self._owner_id:
            raise ValueError("orphan recovery owner id is required")
        self._settle = settle
        self._lease_duration_ms = int(lease_duration_ms)
        if self._lease_duration_ms <= 0:
            raise ValueError("orphan recovery lease duration must be positive")
        self._clock_ms = clock_ms or (lambda: int(time() * 1_000))

    async def recover(
        self,
        *,
        timestamp_ms: int | None = None,
        after_restart: bool = False,
    ) -> tuple[str, ...]:
        checked_at = (
            self._clock_ms() if timestamp_ms is None else int(timestamp_ms)
        )
        candidates = await self._control.list_orphans(
            timestamp_ms=checked_at,
            after_restart=after_restart,
        )
        priority = {
            OrphanRunDisposition.CANCEL: 0,
            OrphanRunDisposition.PAUSE_RECOVERABLE: 1,
            OrphanRunDisposition.FAIL: 2,
            OrphanRunDisposition.DEFER_ACTIVE: 3,
        }
        decisions = sorted(
            ((item, decide_orphan_run(item)) for item in candidates),
            key=lambda item: (priority[item[1].disposition], item[0].run_id),
        )
        recovered: list[str] = []
        for candidate, decision in decisions:
            if decision.disposition is OrphanRunDisposition.DEFER_ACTIVE:
                continue
            if await self._recover_one(
                candidate,
                decision,
                checked_at=checked_at,
                after_restart=after_restart,
            ):
                recovered.append(candidate.run_id)
        return tuple(recovered)

    async def _recover_one(
        self,
        candidate: OrphanRunCandidate,
        decision: OrphanRunDecision,
        *,
        checked_at: int,
        after_restart: bool,
    ) -> bool:
        claimed = await self._control.claim_orphan(
            candidate,
            self._owner_id,
            lease_duration_ms=self._lease_duration_ms,
            timestamp_ms=checked_at,
            after_restart=after_restart,
        )
        if not claimed:
            return False
        current = await self._control.get(candidate.run_id)
        if (
            current is None
            or current.status is not RunStatus.RUNNING
            or current.owner_id != self._owner_id
        ):
            return False
        if (
            current.cancellation_requested_at_ms is not None
            and decision.disposition is not OrphanRunDisposition.CANCEL
        ):
            decision = decide_orphan_run(replace(
                candidate,
                cancellation_requested_at_ms=(
                    current.cancellation_requested_at_ms
                ),
            ))
        try:
            if (
                decision.disposition is OrphanRunDisposition.PAUSE_RECOVERABLE
                and not await self._pause_recoverable_tasks(decision)
            ):
                await self._control.release(candidate.run_id, self._owner_id)
                return False
            await self._settle(decision, after_restart)
        except BaseException:
            await self._control.release(candidate.run_id, self._owner_id)
            raise
        return True

    async def _pause_recoverable_tasks(
        self,
        decision: OrphanRunDecision,
    ) -> bool:
        for evidence in decision.recoverable_tasks:
            task = await self._long_tasks.load(evidence.task_id)
            if task is None or task.status.terminal:
                return False
            try:
                paused = await self._long_tasks.pause(
                    evidence.task_id,
                    expected_revision=evidence.revision,
                    reason_code=decision.reason.value,
                )
            except ValueError:
                return False
            if paused.status is not LongTaskStatus.PAUSED:
                return False
        return True


__all__ = ["OrphanRecoveryCoordinator", "OrphanRunSettlement"]
