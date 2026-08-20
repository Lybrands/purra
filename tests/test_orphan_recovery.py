from types import SimpleNamespace

import pytest

from purra.contracts import RunExecutionLease, RunStatus
from purra.long_tasks.contracts import LongTaskStatus
from purra.orphan_recovery import OrphanRecoveryCoordinator
from purra.run_control import (
    OrphanRunCandidate,
    OrphanRunDisposition,
    OrphanTaskEvidence,
)


class _RunControl:
    def __init__(
        self,
        *candidates: OrphanRunCandidate,
        cancellation_after_claim_ms: int | None = None,
    ) -> None:
        self.candidates = tuple(candidates)
        self.cancellation_after_claim_ms = cancellation_after_claim_ms
        self.claimed: list[str] = []
        self.released: list[str] = []
        self.lease: RunExecutionLease | None = None

    async def list_orphans(self, **_kwargs):
        return self.candidates

    async def claim_orphan(self, candidate, owner_id, **_kwargs):
        self.claimed.append(candidate.run_id)
        self.lease = RunExecutionLease(
            run_id=candidate.run_id,
            status=RunStatus.RUNNING,
            owner_id=owner_id,
            cancellation_requested_at_ms=self.cancellation_after_claim_ms,
        )
        return True

    async def get(self, _run_id):
        return self.lease

    async def release(self, run_id, _owner_id):
        self.released.append(run_id)
        return True


class _LongTasks:
    def __init__(self, **revisions: int) -> None:
        self.tasks = {
            task_id: SimpleNamespace(
                status=LongTaskStatus.RUNNING,
                revision=revision,
            )
            for task_id, revision in revisions.items()
        }
        self.paused: list[tuple[str, int | None, str | None]] = []

    async def load(self, task_id):
        return self.tasks.get(task_id)

    async def pause(
        self,
        task_id,
        *,
        expected_revision=None,
        reason_code=None,
    ):
        task = self.tasks[task_id]
        if task.revision != expected_revision:
            raise ValueError("stale task revision")
        self.paused.append((task_id, expected_revision, reason_code))
        paused = SimpleNamespace(
            status=LongTaskStatus.PAUSED,
            revision=task.revision + 1,
        )
        self.tasks[task_id] = paused
        return paused


@pytest.mark.asyncio
async def test_recovery_defers_run_while_durable_task_is_active():
    control = _RunControl(OrphanRunCandidate(
        "run-1",
        active_tasks=(OrphanTaskEvidence("task-1", 2),),
    ))
    settled = []

    async def settle(decision, after_restart):
        settled.append((decision, after_restart))

    coordinator = OrphanRecoveryCoordinator(
        control=control,
        long_tasks=_LongTasks(),
        owner_id="worker-1",
        settle=settle,
        clock_ms=lambda: 100,
    )

    assert await coordinator.recover() == ()
    assert control.claimed == []
    assert settled == []


@pytest.mark.asyncio
async def test_recovery_pauses_durable_task_with_scanned_revision_then_settles():
    control = _RunControl(OrphanRunCandidate(
        "run-1",
        recoverable_tasks=(OrphanTaskEvidence("task-1", 2),),
    ))
    tasks = _LongTasks(**{"task-1": 2})
    settled = []

    async def settle(decision, after_restart):
        settled.append((decision, after_restart))

    coordinator = OrphanRecoveryCoordinator(
        control=control,
        long_tasks=tasks,
        owner_id="worker-1",
        settle=settle,
        clock_ms=lambda: 100,
    )

    assert await coordinator.recover(after_restart=True) == ("run-1",)
    assert tasks.paused == [
        ("task-1", 2, "durable_task_interrupted"),
    ]
    assert settled[0][0].disposition is (
        OrphanRunDisposition.PAUSE_RECOVERABLE
    )
    assert settled[0][1] is True


@pytest.mark.asyncio
async def test_recovery_rechecks_cancellation_after_claim():
    control = _RunControl(
        OrphanRunCandidate(
            "run-1",
            recoverable_tasks=(OrphanTaskEvidence("task-1", 2),),
        ),
        cancellation_after_claim_ms=101,
    )
    tasks = _LongTasks(**{"task-1": 2})
    settled = []

    async def settle(decision, _after_restart):
        settled.append(decision)

    coordinator = OrphanRecoveryCoordinator(
        control=control,
        long_tasks=tasks,
        owner_id="worker-1",
        settle=settle,
        clock_ms=lambda: 100,
    )

    assert await coordinator.recover() == ("run-1",)
    assert tasks.paused == []
    assert settled[0].disposition is OrphanRunDisposition.CANCEL


@pytest.mark.asyncio
async def test_recovery_releases_claim_when_task_revision_is_stale():
    control = _RunControl(OrphanRunCandidate(
        "run-1",
        recoverable_tasks=(OrphanTaskEvidence("task-1", 2),),
    ))
    tasks = _LongTasks(**{"task-1": 3})
    settled = []

    async def settle(decision, _after_restart):
        settled.append(decision)

    coordinator = OrphanRecoveryCoordinator(
        control=control,
        long_tasks=tasks,
        owner_id="worker-1",
        settle=settle,
        clock_ms=lambda: 100,
    )

    assert await coordinator.recover() == ()
    assert tasks.paused == []
    assert control.released == ["run-1"]
    assert settled == []
