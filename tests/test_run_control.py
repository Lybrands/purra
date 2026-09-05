import pytest

from purra.contracts import RunStatus
from purra.run_control import (
    OrphanRunCandidate,
    OrphanRunDecision,
    OrphanRunDisposition,
    OrphanRunReason,
    OrphanTaskEvidence,
    RunActivitySnapshot,
    RunCancellationReceipt,
    decide_orphan_run,
)


def test_orphan_candidate_normalizes_values_and_ids():
    candidate = OrphanRunCandidate(
        run_id="  run-1 ",
        execution_attempt="2",
        execution_owner_id=" worker ",
        cancellation_requested_at_ms=" 10 ",
        active_tasks=(OrphanTaskEvidence(" active-1 ", 1),),
        recoverable_tasks=(
            OrphanTaskEvidence(" task-1 ", 2),
            OrphanTaskEvidence("task-1", 2),
        ),
    )

    assert candidate.run_id == "run-1"
    assert candidate.execution_attempt == 2
    assert candidate.execution_owner_id == "worker"
    assert candidate.cancellation_requested_at_ms == 10
    assert candidate.active_task_ids == ("active-1",)
    assert candidate.recoverable_task_ids == ("task-1",)


@pytest.mark.parametrize(
    ("candidate", "disposition", "reason"),
    (
        (
            OrphanRunCandidate("run-1", cancellation_requested_at_ms=1),
            OrphanRunDisposition.CANCEL,
            OrphanRunReason.CANCELLATION_REQUESTED,
        ),
        (
            OrphanRunCandidate(
                "run-2",
                active_tasks=(OrphanTaskEvidence("task-1", 1),),
            ),
            OrphanRunDisposition.DEFER_ACTIVE,
            OrphanRunReason.DURABLE_TASK_ACTIVE,
        ),
        (
            OrphanRunCandidate(
                "run-3",
                recoverable_tasks=(OrphanTaskEvidence("task-1", 1),),
            ),
            OrphanRunDisposition.PAUSE_RECOVERABLE,
            OrphanRunReason.DURABLE_TASK_INTERRUPTED,
        ),
        (
            OrphanRunCandidate("run-4"),
            OrphanRunDisposition.FAIL,
            OrphanRunReason.EXECUTION_INTERRUPTED,
        ),
    ),
)
def test_decide_orphan_run(candidate, disposition, reason):
    decision = decide_orphan_run(candidate)

    assert decision.run_id == candidate.run_id
    assert decision.disposition is disposition
    assert decision.reason is reason
    assert decision.terminal_status is (
        None
        if disposition is OrphanRunDisposition.DEFER_ACTIVE
        else RunStatus.CANCELED
        if disposition in {
            OrphanRunDisposition.CANCEL,
            OrphanRunDisposition.PAUSE_RECOVERABLE,
        }
        else RunStatus.FAILED
    )


def test_orphan_decision_rejects_conflicting_reason():
    with pytest.raises(ValueError, match="conflicts"):
        OrphanRunDecision(
            run_id="run-1",
            disposition=OrphanRunDisposition.FAIL,
            reason=OrphanRunReason.CANCELLATION_REQUESTED,
        )


def test_orphan_candidate_rejects_overlapping_task_evidence():
    with pytest.raises(ValueError, match="disjoint"):
        OrphanRunCandidate(
            "run-1",
            active_tasks=(OrphanTaskEvidence("task-1", 1),),
            recoverable_tasks=(OrphanTaskEvidence("task-1", 1),),
        )


def test_activity_snapshot_normalizes_subsets_and_quiescence():
    snapshot = RunActivitySnapshot(
        requested_run_ids=(" run-1 ", "run-1"),
        requested_task_ids=(" task-1 ",),
        active_run_ids=("run-1",),
        active_task_ids=("task-1",),
        draining_cancellation_run_ids=(),
    )

    assert snapshot.requested_run_ids == ("run-1",)
    assert not snapshot.quiescent
    assert RunActivitySnapshot(requested_run_ids=("run-1",)).quiescent


def test_activity_snapshot_rejects_activity_outside_requested_sets():
    with pytest.raises(ValueError, match="active task ids"):
        RunActivitySnapshot(active_task_ids=("task-1",))


def test_cancellation_receipt_normalizes_and_enforces_invariants():
    receipt = RunCancellationReceipt(
        run_id=" run-1 ",
        status="running",
        cancellation_epoch="2",
        newly_requested=1,
        draining=1,
    )

    assert receipt.run_id == "run-1"
    assert receipt.status is RunStatus.RUNNING
    assert receipt.cancellation_epoch == 2
    assert receipt.newly_requested is True
    assert receipt.draining is True


def test_cancellation_receipt_accepts_a_quiescent_tombstone():
    receipt = RunCancellationReceipt(
        run_id="run-1",
        status=RunStatus.CANCELED,
        cancellation_epoch=1,
        newly_requested=False,
        tombstoned=True,
    )

    assert receipt.tombstoned is True


def test_cancellation_receipt_preserves_a_competing_terminal_outcome():
    receipt = RunCancellationReceipt(
        run_id="run-1",
        status=RunStatus.DONE,
        cancellation_epoch=1,
        newly_requested=False,
    )

    assert receipt.status is RunStatus.DONE
    assert receipt.terminalized is False


@pytest.mark.parametrize(
    "kwargs",
    (
        {"status": "running", "cancellation_epoch": 1, "draining": False},
        {"status": "canceled", "cancellation_epoch": 0, "draining": True},
        {"status": "canceled", "cancellation_epoch": 0, "draining": False},
        {"status": "failed", "cancellation_epoch": 1, "terminalized": True},
        {
            "status": "running",
            "cancellation_epoch": 1,
            "draining": True,
            "tombstoned": True,
        },
        {"status": "running", "cancellation_epoch": -1, "draining": True},
    ),
)
def test_cancellation_receipt_rejects_invalid_invariants(kwargs):
    with pytest.raises(ValueError):
        RunCancellationReceipt(run_id="run-1", newly_requested=False, **kwargs)
