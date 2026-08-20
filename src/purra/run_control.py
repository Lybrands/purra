"""Pure contracts for durable Agent Run control and recovery."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from purra.contracts import RunStatus
from purra.normalization import (
    non_negative_int,
    optional_non_negative_int,
    optional_text,
    positive_int,
    required_text,
    unique_text_tuple,
)


def _ids(values: Iterable[object] | str | None) -> tuple[str, ...]:
    """Normalize identifiers without turning ``None`` into the text ``None``."""

    if isinstance(values, str):
        values = (values,)
    return unique_text_tuple(str(value or "").strip() for value in (values or ()))


class OrphanRunDisposition(StrEnum):
    CANCEL = "cancel"
    DEFER_ACTIVE = "defer_active"
    PAUSE_RECOVERABLE = "pause_recoverable"
    FAIL = "fail"


class OrphanRunReason(StrEnum):
    CANCELLATION_REQUESTED = "cancellation_requested"
    DURABLE_TASK_ACTIVE = "durable_task_active"
    DURABLE_TASK_INTERRUPTED = "durable_task_interrupted"
    EXECUTION_INTERRUPTED = "execution_interrupted"


@dataclass(frozen=True, slots=True)
class OrphanTaskEvidence:
    task_id: str
    revision: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "task_id",
            required_text(self.task_id, "orphan task id"),
        )
        object.__setattr__(
            self,
            "revision",
            positive_int(self.revision, "orphan task revision"),
        )


def _task_evidence(
    values: Iterable[OrphanTaskEvidence],
) -> tuple[OrphanTaskEvidence, ...]:
    by_id: dict[str, OrphanTaskEvidence] = {}
    for value in values:
        if not isinstance(value, OrphanTaskEvidence):
            raise TypeError("orphan task evidence is required")
        existing = by_id.get(value.task_id)
        if existing is not None and existing.revision != value.revision:
            raise ValueError("orphan task evidence revisions conflict")
        by_id[value.task_id] = value
    return tuple(by_id.values())


@dataclass(frozen=True, slots=True)
class OrphanRunCandidate:
    run_id: str
    execution_attempt: int = 0
    execution_owner_id: str | None = None
    cancellation_requested_at_ms: int | None = None
    active_tasks: tuple[OrphanTaskEvidence, ...] = ()
    recoverable_tasks: tuple[OrphanTaskEvidence, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_id",
            required_text(self.run_id, "orphan run id"),
        )
        object.__setattr__(
            self,
            "execution_attempt",
            non_negative_int(self.execution_attempt, "orphan run execution attempt"),
        )
        object.__setattr__(
            self,
            "execution_owner_id",
            optional_text(self.execution_owner_id),
        )
        object.__setattr__(
            self,
            "cancellation_requested_at_ms",
            optional_non_negative_int(
                self.cancellation_requested_at_ms,
                "orphan run cancellation timestamp",
            ),
        )
        object.__setattr__(
            self,
            "active_tasks",
            _task_evidence(self.active_tasks),
        )
        object.__setattr__(
            self,
            "recoverable_tasks",
            _task_evidence(self.recoverable_tasks),
        )
        if set(self.active_task_ids) & set(self.recoverable_task_ids):
            raise ValueError("orphan task evidence must be disjoint")

    @property
    def active_task_ids(self) -> tuple[str, ...]:
        return tuple(item.task_id for item in self.active_tasks)

    @property
    def recoverable_task_ids(self) -> tuple[str, ...]:
        return tuple(item.task_id for item in self.recoverable_tasks)


@dataclass(frozen=True, slots=True)
class OrphanRunDecision:
    run_id: str
    disposition: OrphanRunDisposition
    reason: OrphanRunReason
    active_tasks: tuple[OrphanTaskEvidence, ...] = ()
    recoverable_tasks: tuple[OrphanTaskEvidence, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_id",
            required_text(self.run_id, "orphan decision run id"),
        )
        disposition = OrphanRunDisposition(self.disposition)
        reason = OrphanRunReason(self.reason)
        expected_reason = {
            OrphanRunDisposition.CANCEL: OrphanRunReason.CANCELLATION_REQUESTED,
            OrphanRunDisposition.DEFER_ACTIVE: OrphanRunReason.DURABLE_TASK_ACTIVE,
            OrphanRunDisposition.PAUSE_RECOVERABLE: (
                OrphanRunReason.DURABLE_TASK_INTERRUPTED
            ),
            OrphanRunDisposition.FAIL: OrphanRunReason.EXECUTION_INTERRUPTED,
        }[disposition]
        if reason is not expected_reason:
            raise ValueError("orphan disposition conflicts with recovery reason")
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(self, "reason", reason)
        active = _task_evidence(self.active_tasks)
        recoverable = _task_evidence(self.recoverable_tasks)
        active_ids = {item.task_id for item in active}
        recoverable_ids = {item.task_id for item in recoverable}
        if active_ids & recoverable_ids:
            raise ValueError("orphan decision task evidence must be disjoint")
        if disposition is OrphanRunDisposition.DEFER_ACTIVE and not active:
            raise ValueError("deferred orphan decision requires active tasks")
        if (
            disposition is OrphanRunDisposition.PAUSE_RECOVERABLE
            and not recoverable
        ):
            raise ValueError("recoverable orphan decision requires tasks")
        if disposition is OrphanRunDisposition.FAIL and (active or recoverable):
            raise ValueError("failed orphan decision cannot retain durable tasks")
        object.__setattr__(self, "active_tasks", active)
        object.__setattr__(
            self,
            "recoverable_tasks",
            recoverable,
        )

    @property
    def active_task_ids(self) -> tuple[str, ...]:
        return tuple(item.task_id for item in self.active_tasks)

    @property
    def recoverable_task_ids(self) -> tuple[str, ...]:
        return tuple(item.task_id for item in self.recoverable_tasks)

    @property
    def terminal_status(self) -> RunStatus | None:
        return {
            OrphanRunDisposition.CANCEL: RunStatus.CANCELED,
            OrphanRunDisposition.DEFER_ACTIVE: None,
            OrphanRunDisposition.PAUSE_RECOVERABLE: RunStatus.CANCELED,
            OrphanRunDisposition.FAIL: RunStatus.FAILED,
        }[self.disposition]


def decide_orphan_run(candidate: OrphanRunCandidate) -> OrphanRunDecision:
    """Choose a durable outcome without mutating the candidate or persistence."""

    if not isinstance(candidate, OrphanRunCandidate):
        raise TypeError("orphan recovery requires an OrphanRunCandidate")
    if candidate.cancellation_requested_at_ms is not None:
        disposition = OrphanRunDisposition.CANCEL
        reason = OrphanRunReason.CANCELLATION_REQUESTED
    elif candidate.active_task_ids:
        disposition = OrphanRunDisposition.DEFER_ACTIVE
        reason = OrphanRunReason.DURABLE_TASK_ACTIVE
    elif candidate.recoverable_task_ids:
        disposition = OrphanRunDisposition.PAUSE_RECOVERABLE
        reason = OrphanRunReason.DURABLE_TASK_INTERRUPTED
    else:
        disposition = OrphanRunDisposition.FAIL
        reason = OrphanRunReason.EXECUTION_INTERRUPTED
    return OrphanRunDecision(
        run_id=candidate.run_id,
        disposition=disposition,
        reason=reason,
        active_tasks=candidate.active_tasks,
        recoverable_tasks=candidate.recoverable_tasks,
    )


@dataclass(frozen=True, slots=True)
class RunActivitySnapshot:
    requested_run_ids: tuple[str, ...] = ()
    requested_task_ids: tuple[str, ...] = ()
    active_run_ids: tuple[str, ...] = ()
    active_delegation_run_ids: tuple[str, ...] = ()
    active_task_ids: tuple[str, ...] = ()
    draining_cancellation_run_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        requested_runs = _ids(self.requested_run_ids)
        requested_tasks = _ids(self.requested_task_ids)
        active_runs = _ids(self.active_run_ids)
        active_delegations = _ids(self.active_delegation_run_ids)
        active_tasks = _ids(self.active_task_ids)
        draining = _ids(self.draining_cancellation_run_ids)
        if set(active_runs) - set(requested_runs):
            raise ValueError("active run ids must be requested")
        if set(active_delegations) - set(requested_runs):
            raise ValueError("active delegation run ids must be requested")
        if set(active_tasks) - set(requested_tasks):
            raise ValueError("active task ids must be requested")
        if set(draining) - set(requested_runs):
            raise ValueError("draining cancellation run ids must be requested")
        for name, value in (
            ("requested_run_ids", requested_runs),
            ("requested_task_ids", requested_tasks),
            ("active_run_ids", active_runs),
            ("active_delegation_run_ids", active_delegations),
            ("active_task_ids", active_tasks),
            ("draining_cancellation_run_ids", draining),
        ):
            object.__setattr__(self, name, value)

    @property
    def quiescent(self) -> bool:
        return not (
            self.active_run_ids
            or self.active_delegation_run_ids
            or self.active_task_ids
            or self.draining_cancellation_run_ids
        )


@dataclass(frozen=True, slots=True)
class RunCancellationReceipt:
    run_id: str
    status: RunStatus
    cancellation_epoch: int
    newly_requested: bool
    delegations_canceled: int = 0
    terminalized: bool = False
    draining: bool = False
    tombstoned: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_id",
            required_text(self.run_id, "cancellation receipt run id"),
        )
        status = RunStatus(self.status)
        epoch = positive_int(self.cancellation_epoch, "cancellation epoch")
        delegations = non_negative_int(
            self.delegations_canceled,
            "canceled delegations",
        )
        terminalized = bool(self.terminalized)
        draining = bool(self.draining)
        tombstoned = bool(self.tombstoned)
        if status is RunStatus.RUNNING and not draining:
            raise ValueError("running cancellation receipt must be draining")
        if terminalized and status is not RunStatus.CANCELED:
            raise ValueError("only canceled receipts may be terminalized")
        if tombstoned and (
            status is not RunStatus.CANCELED
            or draining
            or terminalized
            or bool(self.newly_requested)
        ):
            raise ValueError("tombstoned cancellation receipt is invalid")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "cancellation_epoch", epoch)
        object.__setattr__(self, "newly_requested", bool(self.newly_requested))
        object.__setattr__(self, "delegations_canceled", delegations)
        object.__setattr__(self, "terminalized", terminalized)
        object.__setattr__(self, "draining", draining)
        object.__setattr__(self, "tombstoned", tombstoned)


__all__ = [
    "OrphanRunCandidate",
    "OrphanRunDecision",
    "OrphanRunDisposition",
    "OrphanRunReason",
    "OrphanTaskEvidence",
    "RunActivitySnapshot",
    "RunCancellationReceipt",
    "decide_orphan_run",
]
