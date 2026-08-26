"""Run lifecycle persistence contracts and atomic commit invariants."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from purra.contracts import (
    ModelTokenUsage,
    RunCreateParams,
    ExecutionPlan,
    RunId,
    RunStatus,
    TaskStepUpdate,
    TerminalRunStatus,
    TraceRecord,
)
from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.events import AgentEvent, CoreEventType
from purra.normalization import required_text
from purra.run_state import RunSnapshot


@dataclass(frozen=True, slots=True)
class RunBeginResult:
    """Atomic result of creating a run and its first outbox event."""

    run_id: RunId
    event: AgentEvent

    def __post_init__(self) -> None:
        run_id = required_text(self.run_id, "run begin result run id")
        if self.event.run_id != run_id:
            raise ValueError("run begin event must be bound to the created run")
        object.__setattr__(self, "run_id", run_id)


@dataclass(frozen=True, slots=True)
class RunBudgetSnapshot:
    model_attempts: int
    unreported_usage_attempts: int
    input_tokens: int
    output_tokens: int
    reasoning_tokens: int

    def __post_init__(self) -> None:
        for name in (
            "model_attempts",
            "unreported_usage_attempts",
            "input_tokens",
            "output_tokens",
            "reasoning_tokens",
        ):
            value = int(getattr(self, name))
            if value < 0:
                raise ValueError("Run budget counters must be non-negative")
            object.__setattr__(self, name, value)


@dataclass(frozen=True, slots=True)
class RunCommit:
    """One atomic Run state/outbox write requested by the Core controller."""

    replace_plan: ExecutionPlan | None = None
    step_updates: tuple[TaskStepUpdate, ...] = ()
    terminal_status: TerminalRunStatus | None = None
    final_response: str | None = None
    validated_result: str | None = None
    error: str | None = None
    execution_checkpoint: AgentExecutionCheckpoint | None = None
    events: tuple[AgentEvent, ...] = ()

    def __post_init__(self) -> None:
        replacement = self.replace_plan
        if replacement is not None and not isinstance(replacement, ExecutionPlan):
            raise TypeError("run commit replacement must be an ExecutionPlan")
        updates = tuple(self.step_updates)
        events = tuple(self.events)
        update_ids = [update.step_id for update in updates]
        if len(update_ids) != len(set(update_ids)):
            raise ValueError("run commit step updates must be unique")
        if replacement is not None and updates:
            raise ValueError(
                "run commit cannot replace an execution plan and update steps"
            )

        terminal = self.terminal_status
        if terminal is not None:
            normalized = RunStatus(terminal)
            if normalized is RunStatus.RUNNING:
                raise ValueError("run commit cannot reopen a run")
            terminal = normalized  # type: ignore[assignment]
        if terminal is RunStatus.FAILED and not str(self.error or "").strip():
            raise ValueError("failed run commit requires a non-empty error")
        if terminal is not RunStatus.FAILED and self.error is not None:
            raise ValueError("run commit error is only valid for failed runs")
        if terminal is not RunStatus.DONE and self.final_response is not None:
            raise ValueError(
                "run commit final_response is only valid for completed runs"
            )
        if terminal is not RunStatus.DONE and self.validated_result is not None:
            raise ValueError(
                "run commit validated_result is only valid for completed runs"
            )
        checkpoint = self.execution_checkpoint
        if checkpoint is not None and not isinstance(
            checkpoint,
            AgentExecutionCheckpoint,
        ):
            raise TypeError("run commit execution checkpoint is invalid")
        if terminal is not None and checkpoint is not None:
            raise ValueError(
                "terminal run commit cannot install an execution checkpoint"
            )

        object.__setattr__(self, "replace_plan", replacement)
        object.__setattr__(self, "step_updates", updates)
        object.__setattr__(self, "terminal_status", terminal)
        object.__setattr__(
            self,
            "validated_result",
            (
                None
                if self.validated_result is None
                else str(self.validated_result)
            ),
        )
        object.__setattr__(self, "execution_checkpoint", checkpoint)
        object.__setattr__(self, "events", events)


TERMINAL_RUN_EVENT_TYPES = frozenset({
    CoreEventType.RUN_COMPLETED,
    CoreEventType.RUN_BLOCKED,
    CoreEventType.RUN_FAILED,
    CoreEventType.RUN_CANCELED,
})

CONTROLLER_OWNED_RUN_EVENT_TYPES = frozenset({
    CoreEventType.RUN_STARTED,
    CoreEventType.RUN_TODOS_UPDATED,
    CoreEventType.RUN_TODO_UPDATED,
    *TERMINAL_RUN_EVENT_TYPES,
})


def validate_run_commit_lifecycle(commit: RunCommit) -> None:
    """Validate the one-to-one Run terminal state/outbox invariant."""

    if any(event.type == CoreEventType.RUN_STARTED for event in commit.events):
        raise ValueError("run.started is only valid for repository.begin")

    terminal_events = tuple(
        event
        for event in commit.events
        if event.type in TERMINAL_RUN_EVENT_TYPES
    )
    if commit.terminal_status is None:
        if terminal_events:
            raise ValueError(
                "non-terminal run commit cannot include terminal events"
            )
        return

    expected = {
        RunStatus.DONE: CoreEventType.RUN_COMPLETED,
        RunStatus.BLOCKED: CoreEventType.RUN_BLOCKED,
        RunStatus.FAILED: CoreEventType.RUN_FAILED,
        RunStatus.CANCELED: CoreEventType.RUN_CANCELED,
    }[RunStatus(commit.terminal_status)]
    if len(terminal_events) != 1:
        raise ValueError(
            "terminal run commit requires exactly one "
            f"{expected.value} event"
        )
    actual = terminal_events[0].type
    if actual != expected:
        raise ValueError(
            f"terminal status {commit.terminal_status.value} requires "
            f"{expected.value}, got {actual}"
        )


@runtime_checkable
class RunRepository(Protocol):
    """Atomic, cancellation-linearizable Run persistence.

    Child Run mutations validate ``current_agent_run_lease(run_id)`` against
    the same durable lease authority used by ``RunTreeRepository``. The
    execution-local proof prevents an old Task from borrowing a newer claim
    stored on the Run record after worker recovery.
    """

    async def begin(
        self,
        params: RunCreateParams,
        started_event: AgentEvent,
    ) -> RunBeginResult: ...

    async def get(self, run_id: RunId) -> RunSnapshot:
        """Load detached canonical Run state for recovery reconciliation."""
        ...

    async def commit(
        self,
        run_id: RunId,
        commit: RunCommit,
    ) -> tuple[AgentEvent, ...]: ...

    async def bind_conversation(self, run_id: RunId, conversation_id: int) -> None: ...

    async def append_event(self, run_id: RunId, event: AgentEvent) -> None:
        """Append an event not owned by AgentRunController."""
        ...

    async def append_trace(self, run_id: RunId, trace: TraceRecord) -> None: ...

    async def reserve_model_attempt(
        self,
        run_id: RunId,
        invocation_id: str,
    ) -> RunBudgetSnapshot: ...

    async def settle_model_attempt(
        self,
        run_id: RunId,
        invocation_id: str,
        usage: ModelTokenUsage | None,
    ) -> RunBudgetSnapshot: ...
