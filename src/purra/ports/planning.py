"""Planning, execution-state, and response validation ports."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    ExecutionState,
    PlanningCapabilities,
    PlanningConstraints,
    PlanningResult,
    PlanningTurn,
    ReasoningMode,
    ResponseValidationResult,
    TaskSpec,
)
from purra.ports.model import CancellationSignal


@runtime_checkable
class PlanningPolicy(Protocol):
    def should_plan(
        self,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
    ) -> bool: ...

    def planning_constraints(
        self,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
    ) -> PlanningConstraints: ...


@runtime_checkable
class WorkPlanningConstraintProvider(Protocol):
    """Refine host constraints after semantic TaskSpec selection."""

    def planning_constraints_for_task(
        self,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
        task_spec: TaskSpec,
    ) -> PlanningConstraints: ...


@runtime_checkable
class WorkPlanner(Protocol):
    async def create_plan(
        self,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
        signal: CancellationSignal | None = None,
        *,
        run_id: str | None = None,
        turn_id: str | None = None,
        reasoning_mode: ReasoningMode = ReasoningMode.DEFAULT,
    ) -> PlanningResult: ...


@runtime_checkable
class DynamicWorkPlanner(Protocol):
    """Optional planner capability for result-driven runtime revisions."""

    async def revise_plan(
        self,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
        turn: PlanningTurn,
        signal: CancellationSignal | None = None,
        *,
        run_id: str | None = None,
        turn_id: str | None = None,
        reasoning_mode: ReasoningMode = ReasoningMode.DEFAULT,
    ) -> PlanningResult: ...


@runtime_checkable
class ExecutionStateFactory(Protocol):
    def create(self, request: AgentRunRequest) -> ExecutionState: ...


@runtime_checkable
class ResponseValidator(Protocol):
    """Validate a buffered final response without embedding domain semantics."""

    def validate(
        self,
        *,
        content: str,
        messages: Sequence[AgentMessage],
    ) -> ResponseValidationResult: ...


@runtime_checkable
class ResponseJudge(Protocol):
    """Asynchronously judge buffered output without owning domain semantics."""

    async def judge(
        self,
        *,
        content: str,
        messages: Sequence[AgentMessage],
        signal: CancellationSignal | None = None,
    ) -> ResponseValidationResult: ...


@runtime_checkable
class ResponseJudgePolicy(Protocol):
    """Domain prompt/evaluator pair; PurrA owns the actual model call."""

    def build_messages(
        self,
        *,
        content: str,
        messages: Sequence[AgentMessage],
    ) -> tuple[AgentMessage, ...]: ...

    def evaluate(
        self,
        *,
        judgment_content: str,
        candidate_content: str,
    ) -> ResponseValidationResult: ...


__all__ = [name for name in globals() if not name.startswith("_")]
