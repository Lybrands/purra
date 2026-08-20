"""Dependency-inversion ports for execution admission and handoff."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from purra.contracts import AgentRunRequest, ExecutionPlan
from purra.ports import CancellationSignal
from purra.task_admission.contracts import (
    LongTaskDispatchReceipt,
    LongTaskExecutionResult,
    LongTaskExecutionUpdate,
    TaskAdmissionDecision,
)


LongTaskExecutionObserver = Callable[
    [LongTaskExecutionUpdate],
    Awaitable[None],
]


@runtime_checkable
class TaskAdmissionEvaluator(Protocol):
    async def evaluate(
        self,
        request: AgentRunRequest,
        plan: ExecutionPlan,
        signal: CancellationSignal | None = None,
    ) -> TaskAdmissionDecision: ...


@runtime_checkable
class LongTaskDispatcher(Protocol):
    async def dispatch(
        self,
        request: AgentRunRequest,
        plan: ExecutionPlan,
        decision: TaskAdmissionDecision,
        *,
        run_id: str,
        signal: CancellationSignal | None = None,
    ) -> LongTaskDispatchReceipt: ...

    async def execute(
        self,
        task_id: str,
        *,
        run_id: str,
        observer: LongTaskExecutionObserver,
        signal: CancellationSignal | None = None,
    ) -> LongTaskExecutionResult: ...


__all__ = [
    "LongTaskDispatcher",
    "LongTaskExecutionObserver",
    "TaskAdmissionEvaluator",
]
