"""Optional admission and durable-task orchestration capability."""

from __future__ import annotations

from collections.abc import AsyncIterator

from purra.cancellation import await_with_cancellation
from purra.contracts import AgentRunRequest, ExecutionPlan
from purra.engine.durable_execution import (
    BufferedEventSink,
    complete_admitted_task,
    complete_durable_continuation,
    validate_task_admission_coverage,
)
from purra.engine.options import DurableTaskContinuation
from purra.errors import ContractViolationError
from purra.events import AgentEvent, CoreEventType
from purra.ports import CancellationSignal
from purra.run_controller import AgentRunController
from purra.task_admission import (
    LongTaskDispatcher,
    TaskAdmissionDecision,
    TaskAdmissionEvaluator,
)


class TaskOrchestrationCapability:
    """Own optional task admission, durable handoff, and continuation."""

    def __init__(
        self,
        evaluator: TaskAdmissionEvaluator | None,
        dispatcher: LongTaskDispatcher | None,
    ) -> None:
        self._evaluator = evaluator
        self._dispatcher = dispatcher

    async def evaluate(
        self,
        request: AgentRunRequest,
        plan: ExecutionPlan,
        controller: AgentRunController,
        signal: CancellationSignal | None,
    ) -> TaskAdmissionDecision | None:
        if plan.task_spec is None:
            return None
        if self._evaluator is None:
            return None
        admission = await await_with_cancellation(
            self._evaluator.evaluate(request, plan, signal),
            signal,
        )
        validate_task_admission_coverage(plan, admission)
        await controller.record_event(
            CoreEventType.TASK_ADMISSION_DECIDED,
            admission.to_event_payload(),
        )
        return admission

    async def continue_durable(
        self,
        controller: AgentRunController,
        request: AgentRunRequest,
        continuation: DurableTaskContinuation,
        sink: BufferedEventSink,
        signal: CancellationSignal | None,
    ) -> AsyncIterator[AgentEvent]:
        async for event in complete_durable_continuation(
            controller,
            request,
            continuation,
            self._dispatcher,
            sink,
            signal,
        ):
            yield event

    async def complete_admission(
        self,
        *,
        controller: AgentRunController,
        request: AgentRunRequest,
        plan: ExecutionPlan,
        admission: TaskAdmissionDecision,
        sink: BufferedEventSink,
        signal: CancellationSignal | None,
    ) -> AsyncIterator[AgentEvent]:
        async for event in complete_admitted_task(
            controller=controller,
            request=request,
            plan=plan,
            admission=admission,
            dispatcher=self._dispatcher,
            sink=sink,
            signal=signal,
        ):
            yield event


__all__ = ["TaskOrchestrationCapability"]
