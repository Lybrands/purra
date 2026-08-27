"""Dynamic-plan revision over durable Run authority."""

from __future__ import annotations

from time import perf_counter
from typing import Sequence

from purra.cancellation import await_with_cancellation
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    PlanningCapabilities,
    PlanningTurn,
    ReasoningMode,
    StepExecutor,
    StepStatus,
    StepType,
    ExecutionPlan,
    TaskStep,
    ToolBatchOutcome,
    ToolRiskLevel,
    TraceRecord,
)
from purra.errors import (
    ContractViolationError,
    InvalidPlannerOutputError,
    ModelGatewayError,
)
from purra.events import CoreEventType
from purra.plan_compiler import (
    compile_work_plan,
    project_completed_steps_for_planning,
)
from purra.planner import build_execution_message
from purra.ports import CancellationSignal, DynamicWorkPlanner, ToolRegistration
from purra.run_controller import AgentRunController
from purra.run_state import RunStateMachine
from purra.timing import duration_ms

from .planning_validation import validate_plan_authority


class DynamicPlanningOrchestrator:
    """Bridge runtime evidence to a dynamic planner and durable Run authority."""

    def __init__(
        self,
        *,
        planner: DynamicWorkPlanner,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
        controller: AgentRunController,
        enabled_names: frozenset[str],
        registrations: Sequence[ToolRegistration],
        turn_id: str | None = None,
        reasoning_mode: ReasoningMode = ReasoningMode.DEFAULT,
    ) -> None:
        self._planner = planner
        self._request = request
        self._capabilities = capabilities
        self._controller = controller
        self._enabled_names = enabled_names
        self._registrations = tuple(registrations)
        self._turn_id = str(turn_id or "").strip() or None
        self._reasoning_mode = ReasoningMode(reasoning_mode)
        self._revision = 0

    async def replan_after_tool(
        self,
        messages: Sequence[AgentMessage],
        *,
        round_number: int,
        remaining_model_rounds: int,
        outcome: ToolBatchOutcome,
        signal: CancellationSignal | None = None,
    ) -> AgentMessage:
        started = perf_counter()
        self._revision += 1
        snapshot = self._controller.snapshot
        if snapshot is None:
            raise ContractViolationError("dynamic planning requires a live run")
        if outcome is ToolBatchOutcome.FAILED:
            await self._controller.on_tool_round_failed()
            snapshot = self._controller.snapshot
            if snapshot is None:  # pragma: no cover - controller invariant
                raise ContractViolationError("dynamic planning lost its live run")
        runtime_completed_steps = tuple(
            step
            for step in snapshot.steps
            if step.status in {
                StepStatus.DONE,
                StepStatus.BLOCKED,
                StepStatus.FAILED,
            }
        )
        completed_steps = project_completed_steps_for_planning(
            runtime_completed_steps,
            self._registrations,
            self._enabled_names,
        )
        try:
            planning = await await_with_cancellation(
                self._planner.revise_plan(
                    self._request,
                    self._capabilities,
                    PlanningTurn(
                        revision=self._revision,
                        round_number=round_number,
                        remaining_model_rounds=remaining_model_rounds,
                        messages=tuple(messages),
                        completed_steps=completed_steps,
                        last_tool_outcome=outcome,
                    ),
                    signal,
                    run_id=self._controller.run_id,
                    turn_id=self._turn_id,
                    reasoning_mode=self._reasoning_mode,
                ),
                signal,
            )
        except (InvalidPlannerOutputError, ModelGatewayError) as error:
            reason_code = getattr(error, "code", "replanning_failed")
            validation_reason = (
                str(error)[:240]
                if isinstance(error, InvalidPlannerOutputError)
                else None
            )
            if outcome is ToolBatchOutcome.FAILED:
                recovery_plan = safe_model_only_plan(
                    title=snapshot.title,
                    goal=snapshot.goal,
                    step_id=f"respond-after-tool-failure-{self._revision}",
                )
                validate_plan_authority(
                    recovery_plan,
                    self._enabled_names,
                    constraints=self._capabilities.constraints,
                    max_tool_steps=0,
                )
                revised = await self._controller.revise_plan(recovery_plan)
                remaining_plan = ExecutionPlan(
                    title=revised.title,
                    goal=revised.goal,
                    steps=tuple(
                        step
                        for step in revised.steps
                        if step.status in {
                            StepStatus.PENDING,
                            StepStatus.RUNNING,
                        }
                    ),
                )
                await self._controller.record_trace(TraceRecord(
                    stage="planning",
                    outcome="fallback_safe_response",
                    details={
                        "dynamic": True,
                        "revision": self._revision,
                        "round": round_number,
                        "errorType": type(error).__name__,
                        "reasonCode": reason_code,
                        "validationReason": validation_reason,
                        "fallbackToolCount": 0,
                    },
                    duration_ms=duration_ms(started),
                ))
                return build_execution_message(remaining_plan)
            remaining_steps = tuple(
                step
                for step in snapshot.steps
                if step.status in {StepStatus.PENDING, StepStatus.RUNNING}
            )
            if not remaining_steps:
                remaining_steps = (TaskStep(
                    id="respond-after-replan-fallback",
                    title="Respond from completed work",
                    type=StepType.REVIEW,
                    executor=StepExecutor.MODEL,
                    risk_level=ToolRiskLevel.READ,
                ),)
            fallback_plan = ExecutionPlan(
                title=snapshot.title,
                goal=snapshot.goal,
                steps=remaining_steps,
            )
            fallback_tool_count = sum(
                step.executor is StepExecutor.TOOL
                for step in fallback_plan.steps
            )
            validate_plan_authority(
                fallback_plan,
                self._enabled_names,
                constraints=self._capabilities.constraints,
                max_tool_steps=fallback_tool_count,
            )
            await self._controller.record_trace(TraceRecord(
                stage="planning",
                outcome="fallback_previous_plan",
                details={
                    "dynamic": True,
                    "revision": self._revision,
                    "round": round_number,
                    "errorType": type(error).__name__,
                    "reasonCode": reason_code,
                    "validationReason": validation_reason,
                    "remainingStepCount": len(remaining_steps),
                    "trustedPlanToolCount": fallback_tool_count,
                },
                duration_ms=duration_ms(started),
            ))
            return build_execution_message(fallback_plan)
        if planning.model_call_parameters:
            for parameters in planning.model_call_parameters:
                await self._controller.record_event(
                    CoreEventType.MODEL_CALL_RECORDED,
                    {
                        "phase": "replanning",
                        "count": 1,
                        "toolNames": [],
                        "toolChoice": "none",
                        "round": round_number,
                        "revision": self._revision,
                        "parameters": dict(parameters),
                    },
                )
        elif planning.model_call_count > 0:
            await self._controller.record_event(
                CoreEventType.MODEL_CALL_RECORDED,
                {
                    "phase": "replanning",
                    "count": planning.model_call_count,
                    "toolNames": [],
                    "toolChoice": "none",
                    "round": round_number,
                    "revision": self._revision,
                },
            )
        satisfied_tool_names = frozenset(
            name
            for step in runtime_completed_steps
            if step.status is StepStatus.DONE
            for name in step.suggested_tools
        ) | self._capabilities.constraints.execution_satisfied_tool_names
        compiled = compile_work_plan(
            planning.work_plan,
            self._registrations,
            constraints=self._capabilities.constraints,
            satisfied_tool_names=satisfied_tool_names,
            enabled_tool_names=self._enabled_names,
        )
        prospective = RunStateMachine.revise_plan(
            snapshot,
            compiled.execution_plan,
        )
        remaining_plan = ExecutionPlan(
            title=prospective.title,
            goal=prospective.goal,
            task_spec=compiled.execution_plan.task_spec,
            steps=tuple(
                step
                for step in prospective.steps
                if step.status in {StepStatus.PENDING, StepStatus.RUNNING}
            ),
        )
        validate_plan_authority(
            remaining_plan,
            self._enabled_names,
            constraints=self._capabilities.constraints,
            max_tool_steps=max(0, remaining_model_rounds - 1),
        )
        revised = await self._controller.revise_plan(compiled.execution_plan)
        await self._controller.record_trace(TraceRecord(
            stage="planning",
            outcome="replanned",
            details={
                "dynamic": True,
                "revision": self._revision,
                "round": round_number,
                "planningKind": planning.kind.value,
                "completedStepCount": len(completed_steps),
                "workPlanStepCount": len(planning.work_plan.steps),
                "executionPlanStepCount": len(compiled.execution_plan.steps),
                "workPlanModelStepCount": sum(
                    step.executor is StepExecutor.MODEL
                    for step in planning.work_plan.steps
                ),
                "workPlanToolStepCount": sum(
                    step.executor is StepExecutor.TOOL
                    for step in planning.work_plan.steps
                ),
                "executionToolStepCount": sum(
                    step.executor is StepExecutor.TOOL
                    for step in compiled.execution_plan.steps
                ),
                "plannerRepairCount": max(
                    0,
                    planning.model_call_count - 1,
                ),
                "remainingStepCount": sum(
                    step.status in {StepStatus.PENDING, StepStatus.RUNNING}
                    for step in revised.steps
                ),
                "hostInsertedPrerequisiteCount": len(
                    compiled.inserted_tool_names
                ),
                "hostLoweredProtocolToolCount": len(
                    compiled.lowered_tool_names
                ),
            },
            duration_ms=duration_ms(started),
        ))
        return build_execution_message(remaining_plan)


def safe_model_only_plan(
    *,
    title: str,
    goal: str | None,
    step_id: str,
) -> ExecutionPlan:
    """Return the only fail-open plan Core may author without model trust."""

    return ExecutionPlan(
        title=title,
        goal=goal,
        steps=(TaskStep(
            id=step_id,
            title="说明当前结果",
            type=StepType.REVIEW,
            executor=StepExecutor.MODEL,
            risk_level=ToolRiskLevel.READ,
        ),),
    )


__all__ = ["DynamicPlanningOrchestrator", "safe_model_only_plan"]
