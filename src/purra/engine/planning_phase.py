"""Optional plan compilation and task-admission capability for AgentCore."""

from __future__ import annotations

from dataclasses import dataclass, replace
from time import perf_counter
from typing import Sequence

from purra.cancellation import OperationCanceled, await_with_cancellation
from purra.contracts import (
    AgentRunRequest,
    ContextBundle,
    PlanningCapabilities,
    PlanningKind,
    RuntimeLimits,
    ReasoningMode,
    ExecutionPlan,
    StepExecutor,
    TraceRecord,
)
from purra.engine.context_phase import (
    planning_tool_guidance,
)
from purra.engine.dynamic_planning import (
    DynamicPlanningOrchestrator,
    safe_model_only_plan,
)
from purra.engine.planning_validation import (
    validate_plan_authority,
    validate_planning_constraints,
    validate_task_constraint_refinement,
)
from purra.engine.task_orchestration import TaskOrchestrationCapability
from purra.errors import (
    ContextOverflowError,
    ContractViolationError,
    InvalidPlannerOutputError,
)
from purra.events import CoreEventType
from purra.plan_compiler import compile_work_plan, projected_planning_tool_names
from purra.ports import (
    CancellationSignal,
    DynamicWorkPlanner,
    PlanningPolicy,
    WorkPlanner,
    WorkPlanningConstraintProvider,
    ToolRegistration,
)
from purra.run_controller import AgentRunController
from purra.task_admission import TaskAdmissionDecision
from purra.timing import duration_ms


@dataclass(frozen=True, slots=True)
class PlanningPhaseResult:
    """Everything later execution stages may consume from planning."""

    should_plan: bool = False
    execution_plan: ExecutionPlan | None = None
    capabilities: PlanningCapabilities | None = None
    admission: TaskAdmissionDecision | None = None
    dynamic_planning: DynamicPlanningOrchestrator | None = None
    fallback_model_only: bool = False
    terminal: bool = False

    @classmethod
    def reactive(cls) -> "PlanningPhaseResult":
        return cls()


class PlanningCapability:
    """Compile optional model intent into durable Run authority."""

    def __init__(
        self,
        *,
        planner: WorkPlanner | None,
        policy: PlanningPolicy,
        runtime_limits: RuntimeLimits,
        task_orchestration: TaskOrchestrationCapability | None,
    ) -> None:
        self._planner = planner
        self._policy = policy
        self._runtime_limits = runtime_limits
        self._task_orchestration = task_orchestration

    async def execute(
        self,
        *,
        request: AgentRunRequest,
        planning_bundle: ContextBundle,
        registrations: Sequence[ToolRegistration],
        enabled_names: frozenset[str],
        display_locale: str,
        model_supports_tools: bool,
        controller: AgentRunController,
        signal: CancellationSignal | None,
        turn_id: str | None,
        reasoning_mode: ReasoningMode,
    ) -> PlanningPhaseResult:
        started = perf_counter()
        capabilities: PlanningCapabilities | None = None
        admission: TaskAdmissionDecision | None = None
        try:
            planning_tool_names = projected_planning_tool_names(
                registrations,
                enabled_names,
            )
            base_capabilities = PlanningCapabilities(
                available_tool_names=planning_tool_names,
                model_supports_tools=model_supports_tools,
                planning_context_blocks=planning_bundle.blocks,
                tool_guidance=planning_tool_guidance(
                    registrations,
                    enabled_names,
                    display_locale,
                ),
            )
            constraints = self._policy.planning_constraints(
                request,
                base_capabilities,
            )
            validate_planning_constraints(
                base_capabilities,
                constraints,
                runtime_tool_names=enabled_names,
            )
            capabilities = replace(
                base_capabilities,
                constraints=constraints,
            )
            should_plan = bool(self._policy.should_plan(request, capabilities))
            plan: ExecutionPlan | None = None
            planning_kind: PlanningKind | None = None
            if should_plan:
                if self._planner is None:
                    raise ContractViolationError(
                        "planning policy requested a plan without a planner"
                    )
                planning = await await_with_cancellation(
                    self._planner.create_plan(
                        request,
                        capabilities,
                        signal,
                        run_id=controller.run_id,
                        turn_id=turn_id,
                        reasoning_mode=reasoning_mode,
                    ),
                    signal,
                )
                if planning.model_call_parameters:
                    for parameters in planning.model_call_parameters:
                        await controller.record_event(
                            CoreEventType.MODEL_CALL_RECORDED,
                            {
                                "phase": "planning",
                                "count": 1,
                                "toolNames": [],
                                "toolChoice": "none",
                                "parameters": dict(parameters),
                            },
                        )
                elif planning.model_call_count > 0:
                    await controller.record_event(
                        CoreEventType.MODEL_CALL_RECORDED,
                        {
                            "phase": "planning",
                            "count": planning.model_call_count,
                            "toolNames": [],
                            "toolChoice": "none",
                        },
                    )
                if (
                    planning.work_plan.task_spec is not None
                    and isinstance(
                        self._policy,
                        WorkPlanningConstraintProvider,
                    )
                ):
                    constraints = self._policy.planning_constraints_for_task(
                        request,
                        capabilities,
                        planning.work_plan.task_spec,
                    )
                    validate_task_constraint_refinement(
                        capabilities.constraints,
                        constraints,
                    )
                    validate_planning_constraints(
                        base_capabilities,
                        constraints,
                        runtime_tool_names=enabled_names,
                    )
                    capabilities = replace(
                        capabilities,
                        constraints=constraints,
                    )
                compiled = compile_work_plan(
                    planning.work_plan,
                    registrations,
                    constraints=constraints,
                    satisfied_tool_names=(
                        constraints.execution_satisfied_tool_names
                    ),
                    enabled_tool_names=enabled_names,
                )
                plan = compiled.execution_plan
                planning_kind = planning.kind
                validate_plan_authority(
                    plan,
                    enabled_names,
                    constraints=constraints,
                    max_tool_steps=max(
                        0,
                        self._runtime_limits.max_model_rounds - 2,
                    ),
                )
                if self._task_orchestration is not None:
                    admission = await self._task_orchestration.evaluate(
                        request,
                        plan,
                        controller,
                        signal,
                    )
                await controller.install_plan(plan)
            await controller.record_trace(TraceRecord(
                stage="planning",
                outcome=(planning_kind.value if planning_kind else "skipped"),
                details={
                    "toolCount": len(enabled_names),
                    "planningCapabilityCount": len(
                        capabilities.available_tool_names
                    ),
                    "contextSatisfiedToolCount": len(
                        constraints.context_satisfied_tool_names
                    ),
                    "planningExcludedToolCount": len(
                        constraints.planning_excluded_tool_names
                    ),
                    "satisfiedToolDependencyEdgeCount": len(
                        constraints.satisfied_tool_dependency_edges
                    ),
                    "requiredAnyToolCount": len(
                        constraints.required_any_tool_names
                    ),
                    "planningExcludedExecutorCount": len(
                        constraints.planning_excluded_executors
                    ),
                    "allowModelOnlyFallback": (
                        constraints.allow_model_only_fallback
                    ),
                    "planned": should_plan,
                    "workPlanStepCount": (
                        len(planning.work_plan.steps) if should_plan else 0
                    ),
                    "executionPlanStepCount": (
                        len(plan.steps) if plan is not None else 0
                    ),
                    "workPlanModelStepCount": (
                        sum(
                            step.executor is StepExecutor.MODEL
                            for step in planning.work_plan.steps
                        )
                        if should_plan else 0
                    ),
                    "workPlanToolStepCount": (
                        sum(
                            step.executor is StepExecutor.TOOL
                            for step in planning.work_plan.steps
                        )
                        if should_plan else 0
                    ),
                    "executionToolStepCount": (
                        sum(
                            step.executor is StepExecutor.TOOL
                            for step in plan.steps
                        )
                        if plan is not None else 0
                    ),
                    "plannerRepairCount": (
                        max(0, planning.model_call_count - 1)
                        if should_plan else 0
                    ),
                    "hostInsertedPrerequisiteCount": (
                        len(compiled.inserted_tool_names) if should_plan else 0
                    ),
                    "hostLoweredProtocolToolCount": (
                        len(compiled.lowered_tool_names) if should_plan else 0
                    ),
                },
                duration_ms=duration_ms(started),
            ))
            return PlanningPhaseResult(
                should_plan=should_plan,
                execution_plan=plan,
                capabilities=capabilities,
                admission=admission,
                dynamic_planning=(
                    DynamicPlanningOrchestrator(
                        planner=self._planner,
                        request=request,
                        capabilities=capabilities,
                        controller=controller,
                        enabled_names=enabled_names,
                        registrations=registrations,
                        turn_id=turn_id,
                        reasoning_mode=reasoning_mode,
                    )
                    if should_plan
                    and plan is not None
                    and isinstance(self._planner, DynamicWorkPlanner)
                    else None
                ),
            )
        except OperationCanceled:
            await controller.record_trace(TraceRecord(
                stage="planning",
                outcome="canceled",
                duration_ms=duration_ms(started),
            ))
            await controller.cancel("request_canceled")
            return PlanningPhaseResult(terminal=True)
        except InvalidPlannerOutputError as error:
            await _record_exception(
                controller,
                outcome="invalid",
                error=error,
                started=started,
                safe_details={
                    "reasonCode": error.code,
                    "validationReason": str(error)[:240],
                },
            )
            if (
                capabilities is not None
                and not capabilities.constraints.allow_model_only_fallback
            ):
                await controller.record_trace(TraceRecord(
                    stage="planning",
                    outcome="fallback_denied",
                    details={
                        "reasonCode": error.code,
                        "hostPolicy": "deny_model_only_fallback",
                    },
                    duration_ms=duration_ms(started),
                ))
                await controller.fail("planning_invalid")
                return PlanningPhaseResult(terminal=True)
            plan = safe_model_only_plan(
                title="安全降级回复",
                goal="在不调用工具的情况下回应用户",
                step_id="respond-after-invalid-plan",
            )
            await controller.install_plan(plan)
            await controller.record_trace(TraceRecord(
                stage="planning",
                outcome="fallback_model_only",
                details={
                    "reasonCode": error.code,
                    "fallbackToolCount": 0,
                },
                duration_ms=duration_ms(started),
            ))
            return PlanningPhaseResult(
                should_plan=True,
                execution_plan=plan,
                capabilities=capabilities,
                fallback_model_only=True,
            )
        except ContractViolationError as error:
            await _record_exception(
                controller,
                outcome="contract_violation",
                error=error,
                started=started,
            )
            await controller.fail("planning_contract_violation")
            return PlanningPhaseResult(terminal=True)
        except Exception as error:
            await _record_exception(
                controller,
                outcome="failed",
                error=error,
                started=started,
            )
            await controller.fail("planning_failed")
            return PlanningPhaseResult(terminal=True)


async def _record_exception(
    controller: AgentRunController,
    *,
    outcome: str,
    error: Exception,
    started: float,
    safe_details: Mapping[str, Any] | None = None,
) -> None:
    overflow_details = (
        {
            "reasonCode": error.reason_code,
            **error.details,
        }
        if isinstance(error, ContextOverflowError)
        else {}
    )
    await controller.record_trace(TraceRecord(
        stage="planning",
        outcome=outcome,
        details={
            "errorType": type(error).__name__,
            **overflow_details,
            **(safe_details or {}),
        },
        duration_ms=duration_ms(started),
    ))


__all__ = ["PlanningCapability", "PlanningPhaseResult"]
