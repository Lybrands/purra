from __future__ import annotations

import pytest

from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    DomainContext,
    ExecutionRecipe,
    ExecutionRecipeStep,
    MessageRole,
    ModelRequest,
    StepExecutor,
    StepType,
    ExecutionPlan,
    TaskSpec,
    TaskStep,
    TaskStepUpdate,
    StepStatus,
    RunStatus,
)
from purra.api import AgentCoreRunOptions, DurableTaskContinuation
from purra.errors import ContractViolationError
from purra.ports import RunCommit
from purra.run_recovery import RunRecoverySnapshot
from purra.task_admission import (
    ExecutionMode,
    LongTaskDispatchReceipt,
    LongTaskExecutionResult,
    LongTaskExecutionStatus,
    TaskAdmissionDecision,
)
from purra.testing import (
    assert_long_task_dispatcher_conforms,
    assert_task_orchestration_conforms,
)


def _request() -> AgentRunRequest:
    return AgentRunRequest(
        messages=(AgentMessage(
            role=MessageRole.USER,
            content="Complete the portable task.",
        ),),
        model=ModelRequest(provider="portable", model="portable-model"),
        domain_context=DomainContext(namespace="portable.task"),
        session_id="portable-session",
        mode="agent",
    )


def _plan() -> ExecutionPlan:
    return ExecutionPlan(
        title="Portable task",
        task_spec=TaskSpec(goal="Complete two bounded steps"),
        steps=(
            TaskStep(
                id="prepare",
                title="Prepare",
                type=StepType.ANALYZE,
                executor=StepExecutor.MODEL,
            ),
            TaskStep(
                id="deliver",
                title="Deliver",
                type=StepType.WRITE,
                executor=StepExecutor.MODEL,
                depends_on=("prepare",),
            ),
        ),
    )


def _decision(mode: ExecutionMode) -> TaskAdmissionDecision:
    if mode is ExecutionMode.DURABLE:
        return TaskAdmissionDecision(
            mode=mode,
            reason_code="portable_durable",
            covered_step_ids=("prepare", "deliver"),
            execution_recipe=ExecutionRecipe(
                kind="portable.task",
                steps=(
                    ExecutionRecipeStep(id="prepare", kind="model"),
                    ExecutionRecipeStep(
                        id="deliver",
                        kind="model",
                        depends_on=("prepare",),
                    ),
                ),
            ),
        )
    return TaskAdmissionDecision(
        mode=mode,
        reason_code=f"portable_{mode.value}",
        message=f"Portable {mode.value} decision.",
    )


class _Evaluator:
    def __init__(self, decision: TaskAdmissionDecision) -> None:
        self.decision = decision

    async def evaluate(self, request, plan, signal=None):
        del request, plan, signal
        return self.decision


class _Dispatcher:
    def __init__(self) -> None:
        self.dispatches = 0
        self.executions = 0

    async def dispatch(
        self,
        request,
        plan,
        decision,
        *,
        run_id,
        signal=None,
    ):
        del request, plan, run_id, signal
        self.dispatches += 1
        return LongTaskDispatchReceipt(
            task_id="portable-task",
            message="Portable task accepted.",
            admission=decision,
        )

    async def execute(
        self,
        task_id,
        *,
        run_id,
        observer,
        signal=None,
    ):
        del run_id, observer, signal
        self.executions += 1
        return LongTaskExecutionResult(
            task_id=task_id,
            status=LongTaskExecutionStatus.COMPLETED,
            final_response="Portable task completed.",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", tuple(ExecutionMode))
async def test_all_admission_modes_pass_shared_orchestration_conformance(mode):
    dispatcher = _Dispatcher() if mode is ExecutionMode.DURABLE else None
    decision = await assert_task_orchestration_conforms(
        evaluator=_Evaluator(_decision(mode)),
        request=_request(),
        plan=_plan(),
        dispatcher=dispatcher,
    )

    assert decision.mode is mode
    if dispatcher is not None:
        assert (dispatcher.dispatches, dispatcher.executions) == (1, 1)


@pytest.mark.asyncio
async def test_shared_orchestration_rejects_partial_durable_coverage():
    invalid = TaskAdmissionDecision(
        mode=ExecutionMode.DURABLE,
        reason_code="portable_partial",
        covered_step_ids=("deliver",),
        execution_recipe=ExecutionRecipe(
            kind="portable.partial",
            steps=(ExecutionRecipeStep(id="deliver", kind="model"),),
        ),
    )

    with pytest.raises(ContractViolationError, match="cover every planned step"):
        await assert_task_orchestration_conforms(
            evaluator=_Evaluator(invalid),
            request=_request(),
            plan=_plan(),
        )


@pytest.mark.asyncio
async def test_dispatcher_passes_idempotent_handoff_and_continuation_conformance():
    dispatcher = _Dispatcher()
    receipt, result, updates = await assert_long_task_dispatcher_conforms(
        dispatcher=dispatcher,
        request=_request(),
        plan=_plan(),
        admission=_decision(ExecutionMode.DURABLE),
    )

    assert receipt.task_id == result.task_id == "portable-task"
    assert updates == ()
    assert (dispatcher.dispatches, dispatcher.executions) == (2, 2)


def test_run_recovery_snapshot_is_the_only_continuation_plan_authority():
    plan = _plan()
    admission = _decision(ExecutionMode.DURABLE)
    source = RunRecoverySnapshot(
        run_id="source-run",
        status=RunStatus.CANCELED,
        execution_plan=plan,
    )
    continuation = DurableTaskContinuation(
        source=source,
        continuation_command="continue-1",
        receipt=LongTaskDispatchReceipt(
            task_id="portable-task",
            message="Resume portable task.",
            admission=admission,
        ),
    )

    assert continuation.source.execution_plan is plan
    assert continuation.receipt.admission is admission
    assert not hasattr(continuation, "plan")
    assert not hasattr(continuation, "admission")
    assert not hasattr(continuation, "source_root_run_id")


def test_continuation_cannot_change_the_source_agent_preset():
    continuation = DurableTaskContinuation(
        source=RunRecoverySnapshot(
            run_id="source-run",
            status=RunStatus.CANCELED,
            execution_plan=_plan(),
            agent_preset_snapshot={"id": "persisted-preset"},
        ),
        continuation_command="continue-1",
        receipt=LongTaskDispatchReceipt(
            task_id="portable-task",
            message="Resume portable task.",
            admission=_decision(ExecutionMode.DURABLE),
        ),
    )

    with pytest.raises(ValueError, match="source AgentPreset"):
        AgentCoreRunOptions(durable_continuation=continuation)


def test_run_plan_replacement_cannot_drop_execution_lineage():
    plan = _plan()
    commit = RunCommit(replace_plan=plan)

    assert commit.replace_plan is plan
    with pytest.raises(TypeError, match="unexpected keyword"):
        RunCommit(replace_steps=plan.steps)  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="cannot replace"):
        RunCommit(
            replace_plan=plan,
            step_updates=(TaskStepUpdate(
                step_id="prepare",
                status=StepStatus.DONE,
            ),),
        )
