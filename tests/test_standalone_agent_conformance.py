"""Executable proof that a non-product host can compose PurrA by public ports."""

from __future__ import annotations

from dataclasses import replace

import pytest

from purra.api import (
    AgentCore,
    AgentModelTaskRunner,
    AgentPreset,
    ContextStrategy,
    ExecutionProfile,
    InMemoryAgentAdapters,
    PromptSection,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    ContextBlock,
    ContextBundle,
    DomainContext,
    ExecutionRecipe,
    ExecutionRecipeStep,
    MessageRole,
    ModelCompletion,
    ModelFinishReason,
    ModelRequest,
    ModelStream,
    ModelStreamChunk,
    PlanningConstraints,
    PlanningKind,
    PlanningResult,
    RunStatus,
    StepExecutor,
    StepType,
    TaskSpec,
    WorkPlan,
    WorkStep,
)
from purra.model_protocol import generic_capability_snapshot
from purra.tools import InMemoryToolCatalog
from purra.task_admission import (
    ExecutionMode,
    LongTaskDispatchReceipt,
    LongTaskExecutionResult,
    LongTaskExecutionStatus,
    TaskAdmissionDecision,
)


async def _chunks(text: str):
    yield ModelStreamChunk(
        content_delta=text,
        finish_reason=ModelFinishReason.STOP,
    )


class _Gateway:
    def __init__(self, response: str) -> None:
        self.response = response
        self.messages = []

    async def stream(self, messages, invocation, signal=None):
        del invocation, signal
        self.messages.append(tuple(messages))
        return ModelStream(chunks=_chunks(self.response), model="portable-model")

    async def complete(self, messages, invocation, signal=None):
        del messages, invocation, signal
        return ModelCompletion(
            message=AgentMessage(
                role=MessageRole.ASSISTANT,
                content=self.response,
            ),
            model="portable-model",
            finish_reason=ModelFinishReason.STOP,
        )


class _Context:
    def __init__(self) -> None:
        self.single_pass_calls = 0
        self.planning_calls = 0
        self.task_calls = 0

    async def build_context(self, request, budget, signal=None):
        del request, budget, signal
        self.single_pass_calls += 1
        return ContextBundle(blocks=(ContextBlock(
            name="portable-context",
            content="Prefer small, verifiable answers.",
            token_count=6,
            untrusted=False,
        ),))

    async def build_planning_context(self, request, budget, signal=None):
        del request, budget, signal
        self.planning_calls += 1
        return ContextBundle()

    async def build_task_context(self, request, budget, task, signal=None):
        del request, budget, task, signal
        self.task_calls += 1
        return ContextBundle(blocks=(ContextBlock(
            name="portable-task-context",
            content="The portable fact is 42.",
            token_count=6,
            untrusted=False,
        ),))


class _ContextFactory:
    def __init__(self, context: _Context) -> None:
        self.context = context
        self.runners = []

    def __call__(self, runner):
        assert isinstance(runner, AgentModelTaskRunner)
        self.runners.append(runner)
        return self.context


class _AlwaysPlan:
    def planning_constraints(self, request, capabilities):
        del request, capabilities
        return PlanningConstraints()

    def should_plan(self, request, capabilities):
        del request, capabilities
        return True


class _Planner:
    def __init__(self) -> None:
        self.calls = 0
        self.plan = WorkPlan(
            title="Portable task",
            task_spec=TaskSpec(goal="Answer from portable context"),
            steps=(WorkStep(
                id="answer",
                title="Answer",
                type=StepType.WRITE,
                executor=StepExecutor.MODEL,
            ),),
        )

    async def create_plan(
        self,
        request,
        capabilities,
        signal=None,
        *,
        run_id=None,
        turn_id=None,
    ):
        del request, capabilities, signal, run_id, turn_id
        self.calls += 1
        return PlanningResult(
            kind=PlanningKind.PLANNED,
            work_plan=self.plan,
        )


class _Admission:
    async def evaluate(self, request, plan, signal=None):
        del request, signal
        return TaskAdmissionDecision(
            mode=ExecutionMode.DURABLE,
            reason_code="portable_durable",
            covered_step_ids=tuple(step.id for step in plan.steps),
            execution_recipe=ExecutionRecipe(
                kind="portable.answer",
                steps=(ExecutionRecipeStep(
                    id="answer-unit",
                    kind="model",
                    plan_step_id="answer",
                ),),
            ),
        )


class _Dispatcher:
    def __init__(self) -> None:
        self.dispatches = 0

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
        return LongTaskExecutionResult(
            task_id=task_id,
            status=LongTaskExecutionStatus.COMPLETED,
            final_response="portable durable answer",
        )


def _request() -> AgentRunRequest:
    return AgentRunRequest(
        messages=(AgentMessage(role=MessageRole.USER, content="Answer portably."),),
        model=ModelRequest(
            provider="portable",
            model="portable-model",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="portable:model",
                max_output_tokens=1_024,
            ),
            options={"max_tokens": 512},
        ),
        domain_context=DomainContext(namespace="portable.demo"),
        context_window=8_192,
    )


def _core(*, gateway, context, profile=None):
    adapters = InMemoryAgentAdapters()
    return AgentCore(
        model_gateway=gateway,
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=AgentPreset(
            id="portable",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=context,
            execution_profile=profile or ExecutionProfile(),
            prompt_sections=(PromptSection(
                name="identity",
                order=-100,
                text="Be concise, explicit, and calm.",
            ),),
        ),
    )


@pytest.mark.asyncio
async def test_portable_reactive_agent_runs_through_public_submit():
    gateway = _Gateway("portable reactive answer")
    context = _Context()
    core = _core(gateway=gateway, context=context)
    try:
        result = await (await core.submit(_request())).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "portable reactive answer"
    assert context.single_pass_calls == 1
    assert gateway.messages[0][0].content == "Be concise, explicit, and calm."


@pytest.mark.asyncio
async def test_preset_context_factory_is_resolved_once_per_run():
    gateway = _Gateway("portable answer")
    context = _Context()
    factory = _ContextFactory(context)
    adapters = InMemoryAgentAdapters()
    core = AgentCore(
        model_gateway=gateway,
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=AgentPreset(
            id="portable",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider_factory=factory,
        ),
    )
    try:
        first = await (await core.submit(_request())).wait()
        second = await (await core.submit(_request())).wait()
    finally:
        await core.close()

    assert (first.status, second.status) == (RunStatus.DONE, RunStatus.DONE)
    assert len(factory.runners) == 2
    assert factory.runners[0] is not factory.runners[1]


@pytest.mark.asyncio
async def test_portable_planned_agent_uses_staged_task_context():
    gateway = _Gateway("portable planned answer")
    context = _Context()
    planner = _Planner()
    core = _core(
        gateway=gateway,
        context=context,
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
            context_strategy=ContextStrategy.STAGED,
        ),
    )
    try:
        result = await (await core.submit(_request())).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "portable planned answer"
    assert planner.calls == 1
    assert (context.planning_calls, context.task_calls) == (1, 1)
    assert any(
        "The portable fact is 42." in message.content
        for message in gateway.messages[0]
    )


@pytest.mark.asyncio
async def test_portable_durable_agent_hands_off_without_product_runtime():
    gateway = _Gateway("must not be used")
    context = _Context()
    planner = _Planner()
    dispatcher = _Dispatcher()
    core = _core(
        gateway=gateway,
        context=context,
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
            context_strategy=ContextStrategy.STAGED,
            task_admission_evaluator=_Admission(),
            long_task_dispatcher=dispatcher,
        ),
    )
    try:
        result = await (await core.submit(_request())).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "portable durable answer"
    assert dispatcher.dispatches == 1
    assert gateway.messages == []
