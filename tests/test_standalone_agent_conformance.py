"""Executable proof that a non-product host can compose PurrA by public ports."""

from __future__ import annotations

from dataclasses import replace

import pytest

from purra.api import (
    AgentComponentBinding,
    AgentCore,
    AgentCoreRunOptions,
    AgentModelTaskRunner,
    AgentPreset,
    ContextStrategy,
    DelegationPolicy,
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
    PlanningMode,
    PlanningResult,
    RunStatus,
    RuntimeLimits,
    StepExecutor,
    StepType,
    TaskSpec,
    ToolCallDelta,
    ToolHandlerResult,
    ToolPlanningRequirement,
    ToolPolicy,
    ToolSchema,
    WorkPlan,
    WorkStep,
)
from purra.model_protocol import generic_capability_snapshot
from purra.errors import ContractViolationError
from purra.output import (
    PublicFact,
    PublicFactBundle,
    PublicPresentationMode,
    ResponseTransactionMode,
    ResponseTransactionPolicy,
)
from purra.ports import ToolRegistration
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
        self.invocations = []

    async def stream(self, messages, invocation, signal=None):
        del signal
        self.messages.append(tuple(messages))
        self.invocations.append(invocation)
        return ModelStream(
            chunks=_chunks(self.response),
            model="portable-model",
            applied_output_limit=invocation.output_limit.max_tokens,
        )

    async def complete(self, messages, invocation, signal=None):
        del messages, signal
        return ModelCompletion(
            message=AgentMessage(
                role=MessageRole.ASSISTANT,
                content=self.response,
            ),
            model="portable-model",
            applied_output_limit=invocation.output_limit.max_tokens,
            finish_reason=ModelFinishReason.STOP,
        )


class _ScriptedToolGateway(_Gateway):
    def __init__(self, steps) -> None:
        super().__init__("planned answer")
        self.steps = list(steps)

    async def stream(self, messages, invocation, signal=None):
        del signal
        self.messages.append(tuple(messages))
        self.invocations.append(invocation)
        step = self.steps.pop(0)

        async def chunks():
            if step is None:
                yield ModelStreamChunk(
                    content_delta=self.response,
                    finish_reason=ModelFinishReason.STOP,
                )
                return
            tool_name, public_text = (
                step if isinstance(step, tuple) else (step, None)
            )
            if public_text:
                yield ModelStreamChunk(content_delta=public_text)
            yield ModelStreamChunk(
                tool_call_deltas=(ToolCallDelta(
                    index=0,
                    id=f"call-{tool_name}-{len(self.invocations)}",
                    name=tool_name,
                    arguments_fragment="{}",
                ),),
                finish_reason=ModelFinishReason.TOOL_CALLS,
            )

        return ModelStream(
            chunks=chunks(),
            model="portable-model",
            applied_output_limit=invocation.output_limit.max_tokens,
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
        reasoning_mode=None,
    ):
        del request, capabilities, signal, run_id, turn_id, reasoning_mode
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


class _DurableFacts:
    async def facts_for(self, run_id, result):
        assert result.run_id == run_id
        assert result.final_response == "portable durable answer"
        return PublicFactBundle(facts=(
            PublicFact("result", "committed durable evidence"),
        ))


def _request() -> AgentRunRequest:
    return AgentRunRequest(
        messages=(AgentMessage(role=MessageRole.USER, content="Answer portably."),),
        model=ModelRequest(
            provider="portable",
            model="portable-model",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="portable:model",
                max_call_output_tokens=1_024,
            ),
            options={"max_tokens": 512},
        ),
        domain_context=DomainContext(namespace="portable.demo"),
        context_window=8_192,
    )


def _core(
    *,
    gateway,
    context,
    profile=None,
    adapters=None,
    runtime_limits=None,
    tool_catalog=None,
    delegation=False,
):
    adapters = adapters or InMemoryAgentAdapters()
    resolved_profile = profile or ExecutionProfile()
    bindings = {
        "contextProvider": AgentComponentBinding("portable.context", "1"),
    }
    for role, value in (
        ("planner", resolved_profile.planner),
        ("planningPolicy", resolved_profile.planning_policy),
        ("taskAdmissionEvaluator", resolved_profile.task_admission_evaluator),
        ("longTaskDispatcher", resolved_profile.long_task_dispatcher),
    ):
        if value is not None and (
            role != "planningPolicy" or resolved_profile.planning_enabled
        ):
            bindings[role] = AgentComponentBinding(f"portable.{role}", "1")
    return AgentCore(
        model_gateway=gateway,
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        delegation_repository=(adapters.delegations if delegation else None),
        tool_idempotency_gateway=(adapters.idempotency if delegation else None),
        preset=AgentPreset(
            id="portable",
            revision="1",
            tool_catalog=tool_catalog or InMemoryToolCatalog(()),
            context_provider=context,
            runtime_limits=runtime_limits or RuntimeLimits(max_run_output_tokens=None),
            execution_profile=resolved_profile,
            delegation_policy=(DelegationPolicy() if delegation else None),
            component_bindings=bindings,
            prompt_sections=(PromptSection(
                name="identity",
                order=-100,
                text="Be concise, explicit, and calm.",
            ),),
        ),
    )


class _BudgetExhaustingGateway(_Gateway):
    def __init__(self) -> None:
        super().__init__("")
        self.calls = 0

    async def stream(self, messages, invocation, signal=None):
        del messages, signal
        self.calls += 1

        async def chunks():
            yield ModelStreamChunk(reasoning_delta="private reasoning")
            yield ModelStreamChunk(
                content_delta="answer",
                finish_reason=ModelFinishReason.STOP,
            )

        return ModelStream(
            chunks=chunks(),
            model="portable-model",
            applied_output_limit=invocation.output_limit.max_tokens,
        )


class _UnknownStreamFailureGateway(_Gateway):
    def __init__(self) -> None:
        super().__init__("")

    async def stream(self, messages, invocation, signal=None):
        del messages, signal

        async def chunks():
            raise OSError("socket vanished")
            yield

        return ModelStream(
            chunks=chunks(),
            model="portable-model",
            applied_output_limit=invocation.output_limit.max_tokens,
        )


class _RetryableStreamFailureGateway(_Gateway):
    def __init__(self) -> None:
        super().__init__("")
        self.calls = 0

    async def stream(self, messages, invocation, signal=None):
        del messages, signal
        self.calls += 1

        async def chunks():
            if self.calls == 1:
                yield ModelStreamChunk(reasoning_delta="private partial")
                return
            yield ModelStreamChunk(
                content_delta="recovered",
                finish_reason=ModelFinishReason.STOP,
            )

        return ModelStream(
            chunks=chunks(),
            model="portable-model",
            applied_output_limit=invocation.output_limit.max_tokens,
        )


@pytest.mark.asyncio
async def test_portable_auto_agent_without_planner_runs_through_public_submit():
    gateway = _Gateway("portable reactive answer")
    context = _Context()
    core = _core(gateway=gateway, context=context)
    try:
        result = await (await core.submit(replace(
            _request(), context_window=32_768
        ))).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "portable reactive answer"
    assert context.single_pass_calls == 1
    assert gateway.messages[0][0].content == "Be concise, explicit, and calm."


@pytest.mark.asyncio
async def test_reactive_run_bypasses_configured_planner_and_staged_context():
    gateway = _Gateway("portable reactive answer")
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
        result = await (await core.submit(replace(
            _request(), planning_mode=PlanningMode.REACTIVE
        ))).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert planner.calls == 0
    assert context.single_pass_calls == 1
    assert (context.planning_calls, context.task_calls) == (0, 0)


@pytest.mark.asyncio
async def test_auto_direct_answer_uses_one_model_call_and_no_planner_call():
    async def lookup(state, arguments, signal=None):
        del state, arguments, signal
        return ToolHandlerResult(content="found", effect_state="not_started")

    catalog = InMemoryToolCatalog((ToolRegistration(
        schema=ToolSchema(
            name="lookup",
            description="Look up a fact.",
            parameters={"type": "object", "properties": {}},
        ),
        handler=lookup,
        policy=ToolPolicy(mode="read", title="Lookup"),
    ),))
    gateway = _Gateway("portable direct answer")
    context = _Context()
    planner = _Planner()
    adapters = InMemoryAgentAdapters()
    core = _core(
        gateway=gateway,
        context=context,
        adapters=adapters,
        tool_catalog=catalog,
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
            context_strategy=ContextStrategy.STAGED,
        ),
    )
    try:
        handle = await core.submit(replace(
            _request(),
            context_window=32_768,
            tools_enabled=True,
        ))
        result = await handle.wait()
        events = await adapters.outputs.list_events(
            result.run_id,
            after_sequence=0,
        )
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "portable direct answer"
    assert planner.calls == 0
    assert len(gateway.invocations) == 1
    assert {tool.name for tool in gateway.invocations[0].tools} == {
        "lookup",
        "request_plan",
    }
    assert (context.single_pass_calls, context.planning_calls) == (1, 0)
    assert [
        event.payload["delta"]
        for event in events
        if event.kind.value == "provider.content_delta"
        and event.channel.value == "final"
        and event.visibility.value == "public"
    ] == ["portable direct answer"]


@pytest.mark.asyncio
async def test_auto_does_not_advertise_control_without_tool_calling_support():
    gateway = _Gateway("portable direct answer")
    planner = _Planner()
    core = _core(
        gateway=gateway,
        context=_Context(),
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
        ),
    )
    try:
        result = await (await core.submit(
            _request(),
            options=AgentCoreRunOptions(model_supports_tools=False),
        )).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert gateway.invocations[0].tools == ()
    assert planner.calls == 0


@pytest.mark.asyncio
async def test_auto_request_plan_promotes_before_execution_and_stays_private():
    public_intent = "I will inspect the scope before planning the remaining work."
    gateway = _ScriptedToolGateway((("request_plan", public_intent), None))
    context = _Context()
    planner = _Planner()
    adapters = InMemoryAgentAdapters()
    core = _core(
        gateway=gateway,
        context=context,
        adapters=adapters,
        delegation=True,
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
            context_strategy=ContextStrategy.STAGED,
        ),
    )
    try:
        handle = await core.submit(replace(
            _request(),
            context_window=32_768,
            tools_enabled=True,
        ))
        result = await handle.wait()
        events = await adapters.outputs.list_events(result.run_id, after_sequence=0)
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "planned answer"
    assert planner.calls == 1
    assert len(gateway.invocations) == 2
    assert {tool.name for tool in gateway.invocations[0].tools} == {
        "delegateToAgents",
        "request_plan",
    }
    assert gateway.invocations[1].tools == ()
    assert (context.single_pass_calls, context.planning_calls, context.task_calls) == (
        1,
        1,
        1,
    )
    public_payloads = "\n".join(
        str(event.payload)
        for event in events
        if event.visibility.value == "public"
    )
    assert "request_plan" not in public_payloads
    assert public_intent in public_payloads


@pytest.mark.asyncio
async def test_auto_required_tool_plans_before_effect_and_reactive_fails_closed():
    tool_calls = 0

    async def publish(state, arguments, signal=None):
        nonlocal tool_calls
        del state, arguments, signal
        tool_calls += 1
        return ToolHandlerResult(content="published", effect_state="not_started")

    registration = ToolRegistration(
        schema=ToolSchema(
            name="publish",
            description="Publish the prepared result.",
            parameters={"type": "object", "properties": {}},
        ),
        handler=publish,
        policy=ToolPolicy(mode="read", title="Publish"),
        planning_requirement=ToolPlanningRequirement.REQUIRED,
    )
    catalog = InMemoryToolCatalog((registration,))
    planner = _Planner()
    planner.plan = WorkPlan(
        title="Publish safely",
        steps=(WorkStep(
            id="publish",
            title="Publish",
            type=StepType.WRITE,
            executor=StepExecutor.TOOL,
            capability_names=("publish",),
        ),),
    )
    auto_gateway = _ScriptedToolGateway(("publish", "publish", None))
    auto = _core(
        gateway=auto_gateway,
        context=_Context(),
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
        ),
        tool_catalog=catalog,
    )
    try:
        result = await (await auto.submit(replace(
            _request(), tools_enabled=True, context_window=32_768
        ))).wait()
    finally:
        await auto.close()

    assert result.status is RunStatus.DONE
    assert planner.calls == 1
    assert tool_calls == 1

    reactive_planner = _Planner()
    reactive = _core(
        gateway=_ScriptedToolGateway(("publish",)),
        context=_Context(),
        profile=ExecutionProfile(
            planner=reactive_planner,
            planning_policy=_AlwaysPlan(),
        ),
        tool_catalog=catalog,
    )
    try:
        result = await (await reactive.submit(replace(
            _request(),
            tools_enabled=True,
            context_window=32_768,
            planning_mode=PlanningMode.REACTIVE,
        ))).wait()
    finally:
        await reactive.close()

    assert result.status is RunStatus.FAILED
    assert result.error == "planning_required"
    assert reactive_planner.calls == 0
    assert tool_calls == 1


@pytest.mark.asyncio
async def test_require_tool_call_does_not_assume_provider_required_choice_support():
    tool_calls = 0

    async def lookup(state, arguments, signal=None):
        nonlocal tool_calls
        del state, arguments, signal
        tool_calls += 1
        return ToolHandlerResult(content="found", effect_state="not_started")

    gateway = _ScriptedToolGateway(("lookup", None))
    core = _core(
        gateway=gateway,
        context=_Context(),
        tool_catalog=InMemoryToolCatalog((ToolRegistration(
            schema=ToolSchema(
                name="lookup",
                description="Look up data.",
                parameters={"type": "object", "properties": {}},
            ),
            handler=lookup,
            policy=ToolPolicy(mode="read", title="Lookup"),
        ),)),
    )
    try:
        result = await (await core.submit(
            replace(_request(), tools_enabled=True, context_window=32_768),
            options=AgentCoreRunOptions(
                require_tool_call=True,
                force_planned_tool_choice=False,
            ),
        )).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert tool_calls == 1
    assert gateway.invocations[0].tool_choice.value == "auto"


@pytest.mark.asyncio
async def test_auto_plans_remaining_work_before_required_tool_effect():
    calls = []

    async def run_tool(state, arguments, signal=None):
        del state, arguments, signal
        calls.append("executed")
        return ToolHandlerResult(content="ok", effect_state="not_started")

    catalog = InMemoryToolCatalog((
        ToolRegistration(
            schema=ToolSchema(
                name="lookup",
                description="Look up data.",
                parameters={"type": "object", "properties": {}},
            ),
            handler=run_tool,
            policy=ToolPolicy(mode="read", title="Lookup"),
        ),
        ToolRegistration(
            schema=ToolSchema(
                name="publish",
                description="Publish data.",
                parameters={"type": "object", "properties": {}},
            ),
            handler=run_tool,
            policy=ToolPolicy(mode="read", title="Publish"),
            planning_requirement=ToolPlanningRequirement.REQUIRED,
        ),
    ))
    planner = _Planner()
    planner.plan = WorkPlan(
        title="Publish remaining work",
        steps=(WorkStep(
            id="publish",
            title="Publish",
            type=StepType.WRITE,
            executor=StepExecutor.TOOL,
            capability_names=("publish",),
        ),),
    )
    gateway = _ScriptedToolGateway(("lookup", "publish", "publish", None))
    core = _core(
        gateway=gateway,
        context=_Context(),
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
        ),
        tool_catalog=catalog,
    )
    try:
        result = await (await core.submit(replace(
            _request(), tools_enabled=True, context_window=32_768
        ))).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert planner.calls == 1
    assert calls == ["executed", "executed"]
    assert "request_plan" in {
        tool.name for tool in gateway.invocations[0].tools
    }
    assert "request_remaining_plan" not in {
        tool.name for tool in gateway.invocations[0].tools
    }
    assert "request_plan" not in {
        tool.name for tool in gateway.invocations[1].tools
    }
    assert "request_remaining_plan" in {
        tool.name for tool in gateway.invocations[1].tools
    }


@pytest.mark.asyncio
async def test_auto_model_can_request_a_plan_for_remaining_work_privately():
    calls = []

    async def lookup(state, arguments, signal=None):
        del state, arguments, signal
        calls.append("lookup")
        return ToolHandlerResult(content="evidence", effect_state="not_started")

    public_intent = "I found evidence and need a plan for the remaining work."
    gateway = _ScriptedToolGateway((
        "lookup",
        ("request_remaining_plan", public_intent),
        None,
    ))
    adapters = InMemoryAgentAdapters()
    planner = _Planner()
    core = _core(
        gateway=gateway,
        context=_Context(),
        adapters=adapters,
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
        ),
        tool_catalog=InMemoryToolCatalog((ToolRegistration(
            schema=ToolSchema(
                name="lookup",
                description="Look up data.",
                parameters={"type": "object", "properties": {}},
            ),
            handler=lookup,
            policy=ToolPolicy(mode="read", title="Lookup"),
        ),)),
    )
    try:
        result = await (await core.submit(replace(
            _request(), tools_enabled=True, context_window=32_768
        ))).wait()
        events = await adapters.outputs.list_events(
            result.run_id,
            after_sequence=0,
        )
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert planner.calls == 1
    assert calls == ["lookup"]
    public_payloads = "\n".join(
        str(event.payload)
        for event in events
        if event.visibility.value == "public"
    )
    assert "request_remaining_plan" not in public_payloads
    commentary = next(
        event for event in events
        if event.visibility.value == "public"
        and public_intent in str(event.payload)
    )
    planning_started = next(
        event for event in events
        if event.kind.value == "operation.started"
        and event.payload.get("kind") == "planning"
    )
    assert commentary.sequence < planning_started.sequence


@pytest.mark.asyncio
async def test_auto_planning_activation_uses_the_run_model_round_budget():
    planner = _Planner()
    core = _core(
        gateway=_ScriptedToolGateway(("request_plan",)),
        context=_Context(),
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
        ),
        runtime_limits=RuntimeLimits(
            max_model_rounds=1,
            max_run_output_tokens=None,
        ),
    )
    try:
        result = await (await core.submit(replace(
            _request(), context_window=32_768
        ))).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.FAILED
    assert result.error == "planning_activation_budget_exhausted"
    assert planner.calls == 0


@pytest.mark.asyncio
async def test_planned_run_fails_closed_without_a_configured_planner():
    gateway = _Gateway("must not run")
    context = _Context()
    core = _core(gateway=gateway, context=context)
    try:
        result = await (await core.submit(replace(
            _request(), planning_mode=PlanningMode.PLANNED
        ))).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.FAILED
    assert result.error == "planning_unavailable"
    assert gateway.messages == []
    assert context.single_pass_calls == 0


@pytest.mark.asyncio
async def test_provider_output_budget_failure_reaches_run_without_retry():
    adapters = InMemoryAgentAdapters()
    gateway = _BudgetExhaustingGateway()
    core = _core(
        gateway=gateway,
        context=_Context(),
        adapters=adapters,
        runtime_limits=RuntimeLimits(max_run_output_tokens=None, max_provider_output_bytes=1),
    )
    try:
        result = await (await core.submit(_request())).wait()
        events = await adapters.outputs.list_events(
            result.run_id,
            after_sequence=0,
        )
    finally:
        await core.close()

    assert result.status is RunStatus.FAILED
    assert result.error == "runtime_budget_exceeded", "\n".join(
        f"{event.kind.value} status={event.payload.get('status')} "
        f"error={event.payload.get('errorCode')} "
        f"event={event.payload.get('eventType')}"
        for event in events
    )
    assert gateway.calls == 1
    assert sum(event.kind.value == "stream.aborted" for event in events) == 1
    assert sum(
        event.kind.value == "run.lifecycle"
        and event.payload.get("status") == RunStatus.FAILED.value
        for event in events
    ) == 1


@pytest.mark.asyncio
async def test_unknown_provider_stream_failure_remains_generic():
    core = _core(
        gateway=_UnknownStreamFailureGateway(),
        context=_Context(),
    )
    try:
        result = await (await core.submit(_request())).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.FAILED
    assert result.error == "model_stream_error"


@pytest.mark.asyncio
async def test_retryable_gateway_stream_failure_keeps_recovery_path():
    gateway = _RetryableStreamFailureGateway()
    core = _core(gateway=gateway, context=_Context())
    try:
        result = await (await core.submit(_request())).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "recovered"
    assert gateway.calls == 2


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
            runtime_limits=RuntimeLimits(max_run_output_tokens=None),
            id="portable",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider_factory=factory,
            component_bindings={
                "contextProvider": AgentComponentBinding(
                    "portable.context-factory",
                    "1",
                ),
            },
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
    adapters = InMemoryAgentAdapters()
    core = _core(
        gateway=gateway,
        context=context,
        adapters=adapters,
        profile=ExecutionProfile(
            planner=planner,
            planning_policy=_AlwaysPlan(),
            context_strategy=ContextStrategy.STAGED,
        ),
    )
    try:
        result = await (await core.submit(replace(
            _request(), planning_mode=PlanningMode.PLANNED
        ))).wait()
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
    planning_trace = next(
        trace
        for trace in adapters.runs._state.runs[result.run_id].traces
        if trace.stage == "planning"
    )
    assert planning_trace.details["workPlanStepCount"] == 1
    assert planning_trace.details["executionPlanStepCount"] == 1
    assert planning_trace.details["workPlanModelStepCount"] == 1
    assert planning_trace.details["workPlanToolStepCount"] == 0
    assert planning_trace.details["executionToolStepCount"] == 0
    assert planning_trace.details["plannerRepairCount"] == 0


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
        result = await (await core.submit(replace(
            _request(), planning_mode=PlanningMode.PLANNED
        ))).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "portable durable answer"
    assert dispatcher.dispatches == 1
    assert gateway.messages == []


@pytest.mark.asyncio
async def test_portable_durable_agent_presents_committed_result_before_terminal():
    gateway = _Gateway("model-authored final synthesis")
    context = _Context()
    dispatcher = _Dispatcher()
    adapters = InMemoryAgentAdapters()
    core = _core(
        gateway=gateway,
        context=context,
        adapters=adapters,
        profile=ExecutionProfile(
            planner=_Planner(),
            planning_policy=_AlwaysPlan(),
            context_strategy=ContextStrategy.STAGED,
            task_admission_evaluator=_Admission(),
            long_task_dispatcher=dispatcher,
        ),
    )
    try:
        result = await (await core.submit(
            replace(_request(), planning_mode=PlanningMode.PLANNED),
            options=AgentCoreRunOptions(
                response_transaction_policy=ResponseTransactionPolicy(
                    mode=ResponseTransactionMode.VALIDATED_RESULT,
                    public_presentation=PublicPresentationMode.MODEL_LIVE,
                ),
                committed_result_facts_provider=_DurableFacts(),
            ),
        )).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "model-authored final synthesis"
    assert dispatcher.dispatches == 1
    assert len(gateway.messages) == 1
    assert any(
        "committed durable evidence" in message.content
        for message in gateway.messages[0]
    )
    terminal = [
        event
        for event in await adapters.outputs.list_events(
            result.run_id,
            after_sequence=0,
        )
        if event.kind.value == "run.lifecycle"
        and event.payload.get("status") in {"done", "failed", "canceled"}
    ]
    assert len(terminal) == 1


@pytest.mark.asyncio
async def test_durable_dispatch_contract_error_commits_failed_run():
    class _FailingDispatcher(_Dispatcher):
        async def dispatch(self, *args, **kwargs):
            del args, kwargs
            raise ContractViolationError(
                "Invalid persisted durable task contract",
                code="runtime_limits_invalid",
            )

    adapters = InMemoryAgentAdapters()
    core = _core(
        gateway=_Gateway("must not be used"),
        context=_Context(),
        adapters=adapters,
        profile=ExecutionProfile(
            planner=_Planner(),
            planning_policy=_AlwaysPlan(),
            context_strategy=ContextStrategy.STAGED,
            task_admission_evaluator=_Admission(),
            long_task_dispatcher=_FailingDispatcher(),
        ),
    )
    try:
        handle = await core.submit(replace(
            _request(), planning_mode=PlanningMode.PLANNED
        ))
        result = await handle.wait()
        events = [event async for event in handle.subscribe()]
    finally:
        await core.close()

    assert result.status is RunStatus.FAILED
    assert result.error == "runtime_limits_invalid"
    terminal = [
        event for event in events
        if event.kind.value == "run.lifecycle"
        and event.payload.get("status") in {"done", "failed", "canceled"}
    ]
    assert len(terminal) == 1
    assert terminal[0].payload == {
        "status": "failed",
        "error": "runtime_limits_invalid",
    }
