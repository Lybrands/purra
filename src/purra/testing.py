"""Reusable, dependency-free conformance checks for PurrA host adapters."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

from purra.adapters import InMemoryAgentAdapters
from purra.artifacts import (
    ArtifactAppendCommand,
    ArtifactCreateCommand,
    ArtifactFinalizeCommand,
    ArtifactMaintenancePolicy,
    ArtifactMutationLease,
    ArtifactOwnerRef,
    ArtifactStatus,
    ArtifactWriteClaimCommand,
)
from purra.artifacts.ports import (
    ArtifactClaimRepository,
    ArtifactMaintenanceRepository,
    ArtifactRepository,
)
from purra.context_orchestration.compaction import ContextCompressionCoordinator
from purra.context_orchestration.contracts import ConversationCompactionResult
from purra.context_orchestration.ledger import (
    ContextCompactionBudget,
    ContextCompactionPhase,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    ContextBudget,
    ContextBundle,
    MessageOrigin,
    MessageRole,
    ModelCompletion,
    ModelFinishReason,
    ModelInvocation,
    ModelStream,
    ModelStreamChunk,
    RunCreateParams,
    RunStatus,
    TaskContextRequest,
    ExecutionPlan,
    ToolBatchOutcome,
    ToolBatchRequest,
    ToolBatchResult,
    ToolCall,
    ToolHandlerResult,
)
from purra.context_strategies import ContextStrategy
from purra.engine.context_capability import ContextCapability
from purra.engine.context_phase import (
    assemble_messages,
    validate_context_allocations,
)
from purra.engine.options import DurableTaskContinuation
from purra.engine.task_orchestration import TaskOrchestrationCapability
from purra.errors import ContractViolationError
from purra.events import AgentEvent, CoreEventType
from purra.model_protocol import classify_model_termination
from purra.long_tasks import (
    LongTaskBudgetLimits,
    LongTaskCreateCommand,
    LongTaskRunRelation,
    LongTaskStatus,
    LongTaskUnitResult,
    LongTaskUnitSpec,
    LongTaskUnitStatus,
    LongTaskUsage,
)
from purra.long_tasks.ports import LongTaskRepository
from purra.output import (
    AgentOutputEventDraft,
    AgentOutputIntent,
    OutputChannel,
    OutputCommitMode,
    OutputEventKind,
    OutputSource,
    OutputStreamSpec,
    OutputVisibility,
    RunLifecycleOutputDraft,
    TERMINAL_STREAM_ABORT_CAUSE,
    TERMINAL_STREAM_ABORT_ERROR_CODE,
)
from purra.output.ports import AgentOutputPublisher, AgentOutputRepository
from purra.ports import (
    ContextCompressionHook,
    ContextProvider,
    DelegationRepository,
    ExecutionLeaseStore,
    ModelGateway,
    RunCommit,
    RunRepository,
    StagedContextProvider,
    ToolExecutionGateway,
    ToolIdempotencyGateway,
)
from purra.run_controller import AgentRunController
from purra.run_recovery import RunRecoverySnapshot
from purra.runtime.model_round import ModelRoundAccumulator
from purra.task_admission import (
    ExecutionMode,
    LongTaskDispatcher,
    LongTaskDispatchReceipt,
    LongTaskExecutionResult,
    LongTaskExecutionUpdate,
    TaskAdmissionDecision,
    TaskAdmissionEvaluator,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _require_violation(awaitable, message: str) -> None:
    try:
        await awaitable
    except ContractViolationError:
        return
    raise AssertionError(message)


async def _require_failure(awaitable, message: str) -> None:
    try:
        await awaitable
    except Exception:
        return
    raise AssertionError(message)


async def assert_execution_lease_store_conforms(
    store: ExecutionLeaseStore,
    create_unowned_run: Callable[[], Awaitable[str]],
) -> None:
    """Exercise lease ownership, cancellation, renewal, and release."""

    assert isinstance(store, ExecutionLeaseStore)
    run_id = await create_unowned_run()
    assert await store.claim(run_id, "owner-a", lease_duration_ms=60_000)
    assert not await store.claim(run_id, "owner-b", lease_duration_ms=60_000)
    lease = await store.get(run_id)
    assert lease is not None
    assert lease.owner_id == "owner-a"
    assert lease.attempt == 1
    assert not await store.renew(
        run_id,
        "owner-b",
        lease_duration_ms=60_000,
    )
    assert await store.renew(run_id, "owner-a", lease_duration_ms=60_000)
    assert await store.request_cancellation(run_id)
    assert not await store.request_cancellation(run_id)
    assert await store.release(run_id, "owner-a")
    released = await store.get(run_id)
    assert released is not None and released.owner_id is None


async def assert_delegation_repository_conforms(
    repository: DelegationRepository,
    create_run: Callable[[], Awaitable[str]],
) -> None:
    """Exercise one-Run delegation lifecycle and batch aggregation."""

    assert isinstance(repository, DelegationRepository)
    run_id = await create_run()
    batch_id = "contract-batch"
    low = await repository.create(
        run_id=run_id,
        batch_id=batch_id,
        agent_name="role-low",
        agent_title="Low priority Agent",
        agent_instruction="Complete the low-priority objective.",
        objective="low priority work",
        priority=1,
    )
    high = await repository.create(
        run_id=run_id,
        batch_id=batch_id,
        agent_name="role-high",
        agent_title="High priority Agent",
        agent_instruction="Complete the high-priority objective.",
        objective="high priority work",
        priority=9,
    )
    started = await repository.start(
        high.id,
        run_id=run_id,
        batch_id=batch_id,
    )
    assert started is not None and started.status.value == "running"
    await _require_failure(
        repository.start(
            "missing",
            run_id=run_id,
            batch_id=batch_id,
        ),
        "a missing delegation id must fail closed",
    )
    assert await repository.complete(
        high.id,
        run_id=run_id,
        batch_id=batch_id,
        result_summary="done",
    )
    rows = await repository.list_for_run(run_id)
    assert {row.id for row in rows} == {low.id, high.id}
    assert high.agent_title == "High priority Agent"
    assert high.agent_instruction == "Complete the high-priority objective."
    assert (await repository.aggregate_batch(run_id, batch_id)).state == "pending"
    assert await repository.cancel_batch(run_id, batch_id) == 1
    assert (await repository.aggregate_batch(run_id, batch_id)).state == "blocked"


async def assert_tool_idempotency_gateway_conforms(
    gateway: ToolIdempotencyGateway,
    run_id: str,
) -> None:
    """Exercise exactly-once tool execution and cached replay."""

    assert isinstance(gateway, ToolIdempotencyGateway)
    calls = 0

    async def operation() -> ToolHandlerResult:
        nonlocal calls
        calls += 1
        return ToolHandlerResult("durable result")

    tool_call = ToolCall(
        id="contract-call",
        name="contract_write",
        arguments_json='{"value":1}',
    )
    first = await gateway.execute_once(run_id, tool_call, operation)
    replay = await gateway.execute_once(run_id, tool_call, operation)
    assert calls == 1
    assert first.from_cache is False
    assert replay.from_cache is True
    assert replay.content == first.content


async def assert_host_adapters_conform(
    *,
    runs: RunRepository,
    outputs: AgentOutputRepository,
    publisher: AgentOutputPublisher,
    session_id: str | int,
) -> None:
    """Exercise the shared Run, output-journal, and wakeup invariants."""

    assert isinstance(runs, RunRepository)
    assert isinstance(outputs, AgentOutputRepository)
    assert isinstance(publisher, AgentOutputPublisher)

    begun, started = await outputs.begin_run_lifecycle(
        RunCreateParams(
            session_id=session_id,
            prompt="adapter conformance",
            mode="agent",
            turn_id="conformance-turn-1",
        ),
        AgentEvent(
            type=CoreEventType.RUN_STARTED,
            payload={"status": RunStatus.RUNNING.value},
        ),
    )
    assert begun.event.run_id == begun.run_id
    assert started.sequence == 1
    assert await outputs.list_events(
        begun.run_id,
        after_sequence=0,
    ) == (started,)

    waiter = asyncio.create_task(
        publisher.wait_for_sequence(
            begun.run_id,
            after_sequence=started.sequence,
        )
    )
    await asyncio.sleep(0)
    fact = AgentOutputEventDraft(
        run_id=begun.run_id,
        turn_id="conformance-turn-1",
        output_stream_id=None,
        invocation_id=None,
        source_event_key=f"conformance:{begun.run_id}:fact",
        source=OutputSource.RUNTIME,
        kind=OutputEventKind.RUNTIME,
        channel=OutputChannel.DIAGNOSTIC,
        visibility=OutputVisibility.PRIVATE,
        payload={"value": 42},
        occurred_at=_now(),
    )
    persisted = await outputs.append_event(fact)
    assert await outputs.append_event(fact) == persisted
    assert not waiter.done()
    await publisher.publish_committed(persisted)
    await asyncio.wait_for(waiter, timeout=1)

    await _require_violation(
        outputs.append_event(AgentOutputEventDraft(
            run_id=begun.run_id,
            turn_id="conformance-turn-1",
            output_stream_id=None,
            invocation_id=None,
            source_event_key=fact.source_event_key,
            source=OutputSource.RUNTIME,
            kind=OutputEventKind.RUNTIME,
            channel=OutputChannel.DIAGNOSTIC,
            visibility=OutputVisibility.PRIVATE,
            payload={"value": 43},
            occurred_at=_now(),
        )),
        "source-event key conflicts must fail closed",
    )

    stream_started = AgentEvent(
        type=CoreEventType.RUN_STARTED,
        payload={"status": RunStatus.RUNNING.value},
    )
    stream_run_id = (await runs.begin(
        RunCreateParams(
            session_id=session_id,
            prompt="stream conformance",
            mode="agent",
            turn_id="conformance-stream-turn",
        ),
        stream_started,
    )).run_id
    stream = OutputStreamSpec(
        output_stream_id=f"conformance-stream-{stream_run_id}",
        run_id=stream_run_id,
        turn_id="conformance-stream-turn",
        invocation_id=f"conformance-invocation-{stream_run_id}",
        intent=AgentOutputIntent.FINAL_PUBLIC,
        commit_mode=OutputCommitMode.LIVE,
    )
    assert await outputs.open_stream(stream) == stream
    assert await outputs.open_stream(stream) == stream
    delta_drafts = tuple(
        AgentOutputEventDraft.public_text(
            run_id=stream_run_id,
            turn_id=stream.turn_id,
            output_stream_id=stream.output_stream_id,
            invocation_id=stream.invocation_id,
            source_event_key=f"conformance:{stream_run_id}:delta:{index}",
            source=OutputSource.PROVIDER,
            channel=OutputChannel.FINAL,
            delta=value,
            occurred_at=_now(),
        )
        for index, value in enumerate(("port", "able"), start=1)
    )
    deltas = await outputs.append_batch(delta_drafts)
    assert [event.sequence for event in deltas] == [1, 2]
    assert await outputs.append_batch(delta_drafts) == deltas
    committed_stream = await outputs.commit_stream(
        stream.output_stream_id,
        ModelFinishReason.STOP,
    )
    assert await outputs.commit_stream(
        stream.output_stream_id,
        ModelFinishReason.STOP,
    ) == committed_stream
    await _require_violation(
        outputs.abort_stream(stream.output_stream_id, "late_abort"),
        "a committed output stream must not abort",
    )

    terminal, _ = await outputs.begin_run_lifecycle(
        RunCreateParams(
            session_id=session_id,
            prompt="validated conformance",
            mode="agent",
            turn_id="conformance-turn-2",
        ),
        AgentEvent(
            type=CoreEventType.RUN_STARTED,
            payload={"status": RunStatus.RUNNING.value},
        ),
    )
    completed_event = AgentEvent(
        type=CoreEventType.RUN_COMPLETED,
        run_id=terminal.run_id,
        payload={"status": RunStatus.DONE.value},
    )
    terminal_stream = OutputStreamSpec(
        output_stream_id=f"conformance-terminal-stream-{terminal.run_id}",
        run_id=terminal.run_id,
        turn_id="conformance-turn-2",
        invocation_id=f"conformance-terminal-invocation-{terminal.run_id}",
        intent=AgentOutputIntent.STRUCTURED_PRIVATE,
        commit_mode=OutputCommitMode.PRIVATE,
    )
    await outputs.open_stream(terminal_stream)
    commit = RunCommit(
        terminal_status=RunStatus.DONE,
        final_response="portable answer",
        validated_result="validated answer",
        events=(completed_event,),
    )
    lifecycle = RunLifecycleOutputDraft(
        source_event_key=f"run:{terminal.run_id}:done",
        status=RunStatus.DONE,
        payload={"status": RunStatus.DONE.value},
        occurred_at=_now(),
        turn_id="conformance-turn-2",
    )
    committed = await outputs.commit_run_lifecycle(
        terminal.run_id,
        commit,
        lifecycle,
    )
    assert len(committed) == 3
    terminal_abort = committed[-2]
    assert terminal_abort.kind is OutputEventKind.STREAM_ABORTED
    assert terminal_abort.output_stream_id == terminal_stream.output_stream_id
    assert terminal_abort.payload == {
        "errorCode": TERMINAL_STREAM_ABORT_ERROR_CODE,
        "cause": TERMINAL_STREAM_ABORT_CAUSE,
        "runStatus": RunStatus.DONE.value,
    }
    assert committed[-1].kind is OutputEventKind.RUN_LIFECYCLE
    assert await outputs.load_validated_result(terminal.run_id) == (
        "validated answer"
    )
    assert await outputs.commit_run_lifecycle(
        terminal.run_id,
        commit,
        lifecycle,
    ) == committed
    await _require_violation(
        outputs.open_stream(replace(
            terminal_stream,
            output_stream_id=(
                f"conformance-late-stream-{terminal.run_id}"
            ),
            invocation_id=(
                f"conformance-late-invocation-{terminal.run_id}"
            ),
        )),
        "a terminal Run must not open a new output stream",
    )


async def assert_context_provider_conforms(
    *,
    provider: ContextProvider,
    request: AgentRunRequest,
    budget: ContextBudget,
    task_context: TaskContextRequest | None = None,
) -> None:
    """Check budgeted retrieval and safe Core message assembly."""

    assert isinstance(provider, ContextProvider)
    capability = ContextCapability(ContextStrategy.SINGLE_PASS, provider)
    bundle = await capability.build_initial(request, budget, None)
    _assert_context_bundle(bundle, budget)
    _assert_context_assembly(request, bundle)

    if not isinstance(provider, StagedContextProvider):
        return
    if task_context is None:
        raise AssertionError("staged context conformance requires task_context")
    staged = ContextCapability(ContextStrategy.STAGED, provider)
    planning = await staged.build_initial(request, budget, None)
    _assert_context_bundle(planning, budget)
    execution, source = await staged.build_execution(
        request,
        budget,
        planning,
        task_context,
        None,
    )
    assert source == "task_spec"
    _assert_context_bundle(execution, budget)
    _assert_context_assembly(request, execution)


async def assert_context_compression_hook_conforms(
    *,
    hook: ContextCompressionHook,
    request: AgentRunRequest,
) -> None:
    """Pressure a semantic compression hook through Core's safety checks."""

    assert isinstance(hook, ContextCompressionHook)
    system = AgentMessage(
        role=MessageRole.SYSTEM,
        content="Conformance system instruction.",
    )
    call = ToolCall(
        id="conformance-tool-call",
        name="read_context",
        arguments_json="{}",
    )
    current = AgentMessage(
        role=MessageRole.USER,
        content="Conformance current request.",
    )
    pressured = replace(request, messages=(
        system,
        AgentMessage(
            role=MessageRole.USER,
            content="old context " + ("x" * 12_000),
        ),
        AgentMessage(
            role=MessageRole.ASSISTANT,
            content="",
            tool_calls=(call,),
        ),
        AgentMessage(
            role=MessageRole.TOOL,
            content="context result",
            tool_call_id=call.id,
            origin=MessageOrigin.HOST_TOOL_RESULT,
        ),
        AgentMessage(role=MessageRole.ASSISTANT, content="tool complete"),
        current,
    ))
    result = await ContextCompressionCoordinator(hook).prepare(
        pressured,
        budget=ContextCompactionBudget(
            phase=ContextCompactionPhase.MODEL_CALL,
            provider_input_tokens=2_048,
            context_tokens=0,
            context_tokens_are_resolved=True,
            output_reserve_tokens=1_024,
        ),
    )
    assert isinstance(result, ConversationCompactionResult)
    assert system in result.request.messages
    assert current in result.request.messages
    assert result.diagnostics["compressionRequired"] is True


async def assert_model_gateway_conforms(
    *,
    gateway: ModelGateway,
    messages: tuple[AgentMessage, ...],
    invocation: ModelInvocation,
    expected_model: str | None = None,
    expected_tool_names: tuple[str, ...] = (),
) -> tuple[tuple[ModelStreamChunk, ...], ModelCompletion]:
    """Check typed stream/completion output and complete tool-call framing."""

    assert isinstance(gateway, ModelGateway)
    stream = await gateway.stream(messages, invocation, None)
    assert isinstance(stream, ModelStream)
    if expected_model is not None:
        assert stream.model == expected_model
    chunks = tuple([chunk async for chunk in stream.chunks])
    assert chunks and all(isinstance(chunk, ModelStreamChunk) for chunk in chunks)
    terminal = [
        index
        for index, chunk in enumerate(chunks)
        if chunk.finish_reason is not None
    ]
    assert terminal == [len(chunks) - 1]

    accumulator = ModelRoundAccumulator()
    for chunk in chunks:
        accumulator.add(chunk)
    calls, malformed = accumulator.tool_calls()
    assert malformed is None, malformed
    _assert_model_tool_calls(calls, invocation)
    if expected_tool_names:
        assert tuple(call.name for call in calls) == expected_tool_names
    termination = classify_model_termination(
        chunks[-1].finish_reason,
        tool_call_count=len(calls),
    )
    assert not termination.incomplete
    if chunks[-1].finish_reason is ModelFinishReason.TOOL_CALLS:
        assert calls

    completion = await gateway.complete(messages, invocation, None)
    assert isinstance(completion, ModelCompletion)
    assert completion.message.role is MessageRole.ASSISTANT
    assert completion.finish_reason is not None
    if expected_model is not None:
        assert completion.model == expected_model
    _assert_model_tool_calls(completion.message.tool_calls, invocation)
    completion_termination = classify_model_termination(
        completion.finish_reason,
        tool_call_count=len(completion.message.tool_calls),
    )
    assert not completion_termination.incomplete
    if completion.finish_reason is ModelFinishReason.TOOL_CALLS:
        assert completion.message.tool_calls
    return chunks, completion


async def assert_tool_execution_gateway_conforms(
    *,
    gateway: ToolExecutionGateway,
    request: ToolBatchRequest,
) -> tuple[ToolBatchResult, tuple[AgentEvent, ...]]:
    """Check fail-closed tool preflight, cancellation, and result ordering."""

    assert isinstance(gateway, ToolExecutionGateway)

    async def execute(
        candidate: ToolBatchRequest,
        signal: asyncio.Event | None = None,
    ) -> tuple[ToolBatchResult, tuple[AgentEvent, ...]]:
        sink = _ConformanceSink()
        result = await gateway.execute_batch(candidate, sink, signal)
        assert isinstance(result, ToolBatchResult)
        return result, tuple(sink.events)

    unauthorized, events = await execute(replace(
        request,
        allowed_tool_names=frozenset(),
    ))
    assert unauthorized.outcome is ToolBatchOutcome.REJECTED
    assert not events

    first_call = request.calls[0]
    malformed, events = await execute(replace(
        request,
        calls=(replace(first_call, arguments_json="{"),),
    ))
    assert malformed.outcome is ToolBatchOutcome.FAILED
    assert not events

    unknown_name = "__purra_conformance_unknown_tool__"
    unknown, events = await execute(replace(
        request,
        calls=(ToolCall(
            id="conformance-unknown-call",
            name=unknown_name,
            arguments_json="{}",
        ),),
        allowed_tool_names=frozenset({unknown_name}),
    ))
    assert unknown.outcome is ToolBatchOutcome.REJECTED
    assert not events

    canceled_signal = asyncio.Event()
    canceled_signal.set()
    canceled, events = await execute(request, canceled_signal)
    assert canceled.outcome is ToolBatchOutcome.CANCELED
    assert not canceled.results
    assert not events

    result, events = await execute(request)
    assert result.outcome is ToolBatchOutcome.COMPLETED
    assert tuple(item.tool_call_id for item in result.results) == tuple(
        call.id for call in request.calls
    )
    assert tuple(item.tool_name for item in result.results) == tuple(
        call.name for call in request.calls
    )
    completed = [
        event
        for event in events
        if event.type == CoreEventType.TOOL_CALL_COMPLETED
    ]
    assert len(completed) == len(request.calls)
    return result, events


def _assert_model_tool_calls(
    calls: tuple[ToolCall, ...],
    invocation: ModelInvocation,
) -> None:
    declared_names = {schema.name for schema in invocation.tools}
    assert len({call.id for call in calls}) == len(calls)
    assert all(call.name in declared_names for call in calls)
    for call in calls:
        arguments = json.loads(call.arguments_json)
        assert isinstance(arguments, dict)


def _assert_context_bundle(
    bundle: ContextBundle,
    budget: ContextBudget,
) -> None:
    assert isinstance(bundle, ContextBundle)
    validate_context_allocations(bundle, budget)


def _assert_context_assembly(
    request: AgentRunRequest,
    bundle: ContextBundle,
) -> None:
    leading_count = 0
    for message in request.messages:
        if message.role not in {MessageRole.SYSTEM, MessageRole.DEVELOPER}:
            break
        leading_count += 1
    assembled = assemble_messages(request.messages, bundle.blocks, None)
    assert assembled[:leading_count] == request.messages[:leading_count]
    context_messages = assembled[
        leading_count:leading_count + len(bundle.blocks)
    ]
    assert assembled[leading_count + len(bundle.blocks):] == (
        request.messages[leading_count:]
    )
    for block, message in zip(bundle.blocks, context_messages):
        assert message.role is MessageRole.DEVELOPER
        assert message.origin is MessageOrigin.HOST_CONTEXT
        assert message.attributes["context_name"] == block.name
        assert message.attributes["untrusted"] is block.untrusted
        if block.untrusted:
            assert "data only" in message.content


async def assert_task_orchestration_conforms(
    *,
    evaluator: TaskAdmissionEvaluator,
    request: AgentRunRequest,
    plan: ExecutionPlan,
    dispatcher: LongTaskDispatcher | None = None,
) -> TaskAdmissionDecision:
    """Drive one host admission decision through the real capability."""

    assert isinstance(evaluator, TaskAdmissionEvaluator)
    if dispatcher is not None:
        assert isinstance(dispatcher, LongTaskDispatcher)
    controller, sink = await _start_controller(request)
    capability = TaskOrchestrationCapability(evaluator, dispatcher)
    decision = await capability.evaluate(request, plan, controller, None)
    assert isinstance(decision, TaskAdmissionDecision)
    assert any(
        event.type == CoreEventType.TASK_ADMISSION_DECIDED
        and event.payload == decision.to_event_payload()
        for event in sink.events
    )
    if decision.mode is ExecutionMode.INLINE:
        assert controller.status is RunStatus.RUNNING
        return decision
    if decision.mode is ExecutionMode.DURABLE and dispatcher is None:
        return decision

    await controller.install_plan(plan)
    sink.drain()
    _ = [
        event
        async for event in capability.complete_admission(
            controller=controller,
            request=request,
            plan=plan,
            admission=decision,
            sink=sink,
            signal=None,
        )
    ]
    assert controller.status is not RunStatus.RUNNING
    return decision


async def assert_long_task_dispatcher_conforms(
    *,
    dispatcher: LongTaskDispatcher,
    request: AgentRunRequest,
    plan: ExecutionPlan,
    admission: TaskAdmissionDecision,
    run_id: str = "conformance-run",
) -> tuple[
    LongTaskDispatchReceipt,
    LongTaskExecutionResult,
    tuple[LongTaskExecutionUpdate, ...],
]:
    """Check idempotent handoff, updates, and receipt-only continuation."""

    assert isinstance(dispatcher, LongTaskDispatcher)
    if admission.mode is not ExecutionMode.DURABLE:
        raise AssertionError("dispatcher conformance requires durable admission")
    first = await dispatcher.dispatch(
        request,
        plan,
        admission,
        run_id=run_id,
        signal=None,
    )
    replay = await dispatcher.dispatch(
        request,
        plan,
        admission,
        run_id=run_id,
        signal=None,
    )
    assert isinstance(first, LongTaskDispatchReceipt)
    assert replay.task_id == first.task_id

    updates: list[LongTaskExecutionUpdate] = []

    async def observe(update: LongTaskExecutionUpdate) -> None:
        assert isinstance(update, LongTaskExecutionUpdate)
        assert update.event.type != CoreEventType.RUN_TODOS_UPDATED
        assert update.plan_revision is None or update.persist
        updates.append(update)

    result = await dispatcher.execute(
        first.task_id,
        run_id=run_id,
        observer=observe,
        signal=None,
    )
    assert isinstance(result, LongTaskExecutionResult)
    assert result.task_id == first.task_id

    controller, sink = await _start_controller(request)
    continuation = DurableTaskContinuation(
        source=RunRecoverySnapshot(
            run_id=run_id,
            status=RunStatus.CANCELED,
            execution_plan=plan,
        ),
        continuation_command="conformance-continuation",
        receipt=first,
    )
    capability = TaskOrchestrationCapability(
        evaluator=None,
        dispatcher=_ContinuationDispatcher(dispatcher),
    )
    _ = [
        event
        async for event in capability.continue_durable(
            controller,
            request,
            continuation,
            sink,
            None,
        )
    ]
    assert controller.status is not RunStatus.RUNNING
    return first, result, tuple(updates)


async def assert_artifact_store_conforms(
    *,
    artifacts: ArtifactRepository,
    claims: ArtifactClaimRepository,
    maintenance: ArtifactMaintenanceRepository,
) -> None:
    """Check Artifact CAS, idempotency, writer exclusion and retention."""

    assert isinstance(artifacts, ArtifactRepository)
    assert isinstance(claims, ArtifactClaimRepository)
    assert isinstance(maintenance, ArtifactMaintenanceRepository)
    suffix = uuid4().hex
    artifact_id = f"conformance-artifact-{suffix}"
    created = await artifacts.create(
        artifact_id,
        ArtifactCreateCommand(
            namespace="conformance",
            kind="report",
            owner_id=f"owner-{suffix}",
            owner_ref=ArtifactOwnerRef("durable_task", f"task-{suffix}"),
            created_by_run_id=f"run-{suffix}",
            expected_item_count=1,
        ),
    )
    assert created.status is ArtifactStatus.OPEN
    assert await artifacts.create(
        artifact_id,
        ArtifactCreateCommand(
            namespace=created.namespace,
            kind=created.kind,
            owner_id=created.owner_id,
            owner_ref=created.owner_ref,
            created_by_run_id=created.created_by_run_id,
            expected_item_count=1,
        ),
    ) == created
    claim = await claims.acquire(ArtifactWriteClaimCommand(
        artifact_id=created.id,
        run_id=created.created_by_run_id,
        expected_revision=created.revision,
        lease_duration_ms=30_000,
    ))
    assert await claims.acquire(ArtifactWriteClaimCommand(
        artifact_id=created.id,
        run_id=created.created_by_run_id,
        expected_revision=created.revision,
        lease_duration_ms=30_000,
    )) == claim
    await _require_failure(
        claims.acquire(ArtifactWriteClaimCommand(
            artifact_id=created.id,
            run_id=f"competing-{suffix}",
            expected_revision=created.revision,
            lease_duration_ms=30_000,
        )),
        "two Runs must not hold one Artifact writer claim",
    )
    lease = ArtifactMutationLease(
        run_id=claim.run_id,
        claim_token=claim.claim_token,
        lease_duration_ms=30_000,
    )
    append = ArtifactAppendCommand(
        artifact_id=created.id,
        expected_revision=created.revision,
        sequence=created.next_sequence,
        batch_id=f"batch-{suffix}",
        idempotency_key=f"append-{suffix}",
        items=({"value": "portable"},),
        write_lease=lease,
        coverage_keys=("result",),
    )
    receipt = await artifacts.append(append)
    replay = await artifacts.append(append)
    assert replay.replayed
    assert replay.committed_revision == receipt.committed_revision
    assert len(await artifacts.list_batches(created.id)) == 1
    finalized = await artifacts.finalize(
        ArtifactFinalizeCommand(
            artifact_id=created.id,
            expected_revision=receipt.committed_revision,
            write_lease=lease,
            expected_item_count=1,
            expected_coverage_keys=("result",),
            resource_ref=f"memory://{artifact_id}",
        ),
        coverage_digest="conformance-coverage",
    )
    assert finalized.status is ArtifactStatus.FINALIZED
    assert await claims.load_active(created.id) is None
    snapshot = await maintenance.inspect(timestamp_ms=claim.expires_at_ms)
    assert snapshot.finalized_artifacts >= 1
    report = await maintenance.maintain(
        ArtifactMaintenancePolicy(
            terminal_retention_ms=0,
            max_purge_artifacts=1,
        ),
        timestamp_ms=claim.expires_at_ms + 1,
    )
    assert report.purged_artifacts == 1
    assert await artifacts.load(created.id) is None


async def assert_long_task_repository_conforms(
    repository: LongTaskRepository,
) -> None:
    """Check task/Run atomicity, leases, checkpoints and idempotent commits."""

    assert isinstance(repository, LongTaskRepository)
    suffix = uuid4().hex
    task_id = f"conformance-task-{suffix}"
    command = LongTaskCreateCommand(
        namespace="conformance",
        kind="report",
        owner_id=f"owner-{suffix}",
        created_by_run_id=f"run-{suffix}",
        units=(
            LongTaskUnitSpec(id="collect", position=0),
            LongTaskUnitSpec(
                id="summarize",
                position=1,
                dependencies=("collect",),
            ),
        ),
        metadata={"sessionId": f"session-{suffix}"},
    )
    created = await repository.create(task_id, command)
    assert await repository.create(task_id, command) == created
    bindings = await repository.list_run_bindings(task_id)
    assert len(bindings) == 1
    assert bindings[0].relation is LongTaskRunRelation.CREATED
    continuation = await repository.bind_run(
        task_id,
        f"continuation-{suffix}",
        relation=LongTaskRunRelation.CONTINUATION,
    )
    assert await repository.bind_run(
        task_id,
        continuation.run_id,
        relation=continuation.relation,
    ) == continuation
    await _require_failure(
        repository.bind_run(
            task_id,
            continuation.run_id,
            relation=LongTaskRunRelation.REFERENCE,
        ),
        "a durable task Run binding must be immutable",
    )
    running = await repository.start(task_id, expected_revision=created.revision)
    await _require_failure(
        repository.pause(task_id, expected_revision=created.revision),
        "long task revision conflict",
    )
    first, competing = await asyncio.gather(
        repository.claim_ready_unit(
            task_id,
            worker_id="worker-a",
            lease_duration_ms=30_000,
        ),
        repository.claim_ready_unit(
            task_id,
            worker_id="worker-b",
            lease_duration_ms=30_000,
        ),
    )
    claimed = first or competing
    assert claimed is not None
    assert (first is None) != (competing is None)
    worker = claimed.worker_id or ""
    await repository.bind_unit_run(
        task_id,
        claimed.id,
        worker_id=worker,
        lease_epoch=claimed.lease_epoch,
        run_id=created.created_by_run_id,
    )
    result = LongTaskUnitResult(
        output_ref=f"memory://{task_id}/{claimed.id}",
        run_id=created.created_by_run_id,
    )
    settled = await repository.complete_unit(
        task_id,
        claimed.id,
        worker_id=worker,
        lease_epoch=claimed.lease_epoch,
        result=result,
    )
    assert await repository.complete_unit(
        task_id,
        claimed.id,
        worker_id=worker,
        lease_epoch=claimed.lease_epoch,
        result=result,
    ) == settled
    second = await repository.claim_ready_unit(
        task_id,
        worker_id="worker-c",
        lease_duration_ms=30_000,
    )
    assert second is not None and second.id == "summarize"
    recovered = await repository.recover_after_restart()
    assert task_id in recovered
    paused = await repository.load(task_id)
    units = await repository.list_units(task_id)
    assert paused is not None and paused.status is LongTaskStatus.PAUSED
    assert next(unit for unit in units if unit.id == second.id).status is (
        LongTaskUnitStatus.PENDING
    )
    resumed = await repository.resume(task_id)
    reclaimed = await repository.claim_ready_unit(
        task_id,
        worker_id="worker-d",
        lease_duration_ms=30_000,
    )
    assert reclaimed is not None and reclaimed.id == second.id
    completed = await repository.complete_unit(
        task_id,
        reclaimed.id,
        worker_id="worker-d",
        lease_epoch=reclaimed.lease_epoch,
        result=LongTaskUnitResult(
            output_ref=f"memory://{task_id}/{reclaimed.id}",
        ),
    )
    finalized = await repository.finalize_if_complete(task_id)
    assert completed.completed_units == 2
    assert finalized.status is LongTaskStatus.COMPLETED
    usage_task_id = f"conformance-usage-{suffix}"
    usage_task = await repository.create(usage_task_id, replace(
        command,
        owner_id=f"usage-owner-{suffix}",
        metadata={"sessionId": f"usage-session-{suffix}"},
    ))
    usage = LongTaskUsage(invocation_count=1, input_tokens=10, output_tokens=2)
    recorded = await repository.record_usage(
        usage_task.id,
        run_id=command.created_by_run_id,
        usage=usage,
        expected_revision=usage_task.revision,
    )
    assert await repository.record_usage(
        usage_task.id,
        run_id=command.created_by_run_id,
        usage=usage,
        expected_revision=usage_task.revision,
    ) == recorded

    budget_task = await repository.create(
        f"conformance-budget-{suffix}",
        replace(
            command,
            owner_id=f"budget-owner-{suffix}",
            budget_limits=LongTaskBudgetLimits(max_invocation_attempts=1),
            metadata={"sessionId": f"budget-session-{suffix}"},
        ),
    )
    budget_task = await repository.record_usage(
        budget_task.id,
        run_id=command.created_by_run_id,
        usage=LongTaskUsage(invocation_count=1),
        expected_revision=budget_task.revision,
    )
    budget_task = await repository.start(
        budget_task.id,
        expected_revision=budget_task.revision,
    )
    assert await repository.claim_ready_unit(
        budget_task.id,
        worker_id="budget-worker",
        lease_duration_ms=30_000,
    ) is None
    budget_task = await repository.load(budget_task.id)
    assert budget_task is not None
    assert budget_task.status is LongTaskStatus.FAILED
    budget_units = await repository.list_units(budget_task.id)
    assert all(
        unit.error_code == "runtime_budget_exceeded" for unit in budget_units
    )


class _ConformanceSink:
    def __init__(self) -> None:
        self.events: list[AgentEvent] = []

    async def emit(self, event: AgentEvent) -> None:
        self.events.append(event)

    def drain(self) -> tuple[AgentEvent, ...]:
        events = tuple(self.events)
        self.events.clear()
        return events


class _ContinuationDispatcher:
    def __init__(self, delegate: LongTaskDispatcher) -> None:
        self._delegate = delegate

    async def dispatch(self, *args, **kwargs):
        del args, kwargs
        raise AssertionError("durable continuation must not dispatch again")

    async def execute(self, *args, **kwargs):
        return await self._delegate.execute(*args, **kwargs)


async def _start_controller(
    request: AgentRunRequest,
) -> tuple[AgentRunController, _ConformanceSink]:
    adapters = InMemoryAgentAdapters()
    sink = _ConformanceSink()
    controller = AgentRunController(
        repository=adapters.runs,
        event_sink=sink,
    )
    await controller.begin(RunCreateParams(
        session_id=request.session_id,
        prompt=request.latest_user_text(),
        mode=request.mode,
    ))
    return controller, sink


__all__ = [
    "assert_artifact_store_conforms",
    "assert_context_compression_hook_conforms",
    "assert_context_provider_conforms",
    "assert_delegation_repository_conforms",
    "assert_execution_lease_store_conforms",
    "assert_host_adapters_conform",
    "assert_long_task_dispatcher_conforms",
    "assert_long_task_repository_conforms",
    "assert_model_gateway_conforms",
    "assert_task_orchestration_conforms",
    "assert_tool_execution_gateway_conforms",
    "assert_tool_idempotency_gateway_conforms",
]
