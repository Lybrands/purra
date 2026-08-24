from datetime import datetime, timezone

import pytest

from purra.api import InMemoryAgentAdapters
from purra.contracts import (
    ModelTokenUsage,
    RunCreateParams,
    RuntimeLimits,
    ToolCall,
    ToolHandlerResult,
)
from purra.events import AgentEvent
from purra.errors import ContractViolationError
from purra.output import (
    AgentOutputEventDraft,
    AgentOutputIntent,
    OutputChannel,
    OutputCommitMode,
    OutputSource,
    OutputStreamSpec,
)
from purra.testing import (
    assert_delegation_repository_conforms,
    assert_host_adapters_conform,
)


@pytest.mark.asyncio
async def test_memory_host_adapters_pass_the_shared_conformance_suite():
    adapters = InMemoryAgentAdapters()
    await assert_host_adapters_conform(
        runs=adapters.runs,
        outputs=adapters.outputs,
        publisher=adapters.publisher,
        session_id="portable-session",
    )


@pytest.mark.asyncio
async def test_memory_delegations_pass_the_shared_conformance_suite():
    adapters = InMemoryAgentAdapters()

    async def create_run() -> str:
        begun = await adapters.runs.begin(
            RunCreateParams(session_id=None, prompt="delegate", mode="agent"),
            AgentEvent(type="run.started"),
        )
        return begun.run_id

    await assert_delegation_repository_conforms(
        adapters.delegations,
        create_run,
    )


@pytest.mark.asyncio
async def test_memory_idempotency_replays_one_tool_result_and_rejects_key_drift():
    adapters = InMemoryAgentAdapters()
    begin = await adapters.runs.begin(
        RunCreateParams(session_id=None, prompt="work", mode=None),
        AgentEvent(type="run.started"),
    )
    calls = 0

    async def operation():
        nonlocal calls
        calls += 1
        return ToolHandlerResult("done")

    tool_call = ToolCall(
        id="call-1",
        name="delegateToAgents",
        arguments_json='{"delegations":[]}',
    )
    first = await adapters.idempotency.execute_once(
        begin.run_id,
        tool_call,
        operation,
    )
    replay = await adapters.idempotency.execute_once(
        begin.run_id,
        tool_call,
        operation,
    )

    assert first == ToolHandlerResult("done")
    assert replay == ToolHandlerResult("done", from_cache=True)
    assert calls == 1
    with pytest.raises(ContractViolationError, match="different arguments"):
        await adapters.idempotency.execute_once(
            begin.run_id,
            ToolCall(
                id="call-1",
                name="delegateToAgents",
                arguments_json='{"delegations":[{}]}',
            ),
            operation,
        )


@pytest.mark.asyncio
async def test_run_repository_reserves_attempts_and_keeps_authoritative_usage():
    adapters = InMemoryAgentAdapters()
    begun = await adapters.runs.begin(
        RunCreateParams(
            session_id=None,
            prompt="budget",
            mode=None,
            runtime_limits=RuntimeLimits(
                max_model_invocation_attempts=1,
                max_input_tokens=3,
            ),
        ),
        AgentEvent(type="run.started"),
    )

    reserved = await adapters.runs.reserve_model_attempt(
        begun.run_id,
        "invocation-1",
    )
    replay = await adapters.runs.reserve_model_attempt(
        begun.run_id,
        "invocation-1",
    )
    assert reserved == replay
    assert reserved.model_attempts == 1

    with pytest.raises(ContractViolationError) as attempts:
        await adapters.runs.reserve_model_attempt(begun.run_id, "invocation-2")
    assert attempts.value.code == "runtime_budget_exceeded"
    assert attempts.value.details["budgetKind"] == "model_attempts"

    usage = ModelTokenUsage(input_tokens=4, output_tokens=1)
    with pytest.raises(ContractViolationError) as tokens:
        await adapters.runs.settle_model_attempt(
            begun.run_id,
            "invocation-1",
            usage,
        )
    assert tokens.value.code == "runtime_budget_exceeded"
    assert tokens.value.details["budgetKind"] == "input_tokens"
    with pytest.raises(ContractViolationError):
        await adapters.runs.settle_model_attempt(
            begun.run_id,
            "invocation-1",
            usage,
        )


@pytest.mark.asyncio
async def test_output_batch_is_atomic_and_consumes_canonical_provider_budget():
    adapters = InMemoryAgentAdapters()
    begun = await adapters.runs.begin(
        RunCreateParams(
            session_id=None,
            prompt="output budget",
            mode=None,
            runtime_limits=RuntimeLimits(max_provider_output_events=1),
        ),
        AgentEvent(type="run.started"),
    )
    stream = OutputStreamSpec(
        output_stream_id="budget-stream",
        run_id=begun.run_id,
        turn_id=None,
        invocation_id="budget-invocation",
        intent=AgentOutputIntent.FINAL_PUBLIC,
        commit_mode=OutputCommitMode.LIVE,
    )
    await adapters.outputs.open_stream(stream)

    def delta(key: str, text: str) -> AgentOutputEventDraft:
        return AgentOutputEventDraft.public_text(
            run_id=begun.run_id,
            turn_id=None,
            output_stream_id=stream.output_stream_id,
            invocation_id=stream.invocation_id,
            source_event_key=key,
            source=OutputSource.PROVIDER,
            channel=OutputChannel.FINAL,
            delta=text,
            occurred_at=datetime.now(timezone.utc),
        )

    first = delta("provider:first", "甲")
    second = delta("provider:second", "乙")
    with pytest.raises(ContractViolationError) as exceeded:
        await adapters.outputs.append_batch((first, second))
    assert exceeded.value.code == "runtime_budget_exceeded"
    assert exceeded.value.details["budgetKind"] == "provider_output_events"
    assert await adapters.outputs.list_events(begun.run_id, after_sequence=0) == ()

    committed = await adapters.outputs.append_batch((first,))
    assert [event.sequence for event in committed] == [1]
    assert await adapters.outputs.append_batch((first,)) == committed
    with pytest.raises(ContractViolationError):
        await adapters.outputs.append_event(second)


@pytest.mark.asyncio
async def test_provider_output_bytes_use_canonical_utf8_and_reject_before_append():
    adapters = InMemoryAgentAdapters()
    begun = await adapters.runs.begin(
        RunCreateParams(
            session_id=None,
            prompt="byte budget",
            mode=None,
            runtime_limits=RuntimeLimits(max_provider_output_bytes=1),
        ),
        AgentEvent(type="run.started"),
    )
    stream = OutputStreamSpec(
        output_stream_id="byte-stream",
        run_id=begun.run_id,
        turn_id=None,
        invocation_id="byte-invocation",
        intent=AgentOutputIntent.FINAL_PUBLIC,
        commit_mode=OutputCommitMode.LIVE,
    )
    await adapters.outputs.open_stream(stream)
    draft = AgentOutputEventDraft.public_text(
        run_id=begun.run_id,
        turn_id=None,
        output_stream_id=stream.output_stream_id,
        invocation_id=stream.invocation_id,
        source_event_key="provider:utf8",
        source=OutputSource.PROVIDER,
        channel=OutputChannel.FINAL,
        delta="猫",
        occurred_at=datetime.now(timezone.utc),
    )
    with pytest.raises(ContractViolationError) as exceeded:
        await adapters.outputs.append_event(draft)
    assert exceeded.value.details["budgetKind"] == "provider_output_bytes"
    assert await adapters.outputs.list_events(begun.run_id, after_sequence=0) == ()
