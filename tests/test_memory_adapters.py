import asyncio
from datetime import datetime, timezone

import pytest

from purra.api import AgentExecutionCheckpoint, InMemoryAgentAdapters
from purra.contracts import (
    AgentMessage,
    MessageRole,
    ModelTokenUsage,
    RunCreateParams,
    RuntimeLimits,
    TraceRecord,
    ToolCall,
    ToolHandlerResult,
)
from purra.events import AgentEvent
from purra.errors import ContractViolationError
from purra.agent_tree_lease import bind_agent_run_lease
from purra.output import (
    AgentOutputEventDraft,
    AgentOutputIntent,
    OutputChannel,
    OutputCommitMode,
    OutputEventKind,
    OutputSource,
    OutputStreamSpec,
)
from purra.testing import assert_host_adapters_conform
from purra.ports import RunCommit


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
        arguments_json='{"children":[]}',
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
                arguments_json='{"children":[{}]}',
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
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None,
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

    usage = ModelTokenUsage(input_tokens=4, generation_tokens=1)
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
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None, max_provider_output_events=1),
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
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None, max_provider_output_bytes=1),
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


@pytest.mark.asyncio
async def test_child_runs_share_root_budget_and_one_canonical_journal():
    adapters = InMemoryAgentAdapters()
    limits = RuntimeLimits(max_run_generation_tokens=None,
        max_model_invocation_attempts=2,
        max_input_tokens=3,
        max_provider_output_events=1,
    )

    async def begin(params: RunCreateParams):
        return await adapters.outputs.begin_run_lifecycle(
            params,
            AgentEvent(type="run.started"),
        )

    root, _ = await begin(RunCreateParams(
        session_id=None,
        prompt="root",
        mode="agent",
        requested_run_id="root-run",
        agent_id="root-agent",
        runtime_limits=limits,
    ))
    children = []
    for index in (1, 2, 3):
        child, _ = await begin(RunCreateParams(
            session_id=None,
            prompt=f"child-{index}",
            mode="agent",
            requested_run_id=f"child-run-{index}",
            root_run_id=root.run_id,
            agent_id=f"child-agent-{index}",
            parent_run_id=root.run_id,
            runtime_limits=limits,
        ))
        children.append(child)

    attempts = await asyncio.gather(
        *(
            adapters.runs.reserve_model_attempt(
                child.run_id,
                f"invocation-{index}",
            )
            for index, child in enumerate(children, 1)
        ),
        return_exceptions=True,
    )
    assert sum(not isinstance(item, Exception) for item in attempts) == 2
    failure = next(item for item in attempts if isinstance(item, Exception))
    assert isinstance(failure, ContractViolationError)
    assert failure.code == "runtime_budget_exceeded"
    assert failure.details["budgetKind"] == "model_attempts"

    settlements = await asyncio.gather(
        *(
            adapters.runs.settle_model_attempt(
                children[index].run_id,
                f"invocation-{index + 1}",
                ModelTokenUsage(input_tokens=2, generation_tokens=0),
            )
            for index, item in enumerate(attempts)
            if not isinstance(item, Exception)
        ),
        return_exceptions=True,
    )
    assert sum(not isinstance(item, Exception) for item in settlements) == 1
    token_failure = next(
        item for item in settlements if isinstance(item, Exception)
    )
    assert isinstance(token_failure, ContractViolationError)
    assert token_failure.details["budgetKind"] == "input_tokens"

    for index, child in enumerate(children, 1):
        await adapters.outputs.open_stream(OutputStreamSpec(
            output_stream_id=f"child-stream-{index}",
            run_id=child.run_id,
            turn_id=None,
            invocation_id=f"child-output-{index}",
            intent=AgentOutputIntent.FINAL_PUBLIC,
            commit_mode=OutputCommitMode.LIVE,
        ))

    def child_delta(index: int):
        return AgentOutputEventDraft.public_text(
            run_id=children[index - 1].run_id,
            turn_id=None,
            output_stream_id=f"child-stream-{index}",
            invocation_id=f"child-output-{index}",
            source_event_key=f"provider:child:{index}",
            source=OutputSource.PROVIDER,
            channel=OutputChannel.FINAL,
            delta=str(index),
            occurred_at=datetime.now(timezone.utc),
        )

    outputs = await asyncio.gather(
        *(
            adapters.outputs.append_event(child_delta(index))
            for index in (1, 2, 3)
        ),
        return_exceptions=True,
    )
    assert sum(not isinstance(item, Exception) for item in outputs) == 1
    output_failures = tuple(item for item in outputs if isinstance(item, Exception))
    assert len(output_failures) == 2
    assert all(isinstance(item, ContractViolationError) for item in output_failures)
    assert {
        item.details["budgetKind"] for item in output_failures
    } == {"provider_output_events"}

    journal = await adapters.outputs.list_root_events(
        root.run_id,
        after_root_sequence=0,
    )
    assert [event.root_sequence for event in journal] == list(
        range(1, len(journal) + 1)
    )
    assert {event.root_run_id for event in journal} == {root.run_id}
    assert all(event.agent_id and event.source_event_key for event in journal)
    child_events = await adapters.outputs.list_events(
        children[0].run_id,
        after_sequence=0,
    )
    assert all(event.run_id == children[0].run_id for event in child_events)
    assert all(event in journal for event in child_events)


@pytest.mark.asyncio
async def test_expired_agent_tree_lease_fences_canonical_child_writes():
    from purra.agent_tree import (
        AgentCapabilityGrant,
        BeginRootAgentCommand,
        ChildAgentSpec,
        SpawnAgentsCommand,
    )

    now = [100]
    adapters = InMemoryAgentAdapters(agent_tree_clock_ms=lambda: now[0])
    root, _ = await adapters.outputs.begin_run_lifecycle(
        RunCreateParams(
            session_id=None,
            prompt="fenced root",
            mode="agent",
            requested_run_id="fenced-root",
            agent_id="fenced-root-agent",
        ),
        AgentEvent(type="run.started"),
    )
    await adapters.run_tree.begin_root(BeginRootAgentCommand(
        run_id=root.run_id,
        agent_id="fenced-root-agent",
        name="root",
        title="Root",
        instruction="Own the test.",
        objective="Fence stale writes.",
        capability_grant=AgentCapabilityGrant(can_spawn_agents=True),
        idempotency_key="fenced-root-begin",
    ))
    child = (await adapters.run_tree.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="fenced-child-spawn",
        children=(ChildAgentSpec(
            name="child",
            title="Child",
            instruction="Work.",
            objective="Write once.",
        ),),
    ))).items[0]
    await adapters.run_tree.mark_waiting(root.run_id)
    claim = await adapters.run_tree.claim_run(
        child.run.run_id,
        owner_id="worker-a",
        lease_duration_ms=10,
    )
    assert claim is not None
    with bind_agent_run_lease(
        claim.run_id,
        claim.lease_owner_id or "",
        claim.lease_epoch,
    ):
        await adapters.outputs.begin_run_lifecycle(
            RunCreateParams(
                session_id=None,
                prompt="fenced child",
                mode="agent",
                requested_run_id=claim.run_id,
                root_run_id=root.run_id,
                agent_id=claim.agent_id,
                parent_run_id=root.run_id,
                lease_owner_id=claim.lease_owner_id,
                lease_epoch=claim.lease_epoch,
            ),
            AgentEvent(type="run.started"),
        )

    now[0] = 110
    with bind_agent_run_lease(
        claim.run_id,
        claim.lease_owner_id or "",
        claim.lease_epoch,
    ):
        with pytest.raises(ContractViolationError) as attempt:
            await adapters.runs.reserve_model_attempt(
                claim.run_id,
                "stale-attempt",
            )
        with pytest.raises(ContractViolationError) as trace:
            await adapters.runs.append_trace(
                claim.run_id,
                TraceRecord(stage="stale", outcome="rejected"),
            )
        with pytest.raises(ContractViolationError) as output:
            await adapters.outputs.append_event(AgentOutputEventDraft(
                run_id=claim.run_id,
                turn_id=None,
                output_stream_id=None,
                invocation_id=None,
                source_event_key="stale-output",
                source=OutputSource.RUNTIME,
                kind=OutputEventKind.RUNTIME,
                channel=OutputChannel.DIAGNOSTIC,
                visibility="private",
                payload={},
                occurred_at=datetime.now(timezone.utc),
            ))
        with pytest.raises(ContractViolationError) as checkpoint:
            await adapters.runs.commit(
                claim.run_id,
                RunCommit(execution_checkpoint=AgentExecutionCheckpoint(
                    run_id=claim.run_id,
                    next_round=2,
                    round_limit=6,
                    messages=(AgentMessage(
                        role=MessageRole.USER,
                        content="resume",
                    ),),
                )),
            )
    assert attempt.value.code == "agent_run_lease_lost"
    assert trace.value.code == "agent_run_lease_lost"
    assert output.value.code == "agent_run_lease_lost"
    assert checkpoint.value.code == "agent_run_lease_lost"
