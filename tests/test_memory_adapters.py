import pytest

from purra.api import InMemoryAgentAdapters
from purra.contracts import (
    RunCreateParams,
    ToolCall,
    ToolHandlerResult,
)
from purra.events import AgentEvent
from purra.errors import ContractViolationError
from purra.testing import assert_host_adapters_conform


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

    assert first == replay == ToolHandlerResult("done")
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
