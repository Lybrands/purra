import asyncio
import pytest
from purra.api import AgentCoreRunOptions, InMemoryAgentAdapters
from purra.contracts import RunCreateParams, ModelStream, ModelStreamChunk, ModelFinishReason
from purra.events import AgentEvent
from test_standalone_agent_conformance import _core, _Context
from test_second_host_conformance import _request


@pytest.mark.asyncio
async def test_concurrent_model_operations_keep_one_running_owner():
    adapters = InMemoryAgentAdapters()
    owner = await adapters.runs.begin(RunCreateParams(None, "owner", None), AgentEvent("run.started"))
    class Gateway:
        async def complete(self, *args, **kwargs):
            raise AssertionError("stream required")
        async def stream(self, messages, invocation, signal=None):
            async def chunks():
                yield ModelStreamChunk(content_delta="private evidence", finish_reason=ModelFinishReason.STOP)
            return ModelStream(chunks=chunks(), model="test", applied_generation_limit=invocation.output_budget.max_generation_tokens)
    core = _core(gateway=Gateway(), context=_Context(), adapters=adapters)
    try:
        results = await asyncio.gather(*(core.execute_operation(_request(), run_id=owner.run_id,
            operation_id=f"operation-{index}", options=AgentCoreRunOptions()) for index in range(2)))
        assert all(result.outcome.value == "completed" for result in results)
        assert all(result.run_id == owner.run_id for result in results)
        assert adapters.state.run.run_count == 1
        assert not adapters.state.tree.agents
        snapshot = await adapters.runs.get(owner.run_id)
        assert snapshot.status.value == "running"
        assert snapshot.execution_checkpoint is None
        events = await adapters.outputs.list_events(owner.run_id, after_sequence=0)
        assert not any(event.visibility.value == "public" and "private evidence" in str(event.payload) for event in events)
    finally:
        await core.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["missing_owner", "agent_tree", "checkpoint", "tool_checkpoint"])
async def test_operation_rejects_run_execution_scopes(scope):
    from purra.errors import ContractViolationError
    from test_standalone_agent_conformance import _Gateway
    adapters = InMemoryAgentAdapters()
    owner = await adapters.runs.begin(RunCreateParams(None, "owner", None), AgentEvent("run.started"))
    gateway = _Gateway("must not be called")
    core = _core(gateway=gateway, context=_Context(), adapters=adapters, agent_tree=scope == "agent_tree")
    async def checkpoint(_):
        raise AssertionError("Operation must not invoke a Run checkpoint handler")
    options = AgentCoreRunOptions(**(
        {"checkpoint_handler": checkpoint} if scope == "checkpoint" else
        {"tool_checkpoint_handler": checkpoint} if scope == "tool_checkpoint" else {}
    ))
    try:
        with pytest.raises(ContractViolationError) as caught:
            await core.execute_operation(_request(), run_id="missing" if scope == "missing_owner" else owner.run_id,
                                         operation_id="operation", options=options)
        assert caught.value.code == ("run_not_found" if scope == "missing_owner" else "operation_scope_invalid")
        assert not gateway.invocations
        assert (await adapters.runs.get(owner.run_id)).status.value == "running"
        assert adapters.state.run.run_count == 1
    finally:
        await core.close()
