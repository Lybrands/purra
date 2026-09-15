import asyncio
import json

import pytest

from purra.agent_tree import AgentRunAggregation


def aggregate(*ids):
    return AgentRunAggregation(state="ready", pending_run_ids=(), required_failures=(), results=tuple({"runId": value, "status": "done"} for value in ids))


@pytest.mark.asyncio
@pytest.mark.parametrize("present_results", [False, True])
async def test_core_receives_early_results_in_its_own_model_loop(present_results):
    import json
    from test_second_host_conformance import _request
    from purra.api import AgentCore, AgentPreset, AgentTreePolicy, InMemoryAgentAdapters
    from purra.contracts import ModelStream, ModelStreamChunk, ModelFinishReason, ToolCallDelta, RuntimeLimits
    from purra.tools import InMemoryToolCatalog
    release = asyncio.Event()
    observed = []
    class Gateway:
        async def complete(self, *args, **kwargs):
            raise AssertionError("Use managed streaming")
        async def stream(self, messages, invocation, signal=None):
            async def chunks():
                texts = [m.content for m in messages]
                if any('"completedChildResults"' in str(text) for text in texts):
                    yield ModelStreamChunk(content_delta="Main Agent feedback", finish_reason=ModelFinishReason.STOP)
                    return
                if "CHILD_FAST" in texts or "CHILD_SLOW" in texts:
                    if "CHILD_SLOW" in texts:
                        await release.wait()
                    yield ModelStreamChunk(content_delta="private child result", finish_reason=ModelFinishReason.STOP)
                    return
                receipts = [m for m in messages if m.role.value == "tool"]
                if not receipts:
                    name, args = "delegateToAgents", {"children": [
                        {"name": n, "title": n, "instruction": ins, "objective": "Analyze"}
                        for n, ins in [("fast", "CHILD_FAST"), ("slow", "CHILD_SLOW")]]}
                else:
                    data = json.loads(receipts[-1].content)
                    observed.append(data)
                    if data["pendingRunIds"]:
                        assert not release.is_set()
                        release.set()
                        name, args = "receiveAgentResults", {"runIds": data["runIds"], "afterRunIds": [r["runId"] for r in data["results"]]}
                    else:
                        yield ModelStreamChunk(content_delta="Unified final answer", finish_reason=ModelFinishReason.STOP)
                        return
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=0, id=name, type="function", name=name, arguments_fragment=json.dumps(args)),), finish_reason=ModelFinishReason.TOOL_CALLS)
            return ModelStream(chunks=chunks(), model="operations-model", applied_generation_limit=invocation.output_budget.max_generation_tokens)
    adapters = InMemoryAgentAdapters()
    core = AgentCore(model_gateway=Gateway(), run_repository=adapters.runs,
        output_repository=adapters.outputs, output_publisher=adapters.publisher, run_tree_repository=adapters.run_tree,
        preset=AgentPreset(id="inbox", revision="1", tool_catalog=InMemoryToolCatalog(()),
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None), agent_tree_policy=AgentTreePolicy(result_presentation_instruction="Explain this result." if present_results else None)))
    try:
        handle = await core.submit(_request())
        result = await asyncio.wait_for(handle.wait(), 3)
        assert result.status.value == "done", result.error
        assert observed[0]["pendingRunIds"]
        assert not observed[-1]["pendingRunIds"]
        events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
        markers = [e.payload["data"] for e in events if e.payload.get("eventType") == "parent.stage.delivery"]
        assert [m["state"] for m in markers] == (["started", "completed"] * 2 if present_results else [])
        assert len({m["deliveryId"] for m in markers}) == (2 if present_results else 0)
        states = [e.payload["data"]["state"] for e in events if e.payload.get("eventType") == "agent.feedback.state"]
        assert states == (["started", "streaming", "completed"] * 2 if present_results else [])
        if not present_results:
            from purra.errors import ContractViolationError
            with pytest.raises(ContractViolationError) as missing_policy:
                await core.report_agent_results(handle.run_id, ())
            assert missing_policy.value.code == "agent_feedback_policy_required"
            assert await adapters.outputs.list_events(handle.run_id, after_sequence=0) == events
    finally:
        release.set()
        await core.close()


def test_scheduler_does_not_require_public_presentation_configuration():
    from purra.agent_tree import InMemoryRunTreeRepository
    from purra.agent_tree_execution import AgentTreeRunSupervisor
    class Executor:
        async def execute(self, *args):
            raise AssertionError("Construction must not execute work")
    AgentTreeRunSupervisor(repository=InMemoryRunTreeRepository(), executor=Executor())

@pytest.mark.asyncio
async def test_each_result_queues_while_another_feedback_stream_is_open():
    from purra.agent_tree.delivery import deliver_agent_results
    started, arrived = asyncio.Event(), asyncio.Event()
    lock = asyncio.Lock()
    queued, output = [], []

    async def producer(notify):
        await notify(aggregate("first"))
        await started.wait()
        await notify(aggregate("first", "second", "failed"))
        await asyncio.sleep(0)
        assert queued == ["first", "second", "failed"]
        assert output == ["first:start"]
        arrived.set()
        return aggregate("first", "second", "failed")

    async def present(results):
        assert len(results) == 1
        identity = results[0]["runId"]
        queued.append(identity)
        async with lock:
            output.append(identity + ":start")
            if identity == "first":
                started.set()
                await arrived.wait()
            output.append(identity + ":end")

    await asyncio.wait_for(deliver_agent_results(producer, present), 1)
    assert output == ["first:start", "first:end", "second:start", "second:end", "failed:start", "failed:end"]


@pytest.mark.asyncio
async def test_per_result_cancellation_drains_producer_and_presenter():
    from purra.agent_tree.delivery import deliver_agent_results
    started = asyncio.Event()
    producer_closed, presenter_closed = asyncio.Event(), asyncio.Event()

    async def producer(notify):
        try:
            await notify(aggregate('a'))
            await asyncio.Event().wait()
        finally:
            producer_closed.set()

    async def present(results):
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            presenter_closed.set()

    task = asyncio.create_task(deliver_agent_results(producer, present))
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert producer_closed.is_set() and presenter_closed.is_set()


@pytest.mark.asyncio
async def test_per_result_failure_stops_producer_without_replaying():
    from purra.agent_tree.delivery import deliver_agent_results
    closed = asyncio.Event()
    attempts = []

    async def producer(notify):
        try:
            await notify(aggregate('a'))
            await asyncio.Event().wait()
        finally:
            closed.set()

    async def present(results):
        attempts.append(results[0]['runId'])
        raise RuntimeError('presentation failed')

    with pytest.raises(RuntimeError, match='presentation failed'):
        await asyncio.wait_for(deliver_agent_results(producer, present), 1)
    assert attempts == ['a'] and closed.is_set()
