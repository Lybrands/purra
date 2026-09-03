import asyncio
from dataclasses import replace
import pytest
from purra.api import AgentCore, AgentPreset, UserInputRequired
from purra.contracts import AgentMessage, AgentRunRequest, DomainContext, ModelRequest, ModelStream, ModelStreamChunk, ToolCallDelta, ModelTokenUsage, RuntimeLimits
from purra.model_protocol import generic_capability_snapshot
from purra.tools import InMemoryToolCatalog
from purra_sqlite import SqliteAgentAdapters
from purra_interaction import SqliteClarification


class Gateway:
    def __init__(self): self.calls = []
    async def complete(self, *args, **kwargs): raise AssertionError("stream expected")
    async def stream(self, messages, invocation, signal=None):
        self.calls.append(messages)
        answered = any('Answers to requested' in m.content for m in messages)
        async def chunks():
            if answered:
                yield ModelStreamChunk(content_delta="Finished with the answer", finish_reason="stop", usage=ModelTokenUsage(input_tokens=5, output_tokens=7))
            else:
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=0, id="ask-1", name="request_user_input", arguments_fragment='{"questions":[{"id":"length","prompt":"篇幅？","choices":["短","长"],"allowFreeform":false}]}'),), finish_reason="tool_calls", usage=ModelTokenUsage(input_tokens=10, output_tokens=20))
        return ModelStream(chunks=chunks(), model="fixture", applied_output_limit=invocation.output_limit.max_tokens)


def compose(path, *, max_attempts=64):
    storage = SqliteAgentAdapters(path, scope="owner/project")
    interaction = SqliteClarification(storage)
    gateway = Gateway()
    core = AgentCore(model_gateway=gateway, run_repository=storage.runs,
        output_repository=storage.outputs, output_publisher=storage.publisher, execution_lease_store=storage.leases,
        preset=AgentPreset(id="sqlite", revision="1", tool_catalog=InMemoryToolCatalog((interaction.registration,)),
                           runtime_limits=RuntimeLimits(max_run_output_tokens=100, max_model_invocation_attempts=max_attempts)))
    request = AgentRunRequest(messages=(AgentMessage("user", "Write"),),
        model=ModelRequest("fixture", "fixture", replace(generic_capability_snapshot(), max_call_output_tokens=32)),
        domain_context=DomainContext("fixture"), context_window=65536, tools_enabled=True, planning_mode="reactive")
    return storage, interaction, gateway, core, request


@pytest.mark.asyncio
async def test_question_restart_answer_resume_preserves_budget_and_output(tmp_path):
    path = tmp_path / "agent.db"
    storage, interaction, gateway, core, request = compose(path)
    handle = await interaction.submit(core, request)
    with pytest.raises(UserInputRequired) as waiting:
        await handle.wait()
    identifier, run_id = waiting.value.request_id, handle.run_id
    assert (await storage.runs.get(run_id)).status.value == "running"
    assert len(gateway.calls) == 1
    before = await storage.outputs.list_events(run_id, after_sequence=0)
    await core.close(); storage.close()
    storage, interaction, gateway, core, request = compose(path)
    try:
        pending = await interaction.get(identifier)
        assert pending["state"] == "waiting" and "request" not in pending
        with pytest.raises(ValueError, match="invalid answer"):
            await interaction.answer(identifier, revision=1, key="bad", answers={"length": "invalid"})
        ready = await interaction.answer(identifier, revision=1, key="answer", answers={"length": "短"})
        assert ready == await interaction.answer(identifier, revision=1, key="answer", answers={"length": "短"})
        resumed = await interaction.resume(core, identifier)
        result = await resumed.wait()
        assert resumed.run_id == run_id and result.final_response == "Finished with the answer", result
        assert len(gateway.calls) == 2
        assert all(any("Answers to requested" in m.content for m in call) for call in gateway.calls)
        async with storage.transaction() as adapters:
            run = adapters.runs._state.runs[run_id]
            assert len(run.model_attempt_ids) == 3
            assert sum(u.output_tokens for u in run.model_usage_by_invocation.values() if u) == 34
        events = await storage.outputs.list_events(run_id, after_sequence=0)
        assert events[:len(before)] == before
        assert sum(e.payload.get("type") == "input.required" for e in events) == 1
        assert all("checkpoint" not in str(e.payload).lower() or e.kind.value == "runtime.event" for e in events if e.visibility.value == "public")
    finally:
        await core.close(); storage.close()


@pytest.mark.asyncio
async def test_resume_cannot_reset_budget_and_cancel_is_durable(tmp_path):
    for cancel in (False, True):
        storage, interaction, gateway, core, request = compose(tmp_path / f"budget-{cancel}.db", max_attempts=1)
        try:
            handle = await interaction.submit(core, request)
            with pytest.raises(UserInputRequired) as waiting: await handle.wait()
            identifier = waiting.value.request_id
            if cancel:
                assert await interaction.cancel(identifier)
                assert not await interaction.cancel(identifier)
                assert (await interaction.get(identifier))["state"] == "canceled"
                assert not await interaction.list_waiting()
                with pytest.raises(ValueError): await interaction.answer(identifier, revision=1, key="a", answers={"length": "短"})
            else:
                await interaction.answer(identifier, revision=1, key="a", answers={"length": "短"})
                resumed = await interaction.resume(core, identifier)
                result = await resumed.wait()
                assert result.status.value == "failed"
                assert len(gateway.calls) == 1
        finally: await core.close(); storage.close()


@pytest.mark.asyncio
async def test_concurrent_resume_has_one_execution_owner(tmp_path):
    storage, interaction, gateway, core, request = compose(tmp_path / "lease.db")
    entered, release = asyncio.Event(), asyncio.Event()
    try:
        handle = await interaction.submit(core, request)
        with pytest.raises(UserInputRequired) as waiting: await handle.wait()
        identifier = waiting.value.request_id
        await interaction.answer(identifier, revision=1, key="a", answers={"length": "短"})
        original = gateway.stream
        async def blocked(*args, **kwargs):
            entered.set(); await release.wait()
            return await original(*args, **kwargs)
        gateway.stream = blocked
        first = asyncio.create_task(interaction.resume(core, identifier))
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(Exception, match="lease"):
            await interaction.resume(core, identifier)
        release.set()
        resumed = await first
        assert (await resumed.wait()).status.value == "done"
    finally:
        release.set(); await core.close(); storage.close()


