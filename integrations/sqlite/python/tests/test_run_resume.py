"""Public resume behavior against a reopened, durable journal."""
import json
from dataclasses import replace
from pathlib import Path

import pytest

from purra.api import (
    AgentCore, AgentCoreRunOptions, AgentPreset, UserInputRequired,
    AgentTreePolicy, AgentCapabilityGrant, BeginRootAgentCommand,
    ChildAgentSpec, SpawnAgentsCommand,
)
from purra.contracts import (
    AgentMessage, AgentRunRequest, DomainContext, ModelRequest, ModelStream,
    ModelStreamChunk, RunCreateParams, RuntimeLimits, ToolCallDelta,
    ToolHandlerResult, ToolPolicy, ToolSchema,
)
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.model_protocol import generic_capability_snapshot
from purra.ports import ToolRegistration
from purra.tools import InMemoryToolCatalog
from purra_sqlite import SqliteAgentAdapters

CASES = json.loads(
    (Path(__file__).resolve().parents[4] / "conformance/fixtures/run_resume.json").read_text()
)["cases"]


class FirstSnapshotRepository:
    """Model a repository read racing with a committed checkpoint update."""
    def __init__(self, repository, snapshot):
        self.repository, self.snapshot = repository, snapshot

    def __getattr__(self, name):
        return getattr(self.repository, name)

    async def get(self, run_id):
        if self.snapshot is not None:
            snapshot, self.snapshot = self.snapshot, None
            assert snapshot.run_id == run_id
            return snapshot
        return await self.repository.get(run_id)


class Host:
    def __init__(self, path, *, revision="1", leased=True, tree=False, first_snapshot=None):
        self.storage = SqliteAgentAdapters(path, scope="resume")
        self.model_calls = 0
        self.tool_calls = 0
        self.request = AgentRunRequest(
            messages=(AgentMessage("user", "Look up the value"),),
            model=ModelRequest("fixture", "fixture", replace(generic_capability_snapshot(), max_generation_tokens=128)),
            domain_context=DomainContext("resume"), context_window=65536,
            tools_enabled=True, planning_mode="reactive",
        )
        self.core = AgentCore(
            model_gateway=self, run_repository=FirstSnapshotRepository(self.storage.runs, first_snapshot),
            run_tree_repository=self.storage.run_tree if tree else None,
            output_repository=self.storage.outputs, output_publisher=self.storage.publisher,
            execution_lease_store=self.storage.leases if leased else None,
            preset=AgentPreset(id="resume", revision=revision,
                agent_tree_policy=AgentTreePolicy() if tree else None,
                runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
                tool_catalog=InMemoryToolCatalog((ToolRegistration(
                    schema=ToolSchema("lookup", "Read the value", {"type": "object", "properties": {}}),
                    handler=self.lookup, policy=ToolPolicy(mode="read", title="Lookup"),
                ),))),
        )

    async def lookup(self, state, arguments, signal=None):
        self.tool_calls += 1
        return ToolHandlerResult(content="42", effect_state="not_started")

    async def complete(self, *args, **kwargs):
        raise AssertionError("stream expected")

    async def stream(self, messages, invocation, signal=None):
        self.model_calls += 1
        async def chunks():
            if any(m.role.value == "tool" for m in messages) or not invocation.tools:
                yield ModelStreamChunk(content_delta="42", finish_reason="stop")
            else:
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(
                    index=0, id="lookup-1", name="lookup", arguments_fragment="{}",
                ),), finish_reason="tool_calls")
        return ModelStream(chunks=chunks(), model="fixture",
            applied_generation_limit=invocation.output_budget.max_generation_tokens)

    async def pause(self):
        async def checkpoint(saved):
            raise UserInputRequired(saved.run_id, "pause")
        handle = await self.core.submit(self.request, options=AgentCoreRunOptions(checkpoint_handler=checkpoint))
        with pytest.raises(UserInputRequired):
            await handle.wait()
        assert (self.model_calls, self.tool_calls) == (1, 1)
        return handle.run_id

    async def close(self):
        await self.core.close()
        self.storage.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
async def test_public_resume_rejection_preserves_run_and_calls_no_provider(tmp_path, case):
    path = tmp_path / "resume.db"
    original = Host(path)
    try:
        run_id = await original.pause()
    finally:
        await original.close()
    host = Host(path, revision="2" if case["id"] == "preset-mismatch" else "1",
                leased=case["id"] != "lease-required")
    try:
        if case["id"] == "checkpoint-missing":
            run_id = (await host.storage.runs.begin(
                RunCreateParams(None, "waiting", None), AgentEvent("run.started"),
            )).run_id
        elif case["id"] == "terminal":
            await (await host.core.resume(run_id, host.request)).wait()
        elif case["id"] == "lease-conflict":
            assert await host.storage.leases.claim(run_id, "another-worker", lease_duration_ms=30000)
        elif case["id"] == "unreconciled-attempt":
            await host.storage.runs.reserve_model_attempt(run_id, "unfinished-attempt")
        before = await host.storage.runs.get(run_id)
        events = await host.storage.outputs.list_events(run_id, after_sequence=0)
        calls = (host.model_calls, host.tool_calls)
        with pytest.raises(ContractViolationError) as caught:
            await (await host.core.resume(run_id, host.request)).wait()
        assert caught.value.code == case["errorCode"]
        if case["id"] == "lease-required":
            with pytest.raises(ContractViolationError) as direct:
                await host.core.submit(host.request, options=AgentCoreRunOptions(
                    agent_execution_checkpoint=before.execution_checkpoint,
                    deadline_at_ms=before.deadline_at_ms,
                ))
            assert direct.value.code == "run_lease_required"
        assert (host.model_calls, host.tool_calls) == calls
        assert await host.storage.runs.get(run_id) == before
        assert await host.storage.outputs.list_events(run_id, after_sequence=0) == events
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_public_resume_after_reopen_keeps_tool_result_deadline_and_event_prefix(tmp_path):
    path = tmp_path / "resume.db"
    original = Host(path)
    try:
        run_id = await original.pause()
        before = await original.storage.runs.get(run_id)
        events = await original.storage.outputs.list_events(run_id, after_sequence=0)
    finally:
        await original.close()
    host = Host(path)
    try:
        result = await (await host.core.resume(run_id, host.request)).wait()
        assert result.status.value == "done"
        assert result.final_response == "42"
        assert host.model_calls > 0 and host.tool_calls == 0
        after = await host.storage.runs.get(run_id)
        assert after.deadline_at_ms == before.deadline_at_ms
        assert (await host.storage.outputs.list_events(run_id, after_sequence=0))[:len(events)] == events
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_child_resume_requires_root_scheduler_before_execution(tmp_path):
    host = Host(tmp_path / "child.db", tree=True)
    try:
        tree = host.storage.run_tree
        await tree.begin_root(BeginRootAgentCommand(
            "root", "root-agent", "root", "Root", "Own the task", "Read the value",
            AgentCapabilityGrant(can_spawn_agents=True), "begin",
        ))
        receipt = await tree.spawn_agents(SpawnAgentsCommand(
            "root", "spawn", (ChildAgentSpec("child", "Child", "Read", "Read"),),
        ))
        child = receipt.items[0].run
        with pytest.raises(ContractViolationError) as caught:
            await host.core.resume(child.run_id, host.request)
        assert caught.value.code == "child_run_resume_requires_scheduler"
        assert (host.model_calls, host.tool_calls) == (0, 0)
        assert await tree.get_run(child.run_id) == child
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_resume_rejects_checkpoint_changed_since_initial_read(tmp_path):
    from purra.run_controller import AgentRunController

    class Sink:
        async def emit(self, event):
            pass

    path = tmp_path / "race.db"
    first = Host(path)
    try:
        run_id = await first.pause()
        stale = await first.storage.runs.get(run_id)
        controller = AgentRunController(repository=first.storage.runs, event_sink=Sink())
        await controller.attach(stale)
        await controller.save_execution_checkpoint(replace(
            stale.execution_checkpoint, next_round=stale.execution_checkpoint.next_round + 1,
        ))
    finally:
        await first.close()
    host = Host(path, first_snapshot=stale)
    try:
        before = await host.storage.runs.get(run_id)
        events = await host.storage.outputs.list_events(run_id, after_sequence=0)
        with pytest.raises(ContractViolationError) as caught:
            await (await host.core.resume(run_id, host.request)).wait()
        assert caught.value.code == "agent_execution_checkpoint_conflict"
        assert (host.model_calls, host.tool_calls) == (0, 0)
        assert await host.storage.runs.get(run_id) == before
        assert await host.storage.outputs.list_events(run_id, after_sequence=0) == events
    finally:
        await host.close()
