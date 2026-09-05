import asyncio
from datetime import datetime, timezone

import pytest

from purra.contracts import RunCreateParams, ToolCall, ToolHandlerResult
from purra.events import AgentEvent
from purra.errors import ContractViolationError
from purra.output.contracts import AgentOutputEventDraft
from purra.testing import assert_artifact_store_conforms, assert_long_task_repository_conforms
from purra_sqlite import SqliteAgentAdapters
from purra_sqlite.journal import OutputJournal


async def begin(storage):
    return (await storage.runs.begin(RunCreateParams(None, "metadata", None), AgentEvent("run.started"))).run_id


async def emit(storage, run_id):
    return await storage.outputs.append_event(AgentOutputEventDraft(
        run_id=run_id, turn_id=None, output_stream_id=None, invocation_id=None,
        source_event_key="metadata:event", source="domain", kind="domain.effect",
        channel="diagnostic", visibility="private", payload={"text": "preserve"},
        occurred_at=datetime.now(timezone.utc),
    ))


@pytest.mark.asyncio
async def test_independent_metadata_operations_preserve_unloaded_journal(tmp_path, monkeypatch):
    storage = SqliteAgentAdapters(tmp_path / "metadata.db", scope="metadata")
    try:
        run = await begin(storage)
        event = await emit(storage, run)
        assert await storage.leases.claim(run, "owner", lease_duration_ms=30000)

        def forbidden(*args):
            raise AssertionError("metadata operations must not load or flush output history")

        with monkeypatch.context() as patch:
            patch.setattr(OutputJournal, "restore", forbidden)
            patch.setattr(OutputJournal, "append", forbidden)
            assert await storage.leases.renew(run, "owner", lease_duration_ms=30000)
            assert (await storage.leases.get(run)).owner_id == "owner"
            assert await storage.leases.release(run, "owner")
            assert await storage.list_running() == (run,)
            assert await storage.leases.request_cancellation(run)
            async def effect():
                return ToolHandlerResult("receipt")
            call = ToolCall("receipt", "write", "{}")
            await storage.idempotency.execute_once(run, call, effect)
            assert (await storage.idempotency.execute_once(run, call, effect)).from_cache
            await assert_artifact_store_conforms(artifacts=storage.artifacts, claims=storage.artifact_claims, maintenance=storage.artifact_maintenance)
            await assert_long_task_repository_conforms(storage.long_tasks)
        assert await storage.outputs.list_events(run, after_sequence=0) == (event,)
        assert (await storage.runs.get(run)).run_id == run
    finally:
        storage.close()


@pytest.mark.asyncio
async def test_concurrent_tool_claim_and_interleaved_output_preserve_both_commits(tmp_path):
    path = tmp_path / "concurrent.db"
    first = SqliteAgentAdapters(path, scope="shared")
    second = SqliteAgentAdapters(path, scope="shared")
    task = None
    started, finish = asyncio.Event(), asyncio.Event()
    effects = 0
    try:
        run = await begin(first)
        call = ToolCall("once", "write", "{}")
        async def effect():
            nonlocal effects
            effects += 1
            started.set()
            await finish.wait()
            return ToolHandlerResult("committed")
        task = asyncio.create_task(first.idempotency.execute_once(run, call, effect))
        await asyncio.wait_for(started.wait(), 2)
        with pytest.raises(ContractViolationError) as error:
            await second.idempotency.execute_once(run, call, effect)
        assert error.value.code == "tool_effect_unknown"
        event = await emit(second, run)
        assert await second.leases.request_cancellation(run)
        finish.set()
        await task
        assert (await second.idempotency.execute_once(run, call, effect)).from_cache
        assert effects == 1
        assert await first.outputs.list_events(run, after_sequence=0) == (event,)
        assert (await first.leases.get(run)).cancellation_requested_at_ms is not None
        assert (await first.runs.get(run)).run_id == run
    finally:
        finish.set()
        if task is not None:
            await task
        first.close(); second.close()
