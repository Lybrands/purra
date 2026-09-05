from datetime import datetime, timezone
import sqlite3

import pytest

from purra.contracts import ModelFinishReason, RunCreateParams, RuntimeLimits
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.output.contracts import AgentOutputEventDraft, OutputStreamSpec
from purra.execution.ownership import execution_owner
from purra_sqlite import SqliteAgentAdapters
import purra_sqlite.journal as journal


def draft(run, key):
    return AgentOutputEventDraft(
        run_id=run, turn_id=None, output_stream_id=None, invocation_id=None,
        source_event_key=key, source="domain", kind="domain.effect", channel="diagnostic",
        visibility="private", payload={"text": key}, occurred_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_root_writes_defer_history_but_keep_sibling_budgets_and_global_idempotency(tmp_path, monkeypatch):
    path = tmp_path / "roots.db"
    storage = SqliteAgentAdapters(path, scope="roots")
    try:
        limits = RuntimeLimits(max_run_generation_tokens=None, max_model_invocation_attempts=1)
        root = (await storage.runs.begin(RunCreateParams(None, "root", None, runtime_limits=limits), AgentEvent("run.started"))).run_id
        children = [(await storage.runs.begin(RunCreateParams(None, "child", None, root_run_id=root, parent_run_id=root), AgentEvent("run.started"))).run_id for _ in range(2)]
        other = (await storage.runs.begin(RunCreateParams(None, "other", None), AgentEvent("run.started"))).run_id
        for run in [root, *children, other]:
            await storage.outputs.append_event(draft(run, f"initial:{run}"))
        other_events = await storage.outputs.list_events(other, after_sequence=0)
        seen = set()
        original = journal.load_many
        def scoped_decode(texts):
            for event in original(texts):
                assert event.root_run_id == root, "unrelated output history was hydrated"
                seen.add(event.run_id)
                yield event
        with monkeypatch.context() as patch:
            patch.setattr(journal, "load_many", scoped_decode)
            current = draft(children[0], "new-child-event")
            written = await storage.outputs.append_event(current)
            assert await storage.outputs.append_event(current) == written
            await storage.runs.reserve_model_attempt(children[0], "attempt-1")
            with pytest.raises(ContractViolationError) as exhausted:
                await storage.runs.reserve_model_attempt(children[1], "attempt-2")
            assert exhausted.value.code == "runtime_budget_exceeded"
            with pytest.raises(ContractViolationError, match="already bound"):
                await storage.outputs.append_event(draft(root, f"initial:{other}"))
        assert seen == set(), "ordinary writes should not decode historical event bodies"
        assert await storage.outputs.list_events(other, after_sequence=0) == other_events
        assert [event.root_sequence for event in await storage.outputs.list_root_events(root, after_root_sequence=0)] == [1, 2, 3, 4]
        with sqlite3.connect(path) as db:
            plan = db.execute("EXPLAIN QUERY PLAN SELECT body FROM purra_output_events WHERE scope=? AND sdk='python' AND json_extract(body, '$[2].source_event_key[1]')=?", ("roots", f"initial:{other}")).fetchall()
            assert any("purra_python_output_source" in row[3] for row in plan)
            db.execute("DELETE FROM purra_output_events WHERE run_id=?", (other,))
        await storage.outputs.append_event(draft(root, "after-unrelated-corruption"))
        with pytest.raises(ValueError, match="incomplete output journal"):
            await storage.runs.get(other)
        with pytest.raises(ValueError, match="incomplete output journal"):
            async with storage.transaction():
                pass
    finally:
        storage.close()


@pytest.mark.asyncio
async def test_stream_target_uses_its_run_and_fences_a_stale_owner(tmp_path, monkeypatch):
    storage = SqliteAgentAdapters(tmp_path / "stream-root.db", scope="stream")
    try:
        root = (await storage.runs.begin(RunCreateParams(None, "root", None), AgentEvent("run.started"))).run_id
        other = (await storage.runs.begin(RunCreateParams(None, "other", None), AgentEvent("run.started"))).run_id
        await storage.outputs.append_event(draft(root, "root-event"))
        other_event = await storage.outputs.append_event(draft(other, "other-event"))
        assert await storage.leases.claim(root, "first", lease_duration_ms=30000)
        await storage.outputs.open_stream(OutputStreamSpec(
            output_stream_id=other, run_id=root, turn_id=None, invocation_id="invoke",
            intent="structured_private", commit_mode="private",
        ))
        assert await storage.leases.release(root, "first")
        assert await storage.leases.claim(root, "second", lease_duration_ms=30000)
        original = journal.load_many
        def scoped_decode(texts):
            for event in original(texts):
                assert event.root_run_id == root
                yield event
        with monkeypatch.context() as patch:
            patch.setattr(journal, "load_many", scoped_decode)
            token = execution_owner.set("first")
            try:
                with pytest.raises(ContractViolationError) as stale:
                    await storage.outputs.commit_stream(other, ModelFinishReason.STOP)
                assert stale.value.code == "run_lease_lost"
            finally:
                execution_owner.reset(token)
            token = execution_owner.set("second")
            try:
                committed = await storage.outputs.commit_stream(other, ModelFinishReason.STOP)
                assert committed.run_id == root
                assert await storage.outputs.commit_stream(other, ModelFinishReason.STOP) == committed
            finally:
                execution_owner.reset(token)
        assert await storage.outputs.list_events(other, after_sequence=0) == (other_event,)
    finally:
        storage.close()
