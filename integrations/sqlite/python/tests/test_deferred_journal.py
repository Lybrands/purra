from dataclasses import replace
from datetime import datetime, timezone
import sqlite3

import pytest

from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.contracts import AgentMessage, MessageRole, ModelTokenUsage, RunCreateParams
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.output.contracts import AgentOutputEventDraft, OutputStreamSpec, RunLifecycleOutputDraft, provider_delta_batch_digest
from purra.planning_stream import PLANNING_STREAM_SCHEMA, PlanningScope, PlanningStreamParser
from purra.ports.run_lifecycle import RunCommit
from purra_sqlite import SqliteAgentAdapters
from purra_sqlite.journal import OutputJournal


def draft(run, key, **changes):
    return replace(AgentOutputEventDraft(
        run_id=run, turn_id=None, output_stream_id=None, invocation_id=None,
        source_event_key=key, source="domain", kind="domain.effect",
        channel="diagnostic", visibility="private", payload={"text": key},
        occurred_at=datetime.now(timezone.utc),
    ), **changes)


async def begin(storage):
    return (await storage.runs.begin(RunCreateParams(None, "root", None), AgentEvent("run.started"))).run_id


@pytest.mark.asyncio
async def test_same_root_writes_defer_bodies_and_preserve_replay_checkpoint_and_usage(tmp_path, monkeypatch):
    path = tmp_path / "deferred.db"
    storage = SqliteAgentAdapters(path, scope="deferred")
    try:
        run = await begin(storage)
        history = [draft(run, f"history:{index}") for index in range(32)]
        prior = await storage.outputs.append_batch(history)
        checkpoint = AgentExecutionCheckpoint(run_id=run, next_round=2, round_limit=10,
            messages=(AgentMessage(role=MessageRole.USER, content="resume"),))
        def forbid_history(*args, **kwargs):
            raise AssertionError("ordinary writes must not load event history")
        with monkeypatch.context() as patch:
            patch.setattr(OutputJournal, "_load_history", forbid_history)
            await storage.runs.reserve_model_attempt(run, "invocation")
            usage = ModelTokenUsage(input_tokens=3, generation_tokens=5)
            await storage.runs.settle_model_attempt(run, "invocation", usage)
            await storage.runs.settle_model_attempt(run, "invocation", usage)
            await storage.runs.commit(run, RunCommit(execution_checkpoint=checkpoint))
            added = await storage.outputs.append_batch((history[0], draft(run, "new:1"), draft(run, "new:2")))
            assert added[0] == prior[0]
            assert [event.sequence for event in added[1:]] == [33, 34]
            with pytest.raises(ContractViolationError):
                await storage.outputs.append_batch((draft(run, "should-rollback"), replace(history[1], payload={"text": "conflict"})))
        storage.close()
        storage = SqliteAgentAdapters(path, scope="deferred")
        assert await storage.outputs.list_events(run, after_sequence=0) == (*prior, *added[1:])
        assert (await storage.runs.get(run)).execution_checkpoint == checkpoint
        budget = await storage.runs.reserve_model_attempt(run, "invocation")
        assert budget.model_attempts == 1 and budget.generation_tokens == 5
        async with storage.transaction() as adapters:
            assert await adapters.outputs.list_root_events(run, after_root_sequence=32) == added[1:]
    finally:
        storage.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["DELETE FROM purra_output_events WHERE sequence=2",
    "DELETE FROM purra_output_events WHERE sequence=3",
    "UPDATE purra_output_events SET root_sequence=9 WHERE sequence=2"])
async def test_deferred_writes_reject_missing_or_discontinuous_journals(tmp_path, corruption):
    path = tmp_path / "corrupt.db"
    storage = SqliteAgentAdapters(path, scope="deferred")
    try:
        run = await begin(storage)
        await storage.outputs.append_batch(tuple(draft(run, f"e:{i}") for i in range(3)))
        with sqlite3.connect(path) as db:
            before = db.execute("SELECT body FROM purra_state").fetchone()[0]
            db.execute(corruption)
        with pytest.raises(ValueError, match="output journal"):
            await storage.outputs.append_event(draft(run, "rejected"))
        with sqlite3.connect(path) as db:
            assert db.execute("SELECT body FROM purra_state").fetchone()[0] == before
    finally:
        storage.close()


@pytest.mark.asyncio
async def test_planning_reads_persisted_provider_evidence_and_terminal_closes_operation(tmp_path, monkeypatch):
    storage = SqliteAgentAdapters(tmp_path / "planning.db", scope="deferred")
    try:
        run = await begin(storage)
        await storage.outputs.append_event(draft(run, "operation", source="runtime", kind="operation.started",
            channel="operation", payload={"operationId": "plan", "kind": "planning"}))
        spec = OutputStreamSpec(output_stream_id="stream", run_id=run, turn_id=None, invocation_id="invoke",
            intent="structured_private", commit_mode="private", output_protocol=PLANNING_STREAM_SCHEMA,
            planning_scope=PlanningScope(run, "plan"), planning_attempt=1)
        loaded = []
        original = OutputJournal._load_history
        def record_load(self, run_id, root_run_id, **kwargs):
            loaded.append(run_id)
            return original(self, run_id, root_run_id, **kwargs)
        monkeypatch.setattr(OutputJournal, "_load_history", record_load)
        await storage.outputs.open_stream(spec)
        wire = '{"v":1,"type":"progress","text":"核对资料。"}\n'
        progress = PlanningStreamParser().feed(wire)[0]
        entries = ({"sourceChunkIndex": 1, "kind": "provider.content_delta", "payload": {"delta": wire}},)
        raw = draft(run, "raw", source="provider", kind="provider.delta_batch",
            output_stream_id="stream", invocation_id="invoke", payload={
                "schemaVersion": "purra.provider-delta-batch/v1", "entries": entries,
                "sourceChunkStart": 1, "sourceChunkEnd": 1, "payloadDigest": provider_delta_batch_digest(entries),
            })
        projected = draft(run, "planning:invoke:1", source="provider", kind="planning.progress",
            output_stream_id="stream", invocation_id="invoke", channel="commentary", visibility="public",
            payload={"schemaVersion": PLANNING_STREAM_SCHEMA, "operationId": "plan", "revision": 0,
                "attempt": 1, **progress.to_mapping()})
        await storage.outputs.append_event(raw)
        before = await storage.outputs.list_events(run, after_sequence=0)
        with pytest.raises(ContractViolationError, match="Provider source"):
            await storage.outputs.append_event(replace(projected, payload={**projected.payload, "text": "伪造内容"}))
        assert await storage.outputs.list_events(run, after_sequence=0) == before
        accepted = await storage.outputs.append_event(projected)
        assert await storage.outputs.append_event(projected) == accepted
        assert loaded and set(loaded) == {run}
        await storage.outputs.commit_run_lifecycle(run,
            RunCommit(terminal_status="done", final_response="done", events=(AgentEvent("run.completed", run_id=run),)),
            RunLifecycleOutputDraft(turn_id=None, status="done", payload={"status": "done"},
                source_event_key="terminal", occurred_at=datetime.now(timezone.utc)))
        events = await storage.outputs.list_events(run, after_sequence=0)
        assert sum(event.kind == "operation.finished" for event in events) == 1
        assert sum(event.kind == "stream.aborted" for event in events) == 1
    finally:
        storage.close()
