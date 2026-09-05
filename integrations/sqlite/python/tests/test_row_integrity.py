from dataclasses import replace
from datetime import datetime, timezone
import sqlite3

import pytest

from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra.output.contracts import AgentOutputEventDraft, RunLifecycleOutputDraft
from purra.ports.run_lifecycle import RunCommit
from purra_sqlite import SqliteAgentAdapters
from purra_sqlite.codec import dumps


def draft(run, key):
    return AgentOutputEventDraft(run_id=run, turn_id=None, output_stream_id=None, invocation_id=None,
        source_event_key=key, source="domain", kind="domain.effect", channel="diagnostic",
        visibility="private", payload={"text": key}, occurred_at=datetime.now(timezone.utc))


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["run_id", "root_run_id", "sequence", "root_sequence"])
async def test_body_column_mismatch_rejected_on_replay_history_and_reads_without_partial_writes(tmp_path, field):
    path = tmp_path / "integrity.db"
    storage = SqliteAgentAdapters(path, scope="integrity")
    try:
        roots = [(await storage.runs.begin(RunCreateParams(None, "root", None), AgentEvent("run.started"))).run_id for _ in range(2)]
        run = roots[0]
        original = draft(run, "original")
        event = await storage.outputs.append_event(original)
        invalid = roots[1] if field in ("run_id", "root_run_id") else 99
        corrupt = replace(event, **{field: invalid})
        with sqlite3.connect(path) as db:
            db.execute("UPDATE purra_output_events SET body=? WHERE scope=? AND run_id=? AND sequence=1", (dumps(corrupt), "integrity", run))
        storage.close()
        storage = SqliteAgentAdapters(path, scope="integrity")
        with sqlite3.connect(path) as db:
            snapshot = db.execute("SELECT * FROM purra_state").fetchall()
            rows = db.execute("SELECT * FROM purra_output_events").fetchall()

        async def transaction_read():
            async with storage.transaction():
                pass

        operations = (
            lambda: storage.outputs.append_event(original),
            lambda: storage.outputs.append_batch((draft(run, "new"), original)),
            lambda: storage.outputs.commit_run_lifecycle(run,
                RunCommit(terminal_status="done", final_response="done", events=(AgentEvent("run.completed", run_id=run),)),
                RunLifecycleOutputDraft(source_event_key="terminal", status="done", payload={"status": "done"}, occurred_at=datetime.now(timezone.utc))),
            lambda: storage.outputs.list_events(run, after_sequence=0),
            lambda: storage.outputs.list_root_events(run, after_root_sequence=0),
            lambda: storage.runs.get(run),
            transaction_read,
            lambda: storage.leases.claim(run, "owner", lease_duration_ms=30000),
        )
        for operation in operations:
            with pytest.raises(ValueError, match="output journal"):
                await operation()
            with sqlite3.connect(path) as db:
                assert db.execute("SELECT * FROM purra_state").fetchall() == snapshot
                assert db.execute("SELECT * FROM purra_output_events").fetchall() == rows

        # The same persisted identity must remain replayable once its body is restored.
        with sqlite3.connect(path) as db:
            db.execute("UPDATE purra_output_events SET body=? WHERE scope=? AND run_id=? AND sequence=1", (dumps(event), "integrity", run))
        assert await storage.outputs.append_event(original) == event
        added = await storage.outputs.append_event(draft(run, "next"))
        assert added.sequence == 2 and added.root_sequence == 2
        assert await storage.outputs.list_events(run, after_sequence=0) == (event, added)
    finally:
        storage.close()
