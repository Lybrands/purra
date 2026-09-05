from datetime import datetime, timezone
import sqlite3

import pytest

from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra.output.contracts import AgentOutputEventDraft
from purra_sqlite import SqliteAgentAdapters


def draft(run, key):
    return AgentOutputEventDraft(run_id=run, turn_id=None, output_stream_id=None, invocation_id=None,
        source_event_key=key, source="domain", kind="domain.effect", channel="diagnostic",
        visibility="private", payload={"text": key}, occurred_at=datetime.now(timezone.utc))


@pytest.mark.asyncio
async def test_existing_v3_adds_covering_indexes_without_rewriting_state_or_events(tmp_path):
    path = tmp_path / "cover.db"
    storage = SqliteAgentAdapters(path, scope="cover")
    try:
        root = (await storage.runs.begin(RunCreateParams(None, "root", None), AgentEvent("run.started"))).run_id
        child = (await storage.runs.begin(RunCreateParams(None, "child", None, root_run_id=root, parent_run_id=root), AgentEvent("run.started"))).run_id
        other = (await storage.runs.begin(RunCreateParams(None, "other", None), AgentEvent("run.started"))).run_id
        for run in (root, child, other):
            await storage.outputs.append_event(draft(run, f"first:{run}"))
        storage.close()
        with sqlite3.connect(path) as db:
            db.execute("DROP INDEX purra_output_sequence_cover")
            db.execute("DROP INDEX purra_journal_roots")
            snapshot = db.execute("SELECT * FROM purra_state").fetchall()
            events = db.execute("SELECT * FROM purra_output_events ORDER BY run_id,sequence").fetchall()
        storage = SqliteAgentAdapters(path, scope="cover")
        with sqlite3.connect(path) as db:
            assert db.execute("SELECT * FROM purra_state").fetchall() == snapshot
            assert db.execute("SELECT * FROM purra_output_events ORDER BY run_id,sequence").fetchall() == events
        statements = []
        storage._db.set_trace_callback(statements.append)
        added = await storage.outputs.append_event(draft(child, "next"))
        storage._db.set_trace_callback(None)
        query = next(sql for sql in statements if "GROUP BY run_id" in sql)
        plan = [row[3] for row in storage._db.execute("EXPLAIN QUERY PLAN " + query)]
        assert any("USING COVERING INDEX purra_output_sequence_cover" in row and "root_run_id=?" in row for row in plan)
        assert not any("TEMP B-TREE" in row for row in plan)
        assert added.sequence == 2 and added.root_sequence == 3
        assert len(await storage.outputs.list_events(other, after_sequence=0)) == 1
    finally:
        storage.close()
