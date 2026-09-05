from datetime import datetime, timezone
import sqlite3

import pytest

from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra.errors import ContractViolationError
from purra.output.contracts import AgentOutputEventDraft
from purra_sqlite import SqliteAgentAdapters
from purra.storage import load_storage_value as loads


def draft(run_id, key):
    return AgentOutputEventDraft(
        run_id=run_id, turn_id=None, output_stream_id=None, invocation_id=None,
        source_event_key=key, source="domain", kind="domain.effect",
        channel="diagnostic", visibility="private", payload={"text": key},
        occurred_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_indexed_replay_scope_and_child_order_without_snapshot_loading(tmp_path, monkeypatch):
    import purra_sqlite
    path = tmp_path / "journal.db"
    storage = SqliteAgentAdapters(path, scope="a")
    other = SqliteAgentAdapters(path, scope="b")
    try:
        root = (await storage.runs.begin(RunCreateParams(None, "root", None), AgentEvent("run.started"))).run_id
        child = (await storage.runs.begin(RunCreateParams(None, "child", None, root_run_id=root, parent_run_id=root), AgentEvent("run.started"))).run_id
        other_root = (await other.runs.begin(RunCreateParams(None, "other", None), AgentEvent("run.started"))).run_id
        assert other_root == root
        rows = [await storage.outputs.append_event(draft(run, key)) for run, key in [(root, "a"), (child, "b"), (root, "c")]]
        storage.close()
        storage = SqliteAgentAdapters(path, scope="a")

        def reject_snapshot(*args):
            raise AssertionError("event queries must not decode the execution snapshot")

        monkeypatch.setattr(purra_sqlite, "StorageSession", reject_snapshot)
        assert await storage.outputs.list_events(root, after_sequence=1, limit=1) == (rows[2],)
        assert await storage.outputs.list_root_events(root, after_root_sequence=1, limit=2) == tuple(rows[1:])
        assert await storage.outputs.list_events(child, after_sequence=0) == (rows[1],)
        assert await other.outputs.list_events(root, after_sequence=0) == ()
        assert await storage.outputs.list_events(root, after_sequence=99) == ()
        await storage.publisher.publish_committed(rows[2])
        with pytest.raises(ContractViolationError, match="Root journal"):
            await storage.outputs.list_root_events(child, after_root_sequence=0)
        with pytest.raises(ContractViolationError) as missing:
            await storage.outputs.list_events("missing", after_sequence=0)
        assert missing.value.code == "run_not_found"
        with pytest.raises(ValueError):
            await storage.outputs.list_events(root, after_sequence=-1)
        with pytest.raises(ValueError):
            await storage.outputs.list_events(root, after_sequence=0, limit=0)
        with sqlite3.connect(path) as db:
            saved = loads(db.execute("SELECT body FROM purra_state WHERE scope='a'").fetchone()[0])
            assert "output_events" not in saved["groups"]["run"]
            assert "root_output_events" not in saved["groups"]["run"]
            assert "events_by_source_key" not in saved["groups"]["run"]
            plan = db.execute("EXPLAIN QUERY PLAN SELECT body FROM purra_output_events WHERE scope=? AND sdk='python' AND root_run_id=? AND root_sequence>? ORDER BY root_sequence LIMIT ?", ("a", root, 1, 2)).fetchall()
            assert any("SEARCH" in row[3] for row in plan)
            assert not any("TEMP B-TREE" in row[3] for row in plan)
    finally:
        storage.close(); other.close()


@pytest.mark.asyncio
async def test_journal_failure_rolls_back_state_and_replay_only_appends_new_rows(tmp_path):
    path = tmp_path / "atomic.db"
    storage = SqliteAgentAdapters(path, scope="atomic")
    db = sqlite3.connect(path, isolation_level=None)
    try:
        run_id = (await storage.runs.begin(RunCreateParams(None, "root", None), AgentEvent("run.started"))).run_id
        first_draft = draft(run_id, "first")
        first = await storage.outputs.append_event(first_draft)
        original_state = db.execute("SELECT body FROM purra_state").fetchone()[0]
        db.executescript("""
            CREATE TRIGGER reject_third BEFORE INSERT ON purra_output_events WHEN NEW.sequence=3 BEGIN SELECT RAISE(ABORT, 'injected journal failure'); END;
            CREATE TRIGGER no_update BEFORE UPDATE ON purra_output_events BEGIN SELECT RAISE(ABORT, 'journal rewrite'); END;
            CREATE TRIGGER no_delete BEFORE DELETE ON purra_output_events BEGIN SELECT RAISE(ABORT, 'journal rewrite'); END;
        """)
        with pytest.raises(sqlite3.IntegrityError, match="injected journal failure"):
            await storage.outputs.append_batch((draft(run_id, "second"), draft(run_id, "third")))
        assert db.execute("SELECT body FROM purra_state").fetchone()[0] == original_state
        assert await storage.outputs.list_events(run_id, after_sequence=0) == (first,)
        assert await storage.outputs.append_event(first_draft) == first
        storage.close(); storage = SqliteAgentAdapters(path, scope="atomic")
        second = await storage.outputs.append_event(draft(run_id, "second"))
        assert second.sequence == 2
        assert db.execute("SELECT count(*) FROM purra_output_events").fetchone()[0] == 2
        db.executescript("DROP TRIGGER no_delete; DELETE FROM purra_output_events WHERE sequence=2;")
        with pytest.raises(ValueError, match="incomplete output journal"):
            await storage.runs.get(run_id)
    finally:
        db.close(); storage.close()
