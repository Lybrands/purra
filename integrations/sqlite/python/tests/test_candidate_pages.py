import sqlite3
import pytest
from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra_sqlite import SqliteAgentAdapters
import purra_sqlite


@pytest.mark.asyncio
async def test_index_pages_do_not_read_state_or_journal_and_survive_reopen(tmp_path, monkeypatch):
    path = tmp_path / 'pages.db'
    storage = SqliteAgentAdapters(path, scope='pages')
    other = SqliteAgentAdapters(path, scope='other')
    writer = sqlite3.connect(path, isolation_level=None)
    try:
        ids = sorted([(await storage.runs.begin(RunCreateParams(None, 'page', None), AgentEvent('run.started'))).run_id for _ in range(3)])
        await other.runs.begin(RunCreateParams(None, 'other', None), AgentEvent('run.started'))
        storage.close(); storage = SqliteAgentAdapters(path, scope='pages')
        def forbidden(*args, **kwargs): raise AssertionError('candidate query restored Core state')
        monkeypatch.setattr(purra_sqlite, 'StorageSession', forbidden)
        queries = []
        storage._db.set_trace_callback(queries.append)
        writer.execute('BEGIN IMMEDIATE')
        writer.execute("DELETE FROM purra_journal_runs WHERE scope='pages' AND run_id=?", (ids[0],))
        first = await storage.list_run_candidates(limit=2)
        assert first == {'authority': 'candidate_only', 'runIds': tuple(ids[:2]), 'nextAfterRunId': ids[1]}
        last = await storage.list_run_candidates(after_run_id=first['nextAfterRunId'], limit=2)
        assert last['runIds'] == (ids[2],) and last['nextAfterRunId'] is None
        assert (await storage.list_run_candidates(after_run_id=ids[2]))['runIds'] == ()
        writer.rollback()
        assert not any('purra_output_events' in q or ('body' in q.lower() and "sdk='purra.approvals'" not in q) or 'BEGIN IMMEDIATE' in q for q in queries)
        plan = storage._db.execute("EXPLAIN QUERY PLAN SELECT run_id FROM purra_journal_runs WHERE scope=? AND sdk='python' AND run_id>? ORDER BY run_id LIMIT ?", ('pages', ids[0], 3)).fetchall()
        assert any('COVERING INDEX' in row[3] for row in plan)
        assert not any('TEMP B-TREE' in row[3] for row in plan)
    finally:
        if writer.in_transaction: writer.rollback()
        writer.close(); storage.close(); other.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('limit', [True, 0, -1, 1.5, 1001])
async def test_candidate_limit_rejected(tmp_path, limit):
    storage = SqliteAgentAdapters(tmp_path / 'db', scope='page')
    try:
        with pytest.raises(ValueError): await storage.list_run_candidates(limit=limit)
    finally: storage.close()
