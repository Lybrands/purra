import pytest
from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra.ports import RunCommit
from purra_sqlite import SqliteAgentAdapters


@pytest.mark.asyncio
async def test_batch_inspection_matches_individual_and_restores_once(tmp_path):
    storage = SqliteAgentAdapters(tmp_path / 'db', scope='batch')
    try:
        ids = [(await storage.runs.begin(RunCreateParams(None, 'batch', None), AgentEvent('run.started'))).run_id for _ in range(3)]
        await storage.runs.commit(ids[0], RunCommit(terminal_status='done', final_response='done', events=(AgentEvent('run.completed', run_id=ids[0]),)))
        expected = {key: await storage.inspect_recovery(key) for key in ids}
        queries = []
        storage._db.set_trace_callback(queries.append)
        assert await storage.inspect_recovery_many(ids + [ids[0]]) == expected
        reads = [q for q in queries if 'SELECT version,body FROM purra_state' in q and "sdk='python'" in q]
        assert len(reads) == 1
        assert not any('purra_output_events' in q or 'BEGIN IMMEDIATE' in q for q in queries)
        queries.clear()
        assert await storage.inspect_recovery_many([]) == {} and queries == []
        with pytest.raises(Exception): await storage.inspect_recovery_many([ids[0], 'missing'])
        assert await storage.inspect_recovery_many(ids) == expected
        with pytest.raises(ValueError): await storage.inspect_recovery_many(ids * 34)
    finally: storage.close()
