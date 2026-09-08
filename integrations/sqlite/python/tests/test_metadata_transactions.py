import pytest
from purra.storage import dump_storage_value, load_storage_value
from purra_sqlite import SqliteAgentAdapters


@pytest.mark.asyncio
async def test_metadata_transactions_observe_other_connections_and_rollback(tmp_path):
    path = tmp_path / 'db'
    store = SqliteAgentAdapters(path, scope='metadata')
    other = SqliteAgentAdapters(path, scope='metadata')
    try:
        schedule = store.recovery_schedule(clock_ms=lambda: 0)
        token = await schedule.wake('run')
        await other.recovery_schedule().wake('run')
        assert not await schedule.settle('run', token, True)
        current = await schedule.ready('run')
        before = store._db.execute('SELECT body FROM purra_state').fetchone()[0]
        store._db.execute("CREATE TRIGGER fail_metadata BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture'); END")
        with pytest.raises(Exception): await schedule.settle('run', current, True)
        store._db.execute('DROP TRIGGER fail_metadata')
        assert store._db.execute('SELECT body FROM purra_state').fetchone()[0] == before
        assert await schedule.ready('run') == current
        state = load_storage_value(before); state['groups']['run']['run_count'] = False
        other._db.execute('UPDATE purra_state SET body=?', (dump_storage_value(state),))
        with pytest.raises(ValueError): await schedule.check('run')
        other._db.execute('UPDATE purra_state SET body=?', (before,))
        assert await schedule.ready('run') == current
        statements = []; store._db.set_trace_callback(statements.append)
        changes = store._db.total_changes
        await schedule.check('run')
        assert store._db.total_changes == changes
        assert 'BEGIN' in statements and 'BEGIN IMMEDIATE' not in statements
    finally: other.close(); store.close()
