import pytest
from purra.api import RecoveryWorker
from purra.observability.inspection import build_recovery_inspection
from purra_sqlite import SqliteAgentAdapters


@pytest.mark.asyncio
async def test_persisted_backoff_wake_and_stale_settlement(tmp_path):
    path = tmp_path / 'db'
    clock = [100]
    store = SqliteAgentAdapters(path, scope='one')
    schedule = store.recovery_schedule(interval_ms=10, max_backoff_ms=40, clock_ms=lambda: clock[0])
    assert await schedule.ready('run') == 0
    assert await schedule.settle('run', 0, True)
    store.close()
    store = SqliteAgentAdapters(path, scope='one')
    other = SqliteAgentAdapters(path, scope='one')
    isolated = SqliteAgentAdapters(path, scope='two')
    try:
        schedule = store.recovery_schedule(interval_ms=10, max_backoff_ms=40, clock_ms=lambda: clock[0])
        assert await schedule.ready('run') is None
        assert await isolated.recovery_schedule().ready('run') == 0
        clock[0] = 120
        revision = await schedule.ready('run')
        assert await schedule.settle('run', revision, True)
        clock[0] = 159
        assert await schedule.ready('run') is None
        await other.recovery_schedule().wake('run')
        token = await schedule.ready('run')
        assert token is not None
        assert not await schedule.settle('run', revision, True)
        assert await schedule.ready('run') == token
        assert await schedule.settle('run', token, False)
        clock[0] = 169
        assert await schedule.ready('run') is not None
    finally:
        store.close(); other.close(); isolated.close()


@pytest.mark.asyncio
async def test_worker_schedule_preserves_wake_during_failure(tmp_path):
    store = SqliteAgentAdapters(tmp_path / 'db', scope='one')
    schedule = store.recovery_schedule(clock_ms=lambda: 100)
    calls = []
    async def discover(): return ['one', 'two']
    async def inspect(_): return build_recovery_inspection({})
    async def resume(run_id):
        calls.append(run_id)
        if run_id == 'one':
            await schedule.wake(run_id)
            raise RuntimeError('synthetic')
    try:
        worker = RecoveryWorker(discover=discover, inspect=inspect, resume=resume, schedule=schedule)
        await worker.run_once()
        assert await schedule.ready('one') is not None
        assert await schedule.ready('two') is None
        result = await worker.run_once()
        assert calls == ['one', 'two', 'one']
        assert result[1].reasons == ('retry_not_due',)
    finally: store.close()


@pytest.mark.asyncio
async def test_failure_limit_survives_reopen_and_worker_skips_exhausted_run(tmp_path):
    path = tmp_path / 'limit.db'
    now = [100]
    store = SqliteAgentAdapters(path, scope='limit')
    schedule = store.recovery_schedule(interval_ms=1, max_backoff_ms=2, max_failures=2, clock_ms=lambda: now[0])
    for _ in range(2):
        token = await schedule.ready('bad')
        assert token is not None
        assert await schedule.settle('bad', token, True)
        now[0] += 10
    store.close()
    store = SqliteAgentAdapters(path, scope='limit')
    try:
        schedule = store.recovery_schedule(max_failures=2, clock_ms=lambda: now[0] + 1000000)
        assert await schedule.check('bad') == {'revision': None, 'reason': 'retry_exhausted'}
        called = []
        async def discover(): return ['bad', 'good']
        async def inspect(run_id):
            called.append(('inspect', run_id))
            return build_recovery_inspection({})
        async def resume(run_id): called.append(('resume', run_id))
        worker = RecoveryWorker(discover=discover, inspect=inspect, resume=resume, schedule=schedule)
        result = await worker.run_once()
        assert result[0].reasons == ('retry_exhausted',)
        assert called == [('inspect', 'good'), ('resume', 'good')]
        await schedule.wake('bad')
        token = await schedule.ready('bad')
        assert token is not None
        await schedule.settle('bad', token, True)
        assert (await schedule.check('bad'))['reason'] == 'retry_not_due'
        # Compatibility: omission does not turn old scheduling data into a hard limit.
        compatible = store.recovery_schedule(max_failures=2, clock_ms=lambda: now[0]+2000000)
        token = await compatible.ready('bad')
        assert token is not None
        await compatible.settle('bad', token, False)
        assert await store.recovery_schedule(clock_ms=lambda: now[0]+3000000).ready('bad') is not None
    finally: store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('limit', [True, 0, -1, 32, 1.5])
async def test_invalid_failure_limit(tmp_path, limit):
    store = SqliteAgentAdapters(tmp_path / 'db', scope='limit')
    try:
        with pytest.raises(ValueError): store.recovery_schedule(max_failures=limit)
    finally: store.close()
