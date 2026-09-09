import asyncio
import pytest
from purra.api import RecoveryWorker
from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra_sqlite import SqliteAgentAdapters


async def seed(store):
    return sorted([(await store.runs.begin(RunCreateParams(None,'cursor',None),AgentEvent('run.started'))).run_id for _ in range(3)])


@pytest.mark.asyncio
async def test_cursor_reopen_partial_prefix_stale_ack_and_wrap(tmp_path):
    path=tmp_path/'db'; store=SqliteAgentAdapters(path,scope='cursor')
    try:
        ids=await seed(store)
        cursor=store.recovery_cursor('worker',page_size=2)
        competing=store.recovery_cursor('worker',page_size=2)
        assert await cursor.discover()==tuple(ids[:2])
        assert await competing.discover()==tuple(ids[:2])
        with pytest.raises(ValueError): await cursor.acknowledge([ids[1]])
        await cursor.acknowledge([ids[0]])
        with pytest.raises(ValueError,match='recovery_cursor_conflict'): await competing.acknowledge(ids[:2])
        store.close(); store=SqliteAgentAdapters(path,scope='cursor')
        cursor=store.recovery_cursor('worker',page_size=2)
        assert await cursor.discover()==tuple(ids[1:])
        # Discovery without acknowledgement is replayable after restart.
        cursor=store.recovery_cursor('worker',page_size=2)
        assert await cursor.discover()==tuple(ids[1:])
        await cursor.acknowledge(ids[1:])
        assert await cursor.discover()==tuple(ids[:2])
        assert await store.recovery_cursor('other',page_size=1).discover()==(ids[0],)
    finally: store.close()


@pytest.mark.asyncio
async def test_worker_preserves_unsettled_and_stopped_candidates(tmp_path):
    store=SqliteAgentAdapters(tmp_path/'db',scope='cursor')
    try:
        ids=await seed(store); cursor=store.recovery_cursor('worker')
        stop=asyncio.Event(); seen=[]
        async def inspect(run_id):
            if run_id==ids[1]: stop.set()
            return {'blockers':[]}
        async def resume(run_id): seen.append(run_id)
        worker=RecoveryWorker(discover=cursor.discover,acknowledge=cursor.acknowledge,inspect=inspect,resume=resume)
        await worker.run(stop=stop)
        assert seen==ids[:1]
        assert await store.recovery_cursor('worker').discover()==tuple(ids[1:])
        class BrokenSchedule:
            async def ready(self, _): return 0
            async def settle(self, *_): raise RuntimeError('persist failed')
        cursor=store.recovery_cursor('worker')
        worker=RecoveryWorker(discover=cursor.discover,acknowledge=cursor.acknowledge,inspect=lambda _: ready_report(),resume=resume,schedule=BrokenSchedule())
        with pytest.raises(RuntimeError,match='persist failed'): await worker.run_once()
        assert await store.recovery_cursor('worker').discover()==tuple(ids[1:])
        cursor=store.recovery_cursor('worker')
        worker=RecoveryWorker(discover=cursor.discover,acknowledge=cursor.acknowledge,inspect=lambda _: ready_report(),resume=resume,max_runs_per_scan=1)
        await worker.run_once()
        assert await store.recovery_cursor('worker').discover()==tuple(ids[2:])
    finally: store.close()

async def ready_report(): return {'blockers':[]}


@pytest.mark.asyncio
async def test_empty_page_wraps_and_ack_write_failure_replays(tmp_path):
    store=SqliteAgentAdapters(tmp_path/'db',scope='cursor')
    try:
        cursor=store.recovery_cursor('worker')
        assert await cursor.discover()==()
        await cursor.acknowledge([])
        ids=await seed(store)
        cursor=store.recovery_cursor('worker'); assert await cursor.discover()==tuple(ids)
        store._db.execute("CREATE TRIGGER fail_cursor BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture'); END")
        with pytest.raises(Exception): await cursor.acknowledge(ids)
        store._db.execute('DROP TRIGGER fail_cursor')
        assert await store.recovery_cursor('worker').discover()==tuple(ids)
    finally: store.close()
