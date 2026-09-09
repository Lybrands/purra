import asyncio

import pytest

from purra.api import RecoveryWorker
from purra.observability.inspection import build_recovery_inspection


@pytest.mark.asyncio
async def test_scan_isolates_failures_and_does_not_grant_permission():
    calls = []
    async def discover(): return ['corrupt', 'claimed', 'raced', 'good', 'good']
    async def inspect(run_id):
        if run_id == 'corrupt': raise ValueError('secret')
        return build_recovery_inspection({'unknownToolReceipts': 1 if run_id == 'claimed' else 0, 'receiptScope': 'run'})
    async def resume(run_id):
        calls.append(run_id)
        if run_id == 'raced': raise RuntimeError('private lease conflict')
    report = await RecoveryWorker(discover=discover, inspect=inspect, resume=resume).run_once()
    assert calls == ['raced', 'good']
    assert [r.action for r in report] == ['failed', 'blocked', 'failed', 'settled']
    assert report[0].reasons == ('inspection_failed',)
    assert report[2].reasons == ('resume_failed',)
    assert 'secret' not in repr(report) and 'private' not in repr(report)


@pytest.mark.asyncio
async def test_overlap_and_cancellation_release_scan_guard():
    entered = asyncio.Event()
    async def discover(): return ['one']
    async def inspect(_): return build_recovery_inspection({})
    async def resume(_):
        entered.set()
        await asyncio.Event().wait()
    worker = RecoveryWorker(discover=discover, inspect=inspect, resume=resume)
    task = asyncio.create_task(worker.run_once())
    await entered.wait()
    with pytest.raises(RuntimeError, match='recovery_worker_scan_active'):
        await worker.run_once()
    task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    entered.clear()
    task = asyncio.create_task(worker.run_once())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError): await task


@pytest.mark.asyncio
async def test_wake_during_scan_is_retained_and_stop_drains_current_run():
    stop = asyncio.Event()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []
    scans = 0
    async def discover(): return ['one', 'two']
    async def inspect(_): return build_recovery_inspection({})
    async def resume(run_id):
        calls.append(run_id)
        if len(calls) == 1:
            worker.wake()
        elif len(calls) == 3:
            entered.set()
            await release.wait()
    async def observe(_):
        nonlocal scans
        scans += 1
    worker = RecoveryWorker(discover=discover, inspect=inspect, resume=resume)
    task = asyncio.create_task(worker.run(stop=stop, poll_interval_ms=60000, max_backoff_ms=60000, on_scan=observe))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        with pytest.raises(RuntimeError, match='recovery_worker_scan_active'):
            await worker.run_once()
        stop.set()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        await asyncio.wait_for(task, 1)
        assert calls == ['one', 'two', 'one'] and scans == 2
    finally:
        stop.set(); release.set()
        await task


@pytest.mark.asyncio
async def test_stop_during_inspection_prevents_resume():
    stop = asyncio.Event()
    async def discover(): return ['one']
    async def inspect(_):
        stop.set()
        return build_recovery_inspection({})
    async def resume(_): raise AssertionError('must not resume')
    worker = RecoveryWorker(discover=discover, inspect=inspect, resume=resume)
    reports = []
    async def observe(report): reports.append(report)
    await worker.run(stop=stop, on_scan=observe)
    assert reports == [()]


@pytest.mark.asyncio
async def test_failed_scans_back_off_to_cap_and_success_resets(monkeypatch):
    stop = asyncio.Event()
    delays = []
    async def discover(): return ['one']
    async def inspect(_): return build_recovery_inspection({})
    async def resume(_):
        if len(delays) < 3: raise ValueError('temporary')
    async def wait(tasks, *, timeout, return_when):
        delays.append(timeout)
        if len(delays) == 4: stop.set()
        return set(), set(tasks)
    monkeypatch.setattr(asyncio, 'wait', wait)
    await RecoveryWorker(discover=discover, inspect=inspect, resume=resume).run(stop=stop, poll_interval_ms=10, max_backoff_ms=40)
    assert delays == [0.02, 0.04, 0.04, 0.01]


@pytest.mark.asyncio
async def test_idle_stop_and_observer_failure_release_lifecycle():
    stop = asyncio.Event()
    scanned = asyncio.Event()
    async def discover(): return []
    async def inspect(_): return build_recovery_inspection({})
    async def resume(_): pass
    async def observe(_): scanned.set()
    worker = RecoveryWorker(discover=discover, inspect=inspect, resume=resume)
    task = asyncio.create_task(worker.run(stop=stop, poll_interval_ms=60000, max_backoff_ms=60000, on_scan=observe))
    await scanned.wait()
    await asyncio.sleep(0)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert await worker.run_once() == ()
    stop.clear()
    async def fail(_): raise ValueError('observer')
    with pytest.raises(ValueError, match='observer'):
        await worker.run(stop=stop, on_scan=fail)
    assert await worker.run_once() == ()


@pytest.mark.asyncio
async def test_bounded_fair_scans_survive_reorder_waiting_and_churn():
    ids = ['pending', 'deferred', 'ready', 'ready']
    calls = []
    class Schedule:
        async def ready(self, run_id): return None if run_id == 'deferred' else 0
        async def settle(self, *args): return True
    async def discover(): return ids
    async def inspect(run_id):
        return build_recovery_inspection({'approvalState': 'pending'} if run_id == 'pending' else {})
    async def resume(run_id): calls.append(run_id)
    worker = RecoveryWorker(discover=discover, inspect=inspect, resume=resume, schedule=Schedule(), max_runs_per_scan=1)
    assert (await worker.run_once())[0].run_id == 'pending'
    ids[:] = ['new', 'ready', 'deferred', 'pending']
    assert (await worker.run_once())[0].run_id == 'deferred'
    assert (await worker.run_once())[0].run_id == 'ready'
    assert calls == ['ready']
    ids.remove('pending')
    assert (await worker.run_once())[0].run_id == 'new'
    snapshot = worker.diagnostics()
    assert snapshot['lastScan'] == {'outcome': 'complete', 'candidates': 3, 'visited': 1, 'deferred': 2, 'blocked': 0, 'settled': 1, 'failed': 0}
    snapshot['lastScan']['visited'] = 999
    assert worker.diagnostics()['lastScan']['visited'] == 1
    assert worker.diagnostics()['authority'] == 'diagnosis_only'


@pytest.mark.asyncio
async def test_schedule_failure_advances_fair_cursor_and_reports_stage():
    seen = []
    class Schedule:
        async def ready(self, run_id):
            seen.append(worker.diagnostics()['phase'])
            if run_id == 'bad': raise ValueError('private')
            return 0
        async def settle(self, *args): return True
    async def discover(): return ['bad', 'good']
    async def inspect(_): return build_recovery_inspection({})
    async def resume(_): pass
    worker = RecoveryWorker(discover=discover, inspect=inspect, resume=resume, schedule=Schedule(), max_runs_per_scan=1)
    with pytest.raises(ValueError): await worker.run_once()
    assert worker.diagnostics()['lastScan']['outcome'] == 'failed'
    assert worker.diagnostics()['lastScan']['visited'] == 1
    assert 'private' not in repr(worker.diagnostics())
    assert (await worker.run_once())[0].run_id == 'good'
    assert seen == ['scheduling', 'scheduling']


@pytest.mark.parametrize('value', [True, 0, -1, 1.5, 2147483648])
def test_invalid_batch_limit_rejected(value):
    with pytest.raises(ValueError):
        RecoveryWorker(discover=None, inspect=None, resume=None, max_runs_per_scan=value)
