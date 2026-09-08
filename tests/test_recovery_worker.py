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
