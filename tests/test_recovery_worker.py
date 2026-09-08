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
