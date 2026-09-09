"""Bounded multi-process service probe; environment overrides allow longer runs."""
import asyncio
from collections import Counter
import json
import os
import time

import pytest
from test_worker_process import ApprovalHost, start, event, finish, cleanup
from purra.api import AgentCoreRunOptions
from purra.approvals import ApprovalRequired, ApprovalDecisionCommand


@pytest.mark.asyncio
async def test_three_services_drain_waves_and_restart_without_duplicate_effects(tmp_path):
    waves = int(os.environ.get('PURRA_WORKER_LOAD_WAVES', '3'))
    gap_ms = int(os.environ.get('PURRA_WORKER_LOAD_GAP_MS', '100'))
    assert 3 <= waves <= 30 and 0 <= gap_ms <= 10000
    path, effect = tmp_path / 'db', tmp_path / 'effects'
    expiry = int(time.time() * 1000) + 600000
    async def seed_run():
        host = ApprovalHost(path); host.expiry = expiry
        try:
            await host.storage.enable_approvals()
            handle = await host.core.submit(host.request,
                options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
            with pytest.raises(ApprovalRequired): await handle.wait()
            record = (await host.approvals.list_pending(run_id=handle.run_id))[0]
            await host.approvals.decide(ApprovalDecisionCommand(record.approval_id,
                record.revision, record.intent.digest, 'decision-' + handle.run_id, 'approve'), principal_id='host')
            return record
        finally: await host.close()
    record = await seed_run()
    ids = [record.intent.run_id]
    children, active, reports = [], [], []
    started = time.monotonic()

    async def launch(index):
        process = await start(path, effect, expiry, f'service-{index}')
        children.append(process)
        await event(process, 'ready')
        process.stdin.write(b'continue\n'); await process.stdin.drain()
        return process

    async def stop(process):
        process.stdin.write(b'stop\n'); await process.stdin.drain()
        report = await event(process, 'result'); await finish(process)
        assert report['scans'] > 0
        assert report['diagnostics']['phase'] == 'idle' and not report['diagnostics']['serving']
        assert report['failed'] == len(report['errors']), report
        assert set(report['errors']) <= {'run_lease_conflict', 'tool_effect_unknown', 'run_terminal'}
        reports.append(report)

    try:
        for index in range(3): active.append(await launch(index))
        for wave in range(waves):
            for _ in range(3 if wave == 0 else 4):
                record = await seed_run()
                ids.append(record.intent.run_id)
            reader = ApprovalHost(path)
            try:
                async def drained():
                    while True:
                        statuses = [(await reader.storage.runs.get(id)).status.value for id in ids]
                        assert set(statuses) <= {'running', 'done'}, statuses
                        if all(status == 'done' for status in statuses): return
                        await asyncio.sleep(0.05)
                await asyncio.wait_for(drained(), 30)
            finally: await reader.close()
            if wave == waves // 2:
                await stop(active[0]); active[0] = await launch(0)
            await asyncio.sleep(gap_ms / 1000)
        for process in active: await stop(process)
        effects = [json.loads(line) for line in effect.read_text().splitlines()]
        assert Counter(row['runId'] for row in effects) == Counter(ids)
        assert sum(report['tools'] for report in reports) == len(ids)
        assert len({row['worker'] for row in effects}) >= 2
        overlaps = sum(a['worker'] != b['worker'] and a['startNs'] < b['endNs'] and b['startNs'] < a['endNs']
            for i, a in enumerate(effects) for b in effects[i+1:])
        reader = ApprovalHost(path)
        try:
            async with reader.storage.transaction() as session:
                assert not session.claims
                assert len(session.extra['approvalExecutions']) == len(ids)
                assert all(row['state'] == 'complete' for row in session.extra['approvalExecutions'].values())
                assert all(row.owner_id is None for row in session.leases.values())
                assert all(session.extra['recoveryCursors'][f'service-{i}']['revision'] > 0 for i in range(3))
        finally: await reader.close()
        print(json.dumps({'sdk': 'python', 'runs': len(ids), 'waves': waves, 'elapsedSeconds': time.monotonic()-started,
            'overlappingHandlerPairs': overlaps, 'workerReports': reports}))
    finally:
        await cleanup(children)
