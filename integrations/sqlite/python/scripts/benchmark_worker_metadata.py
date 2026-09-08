"""Controlled worker metadata costs with realistic synthetic approval Run snapshots."""
import asyncio
import cProfile
import io
import json
from pathlib import Path
import pstats
import statistics
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tests'))
from test_approval_resume import ApprovalHost
from purra.api import AgentCoreRunOptions
from purra.approvals import ApprovalRequired, ApprovalDecisionCommand


async def main():
    with tempfile.TemporaryDirectory(prefix='purra-worker-profile-') as directory:
        host = ApprovalHost(Path(directory) / 'db'); host.expiry = int(time.time()*1000)+600000
        try:
            await host.storage.enable_approvals()
            ids = []
            for _ in range(24):
                options = AgentCoreRunOptions(tool_checkpoint_handler=host.boundary)
                handle = await host.core.submit(host.request, options=options)
                try: await handle.wait()
                except ApprovalRequired: pass
                else: raise AssertionError('approval required')
                record = (await host.approvals.list_pending(run_id=handle.run_id))[0]
                await host.approvals.decide(ApprovalDecisionCommand(record.approval_id, record.revision,
                    record.intent.digest, handle.run_id, 'approve'), principal_id='host')
                result = await (await host.core.resume(handle.run_id, host.request, options=options)).wait()
                assert result.status.value == 'done'
                ids.append(handle.run_id)
            cursor = host.storage.recovery_cursor('profile', page_size=5)
            schedule = host.storage.recovery_schedule(clock_ms=lambda: 0)
            async def settle():
                revision = await schedule.wake(ids[0])
                assert await schedule.settle(ids[0], revision, False)
            async def ack():
                page = await cursor.discover()
                await cursor.acknowledge(page[:3])
            operations = {'schedule_check': lambda: schedule.check(ids[0]),
                'wake_and_settle': settle, 'discover_and_ack': ack,
                'inspect': lambda: host.storage.inspect_recovery(ids[0])}
            profiler = cProfile.Profile()
            for name, operation in operations.items():
                samples = []; before = host.storage._db.total_changes
                for i in range(7):
                    start = time.perf_counter()
                    await operation()
                    samples.append((time.perf_counter()-start)*1000)
                if name in ('schedule_check', 'inspect'):
                    assert host.storage._db.total_changes == before
                print(json.dumps({'operation': name, 'runs': 24, 'samples': 5,
                    'medianMs': statistics.median(samples[2:]), 'sqliteChangedRowsIncludingWarmup': host.storage._db.total_changes-before}), flush=True)
            profiler.enable()
            for operation in operations.values(): await operation()
            profiler.disable()
            stream = io.StringIO(); pstats.Stats(profiler, stream=stream).sort_stats('cumulative').print_stats(22)
            print(stream.getvalue())
        finally: await host.close()


if __name__ == '__main__': asyncio.run(main())
