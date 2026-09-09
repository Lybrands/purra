"""Large-journal / independent-process writer probe, entirely in temporary data.

PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_recovery_load.py
"""
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time
from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra.output.contracts import AgentOutputEventDraft
from purra_sqlite import SqliteAgentAdapters


def draft(run_id, key):
    return AgentOutputEventDraft(run_id=run_id, turn_id=None, output_stream_id=None, invocation_id=None,
        source_event_key=key, source='domain', kind='domain.effect', channel='diagnostic', visibility='private',
        payload={'text': 'x' * 128}, occurred_at=datetime.now(timezone.utc))


async def writer(path, run_id):
    storage = SqliteAgentAdapters(path, scope='load')
    try:
        print('ready', flush=True)
        if sys.stdin.readline().strip() != 'start': raise RuntimeError('missing start')
        samples = []
        for i in range(20):
            start = time.monotonic_ns()
            await storage.outputs.append_event(draft(run_id, f'writer:{i}'))
            samples.append([start, time.monotonic_ns()])
        print(json.dumps(samples), flush=True)
    finally: storage.close()


async def main():
    history_count = int(os.environ.get('PURRA_BENCHMARK_EVENTS', '5000'))
    if not 1 <= history_count <= 100000: raise ValueError('history count must be 1..100000')
    with tempfile.TemporaryDirectory(prefix='purra-recovery-load-') as directory:
        path = Path(directory) / 'db'
        storage = SqliteAgentAdapters(path, scope='load')
        child = None
        try:
            async with storage.transaction() as session:
                ids = [(await session.runs.begin(RunCreateParams(None, 'load', None), AgentEvent('run.started'))).run_id for _ in range(20)]
                for i in range(history_count): await session.outputs.append_event(draft(ids[-1], f'history:{i}'))
            selected = ids[:5]
            async def individual(): return {key: await storage.inspect_recovery(key) for key in selected}
            async def batch(): return await storage.inspect_recovery_many(selected)
            expected = await individual()
            for name, operation in [('individual', individual), ('batch', batch)]:
                elapsed = []
                for _ in range(7):
                    start = time.monotonic_ns(); result = await operation()
                    elapsed.append((time.monotonic_ns()-start)/1e6)
                    assert result == expected
                print(json.dumps({'sdk':'python','scenario':'unrelated_journal','runs':20,'selected':5,'unrelated_events':history_count,'operation':name,'median_ms':round(statistics.median(elapsed[2:]),3),'samples':5}), flush=True)
            child = await asyncio.create_subprocess_exec(sys.executable, __file__, '--writer', str(path), ids[0], stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            assert await asyncio.wait_for(child.stdout.readline(), 10) == b'ready\n'
            child.stdin.write(b'start\n'); await child.stdin.drain()
            intervals = []
            for _ in range(20):
                start = time.monotonic_ns(); result = await batch()
                intervals.append([start,time.monotonic_ns()]); assert result == expected
            stdout, stderr = await asyncio.wait_for(child.communicate(), 30)
            if child.returncode: raise RuntimeError(stderr.decode())
            writes = json.loads(stdout)
            overlaps = sum(any(a < d and c < b for c,d in intervals) for a,b in writes)
            committed = await storage.outputs.list_events(ids[0], after_sequence=0, limit=100)
            assert len(committed) == 20 and len({e.source_event_key for e in committed}) == 20
            assert overlaps > 0, 'probe did not achieve overlapping reader/writer intervals'
            print(json.dumps({'sdk':'python','scenario':'mixed_processes','reads':20,'writes':20,'observed_overlapping_writes':overlaps,'reader_median_ms':round(statistics.median((b-a)/1e6 for a,b in intervals),3),'writer_median_ms':round(statistics.median((b-a)/1e6 for a,b in writes),3),'committed_events':len(committed),'diagnosis_unchanged':True}), flush=True)
        finally:
            if child is not None and child.returncode is None:
                child.kill(); await child.wait()
            storage.close()

if __name__ == '__main__':
    asyncio.run(writer(sys.argv[2], sys.argv[3]) if len(sys.argv)>1 and sys.argv[1]=='--writer' else main())
