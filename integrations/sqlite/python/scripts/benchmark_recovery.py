"""Synthetic recovery costs; no Provider, model or business data.

PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_recovery.py
"""
import asyncio
import json
from pathlib import Path
import statistics
import tempfile
import time
from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra.ports import RunCommit
from purra_sqlite import SqliteAgentAdapters


async def main():
    for count in (20, 100):
        with tempfile.TemporaryDirectory(prefix='purra-recovery-bench-') as directory:
            storage = SqliteAgentAdapters(Path(directory) / 'db', scope='benchmark')
            try:
                async with storage.transaction() as session:
                    for index in range(count):
                        run_id = (await session.runs.begin(RunCreateParams(None, 'benchmark', None), AgentEvent('run.started'))).run_id
                        if index % 5:
                            await session.runs.commit(run_id, RunCommit(terminal_status='done', final_response='done', events=(AgentEvent('run.completed', run_id=run_id),)))
                async def candidates():
                    return await storage.list_run_candidates(limit=20)
                page = await candidates()
                ids = page['runIds']
                async def individual():
                    return [await storage.inspect_recovery(run_id) for run_id in ids]
                operations = {'candidate_page': candidates, 'individual_inspection': individual}
                if hasattr(storage, 'inspect_recovery_many'):
                    async def batch(): return await storage.inspect_recovery_many(ids)
                    operations['batch_inspection'] = batch
                for name, operation in operations.items():
                    samples = []
                    for _ in range(7):
                        start = time.perf_counter(); await operation()
                        samples.append((time.perf_counter() - start) * 1000)
                    print(json.dumps({'sdk':'python', 'runs':count, 'terminal_runs':count*4//5, 'page_size':len(ids), 'operation':name, 'measured_reads':5, 'median_ms':round(statistics.median(samples[2:]),3)}), flush=True)
            finally: storage.close()

asyncio.run(main())
