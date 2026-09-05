"""Measure active Run output writes beside an unrelated historical Root.

Run from the repository root:
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_run_writes.py
"""

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import tempfile
import time

from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra.output.contracts import AgentOutputEventDraft
from purra_sqlite import SqliteAgentAdapters


async def main():
    for count in (100, 1000, 5000):
        with tempfile.TemporaryDirectory() as directory:
            storage = SqliteAgentAdapters(Path(directory) / "agent.db", scope="benchmark")
            try:
                async with storage.transaction() as adapters:
                    run = (await adapters.runs.begin(RunCreateParams(None, "benchmark", None), AgentEvent("run.started"))).run_id
                    for index in range(count):
                        await adapters.outputs.append_event(AgentOutputEventDraft(
                            run_id=run, turn_id=None, output_stream_id=None, invocation_id=None,
                            source_event_key=f"event:{index}", source="domain", kind="domain.effect",
                            channel="diagnostic", visibility="private", payload={"text": "x" * 128},
                            occurred_at=datetime.now(timezone.utc),
                        ))
                target = (await storage.runs.begin(RunCreateParams(None, "active", None), AgentEvent("run.started"))).run_id
                elapsed = []
                for attempt in range(12):
                    start = time.perf_counter()
                    result = await storage.outputs.append_event(AgentOutputEventDraft(
                        run_id=target, turn_id=None, output_stream_id=None, invocation_id=None,
                        source_event_key=f"active:{attempt}", source="domain", kind="domain.effect",
                        channel="diagnostic", visibility="private", payload={"text": "active"},
                        occurred_at=datetime.now(timezone.utc),
                    ))
                    elapsed.append((time.perf_counter() - start) * 1000)
                    assert result.sequence == attempt + 1
                print(json.dumps({"history_events": count, "median_run_write_ms": round(statistics.median(elapsed[2:]), 3), "measured_writes": 10}), flush=True)
            finally:
                storage.close()


if __name__ == "__main__":
    asyncio.run(main())
