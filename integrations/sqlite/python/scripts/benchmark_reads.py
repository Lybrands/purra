"""Measure empty output polling against growing persisted Run histories.

Run from the repository root with the current Core and SQLite sources:
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_reads.py
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
    for scenario in ("run_history", "output_journal"):
        await measure(scenario)


async def measure(scenario):
    for count in (100, 1000, 5000):
        with tempfile.TemporaryDirectory() as directory:
            storage = SqliteAgentAdapters(Path(directory) / "agent.db", scope="benchmark")
            try:
                async with storage.transaction() as adapters:
                    run = await adapters.runs.begin(
                        RunCreateParams(None, "benchmark", None), AgentEvent("run.started"),
                    )
                    for index in range(count):
                        if scenario == "run_history":
                            await adapters.runs.append_event(run.run_id, AgentEvent(
                                "benchmark.record", {"index": index, "text": "x" * 128},
                            ))
                        else:
                            await adapters.outputs.append_event(AgentOutputEventDraft(
                                run_id=run.run_id, turn_id=None, output_stream_id=None, invocation_id=None,
                                source_event_key=f"benchmark:{index}", source="domain", kind="domain.effect",
                                channel="diagnostic", visibility="private", payload={"text": "x" * 128},
                                occurred_at=datetime.now(timezone.utc),
                            ))
                elapsed = []
                for _ in range(12):
                    start = time.perf_counter()
                    rows = await storage.outputs.list_events(run.run_id, after_sequence=count, limit=1)
                    elapsed.append((time.perf_counter() - start) * 1000)
                    assert rows == ()
                print(json.dumps({
                    "scenario": scenario,
                    "history_events": count,
                    "median_read_ms": round(statistics.median(elapsed[2:]), 3),
                    "measured_reads": 10,
                }), flush=True)
                if scenario == "output_journal":
                    elapsed = []
                    for _ in range(12):
                        start = time.perf_counter()
                        rows = await storage.outputs.list_events(run.run_id, after_sequence=count - 10, limit=10)
                        elapsed.append((time.perf_counter() - start) * 1000)
                        assert len(rows) == 10 and rows[-1].sequence == count
                    print(json.dumps({"scenario": "last_10_outputs", "history_events": count,
                                      "median_read_ms": round(statistics.median(elapsed[2:]), 3)}), flush=True)
            finally:
                storage.close()


if __name__ == "__main__":
    asyncio.run(main())
