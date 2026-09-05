"""Measure tool receipt writes with a growing canonical output journal.

Run from the repository root:
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_writes.py
"""

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import tempfile
import time

from purra.contracts import RunCreateParams, ToolCall, ToolHandlerResult
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
                async def effect():
                    return ToolHandlerResult("committed")
                elapsed = []
                for attempt in range(12):
                    start = time.perf_counter()
                    result = await storage.idempotency.execute_once(run, ToolCall(f"tool:{attempt}", "write", "{}"), effect)
                    elapsed.append((time.perf_counter() - start) * 1000)
                    assert result.content == "committed"
                print(json.dumps({"history_events": count, "median_receipt_write_ms": round(statistics.median(elapsed[2:]), 3), "measured_writes": 10}), flush=True)
            finally:
                storage.close()


if __name__ == "__main__":
    asyncio.run(main())
