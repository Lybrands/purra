"""Measure same-Root writes; run with Core and SQLite src on PYTHONPATH."""

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time

from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.contracts import AgentMessage, MessageRole, RunCreateParams, RuntimeLimits
from purra.events import AgentEvent
from purra.output.contracts import AgentOutputEventDraft
from purra.ports.run_lifecycle import RunCommit
from purra_sqlite import SqliteAgentAdapters
import purra_sqlite
from purra_sqlite.journal import OutputJournal


PROFILE = "--profile" in sys.argv
TIMINGS = {"journal_ms": 0.0, "state_codec_ms": 0.0}


def instrument(owner, name, phase):
    original = getattr(owner, name)
    def measured(*args, **kwargs):
        start = time.perf_counter()
        try:
            return original(*args, **kwargs)
        finally:
            TIMINGS[phase] += (time.perf_counter() - start) * 1000
    setattr(owner, name, measured)


if PROFILE:
    instrument(OutputJournal, "_restore_deferred", "journal_ms")
    instrument(purra_sqlite, "loads", "state_codec_ms")
    instrument(purra_sqlite, "dumps", "state_codec_ms")


def draft(run, key):
    return AgentOutputEventDraft(
        run_id=run, turn_id=None, output_stream_id=None, invocation_id=None,
        source_event_key=key, source="domain", kind="domain.effect",
        channel="diagnostic", visibility="private", payload={"text": "x" * 128},
        occurred_at=datetime.now(timezone.utc),
    )


async def main():
    for count in (100, 1000, 5000):
        with tempfile.TemporaryDirectory() as directory:
            storage = SqliteAgentAdapters(Path(directory) / "agent.db", scope="benchmark")
            try:
                async with storage.transaction() as adapters:
                    run = (await adapters.runs.begin(RunCreateParams(
                        None, "benchmark", None,
                        runtime_limits=RuntimeLimits(max_run_generation_tokens=None, max_model_invocation_attempts=100),
                    ), AgentEvent("run.started"))).run_id
                    for index in range(count):
                        await adapters.outputs.append_event(draft(run, f"event:{index}"))
                samples = {key: [] for key in ("append", "reserve", "checkpoint")}
                phases = {key: [] for key in samples}
                for attempt in range(12):
                    checkpoint = AgentExecutionCheckpoint(
                        run_id=run, next_round=attempt + 1, round_limit=100,
                        messages=(AgentMessage(role=MessageRole.USER, content="benchmark"),),
                    )
                    operations = {
                        "append": lambda: storage.outputs.append_event(draft(run, f"new:{attempt}")),
                        "reserve": lambda: storage.runs.reserve_model_attempt(run, f"invoke:{attempt}"),
                        "checkpoint": lambda: storage.runs.commit(run, RunCommit(execution_checkpoint=checkpoint)),
                    }
                    for name, operation in operations.items():
                        before = dict(TIMINGS)
                        start = time.perf_counter()
                        await operation()
                        elapsed = (time.perf_counter() - start) * 1000
                        samples[name].append(elapsed)
                        costs = {phase: TIMINGS[phase] - before[phase] for phase in TIMINGS}
                        phases[name].append({**costs, "remainder_ms": elapsed - sum(costs.values())})
                assert len(await storage.outputs.list_events(run, after_sequence=count)) == 12
                assert (await storage.runs.get(run)).execution_checkpoint == checkpoint
                result = {"history_events": count, "measured_writes_per_operation": 10,
                    **{f"median_{name}_ms": round(statistics.median(values[2:]), 3) for name, values in samples.items()}}
                if PROFILE:
                    result["profile"] = {name: {phase: round(statistics.median(row[phase] for row in values[2:]), 3)
                        for phase in (*TIMINGS, "remainder_ms")} for name, values in phases.items()}
                print(json.dumps(result), flush=True)
            finally:
                storage.close()


if __name__ == "__main__":
    asyncio.run(main())
