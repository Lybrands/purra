"""Opt-in, separate-process SQLite load and crash verification for both SDKs.

Build TypeScript Core/SQLite first. Run with Python Core/SQLite src on PYTHONPATH.
All data and external-effect markers are synthetic and kept in a temporary folder.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time

from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.contracts import AgentMessage, MessageRole, RunCreateParams, ToolCall, ToolHandlerResult
from purra.events import AgentEvent
from purra.output.contracts import AgentOutputEventDraft
from purra.ports.run_lifecycle import RunCommit
from purra_sqlite import SqliteAgentAdapters


def draft(run, key):
    return AgentOutputEventDraft(run_id=run, turn_id=None, output_stream_id=None, invocation_id=None,
        source_event_key=key, source="domain", kind="domain.effect", channel="diagnostic",
        visibility="private", payload={"text": "x" * 128}, occurred_at=datetime.now(timezone.utc))


async def worker(mode, config):
    storage = SqliteAgentAdapters(config["path"], scope="load", busy_timeout=30)
    try:
        if mode == "seed":
            async with storage.transaction() as adapters:
                ids = []
                for root in range(config["roots"]):
                    root_id = f"root:{root}"
                    for child in range(config["children"] + 1):
                        run = root_id if child == 0 else f"{root_id}:child:{child}"
                        await adapters.runs.begin(RunCreateParams(None, "load", None, requested_run_id=run,
                            root_run_id=None if child == 0 else root_id, parent_run_id=None if child == 0 else root_id), AgentEvent("run.started"))
                        ids.append(run)
                    await adapters.runs.commit(root_id, RunCommit(execution_checkpoint=AgentExecutionCheckpoint(
                        run_id=root_id, next_round=2, round_limit=10,
                        messages=(AgentMessage(role=MessageRole.USER, content="x" * config["checkpoint_chars"]),))))
                for i in range(config["history"]):
                    await adapters.outputs.append_event(draft(ids[i % len(ids)], f"history:{i}"))
            print(json.dumps({"runs": len(ids)}), flush=True)
        elif mode == "write":
            print('{"ready":true}', flush=True)
            assert sys.stdin.readline().strip() == "start"
            samples = []
            for i in range(config["writes"]):
                item = draft("root:0", f"worker:{config['worker']}:{i}")
                start = time.perf_counter()
                event = await storage.outputs.append_event(item)
                samples.append((time.perf_counter() - start) * 1000)
                if i % 5 == 0:
                    assert await storage.outputs.append_event(item) == event
            print(json.dumps({"ms": samples}), flush=True)
        elif mode == "crash":
            storage._db.execute("BEGIN IMMEDIATE")
            storage._db.execute("UPDATE purra_state SET body='partial' WHERE scope='load' AND sdk='python'")
            storage._db.execute("DELETE FROM purra_output_events WHERE scope='load' AND sdk='python' AND run_id='root:0' AND sequence=1")
            print('{"ready":true}', flush=True)
            await asyncio.Event().wait()
        elif mode == "tool-crash":
            async def effect():
                Path(config["effect"]).write_text("executed-once")
                print('{"ready":true}', flush=True)
                await asyncio.Event().wait()
                return ToolHandlerResult("done")
            await storage.idempotency.execute_once("root:0", ToolCall("crash-tool", "write", "{}"), effect)
        elif mode == "recover":
            effects = 0
            async def forbidden():
                nonlocal effects
                effects += 1
                return ToolHandlerResult("unexpected")
            call = ToolCall("crash-tool", "write", "{}")
            try:
                await storage.idempotency.execute_once("root:0", call, forbidden)
            except Exception as error:
                assert getattr(error, "code", None) == "tool_effect_unknown"
            else:
                raise AssertionError("unknown tool effect was replayed")
            await storage.reconcile_tool("root:0", call, result=ToolHandlerResult("done"))
            assert (await storage.idempotency.execute_once("root:0", call, forbidden)).content == "done"
            assert effects == 0 and Path(config["effect"]).read_text() == "executed-once"
            for root in range(config["roots"]):
                saved = await storage.runs.get(f"root:{root}")
                assert saved.execution_checkpoint.next_round == 2
                assert len(saved.execution_checkpoint.messages[0].content) == config["checkpoint_chars"]
            print('{"recovered":true}', flush=True)
    finally:
        storage.close()


def checksum(path, sdk):
    digest = hashlib.sha256()
    with sqlite3.connect(path) as db:
        for table, order in [("purra_state", "scope,sdk"), ("purra_output_events", "run_id,sequence")]:
            for row in db.execute(f"SELECT * FROM {table} WHERE scope='load' AND sdk=? ORDER BY {order}", (sdk,)):
                digest.update(json.dumps(row, separators=(",", ":")).encode())
    return digest.hexdigest()


def verify(config, sdk):
    base = [sys.executable, str(Path(__file__).resolve())] if sdk == "python" else ["node", str(Path(__file__).resolve().parents[2]/"typescript/scripts/load-worker.mjs")]
    processes = []
    def command(mode, values=config):
        return [*base, "--worker", mode, json.dumps(values)]
    def start(mode, values=config):
        process = subprocess.Popen(command(mode, values), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append(process)
        return process
    def ready(process):
        # Readiness has a bounded wait even if a worker fails before opening SQLite.
        import selectors
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(60), "worker readiness timed out"
        line = process.stdout.readline()
        assert line and json.loads(line).get("ready"), process.stderr.read() if process.poll() is not None else "worker was not ready"
    try:
        subprocess.run(command("seed"), check=True, capture_output=True, text=True, timeout=120)
        with sqlite3.connect(config["path"]) as db:
            initial = db.execute("SELECT COUNT(*) FROM purra_output_events WHERE sdk=?", (sdk,)).fetchone()[0]
            state_bytes = db.execute("SELECT length(CAST(body AS BLOB)) FROM purra_state WHERE sdk=?", (sdk,)).fetchone()[0]
        writers = [start("write", {**config, "worker": i}) for i in range(config["workers"])]
        for process in writers:
            ready(process)
        started = time.perf_counter()
        for process in writers:
            process.stdin.write("start\n"); process.stdin.flush()
        samples = []
        for process in writers:
            output, errors = process.communicate(timeout=120)
            assert process.returncode == 0, errors
            samples.extend(json.loads(output)["ms"])
        elapsed = time.perf_counter() - started
        with sqlite3.connect(config["path"]) as db:
            total = db.execute("SELECT COUNT(*) FROM purra_output_events WHERE sdk=?", (sdk,)).fetchone()[0]
            assert total == initial + config["workers"] * config["writes"]
            for identity, sequence in [("run_id", "sequence"), ("root_run_id", "root_sequence")]:
                for count, first, last in db.execute(f"SELECT COUNT(*),MIN({sequence}),MAX({sequence}) FROM purra_output_events WHERE sdk=? GROUP BY {identity}", (sdk,)):
                    assert first == 1 and last == count
        before = checksum(config["path"], sdk)
        crashed = start("crash"); ready(crashed); crashed.kill(); crashed.communicate(timeout=10)
        assert checksum(config["path"], sdk) == before
        crashed = start("tool-crash"); ready(crashed); crashed.kill(); crashed.communicate(timeout=10)
        subprocess.run(command("recover"), check=True, capture_output=True, text=True, timeout=120)
        samples.sort()
        return {"sdk": sdk, "roots": config["roots"], "runs": config["roots"] * (config["children"] + 1),
            "history_events": config["history"], "snapshot_bytes": state_bytes, "writer_processes": config["workers"],
            "writes": len(samples), "p50_ms": round(samples[len(samples)//2], 3), "p95_ms": round(samples[math.ceil(len(samples)*.95)-1], 3),
            "max_ms": round(max(samples), 3), "throughput_writes_per_second": round(len(samples)/elapsed, 2),
            "journal_sequences": "passed", "killed_transaction_rollback": "passed", "unknown_tool_effect_reconciliation": "passed", "checkpoint_reopen": "passed"}
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=10)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", nargs=2)
    parser.add_argument("--roots", type=int, default=20)
    parser.add_argument("--children", type=int, default=2)
    parser.add_argument("--history", type=int, default=20000)
    parser.add_argument("--checkpoint-chars", type=int, default=65536)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--writes", type=int, default=25)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.worker:
        asyncio.run(worker(args.worker[0], json.loads(args.worker[1]))); return
    config = {key: getattr(args, key) for key in ("roots", "children", "history", "checkpoint_chars", "workers", "writes")}
    assert all(value > 0 for key, value in config.items() if key != "children") and config["children"] >= 0
    results = []
    for sdk in ("python", "typescript"):
        with tempfile.TemporaryDirectory(prefix=f"purra-load-{sdk}-") as directory:
            result = verify({**config, "path": str(Path(directory)/"agent.db"), "effect": str(Path(directory)/"effect.txt")}, sdk)
            results.append(result)
            print(json.dumps(result), flush=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({"kind": "synthetic-multiprocess-storage", "results": results}, indent=2) + "\n")


if __name__ == "__main__":
    main()
