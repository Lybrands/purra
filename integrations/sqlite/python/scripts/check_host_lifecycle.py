"""Synthetic host lifecycle check; no Provider, MCP service, or business data."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
import tempfile
import time

from purra.api import AgentCoreRunOptions, RecoveryWorker
from purra.approvals import ApprovalDecisionCommand, ApprovalRequired

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from test_approval_resume import ApprovalHost  # noqa: E402


class HostRegistry:
    """Host-owned request/binding data; PurrA storage remains authoritative for Runs."""

    def __init__(self, path: Path):
        self.path = path

    def save(self, run_id: str, *, expires_at_ms: int) -> None:
        self.path.write_text(json.dumps({run_id: {
            "requestProfile": "fixture-v1",
            "presetRevision": "1",
            "bindingRevision": "1",
            "scopeRevision": "1",
            "expiresAtMs": expires_at_ms,
        }}, separators=(",", ":")))

    def resolve(self, run_id: str) -> dict:
        rows = json.loads(self.path.read_text())
        row = rows.get(run_id)
        if not isinstance(row, dict) or row.get("requestProfile") != "fixture-v1":
            raise ValueError("host_run_configuration_unavailable")
        expected = {"requestProfile", "presetRevision", "bindingRevision",
                    "scopeRevision", "expiresAtMs"}
        if set(row) != expected or row["presetRevision"] != "1" \
                or row["bindingRevision"] != "1" or row["scopeRevision"] != "1" \
                or type(row["expiresAtMs"]) is not int:
            raise ValueError("host_run_configuration_mismatch")
        return row


async def check(directory: Path) -> dict:
    database = directory / "agent.db"
    registry = HostRegistry(directory / "host-runs.json")
    first = ApprovalHost(database)
    first.expiry = int(time.time() * 1000) + 60_000
    try:
        await first.storage.enable_approvals()
        options = AgentCoreRunOptions(tool_checkpoint_handler=first.boundary)
        handle = await first.core.submit(first.request, options=options)
        try:
            await handle.wait()
        except ApprovalRequired:
            pass
        else:
            raise AssertionError("approval wait expected")
        record = (await first.approvals.list_pending(run_id=handle.run_id))[0]
        registry.save(handle.run_id, expires_at_ms=record.expires_at_ms)
        run_id = handle.run_id
    finally:
        await first.close()

    # A new host process would recreate these objects from durable PurrA and host state.
    restored = ApprovalHost(database)
    resolved = registry.resolve(run_id)
    restored.expiry = resolved["expiresAtMs"]
    cursor = restored.storage.recovery_cursor("example-host", page_size=10)
    schedule = restored.storage.recovery_schedule(interval_ms=20, max_backoff_ms=100)
    stop = asyncio.Event()
    scans = []

    async def resume(candidate: str):
        current = registry.resolve(candidate)
        restored.expiry = current["expiresAtMs"]
        result = await (await restored.core.resume(candidate, restored.request,
            options=AgentCoreRunOptions(tool_checkpoint_handler=restored.boundary))).wait()
        if result.status.value != "done":
            raise RuntimeError("host resume did not complete")

    worker = RecoveryWorker(discover=cursor.discover, acknowledge=cursor.acknowledge,
        inspect=restored.storage.inspect_recovery, resume=resume, schedule=schedule)
    service = asyncio.create_task(worker.run(stop=stop, poll_interval_ms=20,
        max_backoff_ms=100, on_scan=lambda report: record_scan(scans, report)))
    try:
        await asyncio.sleep(0.05)
        pending = (await restored.approvals.list_pending(run_id=run_id))[0]
        await restored.approvals.decide(ApprovalDecisionCommand(pending.approval_id,
            pending.revision, pending.intent.digest, "example-approve", "approve"),
            principal_id="authenticated-example-host")
        worker.wake()
        for _ in range(200):
            if (await restored.storage.runs.get(run_id)).status.value == "done":
                break
            await asyncio.sleep(0.01)
        else:
            raise TimeoutError("worker did not complete approved Run")
    finally:
        stop.set()
        worker.wake()
        await service
        await restored.close()

    final = ApprovalHost(database)
    try:
        saved = await final.storage.runs.get(run_id)
        if saved.status.value != "done":
            raise AssertionError("canonical Run is not complete")
        async with final.storage.transaction() as session:
            if session.claims:
                raise AssertionError("tool claim was not settled")
            receipts = list(session.extra.get("approvalExecutions", {}).values())
            if len(receipts) != 1 or receipts[0]["state"] != "complete":
                raise AssertionError("approval execution receipt is incomplete")
    finally:
        await final.close()
    return {"schemaVersion": 1, "runStatus": "done", "toolCalls": restored.tool_calls,
        "modelCalls": restored.model_calls, "workerScans": len(scans),
        "workerStopped": not worker.diagnostics()["serving"],
        "hostConfigurationReconstructed": True, "syntheticOnly": True}


async def record_scan(scans, report):
    scans.append(tuple(row.action for row in report))


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    if args.directory is not None:
        args.directory.mkdir(parents=True, exist_ok=True)
        print(json.dumps(await check(args.directory), indent=2))
        return
    with tempfile.TemporaryDirectory(prefix="purra-host-lifecycle-") as directory:
        print(json.dumps(await check(Path(directory)), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
