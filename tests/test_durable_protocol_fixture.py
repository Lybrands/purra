from __future__ import annotations

import json
from pathlib import Path

from purra.contracts import RunStatus
from purra.run_control import (
    OrphanRunCandidate,
    OrphanRunDisposition,
    OrphanRunReason,
    OrphanTaskEvidence,
    decide_orphan_run,
)


FIXTURE = json.loads(
    (Path(__file__).parents[1] / "conformance" / "fixtures" / "durable_protocol.json").read_text()
)


def test_shared_durable_protocol_fixture() -> None:
    for row in FIXTURE["leaseExpiryCases"]:
        assert (row["leaseExpiresAtMs"] <= row["nowMs"]) is row["expired"], row["name"]

    for row in FIXTURE["orphanCases"]:
        raw = row["candidate"]
        candidate = OrphanRunCandidate(
            run_id=raw["runId"],
            cancellation_requested_at_ms=raw.get("cancellationRequestedAtMs"),
            active_tasks=tuple(
                OrphanTaskEvidence(item["taskId"], item["revision"])
                for item in raw.get("activeTasks", ())
            ),
            recoverable_tasks=tuple(
                OrphanTaskEvidence(item["taskId"], item["revision"])
                for item in raw.get("recoverableTasks", ())
            ),
        )
        decision = decide_orphan_run(candidate)
        assert decision.disposition is OrphanRunDisposition(row["disposition"]), row["name"]
        assert decision.reason is OrphanRunReason(row["reason"]), row["name"]
        expected = row["terminalStatus"]
        assert decision.terminal_status is (
            None if expected is None else RunStatus(expected)
        ), row["name"]
