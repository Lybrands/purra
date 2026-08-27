from __future__ import annotations

import json
from pathlib import Path

from purra.contracts import RunStatus, RuntimeLimits
from purra.output import OutputBatchLimits, provider_delta_batch_digest
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
    assert FIXTURE["protocolVersion"] == 4
    assert FIXTURE["agentPresetSnapshotVersion"] == 4
    limits = RuntimeLimits(max_run_output_tokens=None)
    assert {
        "providerActivityIdleTimeoutMs": limits.provider_activity_idle_timeout_ms,
        "providerProgressIdleTimeoutMs": limits.provider_progress_idle_timeout_ms,
        "providerInvocationTimeoutMs": limits.provider_invocation_timeout_ms,
        "maxProviderOutputBytes": limits.max_provider_output_bytes,
    } == FIXTURE["runtimeDefaults"]
    batch_defaults = OutputBatchLimits()
    assert {
        "maxPayloadBytes": batch_defaults.max_payload_bytes,
        "maxFragments": batch_defaults.max_fragments,
        "maxLatencyMs": batch_defaults.max_latency_ms,
        "maxBackgroundLatencyMs": batch_defaults.max_background_latency_ms,
    } == FIXTURE["outputBatchDefaults"]
    assert FIXTURE["stableErrorCodes"] == {
        "activityDeadline": "model_activity_deadline_exceeded",
        "progressDeadline": "model_progress_deadline_exceeded",
        "invocationDeadline": "model_invocation_deadline_exceeded",
        "runDeadline": "run_deadline_exceeded",
        "taskDeadline": "long_task_deadline_exceeded",
        "leaseLost": "long_task_unit_lease_lost",
        "budgetExceeded": "runtime_budget_exceeded",
        "streamLimit": "model_stream_limit_exceeded",
    }
    for row in FIXTURE["budgetCases"]:
        usage = row["usage"]
        limits = row["limits"]
        if usage["unreportedUsageAttempts"] and any(
            limits[name] is not None
            for name in (
                "maxInputTokens",
                "maxRunOutputTokens",
                "maxReasoningTokens",
            )
        ):
            actual = "provider_usage_unreported"
        else:
            actual = next((
                kind
                for kind, value, limit in (
                    ("model_attempts", usage["invocationCount"], limits["maxInvocationAttempts"]),
                    ("input_tokens", usage["inputTokens"], limits["maxInputTokens"]),
                    (
                        "output_tokens",
                        usage["outputTokens"],
                        limits["maxRunOutputTokens"],
                    ),
                    ("reasoning_tokens", usage["reasoningTokens"], limits["maxReasoningTokens"]),
                )
                if limit is not None
                and (value > limit or (row["inclusive"] and value >= limit))
            ), None)
        assert actual == row["budgetKind"], row["name"]

    batch = FIXTURE["providerDeltaBatch"]
    assert provider_delta_batch_digest(batch["entries"]) == batch["payloadDigest"]

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
