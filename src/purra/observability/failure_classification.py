"""Deterministic, content-free failure classification for Agent Runs."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from purra.observability._values import (
    as_mapping as _mapping,
    non_negative_integer as _non_negative_integer,
)
from purra.observability.diagnostics import (
    build_canonical_run_observation,
    evaluate_agent_run,
)
from purra.observability.stability import evaluate_agent_run_stability


_INVALID_ARGUMENT_CODES = frozenset({
    "invalid_tool_arguments_json",
    "invalid_tool_arguments_schema",
    "invalid_tool_arguments_shape",
    "invalid_tool_arguments_type",
    "invalid_tool_arguments_value",
})
_PLANNER_FAILURE_OUTCOMES = frozenset({
    "exception",
    "invalid_plan",
    "fallback_after_error",
    "invalid",
    "contract_violation",
    "failed",
})


def classify_agent_run_failures(
    run: Mapping[str, Any],
    events: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Classify explicit causes separately from correlated symptoms."""

    event_items = tuple(events)
    operational = evaluate_agent_run(run, event_items)
    stability = evaluate_agent_run_stability(event_items)
    observation = build_canonical_run_observation(event_items)
    metrics = _mapping(stability.get("metrics"))
    run_status = str(run.get("status") or "unknown")
    tool_error_codes = {
        str(code): _non_negative_integer(count)
        for code, count in _mapping(metrics.get("toolErrorCodes")).items()
        if str(code).strip() and _non_negative_integer(count) > 0
    }
    findings: list[dict[str, Any]] = []
    finding_codes: set[str] = set()

    invalid_codes = {
        code: count
        for code, count in tool_error_codes.items()
        if code in _INVALID_ARGUMENT_CODES
    }
    if invalid_codes:
        _append_finding(
            findings,
            finding_codes,
            code="tool.protocol_invalid_arguments",
            category="tool_protocol",
            severity="fail",
            confidence="high",
            evidence={"errorCodes": invalid_codes},
            remediation="inspect_tool_contract",
        )
    if tool_error_codes.get("tool_arguments_too_large"):
        _append_finding(
            findings,
            finding_codes,
            code="tool.arguments_too_large",
            category="tool_protocol",
            severity="fail",
            confidence="high",
            evidence={
                "failureCount": tool_error_codes["tool_arguments_too_large"],
            },
            remediation="inspect_tool_payload_strategy",
        )
    if tool_error_codes.get("tool_call_truncated"):
        _append_finding(
            findings,
            finding_codes,
            code="model.tool_call_truncated",
            category="model_protocol",
            severity="fail",
            confidence="high",
            evidence={
                "failureCount": tool_error_codes["tool_call_truncated"],
            },
            remediation="split_continuation_from_checkpoint",
        )
    incomplete_calls = _non_negative_integer(metrics.get("incompleteToolCalls"))
    if incomplete_calls:
        _append_finding(
            findings,
            finding_codes,
            code="tool.incomplete_terminalization",
            category="tool_lifecycle",
            severity="fail",
            confidence="high",
            evidence={"incompleteCalls": incomplete_calls},
            remediation="inspect_tool_terminalization",
        )
    context_overflows = _non_negative_integer(metrics.get("contextOverflows"))
    if context_overflows:
        _append_finding(
            findings,
            finding_codes,
            code="context.overflow",
            category="context_orchestration",
            severity="fail",
            confidence="high",
            evidence={"overflowCount": context_overflows},
            remediation="inspect_context_budget",
        )
    compaction_failures = _non_negative_integer(metrics.get("compactionFailures"))
    if compaction_failures:
        _append_finding(
            findings,
            finding_codes,
            code="context.compaction_failed",
            category="context_orchestration",
            severity="fail",
            confidence="high",
            evidence={"failureCount": compaction_failures},
            remediation="inspect_context_compaction",
        )
    compaction_fallbacks = _non_negative_integer(metrics.get("compactionFallbacks"))
    if compaction_fallbacks:
        _append_finding(
            findings,
            finding_codes,
            code="context.compaction_fallback",
            category="context_orchestration",
            severity="warn",
            confidence="high",
            evidence={"fallbackCount": compaction_fallbacks},
            remediation="inspect_compaction_summarizer",
        )

    planner_traces = observation.by_stage.get("planner", ())
    planner_outcome = str(
        (planner_traces[-1] if planner_traces else {}).get("outcome") or "unknown"
    )
    if planner_outcome in _PLANNER_FAILURE_OUTCOMES:
        _append_finding(
            findings,
            finding_codes,
            code="planner.contract_failure",
            category="planning",
            severity="fail",
            confidence="high",
            evidence={"outcome": planner_outcome},
            remediation="inspect_planner_contract",
        )

    tool_traces = observation.by_stage.get("tool_round", ())
    missing_required_calls = sum(
        trace.get("outcome") == "missing_required_call" for trace in tool_traces
    )
    if missing_required_calls:
        _append_finding(
            findings,
            finding_codes,
            code="tool.missing_required_call",
            category="tool_governance",
            severity="fail",
            confidence="high",
            evidence={"roundCount": missing_required_calls},
            remediation="inspect_tool_choice_contract",
        )
    rejected_rounds = sum(
        trace.get("outcome") == "rejected" for trace in tool_traces
    )
    if rejected_rounds:
        _append_finding(
            findings,
            finding_codes,
            code="tool.authorization_rejected",
            category="tool_governance",
            severity="fail",
            confidence="high",
            evidence={"roundCount": rejected_rounds},
            remediation="inspect_tool_authorization",
        )
    failed_tool_rounds = sum(
        trace.get("outcome") == "failed" for trace in tool_traces
    )
    failed_tool_calls = _non_negative_integer(metrics.get("failedToolCalls"))
    non_protocol_error_codes = {
        code: count
        for code, count in tool_error_codes.items()
        if code not in _INVALID_ARGUMENT_CODES
        and code not in {"tool_arguments_too_large", "tool_call_truncated"}
    }
    explicit_protocol_failures = sum(invalid_codes.values()) + sum(
        tool_error_codes.get(code, 0)
        for code in ("tool_arguments_too_large", "tool_call_truncated")
    )
    uncategorized_failed_calls = max(
        0,
        failed_tool_calls - explicit_protocol_failures,
    )
    if (
        non_protocol_error_codes
        or uncategorized_failed_calls
        or (failed_tool_rounds and explicit_protocol_failures == 0)
    ):
        _append_finding(
            findings,
            finding_codes,
            code="tool.execution_failed",
            category="tool_execution",
            severity="fail",
            confidence="high" if non_protocol_error_codes else "medium",
            evidence={
                "failedCalls": uncategorized_failed_calls,
                "failedRounds": failed_tool_rounds,
                "errorCodes": non_protocol_error_codes,
            },
            remediation="inspect_tool_handler",
        )

    interrupted_attempts = _non_negative_integer(
        metrics.get("interruptedModelAttempts")
    )
    if interrupted_attempts:
        _append_finding(
            findings,
            finding_codes,
            code="model.interrupted",
            category="model_transport",
            severity="fail" if run_status == "failed" else "warn",
            confidence="medium",
            evidence={"attemptCount": interrupted_attempts},
            remediation="inspect_model_transport",
        )
    retry_attempts = _non_negative_integer(metrics.get("retryAttempts"))
    if retry_attempts and not interrupted_attempts:
        _append_finding(
            findings,
            finding_codes,
            code="model.retry_recovered",
            category="model_transport",
            severity="warn",
            confidence="medium",
            evidence={"attemptCount": retry_attempts},
            remediation="inspect_model_transport",
        )

    if run_status == "failed" and not any(
        finding["severity"] == "fail" for finding in findings
    ):
        _append_finding(
            findings,
            finding_codes,
            code="run.failed_without_specific_cause",
            category="observability",
            severity="fail",
            confidence="low",
            evidence={
                "runStatus": run_status,
                "traceCount": _non_negative_integer(
                    _mapping(operational.get("metrics")).get("traceCount")
                ),
            },
            remediation="inspect_terminal_error_and_trace_coverage",
        )

    verdict = (
        "fail"
        if any(item["severity"] == "fail" for item in findings)
        else "warn"
        if findings
        else "pass"
    )
    category_counts: dict[str, int] = {}
    for finding in findings:
        category = str(finding["category"])
        category_counts[category] = category_counts.get(category, 0) + 1
    return {
        "verdict": verdict,
        "primaryFinding": (
            next(
                (
                    finding
                    for finding in findings
                    if finding["severity"] == "fail"
                ),
                findings[0] if findings else None,
            )
        ),
        "findings": findings,
        "summary": {
            "findingCount": len(findings),
            "categoryCounts": dict(sorted(category_counts.items())),
        },
    }


def _append_finding(
    findings: list[dict[str, Any]],
    finding_codes: set[str],
    *,
    code: str,
    category: str,
    severity: str,
    confidence: str,
    evidence: Mapping[str, Any],
    remediation: str,
) -> None:
    if code in finding_codes:
        return
    finding_codes.add(code)
    findings.append({
        "code": code,
        "category": category,
        "severity": severity,
        "confidence": confidence,
        "evidence": dict(evidence),
        "remediation": remediation,
    })


__all__ = ["classify_agent_run_failures"]
