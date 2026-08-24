"""Content-free operational diagnostics for persisted Agent run evidence."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from purra.events import CoreEventType


TRACE_EVENT_TYPE = "agentRunTrace"

_CANONICAL_STAGE_NAMES = {
    "planning": "planner",
}
_CORE_TERMINAL_OUTCOMES = {
    CoreEventType.RUN_COMPLETED.value: "done",
    CoreEventType.RUN_BLOCKED.value: "blocked",
    CoreEventType.RUN_FAILED.value: "failed",
    CoreEventType.RUN_CANCELED.value: "canceled",
}
_PLANNER_PASS_OUTCOMES = {
    "host_plan",
    "model_plan",
    "skipped",
    "planned",
    "direct_response",
    "replanned",
}
_PLANNER_FAIL_OUTCOMES = {
    "exception",
    "invalid_plan",
    "fallback_after_error",
    "invalid",
    "contract_violation",
    "failed",
}
_TRACE_KEYS = ("stage", "outcome", "durationMs", "round")
_TRACE_DETAIL_KEYS = frozenset({
    "action",
    "allowed",
    "approvalStatus",
    "approvalStatuses",
    "approvalWaitMs",
    "approval_status",
    "approval_statuses",
    "approval_wait_ms",
    "attempt",
    "cause",
    "compactedTurnCount",
    "droppedMessages",
    "effectState",
    "estimatedInputTokens",
    "maxAttempts",
    "mayRepeatSideEffect",
    "projectedTotalTokens",
    "providerAttemptTerminal",
    "reasonCode",
    "remainingModelRounds",
    "requestedTools",
    "round",
    "toolSchemaTokens",
    "windowTokens",
})
_CONTEXT_BUDGET_KEYS = frozenset({
    "droppedMessages",
    "estimatedInputTokens",
    "projectedTotalTokens",
    "toolSchemaTokens",
    "windowTokens",
})


@dataclass(frozen=True, slots=True)
class CanonicalRunObservation:
    """One evaluator-facing view over traces and typed Core events."""

    traces: tuple[dict[str, Any], ...]
    by_stage: Mapping[str, tuple[dict[str, Any], ...]]
    context_budget: Mapping[str, Any]
    context_budget_source: str


def build_canonical_run_observation(
    events: Iterable[Mapping[str, Any]],
) -> CanonicalRunObservation:
    """Normalize event evidence without depending on persistence or a domain."""

    traces: list[dict[str, Any]] = []
    by_stage: dict[str, list[dict[str, Any]]] = {}
    core_context: dict[str, Any] = {}

    for event in events:
        event_type = str(event.get("eventType") or "")
        payload = _mapping_copy(event.get("payload"))
        if event_type == TRACE_EVENT_TYPE:
            payload = _sanitize_trace(payload)
            traces.append(payload)
            raw_stage = str(payload.get("stage") or "unknown")
            stage = _CANONICAL_STAGE_NAMES.get(raw_stage, raw_stage)
            by_stage.setdefault(stage, []).append(payload)
            continue
        if event_type == CoreEventType.CONTEXT_BUDGETED.value:
            core_context = {
                key: payload[key]
                for key in _CONTEXT_BUDGET_KEYS
                if key in payload
            }
            continue
        terminal_outcome = _CORE_TERMINAL_OUTCOMES.get(event_type)
        if terminal_outcome is not None:
            by_stage.setdefault("terminal", []).append({
                "stage": "terminal",
                "outcome": terminal_outcome,
                "sourceEventType": event_type,
            })

    trace_context: dict[str, Any] = {}
    for trace in reversed(traces):
        if trace.get("stage") == "context_budget":
            trace_context = _mapping_copy(trace.get("details"))
            break
    context_budget = {**trace_context, **core_context}
    context_source = (
        "core_event" if core_context
        else "trace" if trace_context
        else "none"
    )
    return CanonicalRunObservation(
        traces=tuple(traces),
        by_stage={stage: tuple(items) for stage, items in by_stage.items()},
        context_budget=context_budget,
        context_budget_source=context_source,
    )


def evaluate_agent_run(
    run: Mapping[str, Any],
    events: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Evaluate terminal, planning, budget, and tool-governance evidence."""

    observation = build_canonical_run_observation(events)
    traces = list(observation.traces)
    by_stage = observation.by_stage

    status = str(run.get("status") or "unknown")
    planner_traces = by_stage.get("planner", ())
    latest_planner = planner_traces[-1] if planner_traces else None
    planner_outcome = str((latest_planner or {}).get("outcome") or "unknown")
    context_traces = by_stage.get("context_budget", ())
    latest_context = context_traces[-1] if context_traces else None
    context_outcome = str((latest_context or {}).get("outcome") or "unknown")
    tool_traces = by_stage.get("tool_round", ())
    unauthorized_tools = sum(
        1 for trace in tool_traces if trace.get("outcome") == "rejected"
    )
    missing_required_calls = sum(
        1
        for trace in tool_traces
        if trace.get("outcome") == "missing_required_call"
    )
    failed_tool_rounds = sum(
        1 for trace in tool_traces if trace.get("outcome") == "failed"
    )
    model_rounds = len(generation_attempt_traces(by_stage))

    checks = [
        {
            "name": "terminalState",
            "status": (
                "pass"
                if status in {"done", "blocked", "failed", "canceled"}
                else "fail"
            ),
            "detail": status,
        },
        {
            "name": "taskOutcome",
            "status": (
                "pass" if status == "done"
                else "warn" if status in {"blocked", "canceled"}
                else "fail" if status == "failed"
                else "warn"
            ),
            "detail": status,
        },
        {
            "name": "traceCoverage",
            "status": (
                "pass"
                if {"planner", "terminal"}.issubset(by_stage)
                else "warn"
            ),
            "detail": sorted(by_stage),
        },
        {
            "name": "plannerHealth",
            "status": (
                "pass" if planner_outcome in _PLANNER_PASS_OUTCOMES
                else "fail" if planner_outcome in _PLANNER_FAIL_OUTCOMES
                else "warn"
            ),
            "detail": planner_outcome,
        },
        {
            "name": "contextSafety",
            "status": (
                "pass" if context_outcome == "within_budget"
                else "fail" if context_outcome.startswith("overflow")
                else "warn"
            ),
            "detail": context_outcome,
        },
        {
            "name": "toolGovernance",
            "status": (
                "pass"
                if unauthorized_tools == 0 and missing_required_calls == 0
                else "fail"
            ),
            "detail": {
                "rejectedRounds": unauthorized_tools,
                "missingRequiredCalls": missing_required_calls,
            },
        },
        {
            "name": "toolReliability",
            "status": "pass" if failed_tool_rounds == 0 else "fail",
            "detail": {"failedRounds": failed_tool_rounds},
        },
    ]
    if any(check["status"] == "fail" for check in checks):
        verdict = "fail"
    elif any(check["status"] == "warn" for check in checks):
        verdict = "warn"
    else:
        verdict = "pass"

    return {
        "runId": run.get("id"),
        "runStatus": status,
        "verdict": verdict,
        "checks": checks,
        "metrics": {
            "traceCount": len(traces),
            "modelRounds": model_rounds,
            "toolRounds": len(tool_traces),
            "rejectedToolRounds": unauthorized_tools,
            "missingRequiredToolCalls": missing_required_calls,
            "failedToolRounds": failed_tool_rounds,
        },
        "traces": traces,
    }


def _mapping_copy(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sanitize_trace(value: Mapping[str, Any]) -> dict[str, Any]:
    trace = {
        key: value[key]
        for key in _TRACE_KEYS
        if key in value and isinstance(value[key], (str, int, float, bool))
    }
    details = _mapping_copy(value.get("details"))
    safe_details: dict[str, Any] = {}
    for key in _TRACE_DETAIL_KEYS:
        item = details.get(key)
        if isinstance(item, (str, int, float, bool)):
            safe_details[key] = item
        elif isinstance(item, (list, tuple)) and all(
            isinstance(entry, str) for entry in item
        ):
            safe_details[key] = list(item)
    if safe_details:
        trace["details"] = safe_details
    return trace


def generation_attempt_traces(
    by_stage: Mapping[str, tuple[dict[str, Any], ...]],
) -> tuple[dict[str, Any], ...]:
    """Return one terminal trace for every generation provider attempt.

    Successful and ordinary failed attempts use ``model_round``. Historical
    interrupted streams used ``stream/interrupted``; newer retry traces carry
    an explicit content-free terminal marker. Counting both prevents an
    upstream timeout from disappearing from round and latency budgets.
    """

    interrupted_streams = tuple(
        trace
        for trace in by_stage.get("stream", ())
        if trace.get("outcome") == "interrupted"
        or _mapping_copy(trace.get("details")).get(
            "providerAttemptTerminal"
        ) is True
    )
    return tuple(by_stage.get("model_round", ())) + interrupted_streams
