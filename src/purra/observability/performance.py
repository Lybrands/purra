"""Performance-budget reports derived from content-free Agent evidence."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from purra.observability._values import (
    as_mapping as _mapping,
    optional_non_negative_integer as _optional_non_negative_integer,
)
from purra.observability.diagnostics import (
    TRACE_EVENT_TYPE,
    build_canonical_run_observation,
    generation_attempt_traces,
)


PERFORMANCE_BUDGETS = {
    "plannerMs": 15_000,
    "modelTotalMs": 60_000,
    "toolTotalMs": 15_000,
    # Response judges are real model work and remain part of the total.  The
    # phase-specific budgets prevent ordinary generation from consuming the
    # extra rounds reserved for one bounded semantic repair and re-judge.
    "modelRounds": 6,
    "generationModelRounds": 4,
    "responseJudgeRounds": 2,
    "toolSchemaTokens": 4_000,
}

_NON_EXECUTING_APPROVAL_STATUSES = frozenset({
    "declined",
    "rejected",
    "timed_out",
    "canceled",
    "cancelled",
    "unavailable",
    "error",
})
_APPROVAL_ONLY_TOOL_OUTCOMES = frozenset({
    "declined",
    "approval_rejected",
    "approval_timed_out",
    "approval_canceled",
    "approval_cancelled",
    "approval_unavailable",
    "approval_error",
})


def evaluate_agent_run_performance(
    events: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Evaluate active latency and token proxies without response content."""

    event_items = tuple(events)
    observation = build_canonical_run_observation(event_items)
    by_stage = observation.by_stage
    planner_ms = sum(_duration(trace) for trace in by_stage.get("planner", ()))
    generation_traces = generation_attempt_traces(by_stage)
    judge_traces = tuple(
        trace
        for trace in by_stage.get("model_output", ())
        if str(trace.get("outcome") or "").startswith("response_judge_")
    )
    generation_model_ms = sum(_duration(trace) for trace in generation_traces)
    response_judge_ms = sum(_duration(trace) for trace in judge_traces)
    model_ms = generation_model_ms + response_judge_ms
    tool_traces = by_stage.get("tool_round", ())
    approval_waits = _approval_waits_by_tool_round(event_items, tool_traces)
    human_approval_wait_ms = sum(approval_waits)
    recorded_tool_ms = sum(_duration(trace) for trace in tool_traces)
    # ``toolTotalMs`` is active tool-stack/handler time. Human decision time is
    # reported separately and remains in ``recordedTotalMs`` for transparency.
    # Unknown failed/canceled rounds stay active: without approval evidence,
    # excluding them could conceal a genuinely slow or stuck handler.
    tool_ms = sum(
        max(0, _duration(trace) - approval_wait_ms)
        for trace, approval_wait_ms in zip(tool_traces, approval_waits)
    )
    generation_model_rounds = len(generation_traces)
    response_judge_rounds = len(judge_traces)
    model_rounds = generation_model_rounds + response_judge_rounds
    tool_rounds = len(tool_traces)
    compatibility_fallbacks = sum(
        1
        for trace in by_stage.get("tool_choice", ())
        if trace.get("outcome") == "provider_fallback_auto"
    )
    context = observation.context_budget
    schema_tokens = _integer(context.get("toolSchemaTokens"))
    estimated_input = _integer(context.get("estimatedInputTokens"))
    projected_total = _integer(context.get("projectedTotalTokens"))
    window_tokens = _integer(context.get("windowTokens"))
    headroom = max(0, window_tokens - projected_total) if window_tokens else 0

    checks = [
        _budget_check(
            "plannerLatency",
            planner_ms,
            PERFORMANCE_BUDGETS["plannerMs"],
            "ms",
        ),
        _budget_check(
            "modelLatency",
            model_ms,
            PERFORMANCE_BUDGETS["modelTotalMs"],
            "ms",
        ),
        _budget_check(
            "toolLatency",
            tool_ms,
            PERFORMANCE_BUDGETS["toolTotalMs"],
            "ms",
        ),
        _budget_check(
            "modelRounds",
            model_rounds,
            PERFORMANCE_BUDGETS["modelRounds"],
            "rounds",
        ),
        _budget_check(
            "generationModelRounds",
            generation_model_rounds,
            PERFORMANCE_BUDGETS["generationModelRounds"],
            "rounds",
        ),
        _budget_check(
            "responseJudgeRounds",
            response_judge_rounds,
            PERFORMANCE_BUDGETS["responseJudgeRounds"],
            "rounds",
        ),
        _budget_check(
            "toolSchemaSize",
            schema_tokens,
            PERFORMANCE_BUDGETS["toolSchemaTokens"],
            "tokens",
        ),
        {
            "name": "providerCompatibility",
            "status": "pass" if compatibility_fallbacks == 0 else "warn",
            "detail": {"fallbackRequests": compatibility_fallbacks},
        },
    ]
    verdict = "warn" if any(
        check["status"] == "warn" for check in checks
    ) else "pass"
    active_total_ms = planner_ms + model_ms + tool_ms
    return {
        "verdict": verdict,
        "budgets": dict(PERFORMANCE_BUDGETS),
        "metrics": {
            "plannerMs": planner_ms,
            "modelTotalMs": model_ms,
            "generationModelTotalMs": generation_model_ms,
            "responseJudgeTotalMs": response_judge_ms,
            "toolTotalMs": tool_ms,
            "humanApprovalWaitMs": human_approval_wait_ms,
            "activeTotalMs": active_total_ms,
            "recordedTotalMs": planner_ms + model_ms + recorded_tool_ms,
            "modelRounds": model_rounds,
            "generationModelRounds": generation_model_rounds,
            "responseJudgeRounds": response_judge_rounds,
            "toolRounds": tool_rounds,
            "compatibilityFallbacks": compatibility_fallbacks,
            "estimatedInputTokens": estimated_input,
            "toolSchemaTokens": schema_tokens,
            "projectedTotalTokens": projected_total,
            "windowTokens": window_tokens,
            "headroomTokens": headroom,
            # Current provider-neutral judge completions do not expose usage.
            # Keep the existing projection explicitly scoped to the main run
            # instead of presenting it as complete judge-inclusive cost.
            "projectedTokensIncludeResponseJudge": False,
        },
        "checks": checks,
    }


def _approval_waits_by_tool_round(
    events: tuple[Mapping[str, Any], ...],
    tool_traces: tuple[dict[str, Any], ...],
) -> tuple[int, ...]:
    """Return the confirmed approval-wait portion of each tool round.

    A ``declined`` tool outcome means the handler did not execute. Other
    failed/canceled tool outcomes remain active unless approval evidence
    proves otherwise.
    """

    statuses_by_round: list[tuple[str, ...]] = []
    pending_statuses: list[str] = []
    for event in events:
        event_type = str(event.get("eventType") or "")
        payload = _mapping(event.get("payload"))
        if event_type == "approval.resolved":
            status = _normalized_status(payload.get("status"))
            if status:
                pending_statuses.append(status)
            continue
        if (
            event_type != TRACE_EVENT_TYPE
            or payload.get("stage") != "tool_round"
        ):
            continue
        statuses_by_round.append(tuple(pending_statuses))
        pending_statuses.clear()

    waits: list[int] = []
    for index, trace in enumerate(tool_traces):
        duration_ms = _duration(trace)
        details = _mapping(trace.get("details"))
        exact_wait = _optional_non_negative_integer(
            details.get("approvalWaitMs", details.get("approval_wait_ms"))
        )
        if exact_wait is not None:
            waits.append(min(duration_ms, exact_wait))
            continue

        statuses = _trace_approval_statuses(details)
        if not statuses and index < len(statuses_by_round):
            statuses = statuses_by_round[index]
        outcome = _normalized_status(trace.get("outcome"))
        if _handler_did_not_execute_for_approval(statuses, outcome):
            waits.append(duration_ms)
            continue

        waits.append(duration_ms if outcome in _APPROVAL_ONLY_TOOL_OUTCOMES else 0)
    return tuple(waits)


def _trace_approval_statuses(details: Mapping[str, Any]) -> tuple[str, ...]:
    raw_statuses = details.get(
        "approvalStatuses",
        details.get("approval_statuses"),
    )
    if isinstance(raw_statuses, (list, tuple, set, frozenset)):
        values = tuple(
            status
            for item in raw_statuses
            if (status := _normalized_status(item))
        )
        if values:
            return values
    status = _normalized_status(
        details.get("approvalStatus", details.get("approval_status"))
    )
    return (status,) if status else ()


def _handler_did_not_execute_for_approval(
    statuses: tuple[str, ...],
    tool_outcome: str,
) -> bool:
    if not statuses or "approved" in statuses or not all(
        status in _NON_EXECUTING_APPROVAL_STATUSES for status in statuses
    ):
        return False
    expected_outcomes: set[str] = set()
    for status in statuses:
        if status in {"declined", "rejected"}:
            expected_outcomes.update({"declined", "approval_rejected"})
        elif status in {"canceled", "cancelled"}:
            expected_outcomes.update({
                "canceled",
                "approval_canceled",
                "approval_cancelled",
            })
        else:
            expected_outcomes.update({
                "failed",
                "approval_timed_out",
                "approval_unavailable",
                "approval_error",
            })
    return tool_outcome in expected_outcomes


def _normalized_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _duration(trace: Mapping[str, Any]) -> int:
    return max(0, _integer(trace.get("durationMs")))


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _budget_check(
    name: str,
    actual: int,
    budget: int,
    unit: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "pass" if actual <= budget else "warn",
        "detail": {"actual": actual, "budget": budget, "unit": unit},
    }
