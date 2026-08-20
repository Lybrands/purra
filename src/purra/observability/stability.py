"""Content-free stability metrics derived from persisted Core events."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from purra.observability._values import (
    as_mapping as _mapping,
    as_sequence as _sequence,
    non_negative_integer as _non_negative_integer,
)
from purra.observability.diagnostics import (
    TRACE_EVENT_TYPE,
    build_canonical_run_observation,
    generation_attempt_traces,
)
from purra.events import CoreEventType


_TOOL_PROTOCOL_ERROR_CODES = frozenset({
    "invalid_tool_arguments_json",
    "invalid_tool_arguments_schema",
    "invalid_tool_arguments_shape",
    "invalid_tool_arguments_type",
    "invalid_tool_arguments_value",
    "tool_arguments_too_large",
    "tool_call_truncated",
})
_FAILED_TOOL_OUTCOMES = frozenset({
    "failed",
    "rejected",
    "canceled",
    "cancelled",
    "declined",
})
_INTERRUPTED_MODEL_OUTCOMES = frozenset({
    "interrupted",
    "interrupted_retry",
    "canceled",
    "cancelled",
})
_COMPACTION_FAILURE_OUTCOMES = frozenset({
    "generation_failed",
    "generation_failed_reused",
    "history_mismatch",
    "repository_unavailable",
})


def evaluate_agent_run_stability(
    events: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Aggregate protocol, tool, model, and context reliability evidence."""

    event_items = tuple(events)
    observation = build_canonical_run_observation(event_items)
    tool_calls = _tool_call_observations(event_items)
    tool_error_codes = Counter(
        str(item.get("errorCode") or "")
        for item in tool_calls.values()
        if str(item.get("errorCode") or "")
    )
    failed_tools = tuple(
        item for item in tool_calls.values()
        if item.get("errorCode")
        or str(item.get("outcome") or "") in _FAILED_TOOL_OUTCOMES
    )
    incomplete_tools = tuple(
        item for item in tool_calls.values()
        if not item.get("errorCode")
        and str(item.get("outcome") or "started") == "started"
    )
    completed_tools = tuple(
        item for item in tool_calls.values()
        if not item.get("errorCode")
        and str(item.get("outcome") or "") == "completed"
    )
    protocol_failure_count = sum(
        count
        for code, count in tool_error_codes.items()
        if code in _TOOL_PROTOCOL_ERROR_CODES
    )
    tool_count = len(tool_calls)

    compaction_traces = observation.by_stage.get(
        "conversation_compaction",
        (),
    )
    compaction_outcomes = Counter(
        str(trace.get("outcome") or "unknown")
        for trace in compaction_traces
    )
    compacted_turns = sum(
        _non_negative_integer(
            _mapping(trace.get("details")).get("compactedTurnCount")
        )
        for trace in compaction_traces
    )
    compaction_failures = sum(
        count
        for outcome, count in compaction_outcomes.items()
        if outcome in _COMPACTION_FAILURE_OUTCOMES
    )
    compaction_fallbacks = compaction_outcomes["compacted_fallback"]

    context_traces = observation.by_stage.get("context_budget", ())
    context_overflows = sum(
        1
        for trace in context_traces
        if str(trace.get("outcome") or "").startswith("overflow")
    )
    dropped_messages = max(
        (
            _non_negative_integer(
                _mapping(trace.get("details")).get("droppedMessages")
            )
            for trace in context_traces
        ),
        default=0,
    )

    generation_traces = generation_attempt_traces(observation.by_stage)
    interrupted_attempts = sum(
        1
        for trace in generation_traces
        if str(trace.get("outcome") or "") in _INTERRUPTED_MODEL_OUTCOMES
    )
    retry_attempts = sum(
        1
        for trace in observation.by_stage.get("stream", ())
        if "retry" in str(trace.get("outcome") or "")
        and _mapping(trace.get("details")).get("providerAttemptTerminal")
        is not False
    )

    checks = [
        {
            "name": "toolProtocol",
            "status": "pass" if protocol_failure_count == 0 else "fail",
            "detail": {"failureCount": protocol_failure_count},
        },
        {
            "name": "toolExecution",
            "status": (
                "fail" if failed_tools or incomplete_tools else "pass"
            ),
            "detail": {
                "failedCalls": len(failed_tools),
                "incompleteCalls": len(incomplete_tools),
                "totalCalls": tool_count,
            },
        },
        {
            "name": "contextOverflow",
            "status": "pass" if context_overflows == 0 else "fail",
            "detail": {"overflowCount": context_overflows},
        },
        {
            "name": "contextCompaction",
            "status": (
                "fail" if compaction_failures
                else "warn" if compaction_fallbacks
                else "pass"
            ),
            "detail": {
                "failureCount": compaction_failures,
                "fallbackCount": compaction_fallbacks,
            },
        },
        {
            "name": "modelContinuity",
            "status": (
                "warn" if interrupted_attempts or retry_attempts else "pass"
            ),
            "detail": {
                "interruptedAttempts": interrupted_attempts,
                "retryAttempts": retry_attempts,
            },
        },
    ]
    verdict = (
        "fail" if any(item["status"] == "fail" for item in checks)
        else "warn" if any(item["status"] == "warn" for item in checks)
        else "pass"
    )
    return {
        "verdict": verdict,
        "metrics": {
            "toolCalls": tool_count,
            "completedToolCalls": len(completed_tools),
            "failedToolCalls": len(failed_tools),
            "incompleteToolCalls": len(incomplete_tools),
            "toolSuccessRate": (
                round(len(completed_tools) / tool_count, 4)
                if tool_count
                else None
            ),
            "toolProtocolFailures": protocol_failure_count,
            "toolErrorCodes": dict(sorted(tool_error_codes.items())),
            "modelAttempts": len(generation_traces),
            "interruptedModelAttempts": interrupted_attempts,
            "retryAttempts": retry_attempts,
            "contextOverflows": context_overflows,
            "maxDroppedMessages": dropped_messages,
            "compactionPasses": len(compaction_traces),
            "compactedTurns": compacted_turns,
            "compactionFallbacks": compaction_fallbacks,
            "compactionFailures": compaction_failures,
            "compactionOutcomes": dict(sorted(compaction_outcomes.items())),
        },
        "checks": checks,
    }


def _tool_call_observations(
    events: tuple[Mapping[str, Any], ...],
) -> dict[str, dict[str, Any]]:
    observations: dict[str, dict[str, Any]] = {}
    generated_id = 0
    for event in events:
        event_type = str(event.get("eventType") or "")
        payload = _mapping(event.get("payload"))
        if event_type == CoreEventType.TOOL_CALLS_STARTED.value:
            for call in _sequence(payload.get("calls")):
                call_map = _mapping(call)
                call_id = str(call_map.get("id") or "").strip()
                if not call_id:
                    generated_id += 1
                    call_id = f"started:{generated_id}"
                observations.setdefault(call_id, {
                    "toolName": str(call_map.get("name") or ""),
                    "outcome": "started",
                })
            continue
        if event_type == CoreEventType.TOOL_CALL_COMPLETED.value:
            call_id = str(
                payload.get("toolCallId")
                or payload.get("tool_call_id")
                or ""
            ).strip()
            if not call_id:
                generated_id += 1
                call_id = f"completed:{generated_id}"
            current = observations.setdefault(call_id, {})
            current.update({
                "toolName": str(
                    payload.get("toolName")
                    or payload.get("tool_name")
                    or current.get("toolName")
                    or ""
                ),
                "outcome": str(payload.get("outcome") or "completed"),
                "errorCode": str(
                    payload.get("errorCode")
                    or payload.get("error_code")
                    or current.get("errorCode")
                    or ""
                ),
            })
            continue
        if event_type != CoreEventType.TOOL_RESULTS.value:
            continue
        for result in _sequence(payload.get("results")):
            result_map = _mapping(result)
            call_id = str(result_map.get("tool_call_id") or "").strip()
            if not call_id:
                generated_id += 1
                call_id = f"result:{generated_id}"
            current = observations.setdefault(call_id, {})
            error_code = str(result_map.get("error") or "").strip()
            current.update({
                "toolName": str(
                    result_map.get("tool_name")
                    or current.get("toolName")
                    or ""
                ),
                "outcome": (
                    current.get("outcome")
                    if current.get("outcome") not in {None, "started"}
                    else "failed" if error_code else "completed"
                ),
                "errorCode": error_code or current.get("errorCode") or "",
            })
    return observations


__all__ = ["evaluate_agent_run_stability"]
