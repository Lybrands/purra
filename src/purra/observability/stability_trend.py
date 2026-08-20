"""Content-free trend evaluation over recent Agent Run stability reports."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from purra.observability._values import (
    as_mapping as _mapping,
    non_negative_integer as _non_negative_integer,
)

_UNHEALTHY_RUN_STATUSES = frozenset({"blocked", "failed"})
_CHECK_NOT_APPLICABLE = "not_applicable"
_CHECK_INSUFFICIENT_DATA = "insufficient_data"


@dataclass(frozen=True)
class StabilityTrendPolicy:
    """Caller-configurable alert thresholds for a bounded recent window."""

    minimum_sample_size: int = 5
    run_failure_rate_warn: float = 0.10
    run_failure_rate_fail: float = 0.25
    stability_failure_rate_warn: float = 0.10
    stability_failure_rate_fail: float = 0.25
    tool_protocol_run_rate_warn: float = 0.05
    tool_protocol_run_rate_fail: float = 0.15
    incomplete_tool_run_rate_warn: float = 0.02
    incomplete_tool_run_rate_fail: float = 0.10
    context_overflow_run_rate_warn: float = 0.05
    context_overflow_run_rate_fail: float = 0.15
    compaction_failure_run_rate_warn: float = 0.05
    compaction_failure_run_rate_fail: float = 0.15
    retry_run_rate_warn: float = 0.20
    retry_run_rate_fail: float = 0.50
    failure_streak_warn: int = 2
    failure_streak_fail: int = 3

    def __post_init__(self) -> None:
        if self.minimum_sample_size < 1:
            raise ValueError("minimum sample size must be positive")
        if self.failure_streak_warn < 1:
            raise ValueError("failure streak warning threshold must be positive")
        if self.failure_streak_fail <= self.failure_streak_warn:
            raise ValueError(
                "failure streak failure threshold must exceed warning threshold"
            )
        for name in (
            "run_failure_rate",
            "stability_failure_rate",
            "tool_protocol_run_rate",
            "incomplete_tool_run_rate",
            "context_overflow_run_rate",
            "compaction_failure_run_rate",
            "retry_run_rate",
        ):
            warn = float(getattr(self, f"{name}_warn"))
            fail = float(getattr(self, f"{name}_fail"))
            if not 0 <= warn < fail <= 1:
                raise ValueError(
                    f"{name} thresholds must satisfy 0 <= warn < fail <= 1"
                )


DEFAULT_STABILITY_TREND_POLICY = StabilityTrendPolicy()


def evaluate_agent_run_stability_trend(
    recent_runs: Iterable[Mapping[str, Any]],
    *,
    policy: StabilityTrendPolicy | None = None,
) -> dict[str, Any]:
    """Evaluate newest-first, content-free stability samples."""

    selected_policy = policy or DEFAULT_STABILITY_TREND_POLICY
    samples = tuple(_sample_projection(item) for item in recent_runs)
    sample_size = len(samples)
    enough_samples = sample_size >= selected_policy.minimum_sample_size
    failed_runs = sum(
        1
        for sample in samples
        if sample["runStatus"] in _UNHEALTHY_RUN_STATUSES
    )
    stability_failures = sum(
        1 for sample in samples if sample["stabilityVerdict"] == "fail"
    )
    tool_samples = tuple(
        sample for sample in samples if sample["metrics"]["toolCalls"] > 0
    )
    model_samples = tuple(
        sample for sample in samples if sample["metrics"]["modelAttempts"] > 0
    )

    protocol_failures = sum(
        1
        for sample in tool_samples
        if sample["metrics"]["toolProtocolFailures"] > 0
    )
    incomplete_tools = sum(
        1
        for sample in tool_samples
        if sample["metrics"]["incompleteToolCalls"] > 0
    )
    context_overflows = sum(
        1
        for sample in samples
        if sample["metrics"]["contextOverflows"] > 0
    )
    compaction_failures = sum(
        1
        for sample in samples
        if sample["metrics"]["compactionFailures"] > 0
    )
    retry_runs = sum(
        1
        for sample in model_samples
        if sample["metrics"]["retryAttempts"] > 0
    )
    failure_streak = _current_failure_streak(samples)

    checks = [
        _rate_check(
            "runFailureRate",
            failed_runs,
            sample_size,
            selected_policy.run_failure_rate_warn,
            selected_policy.run_failure_rate_fail,
            enough_samples=enough_samples,
        ),
        _rate_check(
            "stabilityFailureRate",
            stability_failures,
            sample_size,
            selected_policy.stability_failure_rate_warn,
            selected_policy.stability_failure_rate_fail,
            enough_samples=enough_samples,
        ),
        _rate_check(
            "toolProtocolRunRate",
            protocol_failures,
            len(tool_samples),
            selected_policy.tool_protocol_run_rate_warn,
            selected_policy.tool_protocol_run_rate_fail,
            enough_samples=enough_samples,
        ),
        _rate_check(
            "incompleteToolRunRate",
            incomplete_tools,
            len(tool_samples),
            selected_policy.incomplete_tool_run_rate_warn,
            selected_policy.incomplete_tool_run_rate_fail,
            enough_samples=enough_samples,
        ),
        _rate_check(
            "contextOverflowRunRate",
            context_overflows,
            sample_size,
            selected_policy.context_overflow_run_rate_warn,
            selected_policy.context_overflow_run_rate_fail,
            enough_samples=enough_samples,
        ),
        _rate_check(
            "compactionFailureRunRate",
            compaction_failures,
            sample_size,
            selected_policy.compaction_failure_run_rate_warn,
            selected_policy.compaction_failure_run_rate_fail,
            enough_samples=enough_samples,
        ),
        _rate_check(
            "retryRunRate",
            retry_runs,
            len(model_samples),
            selected_policy.retry_run_rate_warn,
            selected_policy.retry_run_rate_fail,
            enough_samples=enough_samples,
        ),
        _streak_check(
            failure_streak,
            selected_policy,
        ),
    ]
    alerts = [
        {
            "code": check["name"],
            "severity": check["status"],
            "value": check["value"],
            "threshold": (
                check["failAt"]
                if check["status"] == "fail"
                else check["warnAt"]
            ),
        }
        for check in checks
        if check["status"] in {"warn", "fail"}
    ]
    verdict = (
        "fail"
        if any(check["status"] == "fail" for check in checks)
        else "warn"
        if any(check["status"] == "warn" for check in checks)
        else _CHECK_INSUFFICIENT_DATA
        if not enough_samples
        else "pass"
    )
    tool_error_codes: Counter[str] = Counter()
    for sample in samples:
        tool_error_codes.update(sample["metrics"]["toolErrorCodes"])
    sorted_tool_error_codes = dict(sorted(tool_error_codes.items()))

    return {
        "verdict": verdict,
        "sampleSize": sample_size,
        "minimumSampleSize": selected_policy.minimum_sample_size,
        "metrics": {
            "failedRuns": failed_runs,
            "runFailureRate": _rate(failed_runs, sample_size),
            "stabilityFailedRuns": stability_failures,
            "stabilityFailureRate": _rate(stability_failures, sample_size),
            "toolRuns": len(tool_samples),
            "toolProtocolFailureRuns": protocol_failures,
            "toolProtocolRunRate": _rate(protocol_failures, len(tool_samples)),
            "incompleteToolRuns": incomplete_tools,
            "incompleteToolRunRate": _rate(incomplete_tools, len(tool_samples)),
            "contextOverflowRuns": context_overflows,
            "contextOverflowRunRate": _rate(context_overflows, sample_size),
            "compactionFailureRuns": compaction_failures,
            "compactionFailureRunRate": _rate(
                compaction_failures,
                sample_size,
            ),
            "modelRuns": len(model_samples),
            "retryRuns": retry_runs,
            "retryRunRate": _rate(retry_runs, len(model_samples)),
            "currentFailureStreak": failure_streak,
            "toolErrorCodes": sorted_tool_error_codes,
            "topToolErrorCodes": [
                {"code": code, "count": count}
                for code, count in sorted(
                    tool_error_codes.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:5]
            ],
        },
        "checks": checks,
        "alerts": alerts,
        "recentRuns": [
            {
                "runId": sample["runId"],
                "runStatus": sample["runStatus"],
                "stabilityVerdict": sample["stabilityVerdict"],
                "createTime": sample["createTime"],
            }
            for sample in samples
        ],
    }


def _sample_projection(sample: Mapping[str, Any]) -> dict[str, Any]:
    stability = _mapping(sample.get("stability"))
    metrics = _mapping(stability.get("metrics"))
    return {
        "runId": str(sample.get("runId") or ""),
        "runStatus": str(sample.get("runStatus") or "unknown"),
        "stabilityVerdict": str(stability.get("verdict") or "pass"),
        "createTime": sample.get("createTime"),
        "metrics": {
            "toolCalls": _non_negative_integer(metrics.get("toolCalls")),
            "toolProtocolFailures": _non_negative_integer(
                metrics.get("toolProtocolFailures")
            ),
            "incompleteToolCalls": _non_negative_integer(
                metrics.get("incompleteToolCalls")
            ),
            "contextOverflows": _non_negative_integer(
                metrics.get("contextOverflows")
            ),
            "compactionFailures": _non_negative_integer(
                metrics.get("compactionFailures")
            ),
            "modelAttempts": _non_negative_integer(metrics.get("modelAttempts")),
            "retryAttempts": _non_negative_integer(metrics.get("retryAttempts")),
            "toolErrorCodes": {
                str(code): _non_negative_integer(count)
                for code, count in _mapping(metrics.get("toolErrorCodes")).items()
                if str(code).strip() and _non_negative_integer(count) > 0
            },
        },
    }


def _current_failure_streak(samples: tuple[dict[str, Any], ...]) -> int:
    count = 0
    for sample in samples:
        if (
            sample["runStatus"] not in _UNHEALTHY_RUN_STATUSES
            and sample["stabilityVerdict"] != "fail"
        ):
            break
        count += 1
    return count


def _rate_check(
    name: str,
    numerator: int,
    denominator: int,
    warn_at: float,
    fail_at: float,
    *,
    enough_samples: bool,
) -> dict[str, Any]:
    value = _rate(numerator, denominator)
    if not enough_samples:
        status = _CHECK_INSUFFICIENT_DATA
    elif value is None:
        status = _CHECK_NOT_APPLICABLE
    elif value >= fail_at:
        status = "fail"
    elif value >= warn_at:
        status = "warn"
    else:
        status = "pass"
    return {
        "name": name,
        "status": status,
        "value": value,
        "numerator": numerator,
        "denominator": denominator,
        "warnAt": warn_at,
        "failAt": fail_at,
    }


def _streak_check(
    value: int,
    policy: StabilityTrendPolicy,
) -> dict[str, Any]:
    status = (
        "fail"
        if value >= policy.failure_streak_fail
        else "warn"
        if value >= policy.failure_streak_warn
        else "pass"
    )
    return {
        "name": "failureStreak",
        "status": status,
        "value": value,
        "numerator": value,
        "denominator": None,
        "warnAt": policy.failure_streak_warn,
        "failAt": policy.failure_streak_fail,
    }


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


__all__ = [
    "DEFAULT_STABILITY_TREND_POLICY",
    "StabilityTrendPolicy",
    "evaluate_agent_run_stability_trend",
]
