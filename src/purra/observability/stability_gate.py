"""Rolling-baseline quality gate for content-free stability trends."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from purra.observability._values import (
    as_mapping as _mapping,
    non_negative_integer as _non_negative_integer,
)

_RATE_METRICS = (
    "runFailureRate",
    "stabilityFailureRate",
    "toolProtocolRunRate",
    "incompleteToolRunRate",
    "contextOverflowRunRate",
    "compactionFailureRunRate",
    "retryRunRate",
)
_CRITICAL_SIGNAL_METRICS = (
    "failedRuns",
    "stabilityFailedRuns",
    "toolProtocolFailureRuns",
    "incompleteToolRuns",
    "contextOverflowRuns",
    "compactionFailureRuns",
)


@dataclass(frozen=True)
class StabilityRegressionGatePolicy:
    minimum_window_size: int = 5
    max_rate_increase: float = 0.05
    max_failure_streak_increase: int = 1
    fail_on_new_tool_error_code: bool = True

    def __post_init__(self) -> None:
        if self.minimum_window_size < 1:
            raise ValueError("minimum gate window size must be positive")
        if not 0 <= self.max_rate_increase <= 1:
            raise ValueError("maximum rate increase must be between zero and one")
        if self.max_failure_streak_increase < 0:
            raise ValueError("maximum failure streak increase cannot be negative")


DEFAULT_STABILITY_REGRESSION_GATE_POLICY = StabilityRegressionGatePolicy()


def evaluate_stability_regression_gate(
    candidate: Mapping[str, Any],
    baseline: Mapping[str, Any],
    *,
    policy: StabilityRegressionGatePolicy | None = None,
) -> dict[str, Any]:
    """Compare a recent candidate window with the immediately prior window."""

    selected_policy = policy or DEFAULT_STABILITY_REGRESSION_GATE_POLICY
    candidate_size = _non_negative_integer(candidate.get("sampleSize"))
    baseline_size = _non_negative_integer(baseline.get("sampleSize"))
    candidate_metrics = _mapping(candidate.get("metrics"))
    baseline_metrics = _mapping(baseline.get("metrics"))
    comparable = (
        candidate_size >= selected_policy.minimum_window_size
        and baseline_size >= selected_policy.minimum_window_size
    )

    candidate_verdict = str(candidate.get("verdict") or "insufficient_data")
    checks: list[dict[str, Any]] = [{
        "name": "candidateHealth",
        "status": (
            "fail" if candidate_verdict == "fail"
            else "warn" if candidate_verdict == "warn"
            else "pass" if candidate_verdict == "pass"
            else "insufficient_data"
        ),
        "detail": {"verdict": candidate_verdict},
    }]
    metric_deltas: dict[str, float | None] = {}
    for metric_name in _RATE_METRICS:
        candidate_value = _optional_rate(candidate_metrics.get(metric_name))
        baseline_value = _optional_rate(baseline_metrics.get(metric_name))
        delta = (
            round(candidate_value - baseline_value, 4)
            if candidate_value is not None and baseline_value is not None
            else None
        )
        metric_deltas[metric_name] = delta
        status = (
            "insufficient_data"
            if not comparable
            else "not_applicable"
            if delta is None
            else "fail"
            if delta > selected_policy.max_rate_increase
            else "warn"
            if delta > 0
            else "pass"
        )
        checks.append({
            "name": f"rateRegression:{metric_name}",
            "status": status,
            "detail": {
                "baseline": baseline_value,
                "candidate": candidate_value,
                "delta": delta,
                "maxIncrease": selected_policy.max_rate_increase,
            },
        })

    candidate_streak = _non_negative_integer(
        candidate_metrics.get("currentFailureStreak")
    )
    baseline_streak = _non_negative_integer(
        baseline_metrics.get("currentFailureStreak")
    )
    streak_delta = candidate_streak - baseline_streak
    checks.append({
        "name": "failureStreakRegression",
        "status": (
            "insufficient_data"
            if baseline_size < selected_policy.minimum_window_size
            else "fail"
            if streak_delta > selected_policy.max_failure_streak_increase
            else "warn"
            if streak_delta > 0
            else "pass"
        ),
        "detail": {
            "baseline": baseline_streak,
            "candidate": candidate_streak,
            "delta": streak_delta,
            "maxIncrease": selected_policy.max_failure_streak_increase,
        },
    })

    candidate_error_codes = set(
        _mapping(candidate_metrics.get("toolErrorCodes"))
    )
    baseline_error_codes = set(
        _mapping(baseline_metrics.get("toolErrorCodes"))
    )
    new_error_codes = sorted(candidate_error_codes - baseline_error_codes)
    checks.append({
        "name": "newToolErrorCodes",
        "status": (
            "insufficient_data"
            if baseline_size < selected_policy.minimum_window_size
            else "fail"
            if new_error_codes and selected_policy.fail_on_new_tool_error_code
            else "warn" if new_error_codes
            else "pass"
        ),
        "detail": {"codes": new_error_codes},
    })
    new_critical_signals = [
        metric_name
        for metric_name in _CRITICAL_SIGNAL_METRICS
        if _non_negative_integer(baseline_metrics.get(metric_name)) == 0
        and _non_negative_integer(candidate_metrics.get(metric_name)) > 0
    ]
    checks.append({
        "name": "newCriticalSignals",
        "status": (
            "insufficient_data"
            if baseline_size < selected_policy.minimum_window_size
            else "fail" if new_critical_signals
            else "pass"
        ),
        "detail": {"metrics": new_critical_signals},
    })

    verdict = (
        "fail"
        if any(check["status"] == "fail" for check in checks)
        else "warn"
        if any(check["status"] == "warn" for check in checks)
        else "insufficient_data"
        if not comparable
        else "pass"
    )
    return {
        "verdict": verdict,
        "candidateSampleSize": candidate_size,
        "baselineSampleSize": baseline_size,
        "minimumWindowSize": selected_policy.minimum_window_size,
        "metricDeltas": metric_deltas,
        "newToolErrorCodes": new_error_codes,
        "newCriticalSignals": new_critical_signals,
        "checks": checks,
        "alerts": [
            {
                "code": check["name"],
                "severity": check["status"],
                "detail": check["detail"],
            }
            for check in checks
            if check["status"] in {"warn", "fail"}
        ],
    }


def _optional_rate(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    return normalized if 0 <= normalized <= 1 else None


__all__ = [
    "DEFAULT_STABILITY_REGRESSION_GATE_POLICY",
    "StabilityRegressionGatePolicy",
    "evaluate_stability_regression_gate",
]
