"""Generic replay and assertion framework for Agent runtime regressions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from purra.observability._values import as_mapping as _mapping
from purra.observability.diagnostics import (
    build_canonical_run_observation,
    evaluate_agent_run,
)
from purra.observability.failure_classification import (
    classify_agent_run_failures,
)


@dataclass(frozen=True, slots=True)
class AgentRuntimeRegressionCase:
    """A content-neutral runtime evidence contract supplied by a caller."""

    case_id: str
    title: str
    run: Mapping[str, Any]
    events: tuple[Mapping[str, Any], ...]
    expected_report_verdict: str
    expected_planner_outcome: str
    expected_terminal_status: str
    expected_tool_sequence: tuple[str, ...] = ()
    expected_check_statuses: Mapping[str, str] = field(default_factory=dict)
    expected_failure_codes: tuple[str, ...] | None = None


def evaluate_runtime_regression_case(
    case: AgentRuntimeRegressionCase,
) -> dict[str, Any]:
    """Score one caller-owned replay case against generic Core invariants."""

    report = evaluate_agent_run(case.run, case.events)
    observation = build_canonical_run_observation(case.events)
    planner_traces = observation.by_stage.get("planner", ())
    planner_outcome = str(
        (planner_traces[-1] if planner_traces else {}).get("outcome") or "unknown"
    )
    actual_tool_sequence = tuple(
        str(tool)
        for trace in observation.by_stage.get("tool_round", ())
        if trace.get("outcome") == "completed"
        for tool in (_mapping(trace.get("details")).get("requestedTools") or [])
    )
    report_checks = {
        str(check.get("name")): str(check.get("status"))
        for check in report.get("checks") or []
    }
    failure_classification = classify_agent_run_failures(case.run, case.events)
    actual_failure_codes = tuple(
        str(finding.get("code") or "")
        for finding in failure_classification["findings"]
    )

    checks: list[dict[str, Any]] = [
        {
            "name": "reportVerdict",
            "status": (
                "pass"
                if report["verdict"] == case.expected_report_verdict
                else "fail"
            ),
            "detail": {
                "expected": case.expected_report_verdict,
                "actual": report["verdict"],
            },
        },
        {
            "name": "plannerOutcome",
            "status": (
                "pass"
                if planner_outcome == case.expected_planner_outcome
                else "fail"
            ),
            "detail": {
                "expected": case.expected_planner_outcome,
                "actual": planner_outcome,
            },
        },
        {
            "name": "terminalStatus",
            "status": (
                "pass"
                if report["runStatus"] == case.expected_terminal_status
                else "fail"
            ),
            "detail": {
                "expected": case.expected_terminal_status,
                "actual": report["runStatus"],
            },
        },
        {
            "name": "toolSequence",
            "status": (
                "pass"
                if actual_tool_sequence == case.expected_tool_sequence
                else "fail"
            ),
            "detail": {
                "expected": list(case.expected_tool_sequence),
                "actual": list(actual_tool_sequence),
            },
        },
    ]
    for name, expected in case.expected_check_statuses.items():
        actual = report_checks.get(name, "missing")
        checks.append({
            "name": f"diagnostic:{name}",
            "status": "pass" if actual == expected else "fail",
            "detail": {"expected": expected, "actual": actual},
        })
    if case.expected_failure_codes is not None:
        checks.append({
            "name": "failureClassification",
            "status": (
                "pass"
                if actual_failure_codes == case.expected_failure_codes
                else "fail"
            ),
            "detail": {
                "expected": list(case.expected_failure_codes),
                "actual": list(actual_failure_codes),
            },
        })

    return {
        "caseId": case.case_id,
        "title": case.title,
        "verdict": (
            "pass"
            if all(check["status"] == "pass" for check in checks)
            else "fail"
        ),
        "checks": checks,
        "operationalReport": report,
        "failureClassification": failure_classification,
    }


def run_runtime_regression_suite(
    cases: Iterable[AgentRuntimeRegressionCase],
) -> dict[str, Any]:
    """Run caller-owned cases; Core intentionally ships no domain catalogue."""

    results = [evaluate_runtime_regression_case(case) for case in cases]
    passed = sum(1 for result in results if result["verdict"] == "pass")
    return {
        "summary": {
            "total": len(results),
            "passed": passed,
            "failed": len(results) - passed,
        },
        "results": results,
    }
