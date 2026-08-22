from __future__ import annotations

from purra.evaluation import (
    AgentRuntimeRegressionCase,
    evaluate_runtime_regression_case,
)
from purra.observability import evaluate_stability_regression_gate


def test_public_regression_projection_scores_canonical_run_evidence():
    events = (
        {
            "eventType": "agentRunTrace",
            "payload": {"stage": "planning", "outcome": "skipped"},
        },
        {
            "eventType": "agentRunTrace",
            "payload": {
                "stage": "context_budget",
                "outcome": "within_budget",
            },
        },
        {"eventType": "run.completed", "payload": {}},
    )
    result = evaluate_runtime_regression_case(AgentRuntimeRegressionCase(
        case_id="reactive-success",
        title="Reactive success remains healthy",
        run={"id": "run-1", "status": "done"},
        events=events,
        expected_report_verdict="pass",
        expected_planner_outcome="skipped",
        expected_terminal_status="done",
        expected_check_statuses={"contextSafety": "pass"},
        expected_failure_codes=(),
    ))

    assert result["verdict"] == "pass"
    assert result["operationalReport"]["metrics"]["traceCount"] == 2
    assert result["failureClassification"]["findings"] == []


def test_public_stability_gate_fails_on_a_new_tool_error_code():
    baseline = {
        "verdict": "pass",
        "sampleSize": 5,
        "metrics": {"toolErrorCodes": {}},
    }
    candidate = {
        "verdict": "pass",
        "sampleSize": 5,
        "metrics": {
            "toolErrorCodes": {"invalid_tool_arguments_schema": 1},
        },
    }

    result = evaluate_stability_regression_gate(candidate, baseline)

    assert result["verdict"] == "fail"
    assert result["newToolErrorCodes"] == ["invalid_tool_arguments_schema"]
    assert any(
        alert["code"] == "newToolErrorCodes"
        and alert["severity"] == "fail"
        for alert in result["alerts"]
    )
