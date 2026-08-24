from __future__ import annotations

import json
from pathlib import Path

from purra.observability import (
    classify_agent_run_failures,
    evaluate_agent_run,
    evaluate_agent_run_performance,
    evaluate_agent_run_recovery,
    evaluate_agent_run_stability,
)


FIXTURE = json.loads(
    (Path(__file__).parents[1] / "conformance" / "fixtures" / "observability_protocol.json")
    .read_text(encoding="utf-8")
)


def test_frozen_observability_fixture_is_content_free_and_stable():
    for case in FIXTURE["cases"]:
        run = case["run"]
        events = case["events"]
        expected = case["expected"]
        operational = evaluate_agent_run(run, events)
        stability = evaluate_agent_run_stability(events)
        failures = classify_agent_run_failures(run, events)
        recovery = evaluate_agent_run_recovery(events)
        performance = evaluate_agent_run_performance(events)

        assert operational["verdict"] == expected["operationalVerdict"]
        assert stability["verdict"] == expected["stabilityVerdict"]
        assert [item["code"] for item in failures["findings"]] == expected["failureCodes"]
        if "performanceVerdict" in expected:
            assert performance["verdict"] == expected["performanceVerdict"]
        if "recoveryDecisionCount" in expected:
            assert recovery["summary"]["decisionCount"] == expected["recoveryDecisionCount"]
            assert recovery["summary"]["safetyProtectedCount"] == expected["safetyProtectedCount"]
            assert recovery["decisions"][0]["round"] == expected["recoveryRound"]

        reports = (operational, stability, failures, recovery, performance)
        assert "LEAK_" not in json.dumps(reports, sort_keys=True)
