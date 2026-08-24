import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  classifyAgentRunFailures,
  evaluateAgentRun,
  evaluateAgentRunPerformance,
  evaluateAgentRunRecovery,
  evaluateAgentRunStability,
  evaluateAgentRunStabilityTrend,
  evaluateStabilityRegressionGate,
  getCoreSecurityRedTeamCases,
  runRuntimeRegressionSuite,
  runSecurityRedTeamCases,
} from "purra";

const fixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/observability_protocol.json", import.meta.url),
  "utf8",
));

test("frozen observability evidence matches Python and remains content-free", () => {
  for (const item of fixture.cases) {
    const operational = evaluateAgentRun(item.run, item.events);
    const stability = evaluateAgentRunStability(item.events);
    const failures = classifyAgentRunFailures(item.run, item.events);
    const recovery = evaluateAgentRunRecovery(item.events);
    const performance = evaluateAgentRunPerformance(item.events);

    assert.equal(operational.verdict, item.expected.operationalVerdict);
    assert.equal(stability.verdict, item.expected.stabilityVerdict);
    assert.deepEqual(failures.findings.map((finding) => finding.code), item.expected.failureCodes);
    if (item.expected.performanceVerdict !== undefined) {
      assert.equal(performance.verdict, item.expected.performanceVerdict);
    }
    if (item.expected.recoveryDecisionCount !== undefined) {
      assert.equal(recovery.summary.decisionCount, item.expected.recoveryDecisionCount);
      assert.equal(recovery.summary.safetyProtectedCount, item.expected.safetyProtectedCount);
      assert.equal(recovery.decisions[0].round, item.expected.recoveryRound);
    }
    assert.equal(JSON.stringify([operational, stability, failures, recovery, performance]).includes("LEAK_"), false);
  }
});

test("regression, trend, gate, and security suites are deterministic", async () => {
  const success = fixture.cases[0];
  const regression = runRuntimeRegressionSuite([{
    caseId: success.caseId,
    title: "Frozen success",
    run: success.run,
    events: success.events,
    expectedReportVerdict: "pass",
    expectedPlannerOutcome: "skipped",
    expectedTerminalStatus: "done",
    expectedCheckStatuses: { contextSafety: "pass" },
    expectedFailureCodes: [],
  }]);
  assert.deepEqual(regression.summary, { total: 1, passed: 1, failed: 0 });

  const reports = Array.from({ length: 5 }, () => evaluateAgentRunStability(success.events));
  const trend = evaluateAgentRunStabilityTrend(reports);
  assert.equal(trend.verdict, "pass");
  const gate = evaluateStabilityRegressionGate({
    ...trend,
    metrics: { ...trend.metrics, toolErrorCodes: { invalid_tool_arguments_schema: 1 } },
  }, trend);
  assert.equal(gate.verdict, "fail");
  assert.deepEqual(gate.newToolErrorCodes, ["invalid_tool_arguments_schema"]);

  const security = await runSecurityRedTeamCases(getCoreSecurityRedTeamCases());
  assert.deepEqual(security.summary, { total: 5, passed: 5, failed: 0 });
});
