import { ToolCatalog } from "../tools/catalog.js";
import type { ToolDefinition } from "../tools/types.js";
import type { EvidenceEvent, RunEvidence } from "../observability/types.js";
import { buildCanonicalRunObservation, evaluateAgentRun, mapping, text } from "../observability/observation.js";
import { classifyAgentRunFailures } from "../observability/reports.js";

export interface AgentRuntimeRegressionCase {
  readonly caseId: string;
  readonly title: string;
  readonly run: RunEvidence;
  readonly events: readonly EvidenceEvent[];
  readonly expectedReportVerdict: string;
  readonly expectedPlannerOutcome: string;
  readonly expectedTerminalStatus: string;
  readonly expectedToolSequence?: readonly string[];
  readonly expectedCheckStatuses?: Readonly<Record<string, string>>;
  readonly expectedFailureCodes?: readonly string[];
}

export interface SecurityRedTeamCase {
  readonly caseId: string;
  readonly threat: string;
  readonly check: () => Promise<boolean> | boolean;
}

export function evaluateRuntimeRegressionCase(value: AgentRuntimeRegressionCase) {
  const report = evaluateAgentRun(value.run, value.events);
  const observation = buildCanonicalRunObservation(value.events);
  const planner = observation.byStage.planner ?? [];
  const plannerOutcome = text(planner.at(-1)?.outcome) || "unknown";
  const actualTools = (observation.byStage.tool_round ?? [])
    .filter((trace) => trace.outcome === "completed")
    .flatMap((trace) => {
      const requested = mapping(trace.details).requestedTools;
      return Array.isArray(requested) ? requested.map(text) : [];
    });
  const reportChecks = Object.fromEntries(report.checks.map((check) => [check.name, check.status]));
  const classification = classifyAgentRunFailures(value.run, value.events);
  const failureCodes = classification.findings.map((finding) => text(finding.code));
  const checks: Record<string, unknown>[] = [
    comparison("reportVerdict", value.expectedReportVerdict, report.verdict),
    comparison("plannerOutcome", value.expectedPlannerOutcome, plannerOutcome),
    comparison("terminalStatus", value.expectedTerminalStatus, report.runStatus),
    comparison("toolSequence", value.expectedToolSequence ?? [], actualTools),
  ];
  for (const [name, expected] of Object.entries(value.expectedCheckStatuses ?? {})) {
    checks.push(comparison(`diagnostic:${name}`, expected, reportChecks[name] ?? "missing"));
  }
  if (value.expectedFailureCodes !== undefined) {
    checks.push(comparison("failureClassification", value.expectedFailureCodes, failureCodes));
  }
  return Object.freeze({
    caseId: requiredText(value.caseId, "regression case id"),
    title: requiredText(value.title, "regression case title"),
    verdict: checks.every((check) => check.status === "pass") ? "pass" : "fail",
    checks: Object.freeze(checks),
    operationalReport: report,
    failureClassification: classification,
  });
}

export function runRuntimeRegressionSuite(cases: Iterable<AgentRuntimeRegressionCase>) {
  const results = [...cases].map(evaluateRuntimeRegressionCase);
  const passed = results.filter((result) => result.verdict === "pass").length;
  return Object.freeze({
    summary: Object.freeze({ total: results.length, passed, failed: results.length - passed }),
    results: Object.freeze(results),
  });
}

export function getCoreSecurityRedTeamCases(): readonly SecurityRedTeamCase[] {
  const readTool: ToolDefinition = {
    name: "read",
    description: "Read fixture",
    inputSchema: { type: "object", additionalProperties: false },
    policy: { mode: "read", title: "Read" },
    run() { return { content: null, effectState: "not_started" }; },
  };
  return Object.freeze([
    Object.freeze({
      caseId: "RT1-malformed-tool-arguments",
      threat: "Malformed model arguments fail closed instead of becoming an empty object.",
      async check() {
        try {
          await new ToolCatalog([readTool]).executeBatch([
            { id: "malformed", name: "read", arguments: "}{not-json" },
          ], { executionKey: "red-team" });
        } catch { return true; }
        return false;
      },
    }),
    Object.freeze({
      caseId: "RT2-duplicate-tool-call-ids",
      threat: "Duplicate protocol ids reject the entire tool batch.",
      async check() {
        try {
          await new ToolCatalog([readTool]).executeBatch([
            { id: "same", name: "read", arguments: {} },
            { id: "same", name: "read", arguments: {} },
          ], { executionKey: "red-team" });
        } catch { return true; }
        return false;
      },
    }),
    Object.freeze({
      caseId: "RT3-tool-batch-resource-limit",
      threat: "A single model round cannot request an unbounded number of tools.",
      async check() {
        try {
          await new ToolCatalog([readTool]).executeBatch(
            Array.from({ length: 9 }, (_, index) => ({ id: String(index), name: "read", arguments: {} })),
            { executionKey: "red-team" },
          );
        } catch { return true; }
        return false;
      },
    }),
    Object.freeze({
      caseId: "RT4-unsupported-nested-schema",
      threat: "Unsupported nested schema types are rejected during registration.",
      check() {
        try {
          new ToolCatalog([{ ...readTool, inputSchema: {
            type: "object",
            properties: { payload: { type: "unsupported-red-team-type" } },
          } }]);
        } catch { return true; }
        return false;
      },
    }),
    Object.freeze({
      caseId: "RT5-sensitive-error-redaction",
      threat: "Tool exceptions cannot expose secret or filesystem content to the model.",
      async check() {
        const catalog = new ToolCatalog([{ ...readTool, run() {
          throw new Error("LEAK_PRIVATE_PATH LEAK_SECRET_VALUE");
        } }]);
        const result = await catalog.executeBatch([
          { id: "secret", name: "read", arguments: {} },
        ], { executionKey: "red-team" });
        const serialized = JSON.stringify(result);
        return !serialized.includes("LEAK_");
      },
    }),
  ]);
}

export async function runSecurityRedTeamCases(cases: Iterable<SecurityRedTeamCase>) {
  const results: Record<string, unknown>[] = [];
  for (const item of cases) {
    let passed = false;
    let detail = "boundary_missing";
    try {
      passed = Boolean(await item.check());
      detail = passed ? "boundary_enforced" : "boundary_missing";
    } catch (error) {
      detail = error instanceof Error ? error.name : "Error";
    }
    results.push(Object.freeze({
      caseId: requiredText(item.caseId, "security case id"),
      threat: requiredText(item.threat, "security threat"),
      verdict: passed ? "pass" : "fail",
      detail,
    }));
  }
  const passed = results.filter((result) => result.verdict === "pass").length;
  return Object.freeze({
    summary: Object.freeze({ total: results.length, passed, failed: results.length - passed }),
    results: Object.freeze(results),
  });
}

function comparison(name: string, expected: unknown, actual: unknown): Record<string, unknown> {
  return Object.freeze({
    name,
    status: JSON.stringify(expected) === JSON.stringify(actual) ? "pass" : "fail",
    detail: Object.freeze({ expected, actual }),
  });
}

function requiredText(value: unknown, label: string): string {
  const normalized = typeof value === "string" ? value.trim() : "";
  if (normalized === "") throw new TypeError(`${label} must be non-empty text`);
  return normalized;
}
