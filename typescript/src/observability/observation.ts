import type {
  CanonicalRunObservation,
  DiagnosticCheck,
  EvidenceEvent,
  RunEvidence,
} from "./types.js";

export const TRACE_EVENT_TYPE = "agentRunTrace";

const STAGE_NAMES: Readonly<Record<string, string>> = Object.freeze({ planning: "planner" });
const TERMINAL_OUTCOMES: Readonly<Record<string, string>> = Object.freeze({
  "run.completed": "done",
  "run.blocked": "blocked",
  "run.failed": "failed",
  "run.canceled": "canceled",
});
const PLANNER_PASS = new Set(["host_plan", "model_plan", "skipped", "planned", "direct_response", "replanned"]);
const PLANNER_FAIL = new Set(["exception", "invalid_plan", "fallback_after_error", "invalid", "contract_violation", "failed"]);
const TRACE_KEYS = new Set(["stage", "outcome", "durationMs", "round"]);
const DETAIL_KEYS = new Set([
  "action", "allowed", "approvalStatus", "approvalStatuses", "approvalWaitMs",
  "approval_status", "approval_statuses", "approval_wait_ms", "attempt", "cause",
  "compactedTurnCount", "droppedMessages", "effectState", "estimatedInputTokens",
  "maxAttempts", "mayRepeatSideEffect", "projectedTotalTokens", "providerAttemptTerminal",
  "reasonCode", "remainingModelRounds", "requestedTools", "round", "toolSchemaTokens", "windowTokens",
]);
const CONTEXT_KEYS = new Set([
  "droppedMessages", "estimatedInputTokens", "projectedTotalTokens", "toolSchemaTokens", "windowTokens",
]);

export function buildCanonicalRunObservation(events: Iterable<EvidenceEvent>): CanonicalRunObservation {
  const traces: Readonly<Record<string, unknown>>[] = [];
  const byStage = new Map<string, Readonly<Record<string, unknown>>[]>();
  let coreContext: Readonly<Record<string, unknown>> = Object.freeze({});
  for (const event of events) {
    const eventType = text(event.eventType ?? event.kind);
    const payload = mapping(event.payload);
    if (eventType === TRACE_EVENT_TYPE) {
      const trace = sanitizeTrace(payload);
      traces.push(trace);
      const rawStage = text(trace.stage) || "unknown";
      const stage = STAGE_NAMES[rawStage] ?? rawStage;
      const rows = byStage.get(stage) ?? [];
      rows.push(trace);
      byStage.set(stage, rows);
      continue;
    }
    if (eventType === "context.budgeted") {
      coreContext = pickScalars(payload, CONTEXT_KEYS);
      continue;
    }
    const terminal = TERMINAL_OUTCOMES[eventType];
    if (terminal !== undefined) {
      const rows = byStage.get("terminal") ?? [];
      rows.push(Object.freeze({ stage: "terminal", outcome: terminal, sourceEventType: eventType }));
      byStage.set("terminal", rows);
    }
  }
  const traceContext = [...traces].reverse().find((trace) => trace.stage === "context_budget");
  const traceDetails = mapping(traceContext?.details);
  const contextBudget = Object.freeze({ ...traceDetails, ...coreContext });
  const normalizedStages: Record<string, readonly Readonly<Record<string, unknown>>[]> = {};
  for (const [stage, rows] of byStage) normalizedStages[stage] = Object.freeze([...rows]);
  return Object.freeze({
    traces: Object.freeze(traces),
    byStage: Object.freeze(normalizedStages),
    contextBudget,
    contextBudgetSource: Object.keys(coreContext).length > 0
      ? "core_event"
      : Object.keys(traceDetails).length > 0 ? "trace" : "none",
  });
}

export function generationAttemptTraces(
  byStage: CanonicalRunObservation["byStage"],
): readonly Readonly<Record<string, unknown>>[] {
  const interrupted = (byStage.stream ?? []).filter((trace) => (
    trace.outcome === "interrupted" || mapping(trace.details).providerAttemptTerminal === true
  ));
  return Object.freeze([...(byStage.model_round ?? []), ...interrupted]);
}

export function evaluateAgentRun(run: RunEvidence, events: Iterable<EvidenceEvent>) {
  const observation = buildCanonicalRunObservation(events);
  const byStage = observation.byStage;
  const status = normalizeRunStatus(run.status);
  const plannerRows = byStage.planner ?? [];
  const plannerOutcome = text(plannerRows.at(-1)?.outcome) || "unknown";
  const contextRows = byStage.context_budget ?? [];
  const contextOutcome = text(contextRows.at(-1)?.outcome) || "unknown";
  const toolRows = byStage.tool_round ?? [];
  const rejected = toolRows.filter((trace) => trace.outcome === "rejected").length;
  const missing = toolRows.filter((trace) => trace.outcome === "missing_required_call").length;
  const failed = toolRows.filter((trace) => trace.outcome === "failed").length;
  const checks: DiagnosticCheck[] = [
    check("terminalState", ["done", "blocked", "failed", "canceled"].includes(status) ? "pass" : "fail", status),
    check("taskOutcome", status === "done" ? "pass" : status === "failed" ? "fail" : "warn", status),
    check(
      "traceCoverage",
      byStage.planner !== undefined && byStage.terminal !== undefined ? "pass" : "warn",
      Object.keys(byStage).sort(),
    ),
    check("plannerHealth", PLANNER_PASS.has(plannerOutcome) ? "pass" : PLANNER_FAIL.has(plannerOutcome) ? "fail" : "warn", plannerOutcome),
    check("contextSafety", contextOutcome === "within_budget" ? "pass" : contextOutcome.startsWith("overflow") ? "fail" : "warn", contextOutcome),
    check("toolGovernance", rejected === 0 && missing === 0 ? "pass" : "fail", { rejectedRounds: rejected, missingRequiredCalls: missing }),
    check("toolReliability", failed === 0 ? "pass" : "fail", { failedRounds: failed }),
  ];
  return Object.freeze({
    runId: run.id ?? run.runId,
    runStatus: status,
    verdict: verdict(checks),
    checks: Object.freeze(checks),
    metrics: Object.freeze({
      traceCount: observation.traces.length,
      modelRounds: generationAttemptTraces(byStage).length,
      toolRounds: toolRows.length,
      rejectedToolRounds: rejected,
      missingRequiredToolCalls: missing,
      failedToolRounds: failed,
    }),
    traces: observation.traces,
  });
}

export function mapping(value: unknown): Readonly<Record<string, unknown>> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Readonly<Record<string, unknown>>
    : Object.freeze({});
}

export function sequence(value: unknown): readonly unknown[] {
  return Array.isArray(value) ? value : Object.freeze([]);
}

export function nonNegativeInteger(value: unknown): number {
  if (value === null || value === undefined || typeof value === "boolean") return 0;
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
}

export function optionalNonNegativeInteger(value: unknown): number | undefined {
  if (value === null || value === undefined || typeof value === "boolean") return undefined;
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined;
}

export function text(value: unknown): string {
  return typeof value === "string" ? value : value === null || value === undefined ? "" : String(value);
}

export function roundRate(numerator: number, denominator: number): number | null {
  return denominator === 0 ? null : Math.round((numerator / denominator) * 10_000) / 10_000;
}

function sanitizeTrace(value: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> {
  const trace = pickScalars(value, TRACE_KEYS);
  const details = mapping(value.details);
  const safeDetails: Record<string, unknown> = {};
  for (const key of DETAIL_KEYS) {
    const item = details[key];
    if (typeof item === "string" || typeof item === "number" || typeof item === "boolean") safeDetails[key] = item;
    if (Array.isArray(item) && item.every((entry) => typeof entry === "string")) safeDetails[key] = Object.freeze([...item]);
  }
  return Object.freeze({
    ...trace,
    ...(Object.keys(safeDetails).length === 0 ? {} : { details: Object.freeze(safeDetails) }),
  });
}

function pickScalars(
  value: Readonly<Record<string, unknown>>,
  keys: ReadonlySet<string>,
): Readonly<Record<string, unknown>> {
  const result: Record<string, unknown> = {};
  for (const key of keys) {
    const item = value[key];
    if (typeof item === "string" || typeof item === "number" || typeof item === "boolean") result[key] = item;
  }
  return Object.freeze(result);
}

function normalizeRunStatus(value: unknown): string {
  const status = text(value) || "unknown";
  return status === "completed" ? "done" : status;
}

function check(name: string, status: DiagnosticCheck["status"], detail: unknown): DiagnosticCheck {
  return Object.freeze({ name, status, detail });
}

function verdict(checks: readonly DiagnosticCheck[]): "pass" | "warn" | "fail" {
  return checks.some((item) => item.status === "fail")
    ? "fail"
    : checks.some((item) => item.status === "warn") ? "warn" : "pass";
}
