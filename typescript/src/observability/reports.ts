import {
  buildCanonicalRunObservation,
  evaluateAgentRun,
  generationAttemptTraces,
  mapping,
  nonNegativeInteger,
  optionalNonNegativeInteger,
  sequence,
  text,
} from "./observation.js";
import type { DiagnosticCheck, EvidenceEvent, RunEvidence } from "./types.js";

const TOOL_PROTOCOL_CODES = new Set([
  "invalid_tool_arguments_json", "invalid_tool_arguments_schema", "invalid_tool_arguments_shape",
  "invalid_tool_arguments_type", "invalid_tool_arguments_value", "tool_arguments_too_large",
  "tool_call_truncated",
]);
const INVALID_ARGUMENT_CODES = new Set([
  "invalid_tool_arguments_json", "invalid_tool_arguments_schema", "invalid_tool_arguments_shape",
  "invalid_tool_arguments_type", "invalid_tool_arguments_value",
]);
const FAILED_TOOL_OUTCOMES = new Set(["failed", "rejected", "canceled", "cancelled", "declined"]);
const INTERRUPTED_MODEL_OUTCOMES = new Set(["interrupted", "interrupted_retry", "canceled", "cancelled"]);
const COMPACTION_FAILURE_OUTCOMES = new Set([
  "generation_failed", "generation_failed_reused", "history_mismatch", "repository_unavailable",
]);
const PLANNER_FAILURE_OUTCOMES = new Set([
  "exception", "invalid_plan", "fallback_after_error", "invalid", "contract_violation", "failed",
]);

export const PERFORMANCE_BUDGETS = Object.freeze({
  plannerMs: 15_000,
  modelTotalMs: 60_000,
  toolTotalMs: 15_000,
  modelRounds: 6,
  generationModelRounds: 4,
  responseJudgeRounds: 2,
  toolSchemaTokens: 4_000,
});

export function evaluateAgentRunStability(events: Iterable<EvidenceEvent>) {
  const eventItems = [...events];
  const observation = buildCanonicalRunObservation(eventItems);
  const toolCalls = toolCallObservations(eventItems);
  const errorCodes: Record<string, number> = {};
  const failedTools = [];
  const incompleteTools = [];
  const completedTools = [];
  for (const item of toolCalls.values()) {
    const errorCode = text(item.errorCode).trim();
    if (errorCode !== "") errorCodes[errorCode] = (errorCodes[errorCode] ?? 0) + 1;
    const outcome = text(item.outcome) || "started";
    if (errorCode !== "" || FAILED_TOOL_OUTCOMES.has(outcome)) failedTools.push(item);
    else if (outcome === "started") incompleteTools.push(item);
    else if (outcome === "completed") completedTools.push(item);
  }
  const protocolFailures = Object.entries(errorCodes)
    .filter(([code]) => TOOL_PROTOCOL_CODES.has(code))
    .reduce((total, [, count]) => total + count, 0);
  const compactions = observation.byStage.conversation_compaction ?? [];
  const compactionOutcomes = counts(compactions.map((trace) => text(trace.outcome) || "unknown"));
  const compactedTurns = compactions.reduce(
    (total, trace) => total + nonNegativeInteger(mapping(trace.details).compactedTurnCount),
    0,
  );
  const compactionFailures = Object.entries(compactionOutcomes)
    .filter(([outcome]) => COMPACTION_FAILURE_OUTCOMES.has(outcome))
    .reduce((total, [, count]) => total + count, 0);
  const compactionFallbacks = compactionOutcomes.compacted_fallback ?? 0;
  const contextRows = observation.byStage.context_budget ?? [];
  const contextOverflows = contextRows.filter((trace) => text(trace.outcome).startsWith("overflow")).length;
  const maxDroppedMessages = Math.max(0, ...contextRows.map((trace) => (
    nonNegativeInteger(mapping(trace.details).droppedMessages)
  )));
  const generation = generationAttemptTraces(observation.byStage);
  const interrupted = generation.filter((trace) => INTERRUPTED_MODEL_OUTCOMES.has(text(trace.outcome))).length;
  const retries = (observation.byStage.stream ?? []).filter((trace) => (
    text(trace.outcome).includes("retry") && mapping(trace.details).providerAttemptTerminal !== false
  )).length;
  const checks: DiagnosticCheck[] = [
    diagnostic("toolProtocol", protocolFailures === 0 ? "pass" : "fail", { failureCount: protocolFailures }),
    diagnostic("toolExecution", failedTools.length > 0 || incompleteTools.length > 0 ? "fail" : "pass", {
      failedCalls: failedTools.length,
      incompleteCalls: incompleteTools.length,
      totalCalls: toolCalls.size,
    }),
    diagnostic("contextOverflow", contextOverflows === 0 ? "pass" : "fail", { overflowCount: contextOverflows }),
    diagnostic("contextCompaction", compactionFailures > 0 ? "fail" : compactionFallbacks > 0 ? "warn" : "pass", {
      failureCount: compactionFailures,
      fallbackCount: compactionFallbacks,
    }),
    diagnostic("modelContinuity", interrupted > 0 || retries > 0 ? "warn" : "pass", {
      interruptedAttempts: interrupted,
      retryAttempts: retries,
    }),
  ];
  return Object.freeze({
    verdict: reportVerdict(checks),
    metrics: Object.freeze({
      toolCalls: toolCalls.size,
      completedToolCalls: completedTools.length,
      failedToolCalls: failedTools.length,
      incompleteToolCalls: incompleteTools.length,
      toolSuccessRate: toolCalls.size === 0 ? null : Math.round((completedTools.length / toolCalls.size) * 10_000) / 10_000,
      toolProtocolFailures: protocolFailures,
      toolErrorCodes: Object.freeze(sortedRecord(errorCodes)),
      modelAttempts: generation.length,
      interruptedModelAttempts: interrupted,
      retryAttempts: retries,
      contextOverflows,
      maxDroppedMessages,
      compactionPasses: compactions.length,
      compactedTurns,
      compactionFallbacks,
      compactionFailures,
      compactionOutcomes: Object.freeze(sortedRecord(compactionOutcomes)),
    }),
    checks: Object.freeze(checks),
  });
}

export function classifyAgentRunFailures(run: RunEvidence, events: Iterable<EvidenceEvent>) {
  const items = [...events];
  const operational = evaluateAgentRun(run, items);
  const stability = evaluateAgentRunStability(items);
  const observation = buildCanonicalRunObservation(items);
  const metrics = mapping(stability.metrics);
  const runStatus = text(run.status) === "completed" ? "done" : text(run.status) || "unknown";
  const toolErrorCodes: Record<string, number> = {};
  for (const [code, value] of Object.entries(mapping(metrics.toolErrorCodes))) {
    const count = nonNegativeInteger(value);
    if (code.trim() !== "" && count > 0) toolErrorCodes[code] = count;
  }
  const findings: Record<string, unknown>[] = [];
  const codes = new Set<string>();
  const add = (
    code: string,
    category: string,
    severity: "warn" | "fail",
    confidence: "low" | "medium" | "high",
    evidence: Readonly<Record<string, unknown>>,
    remediation: string,
  ): void => {
    if (codes.has(code)) return;
    codes.add(code);
    findings.push(Object.freeze({ code, category, severity, confidence, evidence: Object.freeze({ ...evidence }), remediation }));
  };
  const invalidCodes = Object.fromEntries(Object.entries(toolErrorCodes).filter(([code]) => INVALID_ARGUMENT_CODES.has(code)));
  if (Object.keys(invalidCodes).length > 0) add(
    "tool.protocol_invalid_arguments", "tool_protocol", "fail", "high",
    { errorCodes: Object.freeze(invalidCodes) }, "inspect_tool_contract",
  );
  if ((toolErrorCodes.tool_arguments_too_large ?? 0) > 0) add(
    "tool.arguments_too_large", "tool_protocol", "fail", "high",
    { failureCount: toolErrorCodes.tool_arguments_too_large }, "inspect_tool_payload_strategy",
  );
  if ((toolErrorCodes.tool_call_truncated ?? 0) > 0) add(
    "model.tool_call_truncated", "model_protocol", "fail", "high",
    { failureCount: toolErrorCodes.tool_call_truncated }, "split_continuation_from_checkpoint",
  );
  const incomplete = nonNegativeInteger(metrics.incompleteToolCalls);
  if (incomplete > 0) add("tool.incomplete_terminalization", "tool_lifecycle", "fail", "high", { incompleteCalls: incomplete }, "inspect_tool_terminalization");
  const overflows = nonNegativeInteger(metrics.contextOverflows);
  if (overflows > 0) add("context.overflow", "context_orchestration", "fail", "high", { overflowCount: overflows }, "inspect_context_budget");
  const compactionFailures = nonNegativeInteger(metrics.compactionFailures);
  if (compactionFailures > 0) add("context.compaction_failed", "context_orchestration", "fail", "high", { failureCount: compactionFailures }, "inspect_context_compaction");
  const compactionFallbacks = nonNegativeInteger(metrics.compactionFallbacks);
  if (compactionFallbacks > 0) add("context.compaction_fallback", "context_orchestration", "warn", "high", { fallbackCount: compactionFallbacks }, "inspect_compaction_summarizer");
  const plannerRows = observation.byStage.planner ?? [];
  const plannerOutcome = text(plannerRows.at(-1)?.outcome) || "unknown";
  if (PLANNER_FAILURE_OUTCOMES.has(plannerOutcome)) add("planner.contract_failure", "planning", "fail", "high", { outcome: plannerOutcome }, "inspect_planner_contract");
  const toolRows = observation.byStage.tool_round ?? [];
  const missing = toolRows.filter((trace) => trace.outcome === "missing_required_call").length;
  if (missing > 0) add("tool.missing_required_call", "tool_governance", "fail", "high", { roundCount: missing }, "inspect_tool_choice_contract");
  const rejected = toolRows.filter((trace) => trace.outcome === "rejected").length;
  if (rejected > 0) add("tool.authorization_rejected", "tool_governance", "fail", "high", { roundCount: rejected }, "inspect_tool_authorization");
  const failedRounds = toolRows.filter((trace) => trace.outcome === "failed").length;
  const failedCalls = nonNegativeInteger(metrics.failedToolCalls);
  const nonProtocolCodes = Object.fromEntries(Object.entries(toolErrorCodes).filter(([code]) => (
    !INVALID_ARGUMENT_CODES.has(code) && code !== "tool_arguments_too_large" && code !== "tool_call_truncated"
  )));
  const explicitProtocolFailures = Object.values(invalidCodes).reduce((total, count) => total + Number(count), 0)
    + (toolErrorCodes.tool_arguments_too_large ?? 0) + (toolErrorCodes.tool_call_truncated ?? 0);
  const uncategorized = Math.max(0, failedCalls - explicitProtocolFailures);
  if (Object.keys(nonProtocolCodes).length > 0 || uncategorized > 0 || (failedRounds > 0 && explicitProtocolFailures === 0)) {
    add("tool.execution_failed", "tool_execution", "fail", Object.keys(nonProtocolCodes).length > 0 ? "high" : "medium", {
      failedCalls: uncategorized,
      failedRounds,
      errorCodes: Object.freeze(nonProtocolCodes),
    }, "inspect_tool_handler");
  }
  const interrupted = nonNegativeInteger(metrics.interruptedModelAttempts);
  if (interrupted > 0) add("model.interrupted", "model_transport", runStatus === "failed" ? "fail" : "warn", "medium", { attemptCount: interrupted }, "inspect_model_transport");
  const retries = nonNegativeInteger(metrics.retryAttempts);
  if (retries > 0 && interrupted === 0) add("model.retry_recovered", "model_transport", "warn", "medium", { attemptCount: retries }, "inspect_model_transport");
  if (runStatus === "failed" && !findings.some((finding) => finding.severity === "fail")) {
    add("run.failed_without_specific_cause", "observability", "fail", "low", {
      runStatus,
      traceCount: nonNegativeInteger(mapping(operational.metrics).traceCount),
    }, "inspect_terminal_error_and_trace_coverage");
  }
  const categoryCounts: Record<string, number> = {};
  for (const finding of findings) {
    const category = text(finding.category);
    categoryCounts[category] = (categoryCounts[category] ?? 0) + 1;
  }
  const primary = findings.find((finding) => finding.severity === "fail") ?? findings[0] ?? null;
  return Object.freeze({
    verdict: findings.some((finding) => finding.severity === "fail") ? "fail" : findings.length > 0 ? "warn" : "pass",
    primaryFinding: primary,
    findings: Object.freeze(findings),
    summary: Object.freeze({ findingCount: findings.length, categoryCounts: Object.freeze(sortedRecord(categoryCounts)) }),
  });
}

export function evaluateAgentRunRecovery(events: Iterable<EvidenceEvent>) {
  const observation = buildCanonicalRunObservation(events);
  const decisions = (observation.byStage.recovery_decision ?? [])
    .map(recoveryDecision)
    .filter((item): item is Readonly<Record<string, unknown>> => item !== undefined);
  const allowed = decisions.filter((item) => item.allowed === true);
  const denied = decisions.filter((item) => item.allowed !== true);
  const deniedReasons = counts(denied.map((item) => text(item.reasonCode)));
  const safetyProtectedCount = ["side_effect_committed", "side_effect_state_unknown", "visible_output_already_emitted"]
    .reduce((total, reason) => total + (deniedReasons[reason] ?? 0), 0);
  return Object.freeze({
    summary: Object.freeze({
      decisionCount: decisions.length,
      allowedCount: allowed.length,
      deniedCount: denied.length,
      safetyProtectedCount,
      causes: Object.freeze(sortedRecord(counts(decisions.map((item) => text(item.cause))))),
      allowedActions: Object.freeze(sortedRecord(counts(allowed.map((item) => text(item.action))))),
      deniedReasons: Object.freeze(sortedRecord(deniedReasons)),
    }),
    decisions: Object.freeze(decisions),
  });
}

export function evaluateAgentRunPerformance(events: Iterable<EvidenceEvent>) {
  const items = [...events];
  const observation = buildCanonicalRunObservation(items);
  const byStage = observation.byStage;
  const plannerMs = sumDurations(byStage.planner ?? []);
  const generation = generationAttemptTraces(byStage);
  const judges = (byStage.model_output ?? []).filter((trace) => text(trace.outcome).startsWith("response_judge_"));
  const generationMs = sumDurations(generation);
  const judgeMs = sumDurations(judges);
  const modelMs = generationMs + judgeMs;
  const toolRows = byStage.tool_round ?? [];
  const approvalWaits = approvalWaitsByToolRound(items, toolRows);
  const recordedToolMs = sumDurations(toolRows);
  const toolMs = toolRows.reduce((total, trace, index) => total + Math.max(0, duration(trace) - approvalWaits[index]!), 0);
  const humanApprovalWaitMs = approvalWaits.reduce((total, value) => total + value, 0);
  const fallbacks = (byStage.tool_choice ?? []).filter((trace) => trace.outcome === "provider_fallback_auto").length;
  const context = observation.contextBudget;
  const schemaTokens = integer(context.toolSchemaTokens);
  const estimatedInput = integer(context.estimatedInputTokens);
  const projectedTotal = integer(context.projectedTotalTokens);
  const windowTokens = integer(context.windowTokens);
  const checks = [
    budgetCheck("plannerLatency", plannerMs, PERFORMANCE_BUDGETS.plannerMs, "ms"),
    budgetCheck("modelLatency", modelMs, PERFORMANCE_BUDGETS.modelTotalMs, "ms"),
    budgetCheck("toolLatency", toolMs, PERFORMANCE_BUDGETS.toolTotalMs, "ms"),
    budgetCheck("modelRounds", generation.length + judges.length, PERFORMANCE_BUDGETS.modelRounds, "rounds"),
    budgetCheck("generationModelRounds", generation.length, PERFORMANCE_BUDGETS.generationModelRounds, "rounds"),
    budgetCheck("responseJudgeRounds", judges.length, PERFORMANCE_BUDGETS.responseJudgeRounds, "rounds"),
    budgetCheck("toolSchemaSize", schemaTokens, PERFORMANCE_BUDGETS.toolSchemaTokens, "tokens"),
    diagnostic("providerCompatibility", fallbacks === 0 ? "pass" : "warn", { fallbackRequests: fallbacks }),
  ];
  return Object.freeze({
    verdict: checks.some((item) => item.status === "warn") ? "warn" : "pass",
    budgets: PERFORMANCE_BUDGETS,
    metrics: Object.freeze({
      plannerMs,
      modelTotalMs: modelMs,
      generationModelTotalMs: generationMs,
      responseJudgeTotalMs: judgeMs,
      toolTotalMs: toolMs,
      humanApprovalWaitMs,
      activeTotalMs: plannerMs + modelMs + toolMs,
      recordedTotalMs: plannerMs + modelMs + recordedToolMs,
      modelRounds: generation.length + judges.length,
      generationModelRounds: generation.length,
      responseJudgeRounds: judges.length,
      toolRounds: toolRows.length,
      compatibilityFallbacks: fallbacks,
      estimatedInputTokens: estimatedInput,
      toolSchemaTokens: schemaTokens,
      projectedTotalTokens: projectedTotal,
      windowTokens,
      headroomTokens: windowTokens === 0 ? 0 : Math.max(0, windowTokens - projectedTotal),
      projectedTokensIncludeResponseJudge: false,
    }),
    checks: Object.freeze(checks),
  });
}

function toolCallObservations(events: readonly EvidenceEvent[]): Map<string, Record<string, unknown>> {
  const observations = new Map<string, Record<string, unknown>>();
  let generated = 0;
  for (const event of events) {
    const eventType = text(event.eventType ?? event.kind);
    const payload = mapping(event.payload);
    if (eventType === "tool.calls_started") {
      for (const call of sequence(payload.calls)) {
        const row = mapping(call);
        const id = text(row.id).trim() || `started:${++generated}`;
        if (!observations.has(id)) observations.set(id, { outcome: "started" });
      }
      continue;
    }
    if (eventType === "tool.started") {
      const id = text(payload.toolCallId).trim() || `started:${++generated}`;
      observations.set(id, { outcome: "started" });
      continue;
    }
    if (eventType === "tool.call_completed" || eventType === "tool.completed") {
      const id = text(payload.toolCallId ?? payload.tool_call_id).trim() || `completed:${++generated}`;
      const current = observations.get(id) ?? {};
      observations.set(id, {
        ...current,
        outcome: text(payload.outcome) || "completed",
        errorCode: text(payload.errorCode ?? payload.error_code ?? current.errorCode),
      });
      continue;
    }
    if (eventType !== "tool.results") continue;
    for (const result of sequence(payload.results)) {
      const row = mapping(result);
      const id = text(row.tool_call_id).trim() || `result:${++generated}`;
      const current = observations.get(id) ?? {};
      const errorCode = text(row.error).trim();
      observations.set(id, {
        ...current,
        outcome: current.outcome !== undefined && current.outcome !== "started"
          ? current.outcome : errorCode === "" ? "completed" : "failed",
        errorCode: errorCode || text(current.errorCode),
      });
    }
  }
  return observations;
}

function recoveryDecision(trace: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> | undefined {
  const details = mapping(trace.details);
  const causes = new Set([
    "provider_required_tool_choice_unsupported", "provider_stream_interrupted", "malformed_tool_call_batch",
    "missing_required_tool_call", "missing_required_tool_call_replan", "unstructured_tool_protocol",
    "empty_model_response", "response_constraint_deterministic", "response_constraint_semantic",
    "future_tool_step", "unauthorized_tool", "unauthorized_tool_replan", "tool_input_invalid",
    "tool_execution_failed_replan",
  ]);
  const actions = new Set(["retry_model", "fallback_provider_mode", "replan"]);
  const reasons = new Set([
    "allowed", "attempt_budget_exhausted", "cause_not_retryable", "policy_disabled", "request_canceled",
    "round_budget_exhausted", "side_effect_committed", "side_effect_state_unknown", "visible_output_already_emitted",
  ]);
  const effects = new Set(["not_started", "committed", "unknown"]);
  const cause = text(details.cause);
  const action = text(details.action);
  const reasonCode = text(details.reasonCode);
  const effectState = text(details.effectState);
  const allowed = details.allowed === true;
  if (!causes.has(cause) || !actions.has(action) || !reasons.has(reasonCode) || !effects.has(effectState)) return undefined;
  if (allowed !== (reasonCode === "allowed")) return undefined;
  return Object.freeze({
    round: nonNegativeInteger(details.round),
    cause,
    action,
    allowed,
    reasonCode,
    attempt: nonNegativeInteger(details.attempt),
    maxAttempts: nonNegativeInteger(details.maxAttempts),
    remainingModelRounds: nonNegativeInteger(details.remainingModelRounds),
    effectState,
    mayRepeatSideEffect: details.mayRepeatSideEffect === true,
  });
}

function approvalWaitsByToolRound(
  events: readonly EvidenceEvent[],
  toolRows: readonly Readonly<Record<string, unknown>>[],
): readonly number[] {
  const statusesByRound: string[][] = [];
  let pending: string[] = [];
  for (const event of events) {
    const eventType = text(event.eventType ?? event.kind);
    const payload = mapping(event.payload);
    if (eventType === "approval.resolved") {
      const status = text(payload.status).trim().toLowerCase();
      if (status !== "") pending.push(status);
      continue;
    }
    if (eventType === "agentRunTrace" && payload.stage === "tool_round") {
      statusesByRound.push(pending);
      pending = [];
    }
  }
  return Object.freeze(toolRows.map((trace, index) => {
    const durationMs = duration(trace);
    const details = mapping(trace.details);
    const exact = optionalNonNegativeInteger(details.approvalWaitMs ?? details.approval_wait_ms);
    if (exact !== undefined) return Math.min(durationMs, exact);
    const rawStatuses = details.approvalStatuses ?? details.approval_statuses;
    const statuses = (Array.isArray(rawStatuses) ? rawStatuses.map((item) => text(item).toLowerCase()) : [])
      .filter((item) => item !== "");
    const fallback = statuses.length > 0 ? statuses : statusesByRound[index] ?? [];
    const outcome = text(trace.outcome).toLowerCase();
    const nonExecuting = fallback.length > 0
      && !fallback.includes("approved")
      && fallback.every((status) => ["declined", "rejected", "timed_out", "canceled", "cancelled", "unavailable", "error"].includes(status));
    return nonExecuting || ["declined", "approval_rejected", "approval_timed_out", "approval_canceled", "approval_cancelled", "approval_unavailable", "approval_error"].includes(outcome)
      ? durationMs : 0;
  }));
}

function budgetCheck(name: string, actual: number, budget: number, unit: string): DiagnosticCheck {
  return diagnostic(name, actual <= budget ? "pass" : "warn", { actual, budget, unit });
}

function duration(trace: Readonly<Record<string, unknown>>): number {
  return Math.max(0, integer(trace.durationMs));
}

function sumDurations(rows: readonly Readonly<Record<string, unknown>>[]): number {
  return rows.reduce((total, trace) => total + duration(trace), 0);
}

function integer(value: unknown): number {
  const parsed = Number.parseInt(String(value ?? 0), 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function counts(values: readonly string[]): Record<string, number> {
  const result: Record<string, number> = {};
  for (const value of values) result[value] = (result[value] ?? 0) + 1;
  return result;
}

function sortedRecord<T>(value: Readonly<Record<string, T>>): Record<string, T> {
  return Object.fromEntries(Object.entries(value).sort(([left], [right]) => left.localeCompare(right)));
}

function diagnostic(name: string, status: DiagnosticCheck["status"], detail: unknown): DiagnosticCheck {
  return Object.freeze({ name, status, detail });
}

function reportVerdict(checks: readonly DiagnosticCheck[]): "pass" | "warn" | "fail" {
  return checks.some((item) => item.status === "fail")
    ? "fail" : checks.some((item) => item.status === "warn") ? "warn" : "pass";
}
