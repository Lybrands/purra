import type { JsonValue, ToolCall, ToolSpec } from "../model/types.js";
import type { PlanningMode } from "../run/types.js";
import { AgentError } from "../shared/errors.js";

export const AUTO_PLANNING_TOOL_NAME = "request_plan";
export const AUTO_REMAINING_PLANNING_TOOL_NAME = "request_remaining_plan";

export const AUTO_PLANNING_TOOL_SPEC: ToolSpec = Object.freeze({
  name: AUTO_PLANNING_TOOL_NAME,
  description: "Request a governed execution plan before any business tool runs. Use only when the task needs multiple dependent actions or must be validated for authority, budget, approval, or durable execution. Do not use for direct answers, one-step work, or ordinary read-only tool use.",
  displayNames: Object.freeze({ en: "Plan execution", "zh-CN": "制定执行计划" }),
  inputSchema: Object.freeze({
    type: "object",
    properties: Object.freeze({}),
    additionalProperties: false,
  }),
});
export const AUTO_REMAINING_PLANNING_TOOL_SPEC: ToolSpec = Object.freeze({
  name: AUTO_REMAINING_PLANNING_TOOL_NAME,
  description: "Request a governed plan for remaining work after earlier business tool results changed what must happen next. Do not repeat completed work.",
  displayNames: Object.freeze({ en: "Plan remaining work", "zh-CN": "规划剩余工作" }),
  inputSchema: Object.freeze({
    type: "object",
    properties: Object.freeze({}),
    additionalProperties: false,
  }),
});

export interface AutoPlanningRequest {
  readonly type: "planning_requested";
  readonly trigger: "model_requested" | "tool_required" | "remaining_model_requested" | "remaining_tool_required";
  readonly requestedToolNames: readonly string[];
  readonly rounds: number;
}

export function resolvePlanningActivation(input: {
  readonly calls: readonly ToolCall[];
  readonly mode: PlanningMode;
  readonly planningAvailable: boolean;
  readonly planningRequiredToolNames: ReadonlySet<string>;
  readonly round: number;
  readonly initialPlanningOpen?: boolean;
}): AutoPlanningRequest | undefined {
  if (input.calls.length === 0) return undefined;
  const initialControlCalls = input.calls.filter((call) => call.name === AUTO_PLANNING_TOOL_NAME);
  const remainingControlCalls = input.calls.filter((call) => call.name === AUTO_REMAINING_PLANNING_TOOL_NAME);
  const controlCalls = [...initialControlCalls, ...remainingControlCalls];
  const requiredNames = [...new Set(input.calls
    .map((call) => call.name)
    .filter((name) => input.planningRequiredToolNames.has(name)))].sort();
  if (input.mode === "planned") {
    if (controlCalls.length > 0) invalidControl();
    return undefined;
  }
  if (input.mode === "reactive") {
    if (controlCalls.length > 0 || requiredNames.length > 0) {
      throw new AgentError("planning_required", "This tool call requires governed planning");
    }
    return undefined;
  }
  if (controlCalls.length === 0 && requiredNames.length === 0) return undefined;
  if (!input.planningAvailable) {
    throw new AgentError("planning_unavailable", "Auto planning requires a configured Planner");
  }
  if (
    controlCalls.length > 0
    && (
      controlCalls.length !== 1
      || input.calls.length !== 1
      || !emptyObject(controlCalls[0]!.arguments)
    )
  ) invalidControl();
  const initialPlanningOpen = input.initialPlanningOpen !== false;
  if (
    (initialControlCalls.length > 0 && !initialPlanningOpen)
    || (remainingControlCalls.length > 0 && initialPlanningOpen)
  ) invalidControl();
  return Object.freeze({
    type: "planning_requested",
    trigger: initialControlCalls.length === 1
      ? "model_requested"
      : remainingControlCalls.length === 1
        ? "remaining_model_requested"
        : initialPlanningOpen
          ? "tool_required"
          : "remaining_tool_required",
    requestedToolNames: Object.freeze(requiredNames),
    rounds: input.round,
  });
}

function emptyObject(value: JsonValue): boolean {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    && Object.keys(value).length === 0;
}

function invalidControl(): never {
  throw new AgentError(
    "invalid_planning_control_call",
    "Planner activation must be the only tool call and must use an empty object",
  );
}
