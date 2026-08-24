import type { JsonValue, Message, ToolSpec } from "../model/types.js";
import { AgentError } from "../shared/errors.js";
import type { ContextBudget, ContextBudgetClaim, ContextReserves } from "./types.js";

export function estimateTextTokens(value: unknown): number {
  return estimateUnits(String(value ?? ""), 4);
}

export function estimateJsonTokens(value: unknown): number {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new TypeError("Value must be JSON serializable");
  return estimateUnits(encoded, 2);
}

export function estimateToolSchemaTokens(tools: readonly ToolSpec[]): number {
  if (tools.length === 0) return 0;
  const rows = tools.map((tool) => ({
    tool_protocol_type: "function",
    function_descriptor: {
      tool_function_name: tool.name,
      tool_description: tool.description,
      tool_parameters_schema: tool.inputSchema,
    },
  }));
  return estimateJsonTokens(rows) + 8 * rows.length;
}

export function estimateMessagesTokens(messages: readonly Message[]): number {
  return 2 + messages.reduce(
    (total, message) => total + estimateJsonTokens(messageForBudget(message)) + 4,
    0,
  );
}

export function allocateContextBudget(input: {
  readonly windowTokens: number;
  readonly outputReserveTokens: number;
  readonly tools?: readonly ToolSpec[];
  readonly claims?: readonly ContextBudgetClaim[];
  readonly reserves?: ContextReserves;
}): ContextBudget {
  const windowTokens = positiveInteger(input.windowTokens, "context window");
  const outputReserveTokens = positiveInteger(input.outputReserveTokens, "output reserve");
  const tools = input.tools ?? [];
  const toolSchemaTokens = estimateToolSchemaTokens(tools);
  const safetyReserveTokens = reserve(
    input.reserves?.safetyTokens,
    Math.min(64_000, Math.max(4_096, Math.ceil(windowTokens * 0.05))),
    "safety reserve",
  );
  const runtimeReserveTokens = reserve(
    input.reserves?.runtimeTokens,
    tools.length === 0
      ? Math.min(16_000, Math.max(2_048, Math.floor(windowTokens / 25)))
      : Math.min(64_000, Math.max(4_096, Math.floor(windowTokens / 10))),
    "runtime reserve",
  );
  const minimumMessageTokens = reserve(
    input.reserves?.minimumMessageTokens,
    Math.min(8_192, Math.max(1_024, Math.floor(windowTokens / 100))),
    "minimum message reserve",
  );
  const providerInputTokens = windowTokens
    - outputReserveTokens
    - safetyReserveTokens
    - runtimeReserveTokens
    - toolSchemaTokens;
  if (providerInputTokens < minimumMessageTokens) {
    throw new AgentError(
      "fixed_reserves_exceed_window",
      "Fixed context reserves leave no message budget",
    );
  }
  const claims = normalizeClaims(input.claims ?? []);
  const contextAllocations = allocateClaims(
    claims,
    Math.max(0, providerInputTokens - minimumMessageTokens),
  );
  return Object.freeze({
    windowTokens,
    outputReserveTokens,
    safetyReserveTokens,
    runtimeReserveTokens,
    toolSchemaTokens,
    providerInputTokens,
    minimumMessageTokens,
    contextAllocations: Object.freeze(contextAllocations),
  });
}

export function normalizeClaims(values: readonly ContextBudgetClaim[]): readonly Required<ContextBudgetClaim>[] {
  if (!Array.isArray(values)) throw new TypeError("Context claims must be an array");
  const names = new Set<string>();
  return Object.freeze(values.map((value) => {
    if (value === null || typeof value !== "object") throw new TypeError("Invalid context claim");
    const name = requiredText(value.name, "context claim name");
    if (names.has(name)) throw new TypeError(`Duplicate context claim: ${name}`);
    names.add(name);
    const desiredTokens = nonNegativeInteger(value.desiredTokens, "desired context tokens");
    const minimumTokens = nonNegativeInteger(value.minimumTokens ?? 0, "minimum context tokens");
    const maximumTokens = nonNegativeInteger(
      value.maximumTokens ?? desiredTokens,
      "maximum context tokens",
    );
    const priority = integer(value.priority ?? 0, "context claim priority");
    if (minimumTokens > desiredTokens || desiredTokens > maximumTokens) {
      throw new TypeError("Context claim requires minimum <= desired <= maximum");
    }
    return Object.freeze({ name, desiredTokens, minimumTokens, maximumTokens, priority });
  }));
}

export function trimMessagesByTurn(
  messages: readonly Message[],
  tokenBudget: number,
  maxRecentMessages = 20,
): { readonly messages: readonly Message[]; readonly overflowTokens: number } {
  const protectedRows: Array<readonly [number, Message]> = [];
  const turns: Array<Array<readonly [number, Message]>> = [];
  let current: Array<readonly [number, Message]> = [];
  messages.forEach((message, index) => {
    if (message.role === "system" || message.role === "developer") {
      if (current.length > 0) turns.push(current);
      current = [];
      protectedRows.push([index, message]);
      return;
    }
    if (message.role === "user" && current.length > 0) {
      turns.push(current);
      current = [];
    }
    current.push([index, message]);
  });
  if (current.length > 0) turns.push(current);

  const selected = new Set(protectedRows.map(([index]) => index));
  let selectedCount = 0;
  let used = estimateMessagesTokens(protectedRows.map(([, message]) => message));
  const recentTurns = [...turns].reverse();
  for (let reverseIndex = 0; reverseIndex < recentTurns.length; reverseIndex += 1) {
    const turn = recentTurns[reverseIndex]!;
    const turnMessages = turn.map(([, message]) => message);
    const turnCost = estimateMessagesTokens(turnMessages) - 2;
    const required = reverseIndex === 0;
    if (
      required
      || (selectedCount + turn.length <= maxRecentMessages && used + turnCost <= tokenBudget)
    ) {
      turn.forEach(([index]) => selected.add(index));
      selectedCount += turn.length;
      used += turnCost;
      continue;
    }
    break;
  }
  const result = Object.freeze(messages.filter((_message, index) => selected.has(index)));
  return Object.freeze({
    messages: result,
    overflowTokens: Math.max(0, estimateMessagesTokens(result) - tokenBudget),
  });
}

function allocateClaims(
  claims: readonly Required<ContextBudgetClaim>[],
  availableTokens: number,
): Record<string, number> {
  const minimumTotal = claims.reduce((total, claim) => total + claim.minimumTokens, 0);
  if (minimumTotal > availableTokens) {
    throw new AgentError(
      "minimum_context_demand_exceeds_pool",
      "Minimum context demand exceeds the provider input pool",
    );
  }
  const allocations = Object.fromEntries(claims.map((claim) => [claim.name, claim.minimumTokens]));
  let remaining = availableTokens - minimumTotal;
  const priorities = [...new Set(claims.map((claim) => claim.priority))].sort((a, b) => b - a);
  for (const priority of priorities) {
    const group = claims.filter((claim) => claim.priority === priority);
    const needs = group.map((claim) => claim.desiredTokens - allocations[claim.name]!);
    const total = needs.reduce((sum, need) => sum + need, 0);
    if (total <= remaining) {
      group.forEach((claim, index) => { allocations[claim.name]! += needs[index]!; });
      remaining -= total;
      continue;
    }
    proportionalShares(needs, remaining).forEach((share, index) => {
      allocations[group[index]!.name]! += share;
    });
    break;
  }
  return allocations;
}

function proportionalShares(weights: readonly number[], available: number): number[] {
  const total = weights.reduce((sum, weight) => sum + weight, 0);
  if (total <= 0 || available <= 0) return weights.map(() => 0);
  const products = weights.map((weight) => weight * available);
  const shares = products.map((product) => Math.floor(product / total));
  let remaining = available - shares.reduce((sum, share) => sum + share, 0);
  const order = products
    .map((product, index) => ({ index, remainder: product % total }))
    .sort((left, right) => right.remainder - left.remainder || left.index - right.index);
  for (const row of order) {
    if (remaining === 0) break;
    shares[row.index]! += 1;
    remaining -= 1;
  }
  return shares;
}

function messageForBudget(message: Message): Readonly<Record<string, JsonValue>> {
  return {
    ...(message.attributes ?? {}),
    role: message.role,
    content: message.content,
    ...(message.reasoning === undefined ? {} : { structured_reasoning_content: message.reasoning }),
    ...(message.toolCalls === undefined
      ? {}
      : {
          structured_tool_calls: message.toolCalls.map((call) => ({
            tool_call_identifier: call.id,
            tool_protocol_type: "function",
            function_descriptor: {
              tool_function_name: call.name,
              serialized_arguments_json: JSON.stringify(call.arguments),
            },
          })),
        }),
    ...(message.toolCallId === undefined ? {} : { tool_call_identifier: message.toolCallId }),
  };
}

function estimateUnits(value: string, asciiDivisor: number): number {
  let ascii = 0;
  let nonAscii = 0;
  for (const character of value) {
    if (character.codePointAt(0)! < 128) ascii += 1;
    else nonAscii += 1;
  }
  return nonAscii + Math.ceil(ascii / asciiDivisor);
}

function reserve(value: number | undefined, fallback: number, label: string): number {
  return value === undefined ? fallback : nonNegativeInteger(value, label);
}

function requiredText(value: unknown, label: string): string {
  const result = typeof value === "string" ? value.trim() : "";
  if (result === "") throw new TypeError(`${label} must be non-empty text`);
  return result;
}

function positiveInteger(value: unknown, label: string): number {
  const result = nonNegativeInteger(value, label);
  if (result === 0) throw new TypeError(`${label} must be positive`);
  return result;
}

function nonNegativeInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new TypeError(`${label} must be a non-negative integer`);
  }
  return Number(value);
}

function integer(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value)) throw new TypeError(`${label} must be an integer`);
  return Number(value);
}
