import { AgentError } from "../shared/errors.js";
import type {
  ContinuationKind,
  ContinuationSafety,
  GenerationBudgetSource,
  InvocationOutputBudget,
  JsonValue,
  Message,
  ModelCapabilitySnapshot,
  ModelFinishReason,
  ModelStreamChunk,
  ModelTokenUsage,
  ModelTurn,
  ResultCapacitySource,
  ToolCallDelta,
} from "./types.js";

const MESSAGE_ROLES = ["system", "developer", "user", "assistant", "tool"] as const;
const FINISH_REASONS = ["stop", "tool_calls", "length", "filtered", "other"] as const;
const FEATURE_SUPPORT = ["supported", "unavailable", "unknown"] as const;
const REASONING_CONTROL = ["selectable", "always_enabled", "unavailable"] as const;
const REASONING_REPLAY = ["required", "forbidden", "ignored"] as const;
const THINKING_TOKEN_ACCOUNTING = ["included", "separate", "unknown"] as const;
const REASONING_USAGE_DETAIL = ["required", "optional", "unavailable"] as const;
const REASONING_LIMIT_KIND = ["none", "soft", "hard"] as const;
const VISIBLE_OUTPUT_RESERVATION = ["supported", "unavailable", "unknown"] as const;
const LENGTH_REASON_DETAIL = ["request_cap", "context_cap", "conflated"] as const;
const CONTINUATION_KIND = ["none", "prefix_beta", "opaque_state", "signed_replay"] as const;
const CONTINUATION_SAFETY = ["text", "structured", "tool_call"] as const;
const RESULT_CAPACITY_SOURCE = ["user", "workflow_policy"] as const;
const ASSISTANT_CONTENT_WITH_TOOL_CALLS = ["required", "optional", "forbidden"] as const;

export function copyMessages(messages: readonly Message[]): Message[] {
  if (!Array.isArray(messages)) throw new TypeError("Agent messages must be an array");
  return messages.map(copyMessage);
}

export function validateModelTurn(turn: ModelTurn): ModelTurn {
  if (!isObject(turn)) {
    throw new AgentError("invalid_model_response", "Model gateway returned an invalid turn");
  }
  if (!includes(FINISH_REASONS, turn.finishReason)) {
    throw new AgentError("invalid_model_response", "Model gateway returned an invalid finish reason");
  }
  let message: Message;
  let usage: ModelTokenUsage | undefined;
  let appliedGenerationLimit: number;
  try {
    if (Object.prototype.hasOwnProperty.call(turn, "appliedOutputLimit")) {
      throw new TypeError("Legacy applied output limits are unsupported");
    }
    message = copyMessage(turn.message);
    usage = turn.usage === undefined ? undefined : copyTokenUsage(turn.usage);
    appliedGenerationLimit = positiveInteger(
      turn.appliedGenerationLimit,
      "applied generation limit",
    );
  } catch (error) {
    throw new AgentError("invalid_model_response", "Model gateway returned invalid data", {
      cause: error,
    });
  }
  if (message.role !== "assistant") {
    throw new AgentError("invalid_model_response", "Model gateway must return an assistant message");
  }
  return Object.freeze({
    message,
    finishReason: turn.finishReason,
    appliedGenerationLimit,
    ...(usage === undefined ? {} : { usage }),
  });
}

export function validateModelStreamChunk(chunk: ModelStreamChunk): ModelStreamChunk {
  if (!isObject(chunk)) {
    throw new AgentError("invalid_model_response", "Model gateway returned an invalid stream chunk");
  }
  try {
    const contentDelta = optionalString(chunk.contentDelta, "content delta");
    const reasoningDelta = optionalString(chunk.reasoningDelta, "reasoning delta");
    const progressDelta = optionalString(chunk.progressDelta, "progress delta");
    const finishReason = chunk.finishReason;
    const providerData = chunk.providerData === undefined
      ? undefined : copyJsonMapping(chunk.providerData as Readonly<Record<string, JsonValue>>, "provider data");
    if (providerData !== undefined && (finishReason === undefined || JSON.stringify(providerData).length > 1_000_000)) {
      throw new TypeError("Provider data requires a terminal chunk and must fit the size limit");
    }
    if (finishReason !== undefined && !includes(FINISH_REASONS, finishReason)) {
      throw new TypeError("Invalid finish reason");
    }
    if (chunk.toolCallDeltas !== undefined && !Array.isArray(chunk.toolCallDeltas)) {
      throw new TypeError("Tool call deltas must be an array");
    }
    const toolCallDeltas = chunk.toolCallDeltas?.map(copyToolCallDelta);
    const usage = chunk.usage === undefined
      ? undefined
      : copyTokenUsage(chunk.usage as ModelTokenUsage);
    return Object.freeze({
      ...(contentDelta === undefined ? {} : { contentDelta }),
      ...(reasoningDelta === undefined ? {} : { reasoningDelta }),
      ...(progressDelta === undefined ? {} : { progressDelta }),
      ...(toolCallDeltas === undefined
        ? {}
        : { toolCallDeltas: Object.freeze(toolCallDeltas) }),
      ...(finishReason === undefined ? {} : { finishReason }),
      ...(usage === undefined ? {} : { usage }),
      ...(providerData === undefined ? {} : { providerData }),
    });
  } catch (error) {
    if (error instanceof AgentError) throw error;
    throw new AgentError("invalid_model_response", "Model gateway returned an invalid stream chunk", {
      cause: error,
    });
  }
}

export function copyCapabilitySnapshot(value: ModelCapabilitySnapshot): ModelCapabilitySnapshot {
  if (!isObject(value) || !isObject(value.protocol)) {
    throw new TypeError("Model capabilities must be an object");
  }
  if (value.schemaVersion !== 2) {
    throw new TypeError("Unsupported model capability schema version");
  }
  const protocol = value.protocol;
  const maxGenerationTokens = value.maxGenerationTokens === null
    ? null
    : positiveInteger(
      value.maxGenerationTokens,
      "model max tokens per invocation",
    );
  const source = optionalText(value.source);
  const continuationKind = enumValue(
    CONTINUATION_KIND,
    value.continuationKind ?? "none",
    "continuation kind",
  );
  const continuationSafeFor = copyEnumArray(
    CONTINUATION_SAFETY,
    value.continuationSafeFor ?? [],
    "continuation safety",
  );
  if (continuationKind === "none" && continuationSafeFor.length > 0) {
    throw new TypeError("Continuation safety requires a continuation kind");
  }
  if (value.actionable !== undefined && typeof value.actionable !== "boolean") {
    throw new TypeError("Model capability actionable flag must be a boolean");
  }
  return Object.freeze({
    schemaVersion: 2,
    profileId: requiredText(value.profileId, "capability profile id"),
    providerProtocol: requiredText(value.providerProtocol, "provider protocol"),
    contextWindowTokens: positiveInteger(value.contextWindowTokens, "context window tokens"),
    maxGenerationTokens,
    thinkingTokenAccounting: enumValue(
      THINKING_TOKEN_ACCOUNTING,
      value.thinkingTokenAccounting,
      "thinking token accounting",
    ),
    reasoningUsageDetail: enumValue(
      REASONING_USAGE_DETAIL,
      value.reasoningUsageDetail ?? "optional",
      "reasoning usage detail",
    ),
    reasoningLimitKind: enumValue(
      REASONING_LIMIT_KIND,
      value.reasoningLimitKind ?? "none",
      "reasoning limit kind",
    ),
    visibleOutputReservation: enumValue(
      VISIBLE_OUTPUT_RESERVATION,
      value.visibleOutputReservation ?? "unknown",
      "visible output reservation",
    ),
    lengthReasonDetail: enumValue(
      LENGTH_REASON_DETAIL,
      value.lengthReasonDetail ?? "conflated",
      "length reason detail",
    ),
    continuationKind,
    continuationSafeFor,
    protocol: Object.freeze({
      reasoningControl: enumValue(
        REASONING_CONTROL,
        protocol.reasoningControl,
        "reasoning control",
      ),
      reasoningReplay: enumValue(
        REASONING_REPLAY,
        protocol.reasoningReplay,
        "reasoning replay",
      ),
      toolCalling: enumValue(FEATURE_SUPPORT, protocol.toolCalling, "tool calling support"),
      requiredToolChoice: enumValue(
        FEATURE_SUPPORT,
        protocol.requiredToolChoice,
        "required tool choice support",
      ),
      parallelToolCalls: enumValue(
        FEATURE_SUPPORT,
        protocol.parallelToolCalls,
        "parallel tool call support",
      ),
      streaming: enumValue(FEATURE_SUPPORT, protocol.streaming, "streaming support"),
      cancellation: enumValue(FEATURE_SUPPORT, protocol.cancellation, "cancellation support"),
      publicProgress: enumValue(
        FEATURE_SUPPORT,
        protocol.publicProgress ?? "unknown",
        "public progress support",
      ),
      assistantContentWithToolCalls: enumValue(
        ASSISTANT_CONTENT_WITH_TOOL_CALLS,
        protocol.assistantContentWithToolCalls,
        "assistant content with tool calls",
      ),
      jsonSchemaLevel: requiredText(protocol.jsonSchemaLevel, "JSON Schema level"),
      streamFinishSemantics: requiredText(
        protocol.streamFinishSemantics,
        "stream finish semantics",
      ),
      usageSemantics: requiredText(protocol.usageSemantics, "usage semantics"),
    }),
    actionable: value.actionable ?? true,
    ...(source === undefined ? {} : { source }),
  });
}

export interface InvocationOutputBudgetOptions {
  readonly maxGenerationTokens?: number;
  readonly generationSource?: GenerationBudgetSource;
  readonly resultCapacityTargetTokens?: number;
  readonly resultCapacitySource?: ResultCapacitySource;
}

export function resolveInvocationOutputBudget(
  snapshot: ModelCapabilitySnapshot | undefined,
  options: InvocationOutputBudgetOptions = {},
): InvocationOutputBudget {
  if (snapshot === undefined) {
    throw new AgentError(
      "model_generation_limit_unknown",
      "Model capabilities do not declare a verified generation limit",
    );
  }
  const profileMaximum = snapshot.maxGenerationTokens;
  if (profileMaximum === null) {
    throw new AgentError(
      "model_generation_limit_unknown",
      "Model capabilities do not declare a verified generation limit",
    );
  }
  let maxGenerationTokens = profileMaximum;
  let generationSource: GenerationBudgetSource = "model_profile";
  if (options.maxGenerationTokens !== undefined) {
    try {
      maxGenerationTokens = positiveInteger(
        options.maxGenerationTokens,
        "maximum generated tokens",
      );
    } catch (error) {
      throw new AgentError(
        "model_output_budget_invalid",
        "Invocation output budget values must be positive integers",
        { cause: error },
      );
    }
    if (maxGenerationTokens > profileMaximum) {
      throw new AgentError(
        "model_generation_limit_exceeded",
        "Generation budget exceeds the model profile maximum",
      );
    }
    generationSource = options.generationSource ?? "user";
    if (generationSource !== "user") {
      throw new AgentError("model_output_budget_invalid", "Invalid generation budget source");
    }
  } else if (
    options.generationSource !== undefined
    && options.generationSource !== "model_profile"
  ) {
    throw new AgentError(
      "model_output_budget_invalid",
      "Generation source conflicts with the model-profile allowance",
    );
  }

  let resultCapacityTargetTokens: number | null = null;
  let resultCapacitySource: ResultCapacitySource | null = null;
  if (options.resultCapacityTargetTokens !== undefined) {
    try {
      resultCapacityTargetTokens = positiveInteger(
        options.resultCapacityTargetTokens,
        "result capacity target",
      );
    } catch (error) {
      throw new AgentError(
        "model_output_budget_invalid",
        "Invocation output budget values must be positive integers",
        { cause: error },
      );
    }
    if (resultCapacityTargetTokens > maxGenerationTokens) {
      throw new AgentError(
        "model_output_budget_invalid",
        "Result capacity target exceeds the generation budget",
      );
    }
    if (
      options.resultCapacitySource === undefined
      || !includes(RESULT_CAPACITY_SOURCE, options.resultCapacitySource)
    ) {
      throw new AgentError(
        "model_output_budget_invalid",
        "Result capacity target requires its own source",
      );
    }
    resultCapacitySource = options.resultCapacitySource;
  } else if (options.resultCapacitySource !== undefined) {
    throw new AgentError(
      "model_output_budget_invalid",
      "Result capacity source requires a target",
    );
  }
  return Object.freeze({
    maxGenerationTokens,
    generationSource,
    profileMaxGenerationTokens: profileMaximum,
    requestedUserMaxGenerationTokens: options.maxGenerationTokens ?? null,
    resultCapacityTargetTokens,
    resultCapacitySource,
    nonResultHeadroomTokens: resultCapacityTargetTokens === null
      ? null
      : maxGenerationTokens - resultCapacityTargetTokens,
  });
}

export function constrainOutputBudgetToContext(
  budget: InvocationOutputBudget,
  maxGenerationTokens: number,
): InvocationOutputBudget {
  const contextMaximum = positiveInteger(
    maxGenerationTokens,
    "context maximum generation tokens",
  );
  if (budget.maxGenerationTokens <= contextMaximum) return budget;
  if (
    budget.resultCapacityTargetTokens !== null
    && budget.resultCapacityTargetTokens > contextMaximum
  ) {
    throw new AgentError(
      "model_result_capacity_incompatible",
      "Result capacity target does not fit the selected context window",
    );
  }
  return Object.freeze({
    ...budget,
    maxGenerationTokens: contextMaximum,
    generationSource: "context_capacity",
    nonResultHeadroomTokens: budget.resultCapacityTargetTokens === null
      ? null
      : contextMaximum - budget.resultCapacityTargetTokens,
  });
}

export function throwForIncompleteFinish(
  finishReason: ModelFinishReason,
  toolCallCount: number,
): void {
  if (finishReason === "length") {
    throw new AgentError(
      toolCallCount === 0 ? "model_output_truncated" : "tool_call_truncated",
      "Model output reached its limit",
    );
  }
  if (finishReason === "filtered") {
    throw new AgentError("model_output_filtered", "Model output was filtered");
  }
  if (finishReason === "other") {
    throw new AgentError(
      "unsupported_model_finish_reason",
      "Model gateway returned an unsupported finish reason",
    );
  }
}

export function copyJsonValue(value: unknown, active = new WeakSet<object>()): JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("JSON numbers must be finite");
    return value;
  }
  if (typeof value !== "object") throw new TypeError("Unsupported JSON value");
  if (active.has(value)) throw new TypeError("Cyclic JSON values are not supported");
  active.add(value);
  try {
    if (Array.isArray(value)) {
      return Object.freeze(value.map((item) => copyJsonValue(item, active)));
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError("JSON objects must be plain objects");
    }
    const copy = Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, copyJsonValue(item, active)]),
    );
    return Object.freeze(copy);
  } finally {
    active.delete(value);
  }
}

function copyMessage(message: Message): Message {
  if (!isObject(message)) throw new TypeError("Invalid message");
  if (!includes(MESSAGE_ROLES, message.role)) throw new TypeError("Invalid message role");

  const toolCallId = message.toolCallId === undefined
    ? undefined
    : requiredText(message.toolCallId, "tool call id");
  if (message.role === "tool" && toolCallId === undefined) {
    throw new TypeError("Tool messages require a tool call id");
  }
  if (message.role !== "tool" && toolCallId !== undefined) {
    throw new TypeError("Only tool messages may carry a tool call id");
  }
  if (message.role !== "assistant" && message.toolCalls !== undefined) {
    throw new TypeError("Only assistant messages may carry tool calls");
  }
  if (message.toolCalls !== undefined && !Array.isArray(message.toolCalls)) {
    throw new TypeError("Tool calls must be an array");
  }
  const toolCalls = message.toolCalls?.map((call) => {
    if (!isObject(call)) throw new TypeError("Invalid tool call");
    return Object.freeze({
      id: requiredText(call.id, "tool call id"),
      name: requiredText(call.name, "tool call name"),
      arguments: copyJsonValue(call.arguments),
    });
  });
  const reasoning = optionalText(message.reasoning);
  const attributes = message.attributes === undefined
    ? undefined
    : copyJsonMapping(message.attributes, "message attributes");
  const providerData = message.providerData === undefined
    ? undefined : copyJsonMapping(message.providerData, "provider data");
  return Object.freeze({
    role: message.role,
    content: copyJsonValue(message.content),
    ...(reasoning === undefined ? {} : { reasoning }),
    ...(toolCallId === undefined ? {} : { toolCallId }),
    ...(toolCalls === undefined ? {} : { toolCalls: Object.freeze(toolCalls) }),
    ...(attributes === undefined ? {} : { attributes }),
    ...(providerData === undefined ? {} : { providerData }),
  });
}

function copyToolCallDelta(delta: ToolCallDelta): ToolCallDelta {
  if (!isObject(delta)) throw new TypeError("Invalid tool call delta");
  const index = nonNegativeInteger(delta.index, "tool call delta index");
  const id = delta.id === undefined ? undefined : requiredText(delta.id, "tool call id");
  const type = delta.type === undefined ? undefined : requiredText(delta.type, "tool call type");
  const name = delta.name === undefined ? undefined : requiredText(delta.name, "tool call name");
  const argumentsFragment = optionalString(delta.argumentsFragment, "tool arguments fragment");
  return Object.freeze({
    index,
    ...(id === undefined ? {} : { id }),
    ...(type === undefined ? {} : { type }),
    ...(name === undefined ? {} : { name }),
    ...(argumentsFragment === undefined ? {} : { argumentsFragment }),
  });
}

function copyTokenUsage(usage: ModelTokenUsage): ModelTokenUsage {
  if (!isObject(usage)) throw new TypeError("Invalid model token usage");
  if (
    Object.prototype.hasOwnProperty.call(usage, "outputTokens")
    || Object.prototype.hasOwnProperty.call(usage, "reasoningOutputTokens")
  ) {
    throw new TypeError("Legacy model token usage fields are unsupported");
  }
  const inputTokens = nonNegativeInteger(usage.inputTokens, "input tokens");
  const generationTokens = nonNegativeInteger(usage.generationTokens, "generation tokens");
  const reasoningTokens = usage.reasoningTokens === undefined
    ? undefined
    : nonNegativeInteger(usage.reasoningTokens, "reasoning tokens");
  return Object.freeze({
    inputTokens,
    generationTokens,
    totalTokens: nonNegativeInteger(
      usage.totalTokens ?? inputTokens + generationTokens,
      "total tokens",
    ),
    cachedInputTokens: nonNegativeInteger(usage.cachedInputTokens ?? 0, "cached input tokens"),
    ...(reasoningTokens === undefined ? {} : { reasoningTokens }),
  });
}

function copyJsonMapping(
  value: Readonly<Record<string, JsonValue>>,
  label: string,
): Readonly<Record<string, JsonValue>> {
  if (!isObject(value) || Array.isArray(value)) throw new TypeError(`${label} must be an object`);
  return copyJsonValue(value) as Readonly<Record<string, JsonValue>>;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object";
}

function includes<const T extends readonly unknown[]>(values: T, value: unknown): value is T[number] {
  return values.includes(value);
}

function enumValue<const T extends readonly string[]>(
  values: T,
  value: unknown,
  label: string,
): T[number] {
  if (!includes(values, value)) throw new TypeError(`Invalid ${label}`);
  return value;
}

function copyEnumArray<const T extends readonly string[]>(
  values: T,
  input: unknown,
  label: string,
): readonly T[number][] {
  if (!Array.isArray(input)) throw new TypeError(`${label} must be an array`);
  const result = input.map((value) => enumValue(values, value, label));
  if (new Set(result).size !== result.length) {
    throw new TypeError(`${label} values must be unique`);
  }
  return Object.freeze(result);
}

function requiredText(value: unknown, label: string): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text.length === 0) throw new TypeError(`${label} must be non-empty text`);
  return text;
}

function optionalText(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string") throw new TypeError("Optional text must be text");
  return value.trim() || undefined;
}

function optionalString(value: unknown, label: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string") throw new TypeError(`${label} must be text`);
  return value;
}

function positiveInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 1) {
    throw new TypeError(`${label} must be a positive integer`);
  }
  return Number(value);
}

function nonNegativeInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new TypeError(`${label} must be a non-negative integer`);
  }
  return Number(value);
}
