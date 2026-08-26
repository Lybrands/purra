import type { JsonValue, Message, ToolCall, ToolSpec } from "../model/types.js";
import type { PlanningToolRegistration } from "../planning/types.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import { inspectToolSchema, validateToolArguments, type JsonSchema } from "./schema.js";
import type {
  ToolApprovalGateway,
  ToolApprovalStatus,
  ToolBatchResult,
  ToolDefinition,
  ToolEffectState,
  ToolExecutionEvent,
  ToolExecutionLimits,
  ToolHandlerResult,
  ToolIdempotencyGateway,
  ToolPolicy,
} from "./types.js";

interface CatalogOptions {
  readonly approval?: ToolApprovalGateway;
  readonly idempotency?: ToolIdempotencyGateway;
  readonly limits?: ToolExecutionLimits;
}

interface ExecuteOptions {
  readonly executionKey: string;
  readonly enabledTools?: readonly string[];
  readonly runId?: string;
  readonly rootRunId?: string;
  readonly agentId?: string;
  readonly parentRunId?: string;
  readonly leaseOwnerId?: string;
  readonly leaseEpoch?: number;
  readonly delegationEnabledTools?: readonly string[];
  readonly signal?: AbortSignal;
  readonly onEvent?: (event: ToolExecutionEvent) => Promise<void> | void;
}

interface RegisteredTool {
  readonly definition: ToolDefinition;
  readonly schema: JsonSchema;
  readonly policy: ToolPolicy & { readonly riskLevel: "read" | "write" | "destructive" };
  readonly spec: ToolSpec;
}

const ERROR_CODE = /^[a-z0-9][a-z0-9_.-]{0,127}$/;

export class ToolCatalog {
  readonly #tools = new Map<string, RegisteredTool>();
  readonly #approval: ToolApprovalGateway | undefined;
  readonly #idempotency: ToolIdempotencyGateway | undefined;
  readonly #maxCallsPerBatch: number;
  readonly #maxResultChars: number;
  readonly #approvalSummaryChars: number;
  readonly #approvalTimeoutMs: number;

  public constructor(definitions: readonly ToolDefinition[] = [], options: CatalogOptions = {}) {
    if (!Array.isArray(definitions)) throw new TypeError("Agent tools must be an array");
    this.#approval = options.approval;
    this.#idempotency = options.idempotency;
    this.#maxCallsPerBatch = positiveInteger(options.limits?.maxCallsPerBatch ?? 8, "maxCallsPerBatch");
    this.#maxResultChars = positiveInteger(options.limits?.maxResultChars ?? 64_000, "maxResultChars");
    this.#approvalSummaryChars = positiveInteger(
      options.limits?.approvalSummaryChars ?? 420,
      "approvalSummaryChars",
    );
    this.#approvalTimeoutMs = positiveInteger(
      options.limits?.approvalTimeoutMs ?? 300_000,
      "approvalTimeoutMs",
    );

    for (const definition of definitions) {
      const registered = registerTool(definition);
      const name = registered.definition.name;
      if (this.#tools.has(name)) throw new AgentError("duplicate_tool", `Duplicate tool: ${name}`);
      if (
        registered.definition.enabled !== false
        && registered.policy.mode !== "read"
        && registered.definition.hostManagedDurability !== true
        && this.#idempotency === undefined
      ) {
        throw new AgentError(
          "tool_idempotency_unavailable",
          `Tool ${name} requires an idempotency gateway or host-managed durability`,
        );
      }
      this.#tools.set(name, registered);
    }
  }

  public get size(): number {
    return this.#tools.size;
  }

  public specsFor(enabledTools?: readonly string[]): readonly ToolSpec[] {
    const enabled = this.#enabledNames(enabledTools);
    return Object.freeze([...enabled].map((name) => this.#tools.get(name)!.spec));
  }

  public planningRegistrationsFor(
    enabledTools?: readonly string[],
  ): readonly PlanningToolRegistration[] {
    const enabled = this.#enabledNames(enabledTools);
    return Object.freeze([...enabled].map((name) => {
      const tool = this.#tools.get(name)!;
      return Object.freeze({
        runtimeName: name,
        runtimeSpec: tool.spec,
        ...(tool.definition.planning === undefined
          ? {}
          : { planningCapability: tool.definition.planning.capability }),
        prerequisiteTools: Object.freeze([...(tool.definition.planning?.prerequisiteTools ?? [])]),
        riskLevel: tool.policy.riskLevel,
        title: tool.policy.title,
      });
    }));
  }

  public readToolNamesFor(enabledTools?: readonly string[]): readonly string[] {
    const enabled = this.#enabledNames(enabledTools);
    return Object.freeze([...enabled].filter((name) => (
      name !== "delegateToAgents" && this.#tools.get(name)!.policy.mode === "read"
    )));
  }

  public async executeBatch(
    calls: readonly ToolCall[],
    options: ExecuteOptions,
  ): Promise<ToolBatchResult> {
    const admitted = this.#admitBatch(calls, options.enabledTools);
    await this.#authorizeBatch(admitted, options.signal);

    const messages: Message[] = [];
    const failures: { readonly errorCode: string; readonly effectState: ToolEffectState }[] = [];
    let replan: ToolBatchResult["replan"];
    for (const [call, tool] of admitted) {
      throwIfCanceled(options.signal);
      await options.onEvent?.(Object.freeze({
        type: "tool_started",
        toolCallId: call.id,
        toolName: call.name,
      }));
      const result = await this.#execute(
        call,
        tool,
        options.executionKey,
        options.signal,
        options.runId,
        options.rootRunId,
        options.agentId,
        options.parentRunId,
        options.leaseOwnerId,
        options.leaseEpoch,
        options.delegationEnabledTools,
      );
      await options.onEvent?.(Object.freeze({
        type: "tool_completed",
        toolCallId: call.id,
        toolName: call.name,
        effectState: result.effectState,
        ...(result.errorCode === undefined ? {} : { errorCode: result.errorCode }),
      }));
      messages.push(Object.freeze({
        role: "tool",
        content: result.errorCode === undefined
          ? result.content
          : safeFailure(result.errorCode, result.effectState),
        toolCallId: call.id,
      }));
      if (result.errorCode !== undefined) {
        failures.push(Object.freeze({
          errorCode: result.errorCode,
          effectState: result.effectState,
        }));
      }
      if (result.planningDisposition === "replan") {
        replan = Object.freeze({
          reason: result.planningReason!,
          ...(result.errorCode === undefined ? {} : { errorCode: result.errorCode }),
        });
      }
    }
    return Object.freeze({
      messages: Object.freeze(messages),
      toolNames: Object.freeze(admitted.map(([call]) => call.name)),
      failures: Object.freeze(failures),
      ...(replan === undefined ? {} : { replan }),
    });
  }

  #enabledNames(requested: readonly string[] | undefined): ReadonlySet<string> {
    if (requested === undefined) {
      return new Set([...this.#tools]
        .filter(([, tool]) => tool.definition.enabled !== false)
        .map(([name]) => name));
    }
    if (!Array.isArray(requested)) throw new TypeError("enabledTools must be an array");
    const names = requested.map((name) => requiredText(name, "enabled tool name"));
    if (new Set(names).size !== names.length) throw new TypeError("enabledTools must be unique");
    for (const name of names) {
      const tool = this.#tools.get(name);
      if (tool === undefined || tool.definition.enabled === false) {
        throw new AgentError("tool_not_enabled", `Tool ${name} is not enabled`);
      }
    }
    return new Set(names);
  }

  #admitBatch(
    calls: readonly ToolCall[],
    enabledTools: readonly string[] | undefined,
  ): readonly (readonly [ToolCall, RegisteredTool])[] {
    if (!Array.isArray(calls) || calls.length === 0) {
      throw new AgentError("invalid_tool_batch", "Tool batch must not be empty");
    }
    if (calls.length > this.#maxCallsPerBatch) {
      throw new AgentError("too_many_tool_calls", "Tool batch exceeds its configured limit");
    }
    const enabled = this.#enabledNames(enabledTools);
    const ids = new Set<string>();
    return Object.freeze(calls.map((call) => {
      const id = requiredText(call.id, "tool call id");
      const name = requiredText(call.name, "tool call name");
      if (ids.has(id)) throw new AgentError("duplicate_tool_call", `Duplicate tool call id: ${id}`);
      ids.add(id);
      const tool = this.#tools.get(name);
      if (tool === undefined) throw new AgentError("unknown_tool", `Unknown tool: ${name}`);
      if (!enabled.has(name)) throw new AgentError("tool_not_enabled", `Tool ${name} is not enabled`);
      validateToolArguments(call.arguments, tool.schema, name);
      return [call, tool] as const;
    }));
  }

  async #authorizeBatch(
    admitted: readonly (readonly [ToolCall, RegisteredTool])[],
    signal: AbortSignal | undefined,
  ): Promise<void> {
    for (const [call, tool] of admitted) {
      throwIfCanceled(signal);
      if (tool.definition.scope !== undefined) {
        let decision: string | boolean | void;
        try {
          decision = await awaitWithSignal(
            Promise.resolve(tool.definition.scope(call.arguments, context(call, signal))),
            signal,
            false,
          );
        } catch (error) {
          if (error instanceof AgentCanceledError) throw error;
          throw new AgentError(
            "tool_scope_validation_failed",
            `Tool ${call.name} scope validation failed`,
            { cause: error },
          );
        }
        if (decision === false || (typeof decision === "string" && decision.trim() !== "")) {
          throw new AgentError("tool_scope_violation", `Tool ${call.name} is outside the allowed scope`);
        }
      }
    }

    for (const [call, tool] of admitted) {
      if (tool.policy.mode !== "confirm") continue;
      if (this.#approval === undefined) {
        throw new AgentError("tool_approval_unavailable", `Tool ${call.name} requires approval`);
      }
      let status: ToolApprovalStatus;
      const timeoutController = new AbortController();
      const timeoutHandle = globalThis.setTimeout(
        () => timeoutController.abort(),
        this.#approvalTimeoutMs,
      );
      const timeout = timeoutController.signal;
      const approvalSignal = signal === undefined
        ? timeout
        : AbortSignal.any([signal, timeout]);
      try {
        status = await awaitWithSignal(Promise.resolve(this.#approval.request(Object.freeze({
          call,
          title: tool.policy.title,
          riskLevel: tool.policy.riskLevel,
          summary: summarize(call.arguments, this.#approvalSummaryChars),
        }), approvalSignal)), approvalSignal, false);
      } catch (error) {
        if (error instanceof AgentCanceledError && signal?.aborted === true) throw error;
        if (timeout.aborted) {
          throw new AgentError("tool_approval_timed_out", `Tool ${call.name} approval timed out`);
        }
        throw new AgentError("tool_approval_unavailable", `Tool ${call.name} approval failed`, {
          cause: error,
        });
      } finally {
        globalThis.clearTimeout(timeoutHandle);
      }
      if (status !== "approved") {
        const code = status === "rejected"
          ? "tool_approval_rejected"
          : status === "timed_out" || status === "canceled"
            ? `tool_approval_${status}`
            : "tool_approval_unavailable";
        throw new AgentError(code, `Tool ${call.name} was not approved`);
      }
    }
  }

  async #execute(
    call: ToolCall,
    tool: RegisteredTool,
    executionKey: string,
    signal: AbortSignal | undefined,
    runId: string | undefined,
    rootRunId: string | undefined,
    agentId: string | undefined,
    parentRunId: string | undefined,
    leaseOwnerId: string | undefined,
    leaseEpoch: number | undefined,
    delegationEnabledTools: readonly string[] | undefined,
  ): Promise<ToolHandlerResult> {
    const operation = async (): Promise<ToolHandlerResult> => tool.definition.run(
      call.arguments,
      context(
        call,
        signal,
        runId,
        rootRunId,
        agentId,
        parentRunId,
        leaseOwnerId,
        leaseEpoch,
        delegationEnabledTools,
      ),
    );
    const guarded = tool.policy.mode !== "read" && tool.definition.hostManagedDurability !== true
      ? () => this.#idempotency!.executeOnce(`${executionKey}:${call.name}:${call.id}`, operation)
      : operation;
    const uncertainOnCancel = tool.policy.mode !== "read"
      && tool.definition.cancellationLinearizable !== true;

    let raw: ToolHandlerResult;
    try {
      raw = await awaitWithSignal(Promise.resolve(guarded()), signal, uncertainOnCancel);
    } catch (error) {
      if (error instanceof AgentError && error.code === "tool_effect_unknown") throw error;
      if (error instanceof AgentCanceledError && signal?.aborted === true && !uncertainOnCancel) {
        throw error;
      }
      if (tool.policy.mode !== "read") {
        throw new AgentError(
          "tool_effect_unknown",
          `Tool ${call.name} failed after its effect boundary became uncertain`,
          { cause: error },
        );
      }
      return Object.freeze({
        content: null,
        effectState: "not_started",
        errorCode: "tool_execution_failed",
      });
    }

    let result: ToolHandlerResult;
    try {
      result = normalizeResult(raw, tool.policy.mode);
    } catch (error) {
      if (error instanceof AgentError && error.code === "tool_effect_unknown") throw error;
      if (tool.policy.mode !== "read") {
        throw new AgentError("tool_effect_unknown", `Tool ${call.name} returned an invalid receipt`, {
          cause: error,
        });
      }
      return Object.freeze({
        content: null,
        effectState: "not_started",
        errorCode: "invalid_tool_result",
      });
    }
    const serialized = JSON.stringify(result.content);
    if (serialized.length > this.#maxResultChars) {
      return Object.freeze({
        content: null,
        effectState: result.effectState,
        errorCode: "tool_result_too_large",
      });
    }
    return result;
  }
}

function registerTool(value: ToolDefinition): RegisteredTool {
  if (value === null || typeof value !== "object") throw new TypeError("Invalid tool definition");
  const name = requiredText(value.name, "tool name");
  const description = requiredText(value.description, `Tool ${name} description`);
  if (typeof value.run !== "function") throw new TypeError(`Tool ${name} requires a run function`);
  if (value.scope !== undefined && typeof value.scope !== "function") {
    throw new TypeError(`Tool ${name} scope must be a function`);
  }
  if (value.enabled !== undefined && typeof value.enabled !== "boolean") {
    throw new TypeError(`Tool ${name} enabled must be boolean`);
  }
  if (
    value.cancellationLinearizable !== undefined
    && typeof value.cancellationLinearizable !== "boolean"
  ) {
    throw new TypeError(`Tool ${name} cancellationLinearizable must be boolean`);
  }
  const policy = copyPolicy(value.policy, name);
  const schema = inspectToolSchema(value.inputSchema, name);
  const displayNames = copyDisplayNames(value.displayNames, name);
  const planning = value.planning === undefined
    ? undefined
    : copyPlanning(value.planning, name);
  const spec: ToolSpec = Object.freeze({
    name,
    description,
    ...(displayNames === undefined ? {} : { displayNames }),
    inputSchema: schema,
  });
  return Object.freeze({
    definition: Object.freeze({
      ...value,
      name,
      description,
      policy,
      inputSchema: schema,
      ...(displayNames === undefined ? {} : { displayNames }),
      ...(planning === undefined ? {} : { planning }),
    }),
    schema,
    policy,
    spec,
  });
}

function copyPlanning(
  value: NonNullable<ToolDefinition["planning"]>,
  toolName: string,
): NonNullable<ToolDefinition["planning"]> {
  if (value === null || typeof value !== "object") {
    throw new AgentError("invalid_tool_planning_contract", `Tool ${toolName} planning metadata is invalid`);
  }
  const capability = value.capability;
  if (capability === null || typeof capability !== "object") {
    throw new AgentError("invalid_tool_planning_contract", `Tool ${toolName} requires a planning capability`);
  }
  const name = requiredText(capability.name, `Tool ${toolName} planning capability name`);
  const description = requiredText(
    capability.description,
    `Tool ${toolName} planning capability description`,
  );
  const inputSchema = inspectToolSchema(capability.inputSchema, name);
  const displayNames = copyDisplayNames(capability.displayNames, name);
  const prerequisiteTools = value.prerequisiteTools === undefined
    ? Object.freeze([])
    : Object.freeze(value.prerequisiteTools.map((item) => requiredText(item, "prerequisite tool name")));
  if (new Set(prerequisiteTools).size !== prerequisiteTools.length) {
    throw new AgentError("invalid_tool_planning_contract", `Tool ${toolName} prerequisites must be unique`);
  }
  return Object.freeze({
    capability: Object.freeze({
      name,
      description,
      ...(displayNames === undefined ? {} : { displayNames }),
      inputSchema,
    }),
    prerequisiteTools,
  });
}

function copyPolicy(value: ToolPolicy, toolName: string): RegisteredTool["policy"] {
  if (value === null || typeof value !== "object") {
    throw new AgentError("invalid_tool_policy", `Tool ${toolName} requires an explicit policy`);
  }
  if (value.mode !== "read" && value.mode !== "propose" && value.mode !== "confirm") {
    throw new AgentError("invalid_tool_policy", `Tool ${toolName} has an invalid policy mode`);
  }
  const riskLevel = value.riskLevel ?? (value.mode === "read" ? "read" : "write");
  if (riskLevel !== "read" && riskLevel !== "write" && riskLevel !== "destructive") {
    throw new AgentError("invalid_tool_policy", `Tool ${toolName} has an invalid risk level`);
  }
  return Object.freeze({
    mode: value.mode,
    title: requiredText(value.title, `Tool ${toolName} policy title`),
    riskLevel,
  });
}

function copyDisplayNames(
  value: Readonly<Record<string, string>> | undefined,
  toolName: string,
): Readonly<Record<string, string>> | undefined {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`Tool ${toolName} displayNames must be an object`);
  }
  const result: Record<string, string> = {};
  for (const [locale, displayName] of Object.entries(value)) {
    result[requiredText(locale, "display name locale")] = requiredText(displayName, "display name");
  }
  return Object.freeze(result);
}

function normalizeResult(value: ToolHandlerResult, mode: ToolPolicy["mode"]): ToolHandlerResult {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Tool handler must return a ToolHandlerResult");
  }
  if (
    value.effectState !== "not_started"
    && value.effectState !== "committed"
    && value.effectState !== "unknown"
  ) {
    throw new TypeError("Tool result requires a valid effectState");
  }
  const errorCode = value.errorCode === undefined ? undefined : normalizeErrorCode(value.errorCode);
  const planningDisposition = value.planningDisposition ?? "continue";
  if (planningDisposition !== "continue" && planningDisposition !== "replan") {
    throw new TypeError("Tool result has an invalid planningDisposition");
  }
  const planningReason = value.planningReason === undefined
    ? undefined
    : requiredText(value.planningReason, "planning reason");
  if (planningDisposition === "replan" && planningReason === undefined) {
    throw new TypeError("Tool replan disposition requires a planningReason");
  }
  if (planningDisposition === "continue" && planningReason !== undefined) {
    throw new TypeError("Tool planningReason requires a replan disposition");
  }
  if (mode === "read" && value.effectState !== "not_started") {
    throw new TypeError("Read tools cannot report a side effect");
  }
  if (mode !== "read" && errorCode === undefined && value.effectState !== "committed") {
    throw new TypeError("Successful side-effecting tools must report committed");
  }
  if (mode !== "read" && value.effectState === "unknown") {
    throw new AgentError("tool_effect_unknown", "Tool effect state is unknown");
  }
  return Object.freeze({
    content: copyJsonValue(value.content),
    effectState: value.effectState,
    ...(errorCode === undefined ? {} : { errorCode }),
    ...(planningDisposition === "continue" ? {} : { planningDisposition }),
    ...(planningReason === undefined ? {} : { planningReason }),
  });
}

async function awaitWithSignal<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined,
  uncertainOnCancel: boolean,
): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) {
    void promise.catch(() => undefined);
    throw uncertainOnCancel
      ? new AgentError("tool_effect_unknown", "Cancellation raced an uncertain tool effect")
      : new AgentCanceledError();
  }
  let cancel: (() => void) | undefined;
  const canceled = new Promise<never>((_resolve, reject) => {
    cancel = () => reject(uncertainOnCancel
      ? new AgentError("tool_effect_unknown", "Cancellation raced an uncertain tool effect")
      : new AgentCanceledError());
    signal.addEventListener("abort", cancel, { once: true });
  });
  try {
    return await Promise.race([promise, canceled]);
  } finally {
    if (cancel !== undefined) signal.removeEventListener("abort", cancel);
    void promise.catch(() => undefined);
  }
}

function safeFailure(code: string, effectState: ToolEffectState): JsonValue {
  return copyJsonValue({
    ok: false,
    error: { code, message: "Tool execution did not complete successfully." },
    effectState,
  });
}

function summarize(value: JsonValue, maxChars: number): string {
  const text = JSON.stringify(value);
  return text.length <= maxChars ? text : `${text.slice(0, Math.max(0, maxChars - 1))}…`;
}

function normalizeErrorCode(value: unknown): string {
  const code = typeof value === "string" ? value.trim().toLowerCase() : "";
  if (!ERROR_CODE.test(code)) return "tool_execution_failed";
  return code;
}

function context(
  call: ToolCall,
  signal: AbortSignal | undefined,
  runId?: string,
  rootRunId?: string,
  agentId?: string,
  parentRunId?: string,
  leaseOwnerId?: string,
  leaseEpoch?: number,
  enabledTools?: readonly string[],
) {
  return Object.freeze({
    call,
    ...(signal === undefined ? {} : { signal }),
    ...(runId === undefined ? {} : { runId }),
    ...(rootRunId === undefined ? {} : { rootRunId }),
    ...(agentId === undefined ? {} : { agentId }),
    ...(parentRunId === undefined ? {} : { parentRunId }),
    ...(leaseOwnerId === undefined ? {} : { leaseOwnerId }),
    ...(leaseEpoch === undefined ? {} : { leaseEpoch }),
    ...(enabledTools === undefined ? {} : { enabledTools }),
  });
}

function requiredText(value: unknown, label: string): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text.length === 0) throw new TypeError(`${label} must be non-empty text`);
  return text;
}

function positiveInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 1) {
    throw new TypeError(`${label} must be a positive integer`);
  }
  return Number(value);
}

function throwIfCanceled(signal: AbortSignal | undefined): void {
  if (signal?.aborted === true) throw new AgentCanceledError();
}
