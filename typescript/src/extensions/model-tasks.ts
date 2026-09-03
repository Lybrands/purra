import { AgentOperationController } from "../operations/index.js";
import { estimateMessagesTokens } from "../context/budget.js";
import type {
  ContextEvidenceReceipt,
  ModelInputEvidenceValidator,
} from "../context/types.js";
import { PLANNING_STREAM_SCHEMA, PlanningStreamParser, type PlanningProgress, type PlanningScope } from "../planning/stream.js";
import type { JsonValue } from "../model/types.js";
import type { OperationReceipt } from "../operations/index.js";
import {
  EMPTY_RESPONSE_RETRY_GUIDANCE,
  RecoveryLedger,
  RecoveryPolicy,
  type RecoveryDecision,
} from "../recovery/index.js";
import type {
  InvocationReceiptInput,
  ModelInvocationReceipt,
} from "../run/types.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import {
  constrainModelInvocationTimeout,
  invokeModel,
  type ModelStreamLimits,
} from "../model/stream.js";
import type {
  InvocationOutputLimit,
  Message,
  ModelCapabilitySnapshot,
  ModelGateway,
  ModelRequest,
  ModelStreamChunk,
  ModelTokenUsage,
  ModelTurn,
} from "../model/types.js";
import {
  copyCapabilitySnapshot,
  copyMessages,
  resolveInvocationOutputLimit,
} from "../model/validation.js";

type ManagedModelRequest = ModelRequest & { readonly outputLimit: InvocationOutputLimit };

export const REJECTED_PLANNER_OUTPUT = Symbol("rejectedPlannerOutput");
export type PlannerOutputError = AgentError & { readonly [REJECTED_PLANNER_OUTPUT]?: string };

export interface ModelTaskInvocationAuthority {
  readonly runId: string;
  openInvocation(input: InvocationReceiptInput): Promise<ModelInvocationReceipt>;
  persistChunk(
    receipt: ModelInvocationReceipt,
    index: number,
    chunk: ModelStreamChunk,
  ): Promise<void>;
  persistCompletion(receipt: ModelInvocationReceipt, turn: ModelTurn): Promise<void>;
  settleInvocation(
    receipt: ModelInvocationReceipt,
    status: "completed" | "failed",
    turn?: ModelTurn,
    errorCode?: string,
    usage?: ModelTokenUsage,
  ): Promise<void>;
  persistPlanningProgress?(receipt: ModelInvocationReceipt, progress: PlanningProgress): Promise<boolean>;
  recordModelDiagnostics?(receipt: ModelInvocationReceipt, metrics: Readonly<Record<string, JsonValue>>): Promise<void>;
  publishRecoveryDecision(decision: RecoveryDecision, round: number): Promise<void>;
}

export interface ModelTaskRunnerOptions {
  readonly model: ModelGateway;
  readonly runId: string;
  readonly recovery?: RecoveryPolicy;
  readonly operations?: AgentOperationController;
  readonly authority?: ModelTaskInvocationAuthority;
  readonly runtimeLimits?: ModelStreamLimits;
  readonly maxCallOutputTokens?: number;
  readonly evidenceValidator?: ModelInputEvidenceValidator;
}

export interface ModelTaskOptions {
  readonly maxCallOutputTokens?: number;
  readonly signal?: AbortSignal;
}

export interface ModelTaskStreamTextOptions extends ModelTaskOptions {
  readonly onChunk?: (chunk: ModelStreamChunk) => Promise<void> | void;
}

export interface ModelTaskPlanOptions extends ModelTaskOptions {
  readonly scope?: PlanningScope;
  readonly attempt?: number;
  readonly validatePlan?: (plan: Readonly<Record<string, JsonValue>>) => void;
  readonly attemptTimeoutMs?: number;
}

export interface ModelTaskCompletion {
  readonly turn: ModelTurn;
  readonly outputLimit: InvocationOutputLimit;
}

export interface ModelTaskTextResult {
  readonly content: string;
  readonly reasoning: string;
  readonly finishReason: ModelTurn["finishReason"];
  readonly usage?: ModelTokenUsage;
  readonly attempts: number;
  readonly outputLimit: InvocationOutputLimit;
}

export class ModelTaskRunner {
  readonly #model: ModelGateway;
  readonly #runId: string;
  readonly #capabilities: ModelCapabilitySnapshot | undefined;
  readonly #recovery: RecoveryPolicy;
  readonly #operations: AgentOperationController | undefined;
  readonly #authority: ModelTaskInvocationAuthority | undefined;
  readonly #runtimeLimits: ModelStreamLimits | undefined;
  readonly #maxCallOutputTokens: number | undefined;
  readonly #evidenceValidator: ModelInputEvidenceValidator | undefined;
  #contextEvidence: readonly ContextEvidenceReceipt[] = Object.freeze([]);

  public constructor(options: ModelTaskRunnerOptions) {
    if (typeof options?.model?.invoke !== "function") {
      throw new TypeError("ModelTaskRunner requires a model gateway");
    }
    this.#runId = requiredText(options.runId, "model task Run id");
    if (options.recovery !== undefined && !(options.recovery instanceof RecoveryPolicy)) {
      throw new TypeError("model task recovery must be a RecoveryPolicy");
    }
    if (options.operations !== undefined && !(options.operations instanceof AgentOperationController)) {
      throw new TypeError("model task operations must be an AgentOperationController");
    }
    if (options.authority !== undefined) {
      assertAuthority(options.authority);
      if (options.authority.runId !== this.#runId) {
        throw new TypeError("model task authority belongs to another Run");
      }
    }
    this.#model = options.model;
    this.#capabilities = options.model.capabilities === undefined
      ? undefined
      : copyCapabilitySnapshot(options.model.capabilities);
    if (this.#capabilities?.actionable === false) {
      throw new AgentError("model_capability_incompatible", "Model capability snapshot is not actionable");
    }
    this.#recovery = options.recovery ?? new RecoveryPolicy();
    this.#operations = options.operations;
    this.#authority = options.authority;
    this.#runtimeLimits = options.runtimeLimits;
    this.#maxCallOutputTokens = options.maxCallOutputTokens;
    if (
      options.evidenceValidator !== undefined
      && typeof options.evidenceValidator.validateEvidence !== "function"
    ) {
      throw new TypeError("evidenceValidator must implement validateEvidence");
    }
    this.#evidenceValidator = options.evidenceValidator;
  }

  public get runId(): string {
    return this.#runId;
  }

  public bindEvidence(receipts: readonly ContextEvidenceReceipt[]): void {
    this.#contextEvidence = copyEvidence(receipts);
  }

  public async complete(
    messages: readonly Message[],
    options: ModelTaskOptions = {},
  ): Promise<ModelTaskCompletion> {
    const request = this.#request(messages, options.maxCallOutputTokens);
    const turn = await this.#invoke(request, options.signal, false);
    return Object.freeze({ turn, outputLimit: request.outputLimit });
  }

  public async plan(messages: readonly Message[], options: ModelTaskPlanOptions = {}): Promise<ModelTaskCompletion> {
    if (typeof this.#model.stream !== "function" || this.#capabilities?.protocol.streaming !== "supported") {
      throw new AgentError("model_stream_unavailable", "Planner requires a streaming Gateway");
    }
    if (options.scope !== undefined && options.scope.runId !== this.#runId) {
      throw new AgentError("planning_scope_conflict", "Planning scope belongs to another Run");
    }
    if (options.scope !== undefined && this.#authority !== undefined && this.#authority.persistPlanningProgress === undefined) {
      throw new AgentError("planning_output_unavailable", "Planning authority cannot persist public projections");
    }
    const attempt = options.attempt ?? 0;
    if (!Number.isSafeInteger(attempt) || attempt < 0) throw new TypeError("Invalid planning attempt");
    const parser = new PlanningStreamParser();
    let rejectedOutput = "";
    let retainedCharacters = 0;
    let trailingHighSurrogate = false;
    const started = performance.now();
    const metrics: Record<string, JsonValue> = { attemptStartedAtMs: Date.now(), firstPublicProgressMs: null, planReceivedMs: null, rejectedPublicProgressRecords: 0, validationMs: null };
    const request = this.#request(messages, options.maxCallOutputTokens);
    const turn = await this.#invoke(request, options.signal, true, async (chunk, receipt) => {
      for (const character of chunk.contentDelta ?? "") {
        const joinsSurrogate = trailingHighSurrogate && /^[\udc00-\udfff]$/u.test(character);
        if (retainedCharacters === 65_536 && !joinsSurrogate) break;
        rejectedOutput += character;
        retainedCharacters += joinsSurrogate ? 0 : 1;
        trailingHighSurrogate = /^[\ud800-\udbff]$/u.test(character);
      }
      if ((chunk.toolCallDeltas?.length ?? 0) > 0) {
        throw new AgentError("model_task_tool_call_unsupported", "Planner cannot call tools");
      }
      const records = parser.feed(chunk.contentDelta ?? "");
      metrics.rejectedPublicProgressRecords = parser.rejectedProgressRecords;
      if (parser.planReceived && metrics.planReceivedMs === null) metrics.planReceivedMs = performance.now() - started;
      for (const record of records) {
        if (options.signal?.aborted === true) throw new AgentCanceledError();
        if (receipt !== undefined && options.scope !== undefined) {
          const published = await this.#authority!.persistPlanningProgress!(receipt, record);
          if (published && metrics.firstPublicProgressMs === null) metrics.firstPublicProgressMs = performance.now() - started;
        }
      }
    }, {
      outputProtocol: PLANNING_STREAM_SCHEMA,
      ...(options.scope === undefined ? {} : { planningScope: options.scope }),
      planningAttempt: attempt,
    }, () => {
      const plan = parser.finish();
      metrics.rejectedPublicProgressRecords = parser.rejectedProgressRecords;
      if (metrics.planReceivedMs === null) metrics.planReceivedMs = performance.now() - started;
      const validationStarted = performance.now();
      try { options.validatePlan?.(plan); }
      finally { metrics.validationMs = performance.now() - validationStarted; }
    }, metrics, options.attemptTimeoutMs).catch((error: unknown) => {
      if (error instanceof AgentError && ["invalid_planner_output", "invalid_planning_stream"].includes(error.code)) {
        Object.defineProperty(error, REJECTED_PLANNER_OUTPUT, { value: rejectedOutput, configurable: true });
      }
      throw error;
    });
    return Object.freeze({ turn, outputLimit: request.outputLimit });
  }

  public async streamText(
    messages: readonly Message[],
    options: ModelTaskStreamTextOptions = {},
  ): Promise<ModelTaskTextResult> {
    if (typeof this.#model.stream !== "function" || this.#capabilities?.protocol.streaming === "unavailable") {
      throw new AgentError("model_stream_unavailable", "Model task requires a streaming gateway");
    }
    const original = Object.freeze(copyMessages(messages));
    let active = original;
    const ledger = new RecoveryLedger(this.#recovery);
    let attempts = 0;

    while (true) {
      attempts += 1;
      const request = this.#request(active, options.maxCallOutputTokens);
      const turn = await this.#invoke(request, options.signal, true, async (chunk) => {
        if ((chunk.toolCallDeltas?.length ?? 0) > 0) {
          throw new AgentError("model_task_tool_call_unsupported", "Model task cannot call tools");
        }
        await options.onChunk?.(chunk);
      });
      const content = typeof turn.message.content === "string" ? turn.message.content : "";
      const reasoning = turn.message.reasoning ?? "";
      if (content.trim() !== "") {
        return Object.freeze({
          content,
          reasoning,
          finishReason: turn.finishReason,
          ...(turn.usage === undefined ? {} : { usage: turn.usage }),
          attempts,
          outputLimit: request.outputLimit,
        });
      }

      const remainingModelRounds = this.#recovery.maxAttempts("empty_model_response")
        - ledger.attempts("empty_model_response");
      const decision = ledger.decide({
        cause: "empty_model_response",
        action: "retry_model",
        scope: "model-task",
        remainingModelRounds,
        cancellationRequested: options.signal?.aborted === true,
      });
      await this.#authority?.publishRecoveryDecision(decision, attempts);
      if (!decision.allowed) {
        throw new AgentError("empty_model_response", "Model task returned no official response content");
      }
      active = Object.freeze([
        ...original,
        Object.freeze({
          role: "assistant" as const,
          content: "",
          ...(reasoning === "" ? {} : { reasoning }),
        }),
        Object.freeze({
          role: "developer" as const,
          content: EMPTY_RESPONSE_RETRY_GUIDANCE,
          attributes: Object.freeze({ recovery: true }),
        }),
      ]);
    }
  }

  #request(
    messages: readonly Message[],
    maxCallOutputTokens: number | undefined,
  ): ManagedModelRequest {
    const outputLimit = resolveInvocationOutputLimit(
      this.#capabilities,
      maxCallOutputTokens ?? this.#maxCallOutputTokens,
    );
    if (outputLimit === undefined) {
      throw new AgentError(
        "model_output_limit_unknown",
        "Model task requires capabilities with an exact output limit",
      );
    }
    const copied = Object.freeze(copyMessages(messages));
    if (estimateMessagesTokens(copied) + outputLimit.maxTokens > this.#capabilities!.contextWindowTokens) {
      throw new AgentError("model_task_input_exceeds_budget", "Model task input and output reserve exceed the model window");
    }
    return Object.freeze({
      messages: copied,
      tools: Object.freeze([]),
      ...(this.#capabilities === undefined ? {} : { capabilitySnapshot: this.#capabilities }),
      outputLimit,
    });
  }

  async #invoke(
    request: ManagedModelRequest,
    signal: AbortSignal | undefined,
    useStream: boolean,
    onChunk?: (chunk: ModelStreamChunk, receipt?: ModelInvocationReceipt) => Promise<void> | void,
    planning?: Pick<InvocationReceiptInput, "outputProtocol" | "planningScope" | "planningAttempt">,
    validateTurn?: () => void,
    diagnostics?: Record<string, JsonValue>,
    invocationTimeoutMs?: number,
  ): Promise<ModelTurn> {
    const evidence = this.#contextEvidence;
    const receipt = await this.#authority?.openInvocation({
      messages: request.messages,
      tools: request.tools,
      evidence,
      capabilityProfileId: this.#capabilities?.profileId ?? null,
      outputLimit: request.outputLimit.maxTokens,
      ...planning,
    });
    let operation: OperationReceipt | undefined;
    let chunkIndex = 0;
    let turn: ModelTurn;
    let latestUsage: ModelTokenUsage | undefined;
    let settled = false;
    let diagnosticsRecorded = false;
    try {
      await validateEvidence(this.#evidenceValidator, evidence, signal);
      operation = await this.#operations?.start("model", {
        runId: this.#runId,
        ...(receipt === undefined ? {} : { invocationId: receipt.invocationId }),
        ...(planning?.planningScope === undefined ? {} : { parentOperationId: planning.planningScope.operationId }),
      });
      turn = await invokeModel(
        this.#model,
        request,
        signal,
        useStream,
        async (chunk) => {
          latestUsage = chunk.usage ?? latestUsage;
          if (receipt !== undefined) {
            await this.#authority!.persistChunk(receipt, chunkIndex, chunk);
            chunkIndex += 1;
          }
          await onChunk?.(chunk, receipt);
        },
        invocationTimeoutMs === undefined
          ? this.#runtimeLimits
          : constrainModelInvocationTimeout(
            this.#runtimeLimits,
            invocationTimeoutMs,
          ),
        diagnostics === undefined ? undefined : (metrics) => { Object.assign(diagnostics, metrics); },
      );
      assertNoToolCalls(turn);
      validateTurn?.();
      if (signal?.aborted === true) throw new AgentCanceledError();
      if (receipt !== undefined && !useStream) {
        await this.#authority!.persistCompletion(receipt, turn);
      }
      if (receipt !== undefined && diagnostics !== undefined) {
        diagnosticsRecorded = true;
        await this.#authority!.recordModelDiagnostics?.(receipt, diagnostics);
      }
      if (receipt !== undefined) {
        settled = true;
        await this.#authority!.settleInvocation(receipt, "completed", turn);
      }
    } catch (error) {
      let failure = error;
      if (receipt !== undefined && !settled) {
        try {
          settled = true;
          await this.#authority!.settleInvocation(receipt, "failed", undefined, errorCode(error), latestUsage);
        } catch (terminationError) {
          if (planning !== undefined) failure = planningTerminationFailure(failure, terminationError);
        }
      }
      if (operation !== undefined) {
        try {
          if (signal?.aborted === true || error instanceof AgentCanceledError) {
            await this.#operations!.cancel(operation.operationId, errorCode(error));
          } else {
            await this.#operations!.fail(operation.operationId, errorCode(error));
          }
        } catch (terminationError) {
          if (planning !== undefined) failure = planningTerminationFailure(failure, terminationError);
        }
      }
      if (receipt !== undefined && diagnostics !== undefined && !diagnosticsRecorded) {
        try { await this.#authority!.recordModelDiagnostics?.(receipt, diagnostics); }
        catch { /* Keep the original invocation failure. */ }
      }
      throw failure;
    }
    if (operation !== undefined) await this.#operations!.succeed(operation.operationId);
    return turn;
  }
}

function copyEvidence(
  values: readonly ContextEvidenceReceipt[],
): readonly ContextEvidenceReceipt[] {
  if (!Array.isArray(values)) throw new TypeError("context evidence must be an array");
  const byId = new Map<string, ContextEvidenceReceipt>();
  for (const raw of values) {
    if (raw === null || typeof raw !== "object") throw new TypeError("Invalid context evidence");
    const evidenceId = requiredText(raw.evidenceId, "evidence id");
    const receipt = Object.freeze({
      evidenceId,
      ...(raw.contextBlock === undefined ? {} : { contextBlock: requiredText(raw.contextBlock, "evidence contextBlock") }),
      source: requiredText(raw.source, "evidence source"),
      ...(raw.itemId === undefined ? {} : { itemId: requiredText(raw.itemId, "evidence itemId") }),
      ...(raw.version === undefined ? {} : { version: requiredText(raw.version, "evidence version") }),
    });
    const existing = byId.get(evidenceId);
    if (existing !== undefined && JSON.stringify(existing) !== JSON.stringify(receipt)) {
      throw new TypeError(`Conflicting evidence id: ${evidenceId}`);
    }
    byId.set(evidenceId, receipt);
  }
  return Object.freeze([...byId.values()]);
}

async function validateEvidence(
  validator: ModelInputEvidenceValidator | undefined,
  receipts: readonly ContextEvidenceReceipt[],
  signal: AbortSignal | undefined,
): Promise<void> {
  if (validator === undefined || receipts.length === 0) return;
  throwIfAborted(signal);
  await validator.validateEvidence(
    receipts,
    signal === undefined ? {} : { signal },
  );
  throwIfAborted(signal);
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted === true) throw new AgentCanceledError();
}

function assertNoToolCalls(turn: ModelTurn): void {
  if ((turn.message.toolCalls?.length ?? 0) > 0 || turn.finishReason === "tool_calls") {
    throw new AgentError("model_task_tool_call_unsupported", "Model task cannot call tools");
  }
}

function assertAuthority(value: ModelTaskInvocationAuthority): void {
  if (
    typeof value.openInvocation !== "function"
    || typeof value.persistChunk !== "function"
    || typeof value.persistCompletion !== "function"
    || typeof value.settleInvocation !== "function"
    || typeof value.publishRecoveryDecision !== "function"
  ) {
    throw new TypeError("model task authority is invalid");
  }
}

function errorCode(error: unknown): string {
  return error instanceof AgentError ? error.code : "model_task_failed";
}

function planningTerminationFailure(error: unknown, terminationError: unknown): unknown {
  const repairableCodes = ["invalid_planner_output", "invalid_planning_stream"];
  if (!(error instanceof AgentError) || !repairableCodes.includes(error.code)) return error;
  return new AgentError(
    terminationError instanceof AgentError && !repairableCodes.includes(terminationError.code)
      ? terminationError.code : "output_persistence_failed",
    "Planning termination could not be persisted",
    { cause: new AggregateError([error, terminationError]) },
  );
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`${label} is required`);
  return value.trim();
}
