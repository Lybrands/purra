import { StructuredOutputContract, StructuredOutputError } from "../structured.js";
import { AgentOperationController } from "../operations/index.js";
import { estimateMessagesTokens, maxGenerationTokensForContext } from "../context/budget.js";
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
  modelFailureUsage,
  type ModelStreamLimits,
} from "../model/stream.js";
import type {
  InvocationOutputBudget,
  Message,
  ModelCapabilitySnapshot,
  ModelGateway,
  ModelRequest,
  ModelStreamChunk,
  ModelTokenUsage,
  ModelTurn,
  ResultCapacitySource,
} from "../model/types.js";
import {
  copyCapabilitySnapshot,
  copyMessages,
  constrainOutputBudgetToContext,
  resolveInvocationOutputBudget,
} from "../model/validation.js";

type ManagedModelRequest = ModelRequest & { readonly outputBudget: InvocationOutputBudget };

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
  readonly signal?: AbortSignal;
  readonly maxGenerationTokens?: number;
  readonly evidenceValidator?: ModelInputEvidenceValidator;
}

export interface ModelTaskOptions {
  readonly resultCapacityTargetTokens?: number;
  readonly resultCapacitySource?: ResultCapacitySource;
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
  readonly outputBudget: InvocationOutputBudget;
}

export interface ModelTaskTextResult {
  readonly content: string;
  readonly reasoning: string;
  readonly finishReason: ModelTurn["finishReason"];
  readonly usage?: ModelTokenUsage;
  readonly attempts: number;
  readonly outputBudget: InvocationOutputBudget;
}

export interface StructuredInvocationRef {
  readonly usageState: "reported" | "unknown";
  readonly invocationId: string;
  readonly outputBudget: InvocationOutputBudget;
  readonly usage?: ModelTokenUsage;
  readonly dispatched: boolean;
  readonly settled: boolean;
  readonly errorCode?: string;
}
export interface StructuredModelTaskReceipt {
  readonly runId: string;
  readonly outputContract: Readonly<Record<string, JsonValue>>;
  readonly invocationRefs: readonly StructuredInvocationRef[];
  readonly attempts: number;
  readonly outputBudget: InvocationOutputBudget;
  readonly usage?: ModelTokenUsage;
  readonly usageState: "reported" | "unknown";
  readonly persistence: "bound" | "none";
  readonly rootBudget: "bound" | "not_bound";
  readonly validation: "passed";
}
export interface StructuredModelTaskResult {
  readonly value: Readonly<Record<string, JsonValue>>;
  readonly receipt: StructuredModelTaskReceipt;
}
export interface StructuredModelTaskOptions extends ModelTaskOptions {
  readonly output: StructuredOutputContract;
  readonly repairAttempts?: number;
}
interface StructuredInvocation {
  readonly task: NonNullable<InvocationReceiptInput["structuredTask"]>;
  readonly identity: Readonly<Record<string, JsonValue>>;
  readonly onAttempt: (ref: StructuredInvocationRef) => void;
}

export class ModelTaskRunner {
  readonly #model: ModelGateway;
  readonly #signal: AbortSignal | undefined;
  readonly #runId: string;
  readonly #capabilities: ModelCapabilitySnapshot;
  readonly #recovery: RecoveryPolicy;
  readonly #operations: AgentOperationController | undefined;
  readonly #authority: ModelTaskInvocationAuthority | undefined;
  readonly #runtimeLimits: ModelStreamLimits | undefined;
  readonly #maxGenerationTokens: number | undefined;
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
    this.#signal = options.signal;
    if (Object.prototype.hasOwnProperty.call(options, "maxCallOutputTokens")) {
      throw new AgentError(
        "model_output_budget_invalid",
        "Legacy model-task output limits are unsupported",
      );
    }
    if (options.model.capabilities === undefined) {
      throw new AgentError(
        "model_generation_limit_unknown",
        "Model tasks require a verified model capability snapshot",
      );
    }
    this.#capabilities = copyCapabilitySnapshot(options.model.capabilities);
    if (this.#capabilities.maxGenerationTokens === null) {
      throw new AgentError(
        "model_generation_limit_unknown",
        "Model capabilities do not declare a verified generation limit",
      );
    }
    if (this.#capabilities.actionable === false) {
      throw new AgentError("model_capability_incompatible", "Model capability snapshot is not actionable");
    }
    this.#recovery = options.recovery ?? new RecoveryPolicy();
    this.#operations = options.operations;
    this.#authority = options.authority;
    this.#runtimeLimits = options.runtimeLimits;
    this.#maxGenerationTokens = options.maxGenerationTokens;
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
    const request = this.#request(messages, options);
    const turn = await this.#invoke(request, options.signal, false);
    return Object.freeze({ turn, outputBudget: request.outputBudget });
  }

  public async completeStructured(messages: readonly Message[], options: StructuredModelTaskOptions): Promise<StructuredModelTaskResult> {
    const output = options.output;
    if (!(output instanceof StructuredOutputContract)) throw new StructuredOutputError("structured_output_schema_invalid", "schema");
    const repairs = options.repairAttempts ?? 0;
    if (!Number.isSafeInteger(repairs) || repairs < 0) throw new TypeError("repairAttempts must be a non-negative safe integer");
    const signal = this.#signal === undefined ? options.signal : options.signal === undefined
      ? this.#signal : AbortSignal.any([this.#signal, options.signal]);
    const ledger = new RecoveryLedger(new RecoveryPolicy({ structured_output_invalid: repairs }));
    const refs: StructuredInvocationRef[] = [];
    const taskId = globalThis.crypto.randomUUID();
    const original = copyMessages(messages);
    let value: Readonly<Record<string, JsonValue>> | undefined;
    while (true) {
      try {
        throwIfAborted(signal);
        const feedback: readonly Message[] = refs.length === 0 ? [] : [{ role: "developer", content: "Produce a new complete JSON object satisfying the schema; the previous response failed format validation." }];
        const formatted = [...original, ...feedback, { role: "developer" as const, content: output.instruction() }];
        const budgetMessages: readonly Message[] = output.mode === "native_required"
          ? [...formatted, { role: "developer", content: JSON.stringify(output.schema) }] : formatted;
        const request = Object.freeze({ ...this.#request(formatted, options, budgetMessages), outputContract: output });
        let dialect: string | undefined;
        if (output.mode === "native_required" && (this.#capabilities.protocol.jsonSchemaLevel !== "json_schema" || this.#model.validateOutputContract === undefined)) {
          throw new StructuredOutputError("structured_output_mode_unsupported", "mode");
        }
        dialect = this.#model.validateOutputContract?.(request);
        if (output.mode === "native_required" && (typeof dialect !== "string" || !dialect.trim() || dialect.length > 128)) {
          throw new StructuredOutputError("structured_output_mode_unsupported", "dialect");
        }
        if (output.mode === "native_required" && estimateMessagesTokens([...formatted,
          { role: "developer", content: JSON.stringify(output.schema) }]) + request.outputBudget.maxGenerationTokens > this.#capabilities.contextWindowTokens) {
          throw new AgentError("model_task_input_exceeds_budget", "Native schema exceeds model input budget");
        }
        const identity = Object.freeze({ ...output.identity(), ...(dialect === undefined ? {} : { nativeDialect: dialect }) });
        await this.#invoke(request, signal, false, undefined, undefined, (turn) => {
          if (turn.finishReason !== "stop") throw new AgentError("model_task_tool_call_unsupported", "Structured task requires normal termination");
          value = output.parse(turn.message.content as string);
        }, undefined, undefined, { identity, task: Object.freeze({ taskId, attempt: refs.length + 1, previousInvocationId: refs.at(-1)?.invocationId ?? null }), onAttempt: (ref) => refs.push(ref) });
        const usage = sumStructuredUsage(refs);
        return Object.freeze({ value: value!, receipt: Object.freeze({
          runId: this.#runId, outputContract: identity, invocationRefs: Object.freeze([...refs]),
          attempts: refs.length, outputBudget: request.outputBudget,
          ...(usage === undefined ? {} : { usage }), usageState: usage === undefined ? "unknown" as const : "reported" as const,
          persistence: this.#authority === undefined ? "none" as const : "bound" as const,
          rootBudget: this.#authority === undefined ? "not_bound" as const : "bound" as const, validation: "passed" as const,
        }) });
      } catch (error) {
        if (error instanceof Error) Object.defineProperty(error, "invocationRefs", { value: Object.freeze([...refs]), configurable: true });
        const last = refs.at(-1);
        if (!(error instanceof StructuredOutputError) || !["structured_output_invalid_json", "structured_output_schema_mismatch"].includes(error.code)
          || last?.settled !== true || last.usage === undefined) throw error;
        const decision = ledger.decide({ cause: "structured_output_invalid", action: "retry_model",
          remainingModelRounds: repairs - refs.length + 1, cancellationRequested: signal?.aborted === true });
        if (!decision.allowed) throw error;
        try { await this.#authority?.publishRecoveryDecision(decision, refs.length); }
        catch (error) {
          if (error instanceof Error) Object.defineProperty(error, "invocationRefs", { value: Object.freeze([...refs]), configurable: true });
          throw error;
        }
      }
    }
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
    const request = this.#request(messages, options);
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
    return Object.freeze({ turn, outputBudget: request.outputBudget });
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
      const request = this.#request(active, options);
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
          outputBudget: request.outputBudget,
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
    options: ModelTaskOptions,
    budgetMessages?: readonly Message[],
  ): ManagedModelRequest {
    if (
      Object.prototype.hasOwnProperty.call(options, "maxGenerationTokens")
      || Object.prototype.hasOwnProperty.call(options, "maxCallOutputTokens")
    ) {
      throw new AgentError(
        "model_output_budget_invalid",
        "Model task generation allowance belongs to its bound Run request",
      );
    }
    const baseOutputBudget = resolveInvocationOutputBudget(
      this.#capabilities,
      {
        ...(this.#maxGenerationTokens === undefined
          ? {}
          : { maxGenerationTokens: this.#maxGenerationTokens, generationSource: "user" as const }),
        ...(options.resultCapacityTargetTokens === undefined
          ? {}
          : {
              resultCapacityTargetTokens: options.resultCapacityTargetTokens,
              resultCapacitySource: options.resultCapacitySource ?? "user",
            }),
      },
    );
    const physicalMaximum = budgetMessages === undefined ? undefined
      : this.#capabilities.contextWindowTokens - estimateMessagesTokens(budgetMessages);
    if (physicalMaximum !== undefined && physicalMaximum <= 0) throw new AgentError("model_task_input_exceeds_budget", "Compiled structured input leaves no generation capacity");
    let outputBudget: InvocationOutputBudget;
    try {
      outputBudget = constrainOutputBudgetToContext(
        baseOutputBudget,
        physicalMaximum ?? maxGenerationTokensForContext({
          windowTokens: this.#capabilities.contextWindowTokens,
          tools: [],
        }),
      );
    } catch (error) {
      if (error instanceof AgentError && error.code === "fixed_reserves_exceed_window") {
        throw new AgentError(
          "model_task_input_exceeds_budget",
          "Model task input and output reserve exceed the model window",
          { cause: error },
        );
      }
      throw error;
    }
    const copied = Object.freeze(copyMessages(messages));
    if (estimateMessagesTokens(copied) + outputBudget.maxGenerationTokens > this.#capabilities.contextWindowTokens) {
      throw new AgentError("model_task_input_exceeds_budget", "Model task input and output reserve exceed the model window");
    }
    return Object.freeze({
      messages: copied,
      tools: Object.freeze([]),
      capabilitySnapshot: this.#capabilities,
      outputBudget,
    });
  }

  async #invoke(
    request: ManagedModelRequest,
    signal: AbortSignal | undefined,
    useStream: boolean,
    onChunk?: (chunk: ModelStreamChunk, receipt?: ModelInvocationReceipt) => Promise<void> | void,
    planning?: Pick<InvocationReceiptInput, "outputProtocol" | "planningScope" | "planningAttempt">,
    validateTurn?: (turn: ModelTurn) => void,
    diagnostics?: Record<string, JsonValue>,
    invocationTimeoutMs?: number,
    structured?: StructuredInvocation,
  ): Promise<ModelTurn> {
    const evidence = this.#contextEvidence;
    const receipt = await this.#authority?.openInvocation({
      messages: request.messages,
      tools: request.tools,
      evidence,
      capabilityProfileId: this.#capabilities?.profileId ?? null,
      outputBudget: request.outputBudget,
      ...planning,
      ...(structured === undefined ? {} : { outputContract: structured.identity, structuredTask: structured.task }),
    });
    let operation: OperationReceipt | undefined;
    let chunkIndex = 0;
    let turn: ModelTurn;
    let latestUsage: ModelTokenUsage | undefined;
    let settled = false;
    let diagnosticsRecorded = false;
    let fullySettled = false;
    let dispatched = false;
    let validated = false;
    const invocationId = receipt?.invocationId ?? globalThis.crypto.randomUUID();
    const report = (error?: unknown) => structured?.onAttempt(Object.freeze({
      invocationId, outputBudget: request.outputBudget, dispatched, settled: fullySettled,
      usageState: latestUsage === undefined ? "unknown" : "reported",
      ...(latestUsage === undefined ? {} : { usage: latestUsage }),
      ...(error === undefined ? {} : { errorCode: errorCode(error) }),
    }));
    try {
      await validateEvidence(this.#evidenceValidator, evidence, signal);
      operation = await this.#operations?.start("model", {
        runId: this.#runId,
        ...(receipt === undefined ? {} : { invocationId: receipt.invocationId }),
        ...(planning?.planningScope === undefined ? {} : { parentOperationId: planning.planningScope.operationId }),
      });
      throwIfAborted(signal);
      dispatched = true;
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
      latestUsage = turn.usage ?? latestUsage;
      assertNoToolCalls(turn);
      validateTurn?.(turn);
      validated = true;
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
      if (operation !== undefined) await this.#operations!.succeed(operation.operationId);
      fullySettled = true;
    } catch (error) {
      let failure = error;
      latestUsage ??= modelFailureUsage(error);
      if (receipt !== undefined && !settled) {
        try {
          settled = true;
          await this.#authority!.settleInvocation(
            receipt,
            "failed",
            undefined,
            errorCode(error),
            latestUsage ?? modelFailureUsage(error),
          );
          fullySettled = true;
        } catch (terminationError) {
          fullySettled = false;
          if (structured !== undefined) failure = new AgentError("model_task_settlement_failed", "Structured task settlement failed");
          else if (planning !== undefined) failure = planningTerminationFailure(failure, terminationError);
        }
      }
      if (receipt === undefined) fullySettled = true;
      if (operation !== undefined) {
        try {
          if (signal?.aborted === true || error instanceof AgentCanceledError) {
            await this.#operations!.cancel(operation.operationId, errorCode(error));
          } else {
            await this.#operations!.fail(operation.operationId, errorCode(error));
          }
        } catch (terminationError) {
          fullySettled = false;
          if (structured !== undefined) failure = new AgentError("model_task_settlement_failed", "Structured task settlement failed");
          else if (planning !== undefined) failure = planningTerminationFailure(failure, terminationError);
        }
      }
      if (receipt !== undefined && diagnostics !== undefined && !diagnosticsRecorded) {
        try { await this.#authority!.recordModelDiagnostics?.(receipt, diagnostics); }
        catch { /* Keep the original invocation failure. */ }
      }
      if (structured !== undefined && validated) fullySettled = false;
      report(failure);
      throw failure;
    }
    report();
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

function sumStructuredUsage(refs: readonly StructuredInvocationRef[]): ModelTokenUsage | undefined {
  if (refs.some((ref) => ref.usage === undefined)) return undefined;
  const usages = refs.map((ref) => ref.usage!);
  const fields = ["inputTokens", "generationTokens", "totalTokens", "cachedInputTokens", "reasoningTokens"] as const;
  return Object.freeze(Object.fromEntries(fields.flatMap((key) => {
    const values = usages.map((usage) => usage[key]);
    return values.some((value) => value === undefined) ? [] : [[key, values.reduce<number>((a, b) => a + b!, 0)]];
  }))) as unknown as ModelTokenUsage;
}
