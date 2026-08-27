import { AgentOperationController } from "../operations/index.js";
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
import { invokeModel, type ModelStreamLimits } from "../model/stream.js";
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
  ): Promise<void>;
  publishRecoveryDecision(decision: RecoveryDecision, round: number): Promise<void>;
}

export interface ModelTaskRunnerOptions {
  readonly model: ModelGateway;
  readonly runId: string;
  readonly recovery?: RecoveryPolicy;
  readonly operations?: AgentOperationController;
  readonly authority?: ModelTaskInvocationAuthority;
  readonly runtimeLimits?: ModelStreamLimits;
}

export interface ModelTaskOptions {
  readonly maxCallOutputTokens?: number;
  readonly signal?: AbortSignal;
}

export interface ModelTaskStreamTextOptions extends ModelTaskOptions {
  readonly onChunk?: (chunk: ModelStreamChunk) => Promise<void> | void;
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
  }

  public get runId(): string {
    return this.#runId;
  }

  public async complete(
    messages: readonly Message[],
    options: ModelTaskOptions = {},
  ): Promise<ModelTaskCompletion> {
    const request = this.#request(messages, options.maxCallOutputTokens);
    const turn = await this.#invoke(request, options.signal, false);
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
      maxCallOutputTokens,
    );
    if (outputLimit === undefined) {
      throw new AgentError(
        "model_output_limit_unknown",
        "Model task requires capabilities with an exact output limit",
      );
    }
    return Object.freeze({
      messages: Object.freeze(copyMessages(messages)),
      tools: Object.freeze([]),
      ...(this.#capabilities === undefined ? {} : { capabilitySnapshot: this.#capabilities }),
      outputLimit,
    });
  }

  async #invoke(
    request: ManagedModelRequest,
    signal: AbortSignal | undefined,
    useStream: boolean,
    onChunk?: (chunk: ModelStreamChunk) => Promise<void> | void,
  ): Promise<ModelTurn> {
    const receipt = await this.#authority?.openInvocation({
      messages: request.messages,
      tools: request.tools,
      evidence: Object.freeze([]),
      capabilityProfileId: this.#capabilities?.profileId ?? null,
      outputLimit: request.outputLimit.maxTokens,
    });
    let operation: OperationReceipt | undefined;
    let chunkIndex = 0;
    let turn: ModelTurn;
    try {
      operation = await this.#operations?.start("model", {
        runId: this.#runId,
        ...(receipt === undefined ? {} : { invocationId: receipt.invocationId }),
      });
      turn = await invokeModel(
        this.#model,
        request,
        signal,
        useStream,
        async (chunk) => {
          if (receipt !== undefined) {
            await this.#authority!.persistChunk(receipt, chunkIndex, chunk);
            chunkIndex += 1;
          }
          await onChunk?.(chunk);
        },
        this.#runtimeLimits,
      );
      assertNoToolCalls(turn);
      if (receipt !== undefined && !useStream) {
        await this.#authority!.persistCompletion(receipt, turn);
      }
      if (receipt !== undefined) {
        await this.#authority!.settleInvocation(receipt, "completed", turn);
      }
    } catch (error) {
      if (receipt !== undefined) {
        try {
          await this.#authority!.settleInvocation(receipt, "failed", undefined, errorCode(error));
        } catch {
          // The terminal Run commit fences a receipt left open by persistence failure.
        }
      }
      if (operation !== undefined) {
        try {
          if (signal?.aborted === true || error instanceof AgentCanceledError) {
            await this.#operations!.cancel(operation.operationId, errorCode(error));
          } else {
            await this.#operations!.fail(operation.operationId, errorCode(error));
          }
        } catch {
          // Operation persistence failure must not replace the selected model outcome.
        }
      }
      throw error;
    }
    if (operation !== undefined) await this.#operations!.succeed(operation.operationId);
    return turn;
  }
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

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`${label} is required`);
  return value.trim();
}
