import { prepareContext, resolveContextFactories } from "../context/coordinator.js";
import type { PreparedContext } from "../context/types.js";
import { ModelTaskRunner } from "../extensions/model-tasks.js";
import { invokeModel, type ModelStreamLimits } from "../model/stream.js";
import type { Message, ModelTurn, ToolSpec } from "../model/types.js";
import {
  copyCapabilitySnapshot,
  copyJsonValue,
  resolveInvocationOutputLimit,
  throwForIncompleteFinish,
} from "../model/validation.js";
import type { RunSession } from "../run/session.js";
import { AgentOperationController } from "../operations/index.js";
import { RecoveryPolicy } from "../recovery/index.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import { publicPresentationMessages } from "../shared/public-presentation.js";
import { ToolCatalog } from "../tools/catalog.js";
import type { ToolDefinition, ToolExecutionEvent } from "../tools/types.js";
import type {
  DelegatedAgentRequest,
  DelegatedAgentResult,
  DynamicDelegatedAgentExecutorOptions,
} from "./types.js";

export class DynamicDelegatedAgentExecutor {
  readonly #model: DynamicDelegatedAgentExecutorOptions["model"];
  readonly #capabilities: import("../model/types.js").ModelCapabilitySnapshot | undefined;
  readonly #catalog: ToolCatalog;
  readonly #readTools: ReadonlySet<string>;
  readonly #context: DynamicDelegatedAgentExecutorOptions["context"];
  readonly #maxRounds: number;
  readonly #recovery: RecoveryPolicy;
  readonly #operations: AgentOperationController | undefined;
  readonly #sessions = new Map<string, RunSession>();
  readonly #useStream: boolean;
  readonly #runtimeLimits: ModelStreamLimits | undefined;

  public constructor(options: DynamicDelegatedAgentExecutorOptions) {
    if (typeof options.model?.invoke !== "function") {
      throw new TypeError("Delegated Agent executor requires a model gateway");
    }
    const tools = Object.freeze((options.tools ?? []).filter((tool) => (
      tool.enabled !== false
      && tool.policy?.mode === "read"
      && tool.name !== "delegateToAgents"
    )));
    this.#model = options.model;
    this.#capabilities = options.model.capabilities === undefined
      ? undefined
      : copyCapabilitySnapshot(options.model.capabilities);
    this.#catalog = new ToolCatalog(tools);
    this.#readTools = new Set(tools.map((tool) => tool.name));
    this.#context = options.context;
    this.#maxRounds = positive(options.maxRounds ?? 8, "delegation maxRounds");
    if (options.recovery !== undefined && !(options.recovery instanceof RecoveryPolicy)) {
      throw new TypeError("delegation recovery must be a RecoveryPolicy");
    }
    if (options.operations !== undefined && !(options.operations instanceof AgentOperationController)) {
      throw new TypeError("delegation operations must be an AgentOperationController");
    }
    this.#recovery = options.recovery ?? new RecoveryPolicy();
    this.#operations = options.operations;
    this.#runtimeLimits = options.runtimeLimits;
    this.#useStream = typeof options.model.stream === "function"
      && this.#capabilities?.protocol.streaming !== "unavailable";
  }

  public bindRun(runId: string, session: RunSession): void {
    const id = requiredText(runId, "delegation Run id");
    if (this.#sessions.has(id)) throw new AgentError("delegation_root_run_already_bound", "Root Run is already bound");
    this.#sessions.set(id, session);
  }

  public releaseRun(runId: string): void {
    this.#sessions.delete(runId);
  }

  public async execute(
    request: DelegatedAgentRequest,
    signal?: AbortSignal,
  ): Promise<DelegatedAgentResult> {
    const session = this.#sessions.get(request.runId);
    if (session === undefined) {
      return Object.freeze({ outcome: "failed", errorCode: "delegation_root_run_not_active" });
    }
    if (request.contextMode !== "isolated") {
      return Object.freeze({ outcome: "failed", errorCode: "delegation_context_mode_not_allowed" });
    }
    const enabledTools = Object.freeze((request.enabledTools ?? [...this.#readTools])
      .filter((name) => this.#readTools.has(name)));
    const tools = this.#catalog.specsFor(enabledTools);
    const messages: Message[] = [
      Object.freeze({
        role: "system",
        content: request.agentInstruction,
        attributes: Object.freeze({
          delegatedAgentDefinition: "model",
          delegatedAgentName: request.agentName,
        }),
      }),
      Object.freeze({
        role: "user",
        content: request.objective + inputSuffix(request.input),
      }),
    ];
    const outputLimit = resolveInvocationOutputLimit(this.#capabilities, undefined);
    const contextOptions = this.#contextForExecution(request.runId, session);
    const context = contextOptions === undefined
      ? undefined
      : await prepareContext(contextOptions, {
          request: {
            messages,
            enabledTools,
            metadata: {
              delegationId: request.delegationId,
              delegationBatchId: request.batchId,
            },
          },
          tools,
          windowTokens: this.#capabilities?.contextWindowTokens ?? 128_000,
          outputReserveTokens: outputLimit?.maxTokens ?? 4_096,
          ...(signal === undefined ? {} : { signal }),
        });
    try {
      return await this.#run(request, session, messages, tools, enabledTools, context, signal);
    } catch (error) {
      if (signal?.aborted || error instanceof AgentCanceledError) {
        return Object.freeze({ outcome: "canceled", errorCode: "delegation_canceled" });
      }
      return Object.freeze({
        outcome: "failed",
        errorCode: error instanceof AgentError ? error.code : "delegation_failed",
      });
    }
  }

  #contextForExecution(runId: string, session: RunSession) {
    const options = this.#context;
    if (options === undefined) return undefined;
    if (options.providerFactory === undefined && options.compressionFactory === undefined) return options;
    return resolveContextFactories(options, new ModelTaskRunner({
      model: this.#model,
      runId,
      recovery: this.#recovery,
      authority: session,
      ...(this.#runtimeLimits === undefined ? {} : { runtimeLimits: this.#runtimeLimits }),
      ...(this.#operations === undefined ? {} : { operations: this.#operations }),
    }));
  }

  async #run(
    request: DelegatedAgentRequest,
    session: RunSession,
    messages: Message[],
    tools: readonly ToolSpec[],
    enabledTools: readonly string[],
    context: PreparedContext | undefined,
    signal: AbortSignal | undefined,
  ): Promise<DelegatedAgentResult> {
    const outputLimit = resolveInvocationOutputLimit(this.#capabilities, undefined);
    let publicPresentationPending = false;
    let roundLimit = this.#maxRounds;
    for (let round = 1; round <= roundLimit; round += 1) {
      if (signal?.aborted) throw new AgentCanceledError();
      const projected = context === undefined
        ? Object.freeze([...messages])
        : await context.project(messages, signal);
      const modelRequest = Object.freeze({
        messages: projected,
        tools: publicPresentationPending ? Object.freeze([]) : tools,
        ...(this.#capabilities === undefined ? {} : { capabilitySnapshot: this.#capabilities }),
        ...(outputLimit === undefined ? {} : { outputLimit }),
      });
      const receipt = await session.openInvocation({
        messages: modelRequest.messages,
        tools: modelRequest.tools,
        evidence: context?.evidence ?? [],
        capabilityProfileId: this.#capabilities?.profileId ?? null,
        outputLimit: outputLimit?.maxTokens ?? null,
      });
      let turn: ModelTurn;
      let chunkIndex = 0;
      try {
        turn = await invokeModel(
          this.#model,
          modelRequest,
          signal,
          this.#useStream,
          async (chunk) => {
            await session.persistChunk(receipt, chunkIndex, chunk);
            chunkIndex += 1;
          },
          this.#runtimeLimits,
        );
        if (!this.#useStream) await session.persistCompletion(receipt, turn);
        await session.settleInvocation(receipt, "completed", turn);
      } catch (error) {
        try {
          await session.settleInvocation(
            receipt,
            "failed",
            undefined,
            error instanceof AgentError ? error.code : "delegation_failed",
          );
        } catch {
          // Root Run settlement owns any invocation left open by a repository failure.
        }
        throw error;
      }
      messages.push(turn.message);
      const calls = turn.message.toolCalls ?? [];
      throwForIncompleteFinish(turn.finishReason, calls.length);
      if (publicPresentationPending && calls.length > 0) {
        throw new AgentError(
          "tool_call_during_public_presentation",
          "Tool calls are forbidden during public presentation",
        );
      }
      if (calls.length === 0) {
        if (turn.finishReason === "tool_calls") {
          throw new AgentError("invalid_model_response", "finishReason=tool_calls requires a tool call");
        }
        if (turn.message.content === null || (typeof turn.message.content === "string" && turn.message.content.trim() === "")) {
          throw new AgentError("empty_model_response", "Delegated Agent returned an empty response");
        }
        if (tools.length > 0 && !publicPresentationPending) {
          messages.pop();
          messages.push(...publicPresentationMessages(turn.message));
          publicPresentationPending = true;
          roundLimit += 1;
          continue;
        }
        return Object.freeze({
          outcome: "completed",
          content: copyJsonValue(turn.message.content),
        });
      }
      if (turn.finishReason !== "tool_calls" && turn.finishReason !== "stop") {
        throw new AgentError("invalid_model_response", "Delegated tool calls require a complete model turn");
      }
      const batch = await this.#catalog.executeBatch(calls, {
        executionKey: `${request.runId}:delegation:${request.delegationId}`,
        enabledTools,
        ...(signal === undefined ? {} : { signal }),
        onEvent: (event: ToolExecutionEvent) => session.publishDelegatedTool(
          request.delegationId,
          event,
          round,
        ),
      });
      messages.push(...batch.messages);
    }
    throw new AgentError("max_rounds_exceeded", "Delegated Agent exceeded its model round limit");
  }
}

function inputSuffix(input: Readonly<Record<string, import("../model/types.js").JsonValue>>): string {
  return Object.keys(input).length === 0 ? "" : `\n\nInput:\n${JSON.stringify(input)}`;
}

function positive(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${label} must be positive`);
  return value;
}

function requiredText(value: unknown, label: string): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "") throw new TypeError(`${label} must be non-empty text`);
  return text;
}
