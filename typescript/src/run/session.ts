import type { JsonValue, ModelStreamChunk, ModelTurn } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import type {
  OutputEvent,
  OutputEventDraft,
  OutputEventQuery,
  OutputPolicy,
  OutputPublisher,
} from "../output/types.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import { stableFingerprint } from "../shared/fingerprint.js";
import type { ToolExecutionEvent } from "../tools/types.js";
import type { DelegationLifecycleEvent } from "../delegation/types.js";
import type { RunRepository } from "./store.js";
import type {
  InvocationReceiptInput,
  ModelInvocationReceipt,
  RunBeginParams,
  RunCancellationReceipt,
  RunCommand,
  RunHandle,
  RunResult,
  RunSnapshot,
} from "./types.js";
import type { WorkPlan } from "../planning/types.js";
import type {
  DurableRecoverySnapshot,
  LongTaskDispatchReceipt,
  LongTaskExecutionUpdate,
  TaskAdmissionDecision,
} from "../durable/types.js";
import type { RecoveryDecision } from "../recovery/index.js";
import { recoveryDecisionDetails } from "../recovery/index.js";

export class RunSession {
  readonly #repository: RunRepository;
  readonly #publisher: OutputPublisher;
  readonly #policy: OutputPolicy;
  readonly #controller = new AbortController();
  readonly #runId: string;
  #deadlineExceeded = false;
  #deadlineTimer: number | undefined;

  private constructor(
    repository: RunRepository,
    publisher: OutputPublisher,
    policy: OutputPolicy,
    runId: string,
    deadlineAt: string | null,
  ) {
    this.#repository = repository;
    this.#publisher = publisher;
    this.#policy = policy;
    this.#runId = runId;
    if (deadlineAt !== null) {
      const delay = Math.max(0, Date.parse(deadlineAt) - Date.now());
      this.#deadlineTimer = globalThis.setTimeout(() => {
        this.#deadlineExceeded = true;
        this.#controller.abort();
      }, delay);
    }
  }

  public static async begin(
    repository: RunRepository,
    publisher: OutputPublisher,
    policy: OutputPolicy,
    params: RunBeginParams,
  ): Promise<RunSession> {
    const begun = await repository.begin(params);
    await publisher.publishCommitted(begun.event);
    return new RunSession(
      repository,
      publisher,
      policy,
      begun.snapshot.runId,
      begun.snapshot.deadlineAt,
    );
  }

  public get runId(): string {
    return this.#runId;
  }

  public get signal(): AbortSignal {
    return this.#controller.signal;
  }

  public get deadlineExceeded(): boolean {
    return this.#deadlineExceeded;
  }

  public handle(result: Promise<RunResult>): RunHandle {
    return Object.freeze({
      runId: this.#runId,
      result,
      snapshot: () => this.#repository.get(this.#runId),
      cancel: () => this.cancel(),
      command: (command: RunCommand) => {
        if (command?.type !== "cancel") throw new TypeError("Unsupported Run command");
        return this.cancel();
      },
      events: (query?: OutputEventQuery) => this.events(query),
    });
  }

  public snapshot(): Promise<RunSnapshot> {
    return this.#repository.get(this.#runId);
  }

  public async openInvocation(input: InvocationReceiptInput): Promise<ModelInvocationReceipt> {
    const invocationId = globalThis.crypto.randomUUID();
    const [messageFingerprint, toolFingerprint, evidenceFingerprint] = await Promise.all([
      stableFingerprint(copyJsonValue(input.messages)),
      stableFingerprint(copyJsonValue(input.tools)),
      stableFingerprint(copyJsonValue(input.evidence)),
    ]);
    const requestFingerprint = await stableFingerprint({
      messageFingerprint,
      toolFingerprint,
      evidenceFingerprint,
      capabilityProfileId: input.capabilityProfileId,
      outputLimit: input.outputLimit,
    });
    const opened = await this.#repository.openInvocation(this.#runId, {
      schemaVersion: 1,
      runId: this.#runId,
      invocationId,
      messageFingerprint,
      toolFingerprint,
      requestFingerprint,
      evidenceFingerprint,
      contextEvidence: Object.freeze(input.evidence.map((receipt) => Object.freeze({ ...receipt }))),
      capabilityProfileId: input.capabilityProfileId,
      outputLimit: input.outputLimit,
    });
    await this.#publisher.publishCommitted(opened.event);
    return opened.receipt;
  }

  public async persistChunk(
    receipt: ModelInvocationReceipt,
    index: number,
    chunk: ModelStreamChunk,
  ): Promise<void> {
    if (chunk.reasoningDelta !== undefined && chunk.reasoningDelta !== "") {
      await this.#persist({
        sourceKey: `invocation:${receipt.invocationId}:chunk:${index}:reasoning`,
        kind: "reasoning.delta",
        channel: "reasoning",
        visibility: "private",
        payload: { delta: chunk.reasoningDelta },
      });
    }
    if (
      chunk.contentDelta !== undefined
      || chunk.toolCallDeltas !== undefined
      || chunk.finishReason !== undefined
      || chunk.usage !== undefined
    ) {
      await this.#persist({
        sourceKey: `invocation:${receipt.invocationId}:chunk:${index}:model`,
        kind: "model.delta",
        channel: "model",
        visibility: "private",
        payload: copyJsonValue({
          ...(chunk.contentDelta === undefined ? {} : { contentDelta: chunk.contentDelta }),
          ...(chunk.toolCallDeltas === undefined ? {} : { toolCallDeltas: chunk.toolCallDeltas }),
          ...(chunk.finishReason === undefined ? {} : { finishReason: chunk.finishReason }),
          ...(chunk.usage === undefined ? {} : { usage: chunk.usage }),
        }) as Readonly<Record<string, JsonValue>>,
      });
    }
  }

  public async persistCompletion(
    receipt: ModelInvocationReceipt,
    turn: ModelTurn,
  ): Promise<void> {
    await this.#persist({
      sourceKey: `invocation:${receipt.invocationId}:completion`,
      kind: "model.completed",
      channel: "model",
      visibility: "private",
      payload: copyJsonValue({
        content: turn.message.content,
        ...(turn.message.reasoning === undefined ? {} : { reasoning: turn.message.reasoning }),
        ...(turn.message.toolCalls === undefined ? {} : { toolCalls: turn.message.toolCalls }),
        finishReason: turn.finishReason,
        ...(turn.usage === undefined ? {} : { usage: turn.usage }),
      }) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async settleInvocation(
    receipt: ModelInvocationReceipt,
    status: "completed" | "failed",
    turn?: ModelTurn,
    errorCode?: string,
  ): Promise<void> {
    const settled = await this.#repository.settleInvocation(this.#runId, {
      invocationId: receipt.invocationId,
      status,
      ...(turn?.usage === undefined ? {} : { usage: turn.usage }),
      ...(errorCode === undefined ? {} : { errorCode }),
    });
    await this.#publisher.publishCommitted(settled.event);
    if (settled.budgetError !== undefined) {
      throw new AgentError(settled.budgetError, "Run token budget is exhausted");
    }
  }

  public async publishCommentary(receipt: ModelInvocationReceipt, content: JsonValue): Promise<void> {
    await this.#persist({
      sourceKey: `invocation:${receipt.invocationId}:commentary`,
      kind: "commentary",
      channel: "commentary",
      visibility: "public",
      payload: { content },
    });
  }

  public async publishTool(event: ToolExecutionEvent, round: number): Promise<void> {
    await this.#persist({
      sourceKey: `tool:${round}:${event.toolCallId}:${event.type}`,
      kind: event.type === "tool_started" ? "tool.started" : "tool.completed",
      channel: "tool",
      visibility: "public",
      payload: copyJsonValue(event) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async publishDelegatedTool(
    delegationId: string,
    event: ToolExecutionEvent,
    round: number,
  ): Promise<void> {
    await this.#persist({
      sourceKey: `delegation:${delegationId}:tool:${round}:${event.toolCallId}:${event.type}`,
      kind: event.type === "tool_started" ? "tool.started" : "tool.completed",
      channel: "tool",
      visibility: "public",
      payload: copyJsonValue({ ...event, delegationId }) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async publishDelegation(event: DelegationLifecycleEvent): Promise<void> {
    if (event.runId !== this.#runId) {
      throw new AgentError("delegation_scope_violation", "Delegation event belongs to another Root Run");
    }
    await this.#persist({
      sourceKey: `delegation:${event.batchId}:${event.delegationId}:${event.status}`,
      kind: "delegation.status",
      channel: "lifecycle",
      visibility: "public",
      payload: copyJsonValue({
        batchId: event.batchId,
        delegationId: event.delegationId,
        agentName: event.agentName,
        agentTitle: event.agentTitle,
        status: event.status,
        ...(event.errorCode === undefined ? {} : { errorCode: event.errorCode }),
      }) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async publishPlan(plan: WorkPlan, revision: number): Promise<void> {
    await this.#persist({
      sourceKey: `plan:${revision}`,
      kind: "plan.updated",
      channel: "plan",
      visibility: "public",
      payload: copyJsonValue({ revision, plan }) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async publishAdmission(decision: TaskAdmissionDecision, continuation = false): Promise<void> {
    await this.#persist({
      sourceKey: `admission:${continuation ? "continuation" : "initial"}`,
      kind: "task_admission.decided",
      channel: "lifecycle",
      visibility: "public",
      payload: copyJsonValue({
        mode: decision.mode,
        reasonCode: decision.reasonCode,
        estimatedUnits: decision.estimatedUnits ?? 1,
        estimatedModelCalls: decision.estimatedModelCalls ?? 1,
        requiresConfirmation: decision.requiresConfirmation ?? false,
        coveredStepIds: decision.coveredStepIds ?? [],
        continuation,
      }) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async publishRecoveryDecision(decision: RecoveryDecision, round: number): Promise<void> {
    await this.#persist({
      sourceKey: `recovery:${round}:${decision.cause}:${decision.scope}:${decision.attempt}:${decision.reasonCode}`,
      kind: "agentRunTrace",
      channel: "lifecycle",
      visibility: "private",
      payload: copyJsonValue({
        stage: "recovery_decision",
        outcome: decision.allowed ? "allowed" : "denied",
        details: { round, ...recoveryDecisionDetails(decision) },
      }) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async publishDurableDispatch(
    receipt: LongTaskDispatchReceipt,
    snapshot: DurableRecoverySnapshot,
  ): Promise<void> {
    await this.#persist({
      sourceKey: `long-task:${receipt.taskId}:dispatched`,
      kind: "long_task.dispatched",
      channel: "lifecycle",
      visibility: "public",
      payload: copyJsonValue({
        taskId: receipt.taskId,
        message: receipt.message,
        recipeFingerprint: receipt.recipeFingerprint,
      }) as Readonly<Record<string, JsonValue>>,
    });
    await this.#persist({
      sourceKey: `long-task:${receipt.taskId}:recovery:${this.#runId}`,
      kind: "durable.recovery_snapshot",
      channel: "lifecycle",
      visibility: "private",
      payload: copyJsonValue({ snapshot }) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async publishDurableUpdate(update: LongTaskExecutionUpdate): Promise<void> {
    await this.#persist({
      sourceKey: `${update.type}:${String(update.payload.taskId ?? "task")}:${String(update.payload.unitId ?? update.payload.completedUnits ?? "update")}:${globalThis.crypto.randomUUID()}`,
      kind: update.type,
      channel: "lifecycle",
      visibility: "public",
      payload: update.payload,
    });
  }

  public async complete(result: RunResult): Promise<void> {
    if (this.#deadlineExceeded) {
      throw new AgentError("run_deadline_exceeded", "Run deadline has elapsed");
    }
    const finalEvent = await this.#authorize({
      sourceKey: `run:${this.#runId}:final`,
      kind: "final",
      channel: "final",
      visibility: "public",
      payload: { output: result.output, rounds: result.rounds },
    });
    if (finalEvent === null) {
      throw new AgentError("output_policy_violation", "Output policy cannot remove final output");
    }
    const settled = await this.#repository.settleRun(this.#runId, "completed", {
      relatedEvents: [finalEvent],
      finalOutput: result.output,
    });
    this.#clearDeadline();
    await this.#publishAll(settled.events);
  }

  public async fail(errorCode: string): Promise<void> {
    const snapshot = await this.#repository.get(this.#runId);
    if (snapshot.status !== "running") return;
    const settled = await this.#repository.settleRun(this.#runId, "failed", { errorCode });
    this.#clearDeadline();
    await this.#publishAll(settled.events);
  }

  public async cancel(): Promise<RunCancellationReceipt> {
    const receipt = await this.#repository.cancel(this.#runId);
    if (!receipt.accepted) return receipt;
    this.#clearDeadline();
    this.#controller.abort();
    await this.#publishAll(receipt.events ?? (receipt.event === undefined ? [] : [receipt.event]));
    return receipt;
  }

  public async *events(query: OutputEventQuery = {}): AsyncIterable<OutputEvent> {
    let cursor = query.afterSequence ?? 0;
    if (!Number.isSafeInteger(cursor) || cursor < 0) {
      throw new TypeError("afterSequence must be a non-negative integer");
    }
    const visibility = query.visibility ?? "public";
    while (true) {
      if (query.signal?.aborted === true) throw new AgentCanceledError();
      const rows = await this.#repository.listEvents(this.#runId, cursor);
      for (const event of rows) {
        cursor = event.sequence;
        if (visibility === "all" || event.visibility === "public") yield event;
      }
      const snapshot = await this.#repository.get(this.#runId);
      if (snapshot.status !== "running" && rows.length === 0) return;
      if (rows.length > 0) continue;
      await this.#publisher.waitForSequence(this.#runId, cursor, query.signal);
    }
  }

  async #persist(draft: OutputEventDraft): Promise<OutputEvent | undefined> {
    const authorized = await this.#authorize(draft);
    if (authorized === null) return undefined;
    const event = await this.#repository.appendEvent(this.#runId, authorized);
    await this.#publisher.publishCommitted(event);
    return event;
  }

  async #authorize(draft: OutputEventDraft): Promise<OutputEventDraft | null> {
    const authorized = await this.#policy.authorize(draft);
    if (authorized !== null) validatePolicyResult(draft, authorized);
    return authorized;
  }

  async #publishAll(events: readonly OutputEvent[]): Promise<void> {
    for (const event of events) await this.#publisher.publishCommitted(event);
  }

  #clearDeadline(): void {
    if (this.#deadlineTimer !== undefined) globalThis.clearTimeout(this.#deadlineTimer);
    this.#deadlineTimer = undefined;
  }
}

export const allowAllOutput: OutputPolicy = Object.freeze({
  authorize(event: OutputEventDraft): OutputEventDraft { return event; },
});

function validatePolicyResult(original: OutputEventDraft, authorized: OutputEventDraft): void {
  if (
    authorized.sourceKey !== original.sourceKey
    || authorized.kind !== original.kind
    || authorized.channel !== original.channel
  ) {
    throw new AgentError("output_policy_violation", "Output policy changed event authority fields");
  }
  if (original.visibility === "private" && authorized.visibility !== "private") {
    throw new AgentError("output_policy_violation", "Output policy cannot publish private output");
  }
}
