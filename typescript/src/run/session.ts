import type { JsonValue, ModelStreamChunk, ModelTurn } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import type {
  OutputEvent,
  OutputBatchLimits,
  OutputEventDraft,
  OutputEventQuery,
  OutputPolicy,
  OutputPublisher,
} from "../output/types.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import { stableFingerprint } from "../shared/fingerprint.js";
import type { ToolExecutionEvent } from "../tools/types.js";
import type { DelegationLifecycleEvent } from "../delegation/types.js";
import { normalizeRunSnapshot, type RunRepository } from "./store.js";
import type {
  AgentExecutionCheckpoint,
  InvocationReceiptInput,
  ModelInvocationReceipt,
  RunBeginParams,
  RunCancellationReceipt,
  RunCommand,
  RunHandle,
  RunLeaseClaim,
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
import { AgentOperationController, type OperationEvent } from "../operations/index.js";
import { PLANNING_STREAM_SCHEMA, type PlanningProgress } from "../planning/stream.js";

interface PendingProviderDelta {
  readonly sourceChunkIndex: number;
  readonly sourcePartIndex: number;
  readonly kind: "provider.content_delta" | "provider.reasoning_delta" | "provider.progress_delta" | "provider.tool_call_delta";
  readonly channel: "model" | "reasoning";
  readonly visibility: "private";
  readonly payload: Readonly<Record<string, JsonValue>>;
}

interface PendingOutputBatch {
  readonly entries: PendingProviderDelta[];
  payloadBytes: number;
  serial: Promise<void>;
  timer: ReturnType<typeof globalThis.setTimeout> | undefined;
  error?: unknown;
}

const OUTPUT_BATCH_SCHEMA = "purra.provider-delta-batch/v1";
const AGENT_PROGRESS_SCHEMA = "purra.agent-progress/v1";
const MAX_AGENT_PROGRESS_CHARS = 160;

export class RunSession {
  public operations = new AgentOperationController(this);
  readonly #repository: RunRepository;
  readonly #publisher: OutputPublisher;
  readonly #policy: OutputPolicy;
  readonly #controller = new AbortController();
  readonly #runId: string;
  readonly #rootRunId: string;
  readonly #agentId: string;
  readonly #parentRunId: string | undefined;
  readonly #outwardVisibility: "public" | "private";
  readonly #leaseClaim: RunLeaseClaim;
  readonly #batchLimits: OutputBatchLimits;
  #deadlineExceeded = false;
  #deadlineTimer: number | undefined;
  readonly #batches = new Map<string, PendingOutputBatch>();
  readonly #receipts = new Map<string, ModelInvocationReceipt>();

  private constructor(
    repository: RunRepository,
    publisher: OutputPublisher,
    policy: OutputPolicy,
    runId: string,
    rootRunId: string,
    agentId: string,
    parentRunId: string | undefined,
    leaseClaim: RunLeaseClaim,
    deadlineAt: string | null,
    batchLimits: OutputBatchLimits,
  ) {
    this.#repository = repository;
    this.#publisher = publisher;
    this.#policy = policy;
    this.#runId = runId;
    this.#rootRunId = rootRunId;
    this.#agentId = agentId;
    this.#parentRunId = parentRunId;
    this.#outwardVisibility = parentRunId === undefined ? "public" : "private";
    this.#leaseClaim = Object.freeze({ ...leaseClaim });
    this.#batchLimits = batchLimits;
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
    batchLimits: OutputBatchLimits,
  ): Promise<RunSession> {
    const begun = await repository.begin(params);
    const snapshot = normalizeRunSnapshot(begun.snapshot);
    await publisher.publishCommitted(begun.event);
    return new RunSession(
      repository,
      publisher,
      policy,
      snapshot.runId,
      params.rootRunId ?? snapshot.runId,
      params.agentId ?? snapshot.runId,
      params.parentRunId,
      params.leaseOwnerId === undefined
        ? Object.freeze({})
        : Object.freeze({
            leaseOwnerId: params.leaseOwnerId,
            leaseEpoch: params.leaseEpoch,
          }),
      snapshot.deadlineAt,
      batchLimits,
    );
  }

  public static resume(
    repository: RunRepository,
    publisher: OutputPublisher,
    policy: OutputPolicy,
    snapshot: RunSnapshot,
    scope: {
      readonly rootRunId: string;
      readonly agentId: string;
      readonly parentRunId?: string;
      readonly leaseOwnerId?: string;
      readonly leaseEpoch?: number;
    },
    batchLimits: OutputBatchLimits,
  ): RunSession {
    snapshot = normalizeRunSnapshot(snapshot);
    if (snapshot.status !== "running" || snapshot.executionCheckpoint === undefined) {
      throw new AgentError(
        "agent_run_resume_checkpoint_missing",
        "Running Agent Run has no resumable checkpoint",
      );
    }
    return new RunSession(
      repository,
      publisher,
      policy,
      snapshot.runId,
      scope.rootRunId,
      scope.agentId,
      scope.parentRunId,
      scope.leaseOwnerId === undefined ? {} : {
        leaseOwnerId: scope.leaseOwnerId,
        ...(scope.leaseEpoch === undefined ? {} : { leaseEpoch: scope.leaseEpoch }),
      },
      snapshot.deadlineAt,
      batchLimits,
    );
  }

  public get runId(): string {
    return this.#runId;
  }

  public get rootRunId(): string {
    return this.#rootRunId;
  }

  public get agentId(): string {
    return this.#agentId;
  }

  public get parentRunId(): string | undefined {
    return this.#parentRunId;
  }

  public get leaseClaim(): RunLeaseClaim {
    return this.#leaseClaim;
  }

  public get signal(): AbortSignal {
    return this.#controller.signal;
  }

  public get deadlineExceeded(): boolean {
    return this.#deadlineExceeded;
  }

  public releaseWaitingExecution(): void {
    this.#clearDeadline();
  }

  public handle(result: Promise<RunResult>): RunHandle {
    return Object.freeze({
      runId: this.#runId,
      result,
      snapshot: async () => normalizeRunSnapshot(await this.#repository.get(this.#runId)),
      cancel: () => this.cancel(),
      command: (command: RunCommand) => {
        if (command?.type !== "cancel") throw new TypeError("Unsupported Run command");
        return this.cancel();
      },
      events: (query?: OutputEventQuery) => this.events(query),
    });
  }

  public async snapshot(): Promise<RunSnapshot> {
    return normalizeRunSnapshot(await this.#repository.get(this.#runId));
  }

  public async openInvocation(input: InvocationReceiptInput): Promise<ModelInvocationReceipt> {
    const invocationId = globalThis.crypto.randomUUID();
    const [messageFingerprint, toolFingerprint, evidenceFingerprint] = await Promise.all([
      stableFingerprint(copyJsonValue(input.messages)),
      stableFingerprint(copyJsonValue(input.tools)),
      stableFingerprint(copyJsonValue(input.evidence)),
    ]);
    const requestFingerprint = await stableFingerprint(copyJsonValue({
      messageFingerprint,
      toolFingerprint,
      evidenceFingerprint,
      capabilityProfileId: input.capabilityProfileId,
      outputLimit: input.outputLimit,
      ...(input.outputProtocol === undefined ? {} : { outputProtocol: input.outputProtocol }),
      ...(input.planningScope === undefined ? {} : { planningScope: input.planningScope }),
      ...(input.planningAttempt === undefined ? {} : { planningAttempt: input.planningAttempt }),
    }));
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
      ...(input.outputProtocol === undefined ? {} : { outputProtocol: input.outputProtocol }),
      ...(input.planningScope === undefined ? {} : { planningScope: input.planningScope }),
      ...(input.planningAttempt === undefined ? {} : { planningAttempt: input.planningAttempt }),
    }, this.#leaseClaim);
    this.#receipts.set(opened.receipt.invocationId, opened.receipt);
    await this.#publisher.publishCommitted(opened.event);
    return opened.receipt;
  }

  public async persistChunk(
    receipt: ModelInvocationReceipt,
    index: number,
    chunk: ModelStreamChunk,
  ): Promise<void> {
    if (chunk.progressDelta !== undefined && chunk.progressDelta !== "") {
      requirePublicProgress(chunk.progressDelta);
      if (receipt.outputProtocol === PLANNING_STREAM_SCHEMA) {
        throw new AgentError(
          "agent_progress_protocol_conflict",
          "Planning streams cannot emit agent progress",
        );
      }
    }
    const entries = providerDeltaEntries(index, chunk);
    await this.#withBatch(receipt, async (batch) => {
      batch.entries.push(...entries);
      batch.payloadBytes += entries.reduce(
        (total, entry) => total + canonicalByteLength(entry),
        0,
      );
      if (
        (chunk.progressDelta !== undefined && chunk.progressDelta !== "")
        || chunk.usage !== undefined
        || chunk.finishReason !== undefined
        || batch.payloadBytes >= this.#batchLimits.maxPayloadBytes
        || batch.entries.length >= this.#batchLimits.maxFragments
      ) {
        await this.#flushBatch(receipt, batch);
      } else if (entries.length > 0) {
        this.#scheduleBatchFlush(receipt, batch);
      }
    });
    if (chunk.progressDelta !== undefined && chunk.progressDelta !== "") {
      await this.#persist({
        sourceKey: `agent-progress:${receipt.invocationId}:${index}`,
        kind: "agent.progress",
        channel: "commentary",
        visibility: this.#outwardVisibility,
        payload: {
          schemaVersion: AGENT_PROGRESS_SCHEMA,
          source: "provider",
          invocationId: receipt.invocationId,
          sourceChunkIndex: index,
          text: chunk.progressDelta,
        },
      });
    }
    if (chunk.usage !== undefined) {
      await this.#persist({
        sourceKey: `invocation:${receipt.invocationId}:chunk:${index}:usage`,
        kind: "model.usage",
        channel: "model",
        visibility: "private",
        payload: copyJsonValue({ invocationId: receipt.invocationId, usage: chunk.usage }) as Readonly<Record<string, JsonValue>>,
      });
    }
    if (chunk.finishReason !== undefined) {
      await this.#persist({
        sourceKey: `invocation:${receipt.invocationId}:chunk:${index}:finish`,
        kind: "model.finish",
        channel: "model",
        visibility: "private",
        payload: { finishReason: chunk.finishReason },
      });
    }
  }

  public async persistCompletion(
    receipt: ModelInvocationReceipt,
    turn: ModelTurn,
  ): Promise<void> {
    await this.#flushInvocation(receipt);
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
        ...(turn.appliedOutputLimit === undefined
          ? {}
          : { appliedOutputLimit: turn.appliedOutputLimit }),
        ...(turn.usage === undefined ? {} : { usage: turn.usage }),
      }) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async settleInvocation(
    receipt: ModelInvocationReceipt,
    status: "completed" | "failed",
    turn?: ModelTurn,
    errorCode?: string,
    usage?: import("../model/types.js").ModelTokenUsage,
  ): Promise<void> {
    await this.#flushInvocation(receipt);
    const reportedUsage = usage ?? turn?.usage;
    const settled = await this.#repository.settleInvocation(this.#runId, {
      invocationId: receipt.invocationId,
      status,
      ...(reportedUsage === undefined ? {} : { usage: reportedUsage }),
      ...(errorCode === undefined ? {} : { errorCode }),
    }, this.#leaseClaim);
    this.#receipts.delete(receipt.invocationId);
    this.#batches.delete(receipt.invocationId);
    await this.#publisher.publishCommitted(settled.event);
    if (settled.budgetError !== undefined) {
      throw new AgentError(settled.budgetError, "Run token budget is exhausted");
    }
  }

  public async persistPlanningProgress(receipt: ModelInvocationReceipt, progress: PlanningProgress): Promise<boolean> {
    if (this.signal.aborted) throw new AgentCanceledError();
    if (receipt.runId !== this.#runId || this.#receipts.get(receipt.invocationId) !== receipt
      || receipt.outputProtocol !== PLANNING_STREAM_SCHEMA || receipt.planningScope === undefined) {
      throw new AgentError("planning_scope_conflict", "Planning projection requires an active bound invocation");
    }
    await this.#flushInvocation(receipt);
    const event = await this.#persist({
      sourceKey: `planning:${receipt.invocationId}:${progress.recordIndex}`,
      kind: "planning.progress", channel: "commentary", visibility: this.#outwardVisibility,
      payload: { schemaVersion: PLANNING_STREAM_SCHEMA, source: "provider", invocationId: receipt.invocationId,
        operationId: receipt.planningScope.operationId, revision: receipt.planningScope.revision,
        attempt: receipt.planningAttempt ?? 0, ...progress },
    });
    return event?.visibility === "public";
  }

  public async publishModelCommentary(
    receipt: ModelInvocationReceipt,
    text: string,
  ): Promise<void> {
    if (text.trim() === "") return;
    await this.#persist({
      sourceKey: `auto-planning-intent:${receipt.invocationId}`,
      kind: "commentary",
      channel: "commentary",
      visibility: this.#outwardVisibility,
      payload: {
        source: "provider",
        invocationId: receipt.invocationId,
        text,
      },
    });
  }

  public async recordModelDiagnostics(receipt: ModelInvocationReceipt, metrics: Readonly<Record<string, JsonValue>>): Promise<void> {
    if ((await this.snapshot()).status !== "running") return;
    await this.#persist({ sourceKey: `diagnostics:${receipt.invocationId}`, kind: "model.diagnostics",
      channel: "model", visibility: "private", payload: {
        invocationId: receipt.invocationId, planningScope: copyJsonValue(receipt.planningScope ?? null),
        attempt: receipt.planningAttempt ?? 0, ...metrics,
      } });
  }

  public async countPlanningAttempts(operationId: string): Promise<number> {
    const events = await this.#repository.listEvents(this.#runId, 0);
    return events.filter((event) => event.kind === "invocation.started"
      && (event.payload.receipt as unknown as ModelInvocationReceipt)?.planningScope?.operationId === operationId).length;
  }

  public async acceptOperationEvent(event: OperationEvent): Promise<void> {
    if (event.runId !== this.#runId) throw new AgentError("operation_scope_conflict", "Operation belongs to another Run");
    await this.#persist({ sourceKey: `operation:${event.operationId}:${event.type}`,
      kind: event.type, channel: "lifecycle", visibility: this.#outwardVisibility,
      payload: copyJsonValue(event) as Readonly<Record<string, JsonValue>> });
  }

  async #withBatch<T>(
    receipt: ModelInvocationReceipt,
    operation: (batch: PendingOutputBatch) => Promise<T>,
  ): Promise<T> {
    const batch = this.#batches.get(receipt.invocationId) ?? {
      entries: [],
      payloadBytes: 0,
      serial: Promise.resolve(),
      timer: undefined,
    };
    this.#batches.set(receipt.invocationId, batch);
    const previous = batch.serial;
    let release!: () => void;
    batch.serial = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    try {
      if (batch.error !== undefined) throw batch.error;
      return await operation(batch);
    } finally {
      release();
    }
  }

  async #flushInvocation(receipt: ModelInvocationReceipt): Promise<void> {
    await this.#withBatch(receipt, (batch) => this.#flushBatch(receipt, batch));
    const batch = this.#batches.get(receipt.invocationId);
    if (batch !== undefined && batch.entries.length === 0) {
      if (batch.timer !== undefined) globalThis.clearTimeout(batch.timer);
      this.#batches.delete(receipt.invocationId);
    }
  }

  async #flushBatch(receipt: ModelInvocationReceipt, batch: PendingOutputBatch): Promise<void> {
    if (batch.entries.length === 0) {
      if (batch.timer !== undefined) globalThis.clearTimeout(batch.timer);
      batch.timer = undefined;
      return;
    }
    const drafts = await providerBatchDrafts(receipt, batch.entries);
    const authorized = (await Promise.all(drafts.map((draft) => this.#authorize(draft))))
      .filter((draft): draft is OutputEventDraft => draft !== null);
    let events: readonly OutputEvent[];
    try {
      events = authorized.length === 0 ? [] : await this.#repository.appendBatch(this.#runId, authorized, this.#leaseClaim);
    } catch (error) {
      if (error instanceof AgentError) throw error;
      throw new AgentError("output_persistence_failed", "Canonical output batch could not be persisted", { cause: error });
    }
    batch.entries.splice(0);
    batch.payloadBytes = 0;
    if (batch.timer !== undefined) globalThis.clearTimeout(batch.timer);
    batch.timer = undefined;
    for (const event of events) await this.#publisher.publishCommitted(event);
  }

  #scheduleBatchFlush(receipt: ModelInvocationReceipt, batch: PendingOutputBatch): void {
    if (batch.timer !== undefined) return;
    batch.timer = globalThis.setTimeout(() => {
      batch.timer = undefined;
      void this.#withBatch(receipt, (current) => this.#flushBatch(receipt, current))
        .catch((error: unknown) => { batch.error = error; });
    }, this.#batchLimits.maxBackgroundLatencyMs);
  }

  public async publishTool(event: ToolExecutionEvent, round: number): Promise<void> {
    await this.#persist({
      sourceKey: `tool:${this.#runId}:${round}:${event.toolCallId}:${event.type}`,
      kind: event.type === "tool_started" ? "tool.started" : "tool.completed",
      channel: "tool",
      visibility: this.#outwardVisibility,
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
      visibility: this.#outwardVisibility,
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
      visibility: this.#outwardVisibility,
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
    await this.#persist({ sourceKey: `plan:${this.#runId}:${revision}:private`, kind: "plan.updated",
      channel: "plan", visibility: "private", payload: copyJsonValue({ revision, plan }) as Readonly<Record<string, JsonValue>> });
    await this.#persist({
      sourceKey: `plan:${this.#runId}:${revision}`,
      kind: "plan.updated",
      channel: "plan",
      visibility: this.#outwardVisibility,
      payload: copyJsonValue({ revision, title: plan.title, steps: plan.steps.map(({ id, title }) => ({ id, title })) }) as Readonly<Record<string, JsonValue>>,
    });
  }

  public async publishAdmission(decision: TaskAdmissionDecision, continuation = false): Promise<void> {
    await this.#persist({
      sourceKey: `admission:${this.#runId}:${continuation ? "continuation" : "initial"}`,
      kind: "task_admission.decided",
      channel: "lifecycle",
      visibility: this.#outwardVisibility,
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
      sourceKey: `recovery:${this.#runId}:${round}:${decision.cause}:${decision.scope}:${decision.attempt}:${decision.reasonCode}`,
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

  public async saveExecutionCheckpoint(
    checkpoint: AgentExecutionCheckpoint,
  ): Promise<void> {
    const committed = await this.#repository.saveExecutionCheckpoint(
      this.#runId,
      checkpoint,
      this.#leaseClaim,
    );
    await this.#publisher.publishCommitted(committed.event);
  }

  public async publishDurableDispatch(
    receipt: LongTaskDispatchReceipt,
    snapshot: DurableRecoverySnapshot,
  ): Promise<void> {
    await this.#persist({
      sourceKey: `long-task:${receipt.taskId}:dispatched`,
      kind: "long_task.dispatched",
      channel: "lifecycle",
      visibility: this.#outwardVisibility,
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
      visibility: this.#outwardVisibility,
      payload: update.payload,
    });
  }

  public async complete(result: RunResult): Promise<void> {
    if (this.#deadlineExceeded) {
      throw new AgentError("run_deadline_exceeded", "Run deadline has elapsed");
    }
    await this.#flushAllBatches();
    const finalEvent = await this.#authorize({
      sourceKey: `run:${this.#runId}:final`,
      kind: "final",
      channel: "final",
      visibility: this.#outwardVisibility,
      payload: { output: result.output, rounds: result.rounds },
    });
    if (finalEvent === null) {
      throw new AgentError("output_policy_violation", "Output policy cannot remove final output");
    }
    const settled = await this.#repository.settleRun(this.#runId, "completed", {
      relatedEvents: [finalEvent],
      finalOutput: result.output,
    }, this.#leaseClaim);
    this.#clearDeadline();
    await this.#publishAll(settled.events);
  }

  public async fail(errorCode: string): Promise<void> {
    const snapshot = await this.#repository.get(this.#runId);
    if (snapshot.status !== "running") return;
    await this.#flushAllBatches(true);
    const settled = await this.#repository.settleRun(
      this.#runId,
      "failed",
      { errorCode },
      this.#leaseClaim,
    );
    this.#clearDeadline();
    await this.#publishAll(settled.events);
  }

  public async cancel(): Promise<RunCancellationReceipt> {
    await this.#flushAllBatches(true);
    const receipt = await this.#repository.cancel(this.#runId, this.#leaseClaim);
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
    if ((draft.kind === "planning.progress" || draft.kind === "agent.progress") && this.signal.aborted) throw new AgentCanceledError();
    let event: OutputEvent;
    try {
      event = await this.#repository.appendEvent(this.#runId, authorized, this.#leaseClaim);
    } catch (error) {
      if (error instanceof AgentError) throw error;
      throw new AgentError("output_persistence_failed", "Canonical output could not be persisted", { cause: error });
    }
    try { await this.#publisher.publishCommitted(event); }
    catch (error) { throw new AgentError("output_publish_failed", "Committed output could not be published", { cause: error }); }
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

  async #flushAllBatches(bestEffort = false): Promise<void> {
    for (const receipt of this.#receipts.values()) {
      try {
        await this.#flushInvocation(receipt);
      } catch (error) {
        if (!bestEffort) throw error;
      }
    }
    if (bestEffort) {
      for (const batch of this.#batches.values()) {
        if (batch.timer !== undefined) globalThis.clearTimeout(batch.timer);
      }
      this.#batches.clear();
      this.#receipts.clear();
    }
  }

  #clearDeadline(): void {
    if (this.#deadlineTimer !== undefined) globalThis.clearTimeout(this.#deadlineTimer);
    this.#deadlineTimer = undefined;
  }
}

function providerDeltaEntries(
  index: number,
  chunk: ModelStreamChunk,
): readonly PendingProviderDelta[] {
  const entries: PendingProviderDelta[] = [];
  if (chunk.contentDelta !== undefined && chunk.contentDelta !== "") {
    entries.push(Object.freeze({
      sourceChunkIndex: index,
      sourcePartIndex: 0,
      kind: "provider.content_delta",
      channel: "model",
      visibility: "private",
      payload: Object.freeze({ delta: chunk.contentDelta }),
    }));
  }
  if (chunk.reasoningDelta !== undefined && chunk.reasoningDelta !== "") {
    entries.push(Object.freeze({
      sourceChunkIndex: index,
      sourcePartIndex: 1,
      kind: "provider.reasoning_delta",
      channel: "reasoning",
      visibility: "private",
      payload: Object.freeze({ delta: chunk.reasoningDelta }),
    }));
  }
  if (chunk.toolCallDeltas !== undefined && chunk.toolCallDeltas.length > 0) {
    entries.push(Object.freeze({
      sourceChunkIndex: index,
      sourcePartIndex: 2,
      kind: "provider.tool_call_delta",
      channel: "model",
      visibility: "private",
      payload: copyJsonValue({ deltas: chunk.toolCallDeltas }) as Readonly<Record<string, JsonValue>>,
    }));
  }
  if (chunk.progressDelta !== undefined && chunk.progressDelta !== "") {
    entries.push(Object.freeze({
      sourceChunkIndex: index,
      sourcePartIndex: 3,
      kind: "provider.progress_delta",
      channel: "model",
      visibility: "private",
      payload: Object.freeze({ delta: chunk.progressDelta }),
    }));
  }
  return Object.freeze(entries);
}

function requirePublicProgress(text: string): void {
  if (
    text.trim() !== text
    || text === ""
    || text.includes("\n")
    || text.includes("\r")
    || text.length > MAX_AGENT_PROGRESS_CHARS
  ) {
    throw new AgentError(
      "agent_progress_invalid",
      "Provider progress must be one trimmed line within the public limit",
    );
  }
}

async function providerBatchDrafts(
  receipt: ModelInvocationReceipt,
  pending: readonly PendingProviderDelta[],
): Promise<readonly OutputEventDraft[]> {
  const groups = new Map<string, PendingProviderDelta[]>();
  for (const entry of pending) {
    const key = `${entry.channel}\u0000${entry.visibility}`;
    const group = groups.get(key) ?? [];
    group.push(entry);
    groups.set(key, group);
  }
  const ordered = [...groups.values()].sort((left, right) => (
    left[0]!.sourceChunkIndex - right[0]!.sourceChunkIndex
    || left[0]!.sourcePartIndex - right[0]!.sourcePartIndex
  ));
  return Object.freeze(await Promise.all(ordered.map(async (group) => {
    const normalized = group.map((entry) => Object.freeze({
      sourceChunkIndex: entry.sourceChunkIndex,
      sourcePartIndex: entry.sourcePartIndex,
      kind: entry.kind,
      payload: entry.payload,
    }));
    const start = Math.min(...group.map((entry) => entry.sourceChunkIndex));
    const end = Math.max(...group.map((entry) => entry.sourceChunkIndex));
    return Object.freeze({
      sourceKey: `provider-batch:${receipt.invocationId}:${group[0]!.channel}:${group[0]!.visibility}:${start}:${end}`,
      kind: "provider.delta_batch" as const,
      channel: group[0]!.channel,
      visibility: group[0]!.visibility,
      payload: copyJsonValue({
        invocationId: receipt.invocationId,
        source: "provider",
        schemaVersion: OUTPUT_BATCH_SCHEMA,
        sourceChunkStart: start,
        sourceChunkEnd: end,
        entries: normalized,
        payloadDigest: await stableFingerprint(copyJsonValue(normalized)),
      }) as Readonly<Record<string, JsonValue>>,
    });
  })));
}

function canonicalByteLength(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(sortJson(value))).byteLength;
}

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value !== null && typeof value === "object") {
    const record = value as Readonly<Record<string, unknown>>;
    return Object.fromEntries(
      Object.keys(record).sort().map((key) => [key, sortJson(record[key])]),
    );
  }
  return value;
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
  if (
    (original.kind === "planning.progress"
      || original.kind === "agent.progress"
      || (original.kind === "commentary" && original.payload?.source === "provider"))
    && JSON.stringify(original.payload) !== JSON.stringify(authorized.payload)
  ) {
    throw new AgentError("output_policy_violation", "Output policy cannot rewrite Provider text or provenance");
  }
  if (original.visibility === "private" && authorized.visibility !== "private") {
    throw new AgentError("output_policy_violation", "Output policy cannot publish private output");
  }
}
