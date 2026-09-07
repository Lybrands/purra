import { encodeStorageState, decodeStorageState, requireStorageFields } from "../shared/storage-state.js";
import { PLANNING_STREAM_SCHEMA, PlanningStreamParser } from "../planning/stream.js";
import type { JsonValue, ModelTokenUsage } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import { copyPreparedContextSnapshot } from "../context/coordinator.js";
import type { OutputEvent, OutputEventDraft } from "../output/types.js";
import { AgentError } from "../shared/errors.js";
import type {
  AgentExecutionCheckpoint,
  InvocationSettlement,
  ModelInvocationReceipt,
  RunBeginParams,
  RunBudgets,
  RunCancellationReceipt,
  RunLeaseClaim,
  RunSnapshot,
  RunStatus,
} from "./types.js";

export interface RunRepository {
  executeOwned?<T>(runId: string, operation: () => Promise<T>, checkpoint?: AgentExecutionCheckpoint): Promise<T>;
  begin(params: RunBeginParams): Promise<{ readonly snapshot: RunSnapshot; readonly event: OutputEvent }>;
  openInvocation(
    runId: string,
    receipt: Omit<ModelInvocationReceipt, "attempt" | "openedAt">,
    claim?: RunLeaseClaim,
  ): Promise<{
    readonly snapshot: RunSnapshot;
    readonly receipt: ModelInvocationReceipt;
    readonly event: OutputEvent;
  }>;
  appendEvent(runId: string, draft: OutputEventDraft, claim?: RunLeaseClaim): Promise<OutputEvent>;
  appendBatch(runId: string, drafts: readonly OutputEventDraft[], claim?: RunLeaseClaim): Promise<readonly OutputEvent[]>;
  saveExecutionCheckpoint(
    runId: string,
    checkpoint: AgentExecutionCheckpoint,
    claim?: RunLeaseClaim,
  ): Promise<{ readonly snapshot: RunSnapshot; readonly event: OutputEvent }>;
  settleInvocation(
    runId: string,
    settlement: InvocationSettlement,
    claim?: RunLeaseClaim,
  ): Promise<{ readonly event: OutputEvent; readonly budgetError?: string }>;
  settleRun(
    runId: string,
    status: Exclude<RunStatus, "running">,
    options?: {
      readonly relatedEvents?: readonly OutputEventDraft[];
      readonly finalOutput?: JsonValue;
      readonly errorCode?: string;
    },
    claim?: RunLeaseClaim,
  ): Promise<{ readonly snapshot: RunSnapshot; readonly events: readonly OutputEvent[] }>;
  cancel(runId: string, claim?: RunLeaseClaim): Promise<RunCancellationReceipt>;
  get(runId: string): Promise<RunSnapshot>;
  listEvents(runId: string, afterSequence: number, limit?: number): Promise<readonly OutputEvent[]>;
  listRootEvents(
    rootRunId: string,
    afterRootSequence: number,
    limit?: number,
  ): Promise<readonly OutputEvent[]>;
}

interface StoredRun {
  readonly runId: string;
  readonly rootRunId: string;
  readonly agentId: string;
  readonly parentRunId: string | null;
  readonly leaseOwnerId: string | null;
  readonly leaseEpoch: number | null;
  readonly rootEvents: OutputEvent[];
  readonly rootBySourceKey: Map<string, OutputEvent>;
  snapshot: RunSnapshot;
  readonly events: OutputEvent[];
  readonly bySourceKey: Map<string, OutputEvent>;
  readonly openInvocations: Set<string>;
  readonly invocationReceipts: Map<string, ModelInvocationReceipt>;
  readonly invocationSettlements: Map<string, {
    readonly input: InvocationSettlement;
    readonly event: OutputEvent;
    readonly budgetError?: string;
  }>;
}

function storageRun(run: StoredRun, detached = false): StoredRun {
  return {
    runId: run.runId, rootRunId: run.rootRunId, agentId: run.agentId,
    parentRunId: run.parentRunId, leaseOwnerId: run.leaseOwnerId, leaseEpoch: run.leaseEpoch,
    snapshot: run.snapshot, openInvocations: run.openInvocations,
    invocationReceipts: run.invocationReceipts,
    invocationSettlements: new Map([...run.invocationSettlements].map(([id, value]) => [id, {
      input: value.input, event: value.event, ...(value.budgetError === undefined ? {} : { budgetError: value.budgetError }),
    }])),
    events: detached ? [] : run.events, bySourceKey: detached ? new Map() : run.bySourceKey,
    rootEvents: detached ? [] : run.rootEvents, rootBySourceKey: detached ? new Map() : run.rootBySourceKey,
  };
}

interface DeferredOutputJournal {
  readonly counts: ReadonlyMap<string, number>;
  readonly readRun: (runId: string) => readonly OutputEvent[];
  readonly readRoot: () => readonly OutputEvent[];
  readonly findSource: (sourceKey: string) => OutputEvent | undefined;
}

interface DeferredHistory {
  readonly journal: DeferredOutputJournal;
  readonly count: number;
  readonly rootCount: number;
  loadedRun?: readonly OutputEvent[];
  loadedRoot?: readonly OutputEvent[];
}

// Storage callbacks are transaction-local and never part of an encoded snapshot.
const deferredHistories = new WeakMap<StoredRun, DeferredHistory>();

function eventCount(run: StoredRun, root = false): number {
  const history = deferredHistories.get(run);
  return root ? (history?.rootCount ?? 0) + run.rootEvents.length
    : (history?.count ?? 0) + run.events.length;
}

function historyEvents(run: StoredRun, root = false): readonly OutputEvent[] {
  const history = deferredHistories.get(run);
  const pending = root ? run.rootEvents : run.events;
  if (history === undefined) return pending;
  let saved = root ? history.loadedRoot : history.loadedRun;
  if (saved === undefined) {
    saved = root ? history.journal.readRoot() : history.journal.readRun(run.runId);
    if (saved.length !== (root ? history.rootCount : history.count)) throw new TypeError("incomplete output journal");
    const counts = new Map<string, number>();
    for (const [index, event] of saved.entries()) {
      const count = (counts.get(event.runId) ?? 0) + 1;
      counts.set(event.runId, count);
      if (event.rootRunId !== run.rootRunId || (!root && event.runId !== run.runId)
        || event.sequence !== count || (root && event.rootSequence !== index + 1)) {
        throw new TypeError("invalid output journal sequence");
      }
    }
    if (root && [...history.journal.counts].some(([id, count]) => count !== (counts.get(id) ?? 0))) {
      throw new TypeError("incomplete output journal");
    }
    if (root) history.loadedRoot = saved; else history.loadedRun = saved;
  }
  return [...saved, ...pending];
}

function sourceEvent(run: StoredRun, key: string, root = true): OutputEvent | undefined {
  const cache = root ? run.rootBySourceKey : run.bySourceKey;
  const cached = cache.get(key);
  if (cached !== undefined) return cached;
  const history = deferredHistories.get(run);
  const event = history?.journal.findSource(key);
  if (event === undefined) return undefined;
  if (event.sourceKey !== key || event.rootRunId !== run.rootRunId
    || !Number.isSafeInteger(event.sequence) || event.sequence < 1
    || event.sequence > (history!.journal.counts.get(event.runId) ?? 0)
    || !Number.isSafeInteger(event.rootSequence) || event.rootSequence < 1 || event.rootSequence > history!.rootCount) {
    throw new TypeError("invalid output journal identity");
  }
  if (!root && event.runId !== run.runId) return undefined;
  cache.set(key, event);
  return event;
}

const METERED_KINDS = new Set([
  "planning.progress",
  "planning.delta",
  "model.delta",
  "provider.delta_batch",
  "reasoning.delta",
  "commentary",
  "final",
]);

export class InMemoryRunRepository implements RunRepository {
  readonly #runs = new Map<string, StoredRun>();
  readonly #rootEvents = new Map<string, OutputEvent[]>();
  readonly #rootEventsBySourceKey = new Map<string, Map<string, OutputEvent>>();
  readonly #unloadedRoots = new Set<string>();
  #journalCounts = new Map<string, number>();
  readonly #leaseValidator: (
    runId: string,
    claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number },
  ) => void;

  /** Opaque version-pinned storage data, never a public output projection. */
  public exportState(): string {
    if (this.#unloadedRoots.size) throw new TypeError("cannot export unloaded output journals");
    if ([...this.#runs.values()].some((run) => deferredHistories.has(run))) {
      const checkpoint = this.exportJournalState();
      const restored = new InMemoryRunRepository();
      const events = checkpoint.journals.flatMap((journal) => journal.events)
        .sort((a, b) => a.rootRunId.localeCompare(b.rootRunId) || a.rootSequence - b.rootSequence);
      restored.importState(checkpoint.state, events);
      return restored.exportState();
    }
    return encodeStorageState("purra.run-state/v1", { runs: new Map([...this.#runs].map(([id, run]) => [id, storageRun(run)])), rootEvents: this.#rootEvents, rootEventsBySourceKey: this.#rootEventsBySourceKey });
  }
  /** Execution state without the canonical journal, for transactional row storage. */
  public exportJournalState(options: { readonly incremental?: boolean } = {}): {
    readonly state: string;
    readonly journals: readonly { readonly runId: string; readonly rootRunId: string; readonly afterSequence: number; readonly events: readonly OutputEvent[] }[];
  } {
    const runs = new Map([...this.#runs].map(([id, run]) => [id, storageRun(run, true)]));
    return {
      state: encodeStorageState("purra.run-state/v1", { runs, rootEvents: new Map(), rootEventsBySourceKey: new Map(), journalCounts: new Map([...this.#runs].map(([id, run]) => [id, this.#unloadedRoots.has(run.rootRunId) ? this.#journalCounts.get(id)! : eventCount(run)])) }),
      journals: Object.freeze([...this.#runs.values()].filter((run) => !this.#unloadedRoots.has(run.rootRunId)).map((run) => Object.freeze({
        runId: run.runId, rootRunId: run.rootRunId,
        afterSequence: options.incremental ? deferredHistories.get(run)?.count ?? 0 : 0,
        events: Object.freeze([...(options.incremental ? run.events : historyEvents(run))]),
      }))),
    };
  }

  public importState(text: string, outputEvents?: readonly OutputEvent[], options: { readonly rootRunId?: string; readonly deferredJournal?: DeferredOutputJournal } = {}): void {
    const shape = { runs: this.#runs, rootEvents: this.#rootEvents, rootEventsBySourceKey: this.#rootEventsBySourceKey };
    const saved = decodeStorageState(text, "purra.run-state/v1", shape, { journalCounts: new Map<string, number>() }) as typeof shape & { journalCounts?: Map<string, number> };
    for (const [id, run] of saved.runs) {
      requireStorageFields(run, ["runId", "rootRunId", "agentId", "parentRunId", "leaseOwnerId", "leaseEpoch",
        "snapshot", "openInvocations", "invocationReceipts", "invocationSettlements", "events", "bySourceKey", "rootEvents", "rootBySourceKey"]);
      if (typeof id !== "string" || run.runId !== id || !saved.runs.has(run.rootRunId)
        || !Array.isArray(run.events) || !Array.isArray(run.rootEvents) || !(run.openInvocations instanceof Set)
        || ![run.bySourceKey, run.rootBySourceKey, run.invocationReceipts, run.invocationSettlements].every(v => v instanceof Map)) throw new TypeError("Invalid stored Run");
      for (const receipt of run.invocationReceipts.values()) validateStructuredReceipt(receipt);
      for (const value of run.invocationSettlements.values()) requireStorageFields(value, ["input", "event"], ["budgetError"]);
    }
    const deferred = options.deferredJournal;
    if (deferred !== undefined && (options.rootRunId === undefined || outputEvents !== undefined || saved.journalCounts === undefined)) {
      throw new TypeError("deferred journal requires a detached Root checkpoint");
    }
    if (saved.journalCounts !== undefined && outputEvents === undefined && deferred === undefined) {
      throw new TypeError("output journal is required for this checkpoint");
    }
    if (options.rootRunId !== undefined && ((outputEvents === undefined && deferred === undefined) || ![...saved.runs.values()].some((run) => run.rootRunId === options.rootRunId))) {
      throw new TypeError("Root journal selection requires an existing Root and detached events");
    }
    this.#unloadedRoots.clear();
    this.#journalCounts = saved.journalCounts ?? new Map();
    for (const run of saved.runs.values()) {
      if (options.rootRunId !== undefined && run.rootRunId !== options.rootRunId) this.#unloadedRoots.add(run.rootRunId);
    }
    this.#runs.clear(); for (const [key, value] of saved.runs) this.#runs.set(key, value);
    this.#rootEvents.clear(); for (const [key, value] of saved.rootEvents) this.#rootEvents.set(key, value);
    this.#rootEventsBySourceKey.clear(); for (const [key, value] of saved.rootEventsBySourceKey) this.#rootEventsBySourceKey.set(key, value);
    if (outputEvents !== undefined || deferred !== undefined) {
      for (const run of this.#runs.values()) {
        if (run.events.length || run.bySourceKey.size) throw new TypeError("checkpoint contains an embedded journal");
        this.#rootEvents.set(run.rootRunId, []);
        this.#rootEventsBySourceKey.set(run.rootRunId, new Map());
      }
      for (const event of outputEvents ?? []) {
        const run = this.#require(event.runId);
        const root = this.#rootEvents.get(event.rootRunId)!;
        if (run.rootRunId !== event.rootRunId || event.sequence !== run.events.length + 1 || event.rootSequence !== root.length + 1) {
          throw new TypeError("invalid output journal sequence");
        }
        run.events.push(event); run.bySourceKey.set(event.sourceKey, event);
        root.push(event); this.#rootEventsBySourceKey.get(event.rootRunId)!.set(event.sourceKey, event);
      }
      if (saved.journalCounts?.size !== this.#runs.size || [...this.#runs].some(([id, run]) => {
        const count = saved.journalCounts?.get(id);
        return count === undefined || !Number.isSafeInteger(count) || count < 0
          || (!this.#unloadedRoots.has(run.rootRunId) && count !== (deferred?.counts.get(id) ?? run.events.length));
      })) {
        throw new TypeError("incomplete output journal");
      }
    }
    for (const [id, run] of this.#runs) this.#runs.set(id, { ...run, rootEvents: this.#rootEvents.get(run.rootRunId)!, rootBySourceKey: this.#rootEventsBySourceKey.get(run.rootRunId)! });
    if (deferred !== undefined) {
      const counts = new Map(deferred.counts);
      const rootCount = [...counts.values()].reduce((sum, count) => sum + count, 0);
      if (!Number.isSafeInteger(rootCount) || [...counts.keys()].some((id) => this.#runs.get(id)?.rootRunId !== options.rootRunId)
        || [...this.#runs.values()].some((run) => run.rootRunId === options.rootRunId && !counts.has(run.runId))) {
        throw new TypeError("invalid output journal scope");
      }
      const journal: DeferredOutputJournal = {
        counts, readRun: (id) => deferred.readRun(id),
        readRoot: () => deferred.readRoot(), findSource: (key) => deferred.findSource(key),
      };
      for (const run of this.#runs.values()) if (run.rootRunId === options.rootRunId) {
        deferredHistories.set(run, { journal, count: counts.get(run.runId)!, rootCount });
      }
    }
  }

  public constructor(options: {
    readonly leaseValidator?: (
      runId: string,
      claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number },
    ) => void;
  } = {}) {
    this.#leaseValidator = options.leaseValidator ?? (() => undefined);
  }

  public async begin(
    params: RunBeginParams,
  ): Promise<{ readonly snapshot: RunSnapshot; readonly event: OutputEvent }> {
    const runId = params.requestedRunId === undefined
      ? globalThis.crypto.randomUUID()
      : requiredText(params.requestedRunId, "requested Run id");
    if (this.#runs.has(runId)) {
      throw new AgentError("run_identity_conflict", "Requested Run id already exists");
    }
    const rootRunId = requiredText(params.rootRunId ?? runId, "root Run id");
    const agentId = requiredText(params.agentId ?? runId, "Agent id");
    const parentRunId = params.parentRunId === undefined
      ? null
      : requiredText(params.parentRunId, "parent Run id");
    const leaseOwnerId = params.leaseOwnerId === undefined
      ? null
      : requiredText(params.leaseOwnerId, "lease owner id");
    const leaseEpoch = params.leaseEpoch === undefined
      ? null
      : nonNegativeInteger(params.leaseEpoch, "lease epoch");
    if ((leaseOwnerId === null) !== (leaseEpoch === null)) {
      throw new TypeError("Run lease owner and epoch must be provided together");
    }
    if (leaseOwnerId !== null && leaseEpoch !== null) {
      this.#leaseValidator(runId, { leaseOwnerId, leaseEpoch });
    }
    if (rootRunId === runId) {
      if (parentRunId !== null) {
        throw new AgentError("run_scope_conflict", "Root Run cannot have a parent Run");
      }
    } else {
      const root = this.#require(rootRunId);
      if (root.rootRunId !== rootRunId) {
        throw new AgentError("run_scope_conflict", "Run scope root is not a Root Run");
      }
      if (parentRunId === null) {
        throw new AgentError("run_scope_conflict", "Child Run requires a parent Run");
      }
      if (this.#require(parentRunId).rootRunId !== rootRunId) {
        throw new AgentError("run_scope_conflict", "Parent Run belongs to another Root scope");
      }
    }
    const rootEvents = this.#rootEvents.get(rootRunId) ?? [];
    const rootBySourceKey = this.#rootEventsBySourceKey.get(rootRunId) ?? new Map();
    this.#rootEvents.set(rootRunId, rootEvents);
    this.#rootEventsBySourceKey.set(rootRunId, rootBySourceKey);
    const now = new Date().toISOString();
    const snapshot = freezeSnapshot({
      runId,
      status: "running",
      version: 1,
      createdAt: now,
      updatedAt: now,
      deadlineAt: params.deadlineAt,
      budgets: params.budgets,
      usage: {
        modelAttempts: 0,
        unreportedUsageAttempts: 0,
        inputTokens: 0,
        generationTokens: 0,
        reasoningTokens: 0,
        unreportedReasoningAttempts: 0,
        outputBytes: 0,
        outputEvents: 0,
      },
      preset: params.preset,
    });
    const stored: StoredRun = {
      runId,
      rootRunId,
      agentId,
      parentRunId,
      leaseOwnerId,
      leaseEpoch,
      rootEvents,
      rootBySourceKey,
      snapshot,
      events: [],
      bySourceKey: new Map(),
      openInvocations: new Set(),
      invocationReceipts: new Map(),
      invocationSettlements: new Map(),
    };
    this.#runs.set(runId, stored);
    const rootHistory = rootRunId === runId ? undefined : deferredHistories.get(this.#require(rootRunId));
    if (rootHistory !== undefined) {
      deferredHistories.set(stored, { ...rootHistory, count: 0, loadedRun: [] });
    }
    const event = append(stored, runId, {
      sourceKey: `run:${runId}:started`,
      kind: "run.started",
      channel: "lifecycle",
      visibility: "public",
      payload: copyJsonValue({ status: "running", preset: params.preset }) as Readonly<Record<string, JsonValue>>,
    }, false);
    return Object.freeze({ snapshot: stored.snapshot, event });
  }

  public async openInvocation(
    runId: string,
    input: Omit<ModelInvocationReceipt, "attempt" | "openedAt">,
    claim: RunLeaseClaim = {},
  ) {
    const run = this.#active(runId, claim);
    const root = this.#root(run);
    validateStructuredReceipt(input);
    if (input.schemaVersion !== 3) {
      throw new AgentError("model_invocation_contract_invalid", "Unsupported model invocation receipt schema");
    }
    if (input.runId !== runId || (input.outputProtocol !== undefined && input.outputProtocol !== PLANNING_STREAM_SCHEMA)
      || (input.planningScope !== undefined && (input.outputProtocol !== PLANNING_STREAM_SCHEMA
        || input.planningScope.runId !== runId || !Number.isSafeInteger(input.planningScope.revision) || input.planningScope.revision < 0))) {
      throw new AgentError("planning_scope_conflict", "Invalid invocation planning scope or protocol");
    }
    if (input.planningScope !== undefined) requirePlanningOperation(run, input.planningScope.operationId);
    const existing = run.invocationReceipts.get(input.invocationId);
    if (existing !== undefined) {
      if (!sameInvocationInput(existing, input)) {
        throw new AgentError("model_invocation_conflict", "Model invocation key has different authority");
      }
      return Object.freeze({
        snapshot: run.snapshot,
        receipt: existing,
        event: sourceEvent(run, `invocation:${existing.invocationId}:started`, false)!,
      });
    }
    requireBudgetForNextInvocation(root);
    const nextAttempt = run.snapshot.usage.modelAttempts + 1;
    const nextRootAttempt = root.snapshot.usage.modelAttempts + 1;
    const limit = root.snapshot.budgets.maxModelAttempts;
    if (limit !== null && nextRootAttempt > limit) {
      throw new AgentError("runtime_budget_exceeded", "Root Run model-attempt budget is exhausted");
    }
    const receipt: ModelInvocationReceipt = Object.freeze({
      ...input,
      ...(input.outputContract === undefined ? {} : { outputContract: copyJsonValue(input.outputContract) as Readonly<Record<string, JsonValue>> }),
      ...(input.structuredTask === undefined ? {} : { structuredTask: Object.freeze({ ...input.structuredTask }) }),
      attempt: nextAttempt,
      openedAt: new Date().toISOString(),
    });
    run.openInvocations.add(receipt.invocationId);
    run.invocationReceipts.set(receipt.invocationId, receipt);
    updateUsage(run, { modelAttempts: nextAttempt });
    if (root !== run) updateUsage(root, { modelAttempts: nextRootAttempt });
    const event = append(run, runId, {
      sourceKey: `invocation:${receipt.invocationId}:started`,
      kind: "invocation.started",
      channel: "lifecycle",
      visibility: "private",
      payload: copyJsonValue({ receipt }) as Readonly<Record<string, JsonValue>>,
    }, false);
    return Object.freeze({ snapshot: run.snapshot, receipt, event });
  }

  public async appendEvent(
    runId: string,
    draft: OutputEventDraft,
    claim: RunLeaseClaim = {},
  ): Promise<OutputEvent> {
    const run = this.#active(runId, claim);
    const existing = sourceEvent(run, draft.sourceKey);
    if (existing !== undefined) {
      requireSameEvent(existing, draft, runId);
      return existing;
    }
    return append(run, runId, draft, true, this.#root(run));
  }

  public async appendBatch(
    runId: string,
    drafts: readonly OutputEventDraft[],
    claim: RunLeaseClaim = {},
  ): Promise<readonly OutputEvent[]> {
    const run = this.#active(runId, claim);
    if (!Array.isArray(drafts) || drafts.length === 0) return Object.freeze([]);
    const copied = Object.freeze(drafts.map(copyDraft));
    if (new Set(copied.map((draft) => draft.sourceKey)).size !== copied.length) {
      throw new AgentError("output_batch_conflict", "Output batch source keys must be unique");
    }
    const pending = copied.filter((draft) => {
      const existing = sourceEvent(run, draft.sourceKey);
      if (existing !== undefined) requireSameEvent(existing, draft, runId);
      return existing === undefined;
    });
    const root = this.#root(run);
    for (const draft of pending) validatePlanningProjection(run, draft);
    checkRelatedBudget(root, pending);
    return Object.freeze(copied.map((draft) => (
      sourceEvent(run, draft.sourceKey) ?? append(run, runId, draft, true, root)
    )));
  }

  public async saveExecutionCheckpoint(
    runId: string,
    checkpoint: AgentExecutionCheckpoint,
    claim: RunLeaseClaim = {},
  ): Promise<{ readonly snapshot: RunSnapshot; readonly event: OutputEvent }> {
    const run = this.#active(runId, claim);
    const copied = copyExecutionCheckpoint(checkpoint);
    if (copied.runId !== runId) {
      throw new AgentError(
        "agent_execution_checkpoint_conflict",
        "Agent execution checkpoint belongs to another Run",
      );
    }
    const current = run.snapshot.executionCheckpoint;
    if (current !== undefined) {
      if (copied.nextRound < current.nextRound) {
        throw new AgentError(
          "agent_execution_checkpoint_conflict",
          "Agent execution checkpoint cannot move backwards",
        );
      }
      if (
        copied.nextRound === current.nextRound
        && canonicalJson(copied) !== canonicalJson(current)
        && !isInputCheckpointUpdate(current, copied)
      ) {
        throw new AgentError(
          "agent_execution_checkpoint_conflict",
          "Agent execution checkpoint content conflicts",
        );
      }
    }
    const event = append(run, runId, {
      sourceKey: `agent-checkpoint:${runId}:${copied.nextRound}:${copied.inputRevision ?? 0}`,
      kind: "agent.execution_checkpoint",
      channel: "lifecycle",
      visibility: "private",
      payload: {
        schemaVersion: copied.schemaVersion,
        phase: copied.phase,
        executionProfile: copied.executionProfile,
        nextRound: copied.nextRound,
      },
    }, false);
    run.snapshot = freezeSnapshot({
      ...run.snapshot,
      executionCheckpoint: copied,
    });
    return Object.freeze({ snapshot: run.snapshot, event });
  }

  public async settleInvocation(
    runId: string,
    settlement: InvocationSettlement,
    claim: RunLeaseClaim = {},
  ): Promise<{ readonly event: OutputEvent; readonly budgetError?: string }> {
    const run = this.#active(runId, claim);
    const root = this.#root(run);
    const replay = run.invocationSettlements.get(settlement.invocationId);
    if (replay !== undefined) {
      if (!sameSettlement(replay.input, settlement)) {
        throw new AgentError("model_invocation_settlement_conflict", "Invocation settlement conflicts");
      }
      return Object.freeze({
        event: replay.event,
        ...(replay.budgetError === undefined ? {} : { budgetError: replay.budgetError }),
      });
    }
    if (!run.openInvocations.delete(settlement.invocationId)) {
      throw new AgentError("model_invocation_not_open", "Model invocation is not open");
    }
    const usage = settlement.usage;
    applyInvocationUsage(run, usage);
    if (root !== run) applyInvocationUsage(root, usage);
    const budgetError = exceededTokenBudget(root) === undefined
      ? undefined
      : "runtime_budget_exceeded";
    const status = budgetError === undefined ? settlement.status : "failed";
    const errorCode = budgetError ?? settlement.errorCode;
    const event = append(run, runId, {
      sourceKey: `invocation:${settlement.invocationId}:${status}`,
      kind: status === "completed" ? "invocation.completed" : "invocation.failed",
      channel: "lifecycle",
      visibility: "private",
      payload: {
        invocationId: settlement.invocationId,
        status,
        ...(usage === undefined ? { usageReported: false } : {
          inputTokens: usage.inputTokens,
          generationTokens: usage.generationTokens ?? 0,
          reasoningTokens: usage.reasoningTokens ?? null,
        }),
        ...(errorCode === undefined ? {} : { errorCode }),
      },
    }, false);
    run.invocationSettlements.set(settlement.invocationId, Object.freeze({
      input: copySettlement(settlement),
      event,
      ...(budgetError === undefined ? {} : { budgetError }),
    }));
    return Object.freeze({ event, ...(budgetError === undefined ? {} : { budgetError }) });
  }

  public async settleRun(
    runId: string,
    status: Exclude<RunStatus, "running">,
    options: {
      readonly relatedEvents?: readonly OutputEventDraft[];
      readonly finalOutput?: JsonValue;
      readonly errorCode?: string;
    } = {},
    claim: RunLeaseClaim = {},
  ): Promise<{ readonly snapshot: RunSnapshot; readonly events: readonly OutputEvent[] }> {
    const run = this.#require(runId);
    if (run.snapshot.status !== "running") {
      if (run.snapshot.status === status) return Object.freeze({ snapshot: run.snapshot, events: [] });
      throw new AgentError("run_terminal_conflict", "Run already has a different terminal status");
    }
    this.#requireLease(run, claim);
    if (status === "completed" && options.finalOutput === undefined) {
      throw new TypeError("Completed Run requires finalOutput");
    }
    if (
      status === "completed"
      && run.snapshot.deadlineAt !== null
      && Date.now() >= Date.parse(run.snapshot.deadlineAt)
    ) {
      throw new AgentError("run_deadline_exceeded", "Run deadline has elapsed");
    }
    if (status === "failed" && !validText(options.errorCode)) {
      throw new TypeError("Failed Run requires errorCode");
    }
    const related = Object.freeze((options.relatedEvents ?? []).map(copyDraft));
    const root = this.#root(run);
    checkRelatedBudget(root, related);

    const events: OutputEvent[] = [];
    for (const invocationId of [...run.openInvocations]) {
      run.openInvocations.delete(invocationId);
      // Cancellation can commit before the consuming task's catch runs. Charge
      // its last persisted Provider usage once, rather than losing known tokens.
      const observed = [...historyEvents(run)].reverse().find((event) => event.kind === "model.usage"
        && event.payload.invocationId === invocationId);
      const usage = observed?.payload.usage as ModelTokenUsage | undefined;
      applyInvocationUsage(run, usage);
      if (root !== run) applyInvocationUsage(root, usage);
      events.push(append(run, runId, {
        sourceKey: `invocation:${invocationId}:aborted:${status}`,
        kind: "invocation.aborted",
        channel: "lifecycle",
        visibility: "private",
        payload: { invocationId, cause: "run_terminal_commit", usageReported: usage !== undefined,
          ...(usage === undefined ? {} : { usage: copyJsonValue(usage), usageSource: "last_observed" }) },
      }, false));
    }
    for (const started of historyEvents(run).filter((event) => event.kind === "operation.started")) {
      const operationId = String(started.payload.operationId);
      if (historyEvents(run).some((event) => event.kind === "operation.finished" && event.payload.operationId === operationId)) continue;
      events.push(append(run, runId, { sourceKey: `operation:${operationId}:operation.finished`,
        kind: "operation.finished", channel: "lifecycle", visibility: started.visibility,
        payload: { type: "operation.finished", operationId, runId,
          ...(started.payload.invocationId === undefined ? {} : { invocationId: started.payload.invocationId }),
          ...(started.payload.parentOperationId === undefined ? {} : { parentOperationId: started.payload.parentOperationId }),
          status: status === "canceled" ? "canceled" : "failed", errorCode: "run_terminalized",
          finishedAt: new Date().toISOString(), durationMs: Math.max(0, Date.now() - Date.parse(started.occurredAt)),
          timingSource: "recovery_wall_clock", display: started.payload.display ?? {},
        } }, false));
    }
    for (const draft of related) events.push(append(run, runId, draft, true, root));
    const now = new Date().toISOString();
    run.snapshot = freezeSnapshot({
      ...run.snapshot,
      status,
      version: run.snapshot.version + 1,
      updatedAt: now,
      ...(options.finalOutput === undefined
        ? {}
        : { finalOutput: copyJsonValue(options.finalOutput) }),
      ...(options.errorCode === undefined ? {} : { errorCode: normalizeCode(options.errorCode) }),
    });
    events.push(append(run, runId, {
      sourceKey: `run:${runId}:${status}`,
      kind: `run.${status}`,
      channel: "lifecycle",
      visibility: "public",
      payload: {
        status,
        ...(options.errorCode === undefined ? {} : { errorCode: normalizeCode(options.errorCode) }),
      },
    }, false));
    return Object.freeze({ snapshot: run.snapshot, events: Object.freeze(events) });
  }

  public async cancel(
    runId: string,
    claim: RunLeaseClaim = {},
  ): Promise<RunCancellationReceipt> {
    const run = this.#require(runId);
    if (run.snapshot.status !== "running") {
      return Object.freeze({ runId, accepted: false, status: run.snapshot.status });
    }
    const settled = await this.settleRun(runId, "canceled", {}, claim);
    const event = settled.events.at(-1)!;
    return Object.freeze({
      runId,
      accepted: true,
      status: "canceled",
      event,
      events: settled.events,
    });
  }

  public async get(runId: string): Promise<RunSnapshot> {
    return this.#require(runId).snapshot;
  }

  public async listEvents(
    runId: string,
    afterSequence: number,
    limit = 200,
  ): Promise<readonly OutputEvent[]> {
    const run = this.#require(runId);
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0) {
      throw new TypeError("afterSequence must be a non-negative integer");
    }
    if (!Number.isSafeInteger(limit) || limit < 1) throw new TypeError("limit must be positive");
    return Object.freeze(historyEvents(run).filter((event) => event.sequence > afterSequence).slice(0, limit));
  }

  public async listRootEvents(
    rootRunId: string,
    afterRootSequence: number,
    limit = 200,
  ): Promise<readonly OutputEvent[]> {
    const root = this.#require(rootRunId);
    if (root.rootRunId !== rootRunId) {
      throw new AgentError("run_scope_conflict", "Root journal query requires a Root Run");
    }
    if (!Number.isSafeInteger(afterRootSequence) || afterRootSequence < 0) {
      throw new TypeError("afterRootSequence must be a non-negative integer");
    }
    if (!Number.isSafeInteger(limit) || limit < 1) throw new TypeError("limit must be positive");
    return Object.freeze(
      historyEvents(root, true).filter((event) => event.rootSequence > afterRootSequence).slice(0, limit),
    );
  }

  #active(runId: string, claim: RunLeaseClaim): StoredRun {
    const run = this.#require(runId);
    if (run.snapshot.status !== "running") {
      throw new AgentError("run_not_active", "Run is already terminal");
    }
    if (run.snapshot.deadlineAt !== null && Date.now() >= Date.parse(run.snapshot.deadlineAt)) {
      throw new AgentError("run_deadline_exceeded", "Run deadline has elapsed");
    }
    this.#requireLease(run, claim);
    return run;
  }

  #require(runId: string): StoredRun {
    const run = this.#runs.get(runId);
    if (run === undefined) throw new AgentError("run_not_found", "Run does not exist");
    if (this.#unloadedRoots.has(run.rootRunId)) throw new AgentError("run_scope_not_loaded", "Root journal is not loaded");
    return run;
  }

  #root(run: StoredRun): StoredRun {
    return this.#require(run.rootRunId);
  }

  #requireLease(run: StoredRun, claim: RunLeaseClaim): void {
    if (run.leaseOwnerId === null || run.leaseEpoch === null) return;
    this.#leaseValidator(run.runId, claim);
  }
}

export async function assertRunRepositoryConforms(repository: RunRepository): Promise<void> {
  const begun = await repository.begin({
    requestedRunId: "conformance-run-1",
    preset: {
      schemaVersion: 5,
      presetId: "conformance",
      presetRevision: "1",
      promptFingerprint: "prompt",
      toolFingerprint: "tools",
      capabilityProfileId: null,
      compositionFingerprint: "composition",
      runtimeLimits: {
        runTimeoutMs: 900_000,
        activityIdleTimeoutMs: 30_000,
        progressIdleTimeoutMs: 60_000,
        invocationTimeoutMs: 300_000,
        maxChunks: 100_000,
        maxContentChars: 1_000_000,
        maxReasoningChars: 1_000_000,
        maxToolArgumentChars: 1_000_000,
      },
      agentTree: {
        protocolVersion: 1,
        enabled: false,
      },
    },
    deadlineAt: null,
    budgets: {
      maxModelAttempts: 1,
      maxInputTokens: null,
      maxRunGenerationTokens: null,
      maxReasoningTokens: null,
      maxOutputBytes: 1_000,
      maxOutputEvents: 10,
    },
    metadata: {},
  });
  if (begun.snapshot.runId !== "conformance-run-1") {
    throw new AgentError("run_repository_nonconforming", "Run repository ignored requested identity");
  }
  const begunSnapshot = await repository.get(begun.snapshot.runId);
  if (begunSnapshot.runId !== begun.snapshot.runId || begunSnapshot.status !== "running") {
    throw new AgentError("run_repository_nonconforming", "Run repository read model is inconsistent");
  }
  const batchDrafts = Object.freeze(["a", "b"].map((value) => Object.freeze({
    sourceKey: `conformance:batch:${value}`,
    kind: "provider.delta_batch" as const,
    channel: "model" as const,
    visibility: "private" as const,
    payload: Object.freeze({ value }),
  })));
  const batch = await repository.appendBatch(begun.snapshot.runId, batchDrafts);
  const replay = await repository.appendBatch(begun.snapshot.runId, batchDrafts);
  if (batch.length !== 2 || replay[0]?.eventId !== batch[0]?.eventId) {
    throw new AgentError("run_repository_nonconforming", "Run repository batch replay is not atomic");
  }
  const contextSnapshot = copyPreparedContextSnapshot({
    blocks: [{ name: "facts", content: "checkpoint evidence", untrusted: true,
      evidence: [{ evidenceId: "fact", source: "conformance", itemId: "item", version: "1" }],
    }],
    contextAllocations: { facts: 64 },
    compactions: 1,
    summary: { name: "summary", content: "checkpoint summary", untrusted: true,
      evidence: [{ evidenceId: "summary-fact", source: "conformance", version: "1" }],
    },
  });
  const checkpoint = await repository.saveExecutionCheckpoint(
    begun.snapshot.runId,
    {
      schemaVersion: 2,
      runId: begun.snapshot.runId,
      phase: "model_ready",
      executionProfile: "reactive",
      initialPlanningOpen: false,
      nextRound: 2,
      messages: [{ role: "user", content: "resume" }],
      context: contextSnapshot,
      contextEvidence: Object.freeze([]),
      responseAttempts: 0,
      recoveryAttempts: [],
    },
  );
  const checkpointReplay = await repository.saveExecutionCheckpoint(
    begun.snapshot.runId,
    checkpoint.snapshot.executionCheckpoint!,
  );
  if (
    checkpoint.snapshot.executionCheckpoint?.nextRound !== 2
    || checkpointReplay.event.eventId !== checkpoint.event.eventId
    || canonicalJson((await repository.get(begun.snapshot.runId))?.executionCheckpoint?.context ?? null)
      !== canonicalJson(contextSnapshot)
  ) {
    throw new AgentError(
      "run_repository_nonconforming",
      "Run repository checkpoint commit is not atomic and replayable",
    );
  }
  const final = await repository.settleRun(begun.snapshot.runId, "completed", {
    relatedEvents: [{
      sourceKey: "conformance:final",
      kind: "final",
      channel: "final",
      visibility: "public",
      payload: { output: "ok" },
    }],
    finalOutput: "ok",
  });
  const events = await repository.listEvents(begun.snapshot.runId, 0);
  const rootEvents = await repository.listRootEvents(begun.snapshot.runId, 0);
  if (final.snapshot.status !== "completed" || events.length !== 6) {
    throw new AgentError("run_repository_nonconforming", "Run repository failed atomic lifecycle probe");
  }
  const terminalSnapshot = await repository.get(begun.snapshot.runId);
  if (terminalSnapshot.status !== "completed" || terminalSnapshot.finalOutput !== "ok") {
    throw new AgentError("run_repository_nonconforming", "Run repository lost terminal state");
  }
  if (events.some((event, index) => event.sequence !== index + 1)) {
    throw new AgentError("run_repository_nonconforming", "Run repository sequences are not ordered");
  }
  if (
    rootEvents.length !== events.length
    || rootEvents.some((event, index) => (
      event.eventId !== events[index]?.eventId
      || event.rootRunId !== begun.snapshot.runId
      || event.rootSequence !== index + 1
      || !validText(event.agentId)
      || !validText(event.sourceKey)
    ))
  ) {
    throw new AgentError("run_repository_nonconforming", "Root journal attribution is not canonical");
  }
}

function append(
  run: StoredRun,
  runId: string,
  rawDraft: OutputEventDraft,
  meter: boolean,
  root: StoredRun = run,
): OutputEvent {
  const existing = sourceEvent(run, rawDraft.sourceKey);
  if (existing !== undefined) {
    requireSameEvent(existing, rawDraft, runId);
    return existing;
  }
  const draft = copyDraft(rawDraft);
  validatePlanningProjection(run, draft);
  if (!Number.isSafeInteger(eventCount(run) + 1) || !Number.isSafeInteger(eventCount(run, true) + 1)) {
    throw new TypeError("output journal sequence exceeds safe integer range");
  }
  if (meter) applyBudget(run, root, draft);
  const event: OutputEvent = Object.freeze({
    ...draft,
    eventId: globalThis.crypto.randomUUID(),
    runId,
    rootRunId: run.rootRunId,
    agentId: run.agentId,
    parentRunId: run.parentRunId,
    sequence: eventCount(run) + 1,
    rootSequence: eventCount(run, true) + 1,
    occurredAt: new Date().toISOString(),
  });
  run.events.push(event);
  run.rootEvents.push(event);
  run.bySourceKey.set(event.sourceKey, event);
  run.rootBySourceKey.set(event.sourceKey, event);
  run.snapshot = freezeSnapshot({
    ...run.snapshot,
    version: run.snapshot.version + 1,
    updatedAt: event.occurredAt,
  });
  return event;
}

function requirePlanningOperation(run: StoredRun, operationId: string): void {
  const events = historyEvents(run).filter((event) => event.payload.operationId === operationId
    && (event.kind === "operation.started" || event.kind === "operation.finished"));
  if (events.at(-1)?.kind !== "operation.started" || events.at(-1)?.payload.kind !== "planning") {
    throw new AgentError("planning_scope_conflict", "Planning operation is not active");
  }
}

function validatePlanningProjection(run: StoredRun, draft: OutputEventDraft): void {
  if (draft.kind === "provider.delta_batch" && draft.visibility !== "private") {
    const id = draft.payload?.invocationId;
    const receipt = typeof id === "string" ? run.invocationReceipts.get(id) : undefined;
    if (receipt === undefined || receipt.outputProtocol === PLANNING_STREAM_SCHEMA
      || draft.payload?.source !== "provider" || !Array.isArray(draft.payload.entries)
      || draft.payload.entries.some((entry) => entry === null || typeof entry !== "object"
        || Array.isArray(entry) || entry.kind !== "provider.content_delta")) {
      throw new AgentError("planning_projection_invalid", "Private or unattributed Provider bytes cannot be public");
    }
  }
  if (draft.kind === "commentary" && draft.payload?.source === "provider") {
    const invocationId = draft.payload.invocationId;
    const receipt = typeof invocationId === "string"
      ? run.invocationReceipts.get(invocationId)
      : undefined;
    const text = draft.payload.text;
    const completion = typeof invocationId === "string"
      ? historyEvents(run).find((event) => (
          event.sourceKey === `invocation:${invocationId}:completion`
          && event.kind === "model.completed"
        ))
      : undefined;
    const streamedText = typeof invocationId === "string"
      ? historyEvents(run).filter((event) => (
          event.kind === "provider.delta_batch"
          && event.visibility === "private"
          && event.payload.invocationId === invocationId
        )).flatMap((event) => (
          event.payload.entries as unknown as readonly {
            kind: string;
            payload: { delta?: string };
          }[]
        )).filter((entry) => entry.kind === "provider.content_delta")
          .map((entry) => entry.payload.delta ?? "").join("")
      : "";
    const providerText = streamedText !== ""
      ? streamedText
      : completion?.payload.content;
    if (
      receipt === undefined
      || receipt.outputProtocol === PLANNING_STREAM_SCHEMA
      || typeof text !== "string"
      || text.trim() === ""
      || providerText !== text
      || draft.channel !== "commentary"
      || draft.sourceKey !== `auto-planning-intent:${invocationId}`
      || Object.keys(draft.payload).sort().join(",") !== "invocationId,source,text"
    ) {
      throw new AgentError(
        "planning_projection_invalid",
        "Auto planning commentary lacks Provider authority",
      );
    }
  }
  if (draft.kind === "agent.progress") {
    const p = draft.payload ?? {};
    const invocationId = p.invocationId;
    const chunkIndex = p.sourceChunkIndex;
    const receipt = typeof invocationId === "string"
      ? run.invocationReceipts.get(invocationId)
      : undefined;
    const sourceText = typeof invocationId === "string" && Number.isSafeInteger(chunkIndex)
      ? historyEvents(run).filter((event) => (
          event.kind === "provider.delta_batch"
          && event.visibility === "private"
          && event.payload.invocationId === invocationId
        )).flatMap((event) => (
          event.payload.entries as unknown as readonly {
            sourceChunkIndex: number;
            kind: string;
            payload: { delta?: string };
          }[]
        )).find((entry) => (
          entry.kind === "provider.progress_delta"
          && entry.sourceChunkIndex === chunkIndex
        ))?.payload.delta
      : undefined;
    if (
      receipt === undefined
      || !run.openInvocations.has(receipt.invocationId)
      || receipt.outputProtocol === PLANNING_STREAM_SCHEMA
      || p.schemaVersion !== "purra.agent-progress/v1"
      || p.source !== "provider"
      || draft.channel !== "commentary"
      || typeof p.text !== "string"
      || p.text.trim() !== p.text
      || p.text === ""
      || p.text.includes("\n")
      || p.text.includes("\r")
      || p.text.length > 160
      || !Number.isSafeInteger(chunkIndex)
      || (chunkIndex as number) < 1
      || sourceText !== p.text
      || draft.sourceKey !== `agent-progress:${invocationId}:${chunkIndex}`
      || Object.keys(p).sort().join(",")
        !== "invocationId,schemaVersion,source,sourceChunkIndex,text"
    ) {
      throw new AgentError(
        "agent_progress_invalid",
        "Agent progress lacks exact persisted Provider authority",
      );
    }
  }
  if (draft.kind === "planning.delta") {
    const p = draft.payload ?? {};
    const id = p.invocationId;
    const index = p.sourceChunkIndex;
    const receipt = typeof id === "string" ? run.invocationReceipts.get(id) : undefined;
    const scope = receipt?.planningScope;
    const source = sourceEvent(run, `provider-batch:${id}:model:private:${index}:${index}`);
    const entries = source?.payload.entries as readonly { kind: string; sourceChunkIndex: number; payload: { delta?: string } }[] | undefined;
    if (receipt === undefined || scope === undefined || !run.openInvocations.has(receipt.invocationId)
      || receipt.outputProtocol !== PLANNING_STREAM_SCHEMA || p.schemaVersion !== PLANNING_STREAM_SCHEMA
      || p.source !== "provider" || draft.channel !== "commentary"
      || p.operationId !== scope.operationId || p.revision !== scope.revision || p.attempt !== (receipt.planningAttempt ?? 0)
      || !Number.isSafeInteger(index) || (index as number) < 0
      || typeof p.textDelta !== "string" || p.textDelta.length === 0
      || Object.keys(p).sort().join(",") !== "attempt,invocationId,operationId,revision,schemaVersion,source,sourceChunkIndex,textDelta"
      || draft.sourceKey !== `planning-delta:${id}:${index}`
      || source?.runId !== run.runId || source?.kind !== "provider.delta_batch" || source?.visibility !== "private"
      || source?.payload.invocationId !== id
      || !entries?.some((entry) => entry.kind === "provider.content_delta" && entry.sourceChunkIndex === index && entry.payload.delta === p.textDelta)) {
      throw new AgentError("planning_projection_invalid", "Planning delta differs from persisted Provider chunk");
    }
    requirePlanningOperation(run, scope.operationId);
    return;
  }
  if (draft.kind !== "planning.progress") return;
  const p = draft.payload ?? {};
  const invocationId = p.invocationId;
  const receipt = typeof invocationId === "string" ? run.invocationReceipts.get(invocationId) : undefined;
  const scope = receipt?.planningScope;
  if (receipt === undefined || scope === undefined || !run.openInvocations.has(receipt.invocationId)
    || receipt.outputProtocol !== PLANNING_STREAM_SCHEMA || p.schemaVersion !== PLANNING_STREAM_SCHEMA
    || p.source !== "provider" || draft.channel !== "commentary"
    || p.operationId !== scope.operationId || p.revision !== scope.revision || p.attempt !== (receipt.planningAttempt ?? 0)
    || Object.keys(p).sort().join(",") !== "attempt,invocationId,operationId,recordIndex,revision,schemaVersion,source,sourceEnd,sourceStart,text"
    || draft.sourceKey !== `planning:${receipt.invocationId}:${p.recordIndex}`) {
    throw new AgentError("planning_projection_invalid", "Planning projection lacks Provider authority");
  }
  requirePlanningOperation(run, scope.operationId);
  // ponytail: bounded replay (1 MiB, 16 projections), shared with Python. Index
  // source spans only if profiling shows persistence needs that complexity.
  const text = historyEvents(run).filter((event) => event.kind === "provider.delta_batch"
    && event.payload.invocationId === invocationId).flatMap((event) => (
      event.payload.entries as unknown as readonly { kind: string; payload: { delta?: string } }[]
    )).filter((entry) => entry.kind === "provider.content_delta").map((entry) => entry.payload.delta ?? "").join("");
  const raw = new TextEncoder().encode(text);
  if (!Number.isSafeInteger(p.sourceStart) || !Number.isSafeInteger(p.sourceEnd)
    || (p.sourceStart as number) < 0 || (p.sourceEnd as number) <= (p.sourceStart as number) || (p.sourceEnd as number) > raw.length) {
    throw new AgentError("planning_projection_invalid", "Planning projection source span is invalid");
  }
  try {
    const prefix = new TextDecoder("utf-8", { fatal: true }).decode(raw.subarray(0, p.sourceEnd as number));
    const record = new PlanningStreamParser().feed(prefix).find((row) => row.recordIndex === p.recordIndex);
    if (record === undefined || record.text !== p.text || record.sourceStart !== p.sourceStart || record.sourceEnd !== p.sourceEnd) throw new Error();
  } catch {
    throw new AgentError("planning_projection_invalid", "Planning projection differs from persisted Provider bytes");
  }
}

function requireSameEvent(
  existing: OutputEvent,
  draft: OutputEventDraft,
  runId: string,
): void {
  if (
    existing.runId !== runId
    || existing.kind !== draft.kind
    || existing.channel !== draft.channel
    || existing.visibility !== draft.visibility
    || canonicalJson(existing.payload) !== canonicalJson(draft.payload ?? {})
  ) {
    throw new AgentError("output_source_key_conflict", "Output source key has different content");
  }
}

function copyDraft(draft: OutputEventDraft): OutputEventDraft & { readonly payload: Readonly<Record<string, JsonValue>> } {
  return Object.freeze({
    sourceKey: requiredText(draft.sourceKey, "output sourceKey"),
    kind: draft.kind,
    channel: draft.channel,
    visibility: draft.visibility,
    payload: copyJsonValue(draft.payload ?? {}) as Readonly<Record<string, JsonValue>>,
  });
}

function checkBudget(run: StoredRun, draft: OutputEventDraft): void {
  if (!METERED_KINDS.has(draft.kind)) return;
  const bytes = byteLength(draft.payload ?? {});
  const byteLimit = run.snapshot.budgets.maxOutputBytes;
  const eventLimit = run.snapshot.budgets.maxOutputEvents;
  if (byteLimit !== null && run.snapshot.usage.outputBytes + bytes > byteLimit) {
    throw new AgentError("runtime_budget_exceeded", "Run output-byte budget is exhausted");
  }
  if (eventLimit !== null && run.snapshot.usage.outputEvents + 1 > eventLimit) {
    throw new AgentError("runtime_budget_exceeded", "Run output-event budget is exhausted");
  }
}

function requireBudgetForNextInvocation(run: StoredRun): void {
  const kind = tokenBudgetKind(run, true);
  if (kind !== undefined) {
    throw new AgentError("runtime_budget_exceeded", `Run ${kind} budget is exhausted`);
  }
}

function exceededTokenBudget(run: StoredRun): string | undefined {
  return tokenBudgetKind(run, false);
}

function tokenBudgetKind(run: StoredRun, inclusive: boolean): string | undefined {
  const { budgets, usage } = run.snapshot;
  if (
    usage.unreportedUsageAttempts > 0
    && (
      budgets.maxInputTokens !== null
      || budgets.maxRunGenerationTokens !== null
      || budgets.maxReasoningTokens !== null
    )
  ) return "provider_usage_unreported";
  if (
    usage.unreportedReasoningAttempts > 0
    && budgets.maxReasoningTokens !== null
  ) return "reasoning_tokens_unreported";
  const rows = [
    ["input_tokens", usage.inputTokens, budgets.maxInputTokens],
    ["generation_tokens", usage.generationTokens, budgets.maxRunGenerationTokens],
    ["reasoning_tokens", usage.reasoningTokens, budgets.maxReasoningTokens],
  ] as const;
  return rows.find(([, used, maximum]) => (
    maximum !== null && (used > maximum || (inclusive && used >= maximum))
  ))?.[0];
}

function checkRelatedBudget(run: StoredRun, drafts: readonly OutputEventDraft[]): void {
  const metered = drafts.filter((draft) => METERED_KINDS.has(draft.kind));
  const addedBytes = metered.reduce((total, draft) => total + byteLength(draft.payload ?? {}), 0);
  const byteLimit = run.snapshot.budgets.maxOutputBytes;
  const eventLimit = run.snapshot.budgets.maxOutputEvents;
  if (byteLimit !== null && run.snapshot.usage.outputBytes + addedBytes > byteLimit) {
    throw new AgentError("runtime_budget_exceeded", "Run output-byte budget is exhausted");
  }
  if (eventLimit !== null && run.snapshot.usage.outputEvents + metered.length > eventLimit) {
    throw new AgentError("runtime_budget_exceeded", "Run output-event budget is exhausted");
  }
}

function applyBudget(run: StoredRun, root: StoredRun, draft: OutputEventDraft): void {
  checkBudget(root, draft);
  if (!METERED_KINDS.has(draft.kind)) return;
  const bytes = byteLength(draft.payload ?? {});
  updateUsage(run, {
    outputBytes: run.snapshot.usage.outputBytes + bytes,
    outputEvents: run.snapshot.usage.outputEvents + 1,
  });
  if (root !== run) {
    updateUsage(root, {
      outputBytes: root.snapshot.usage.outputBytes + bytes,
      outputEvents: root.snapshot.usage.outputEvents + 1,
    });
  }
}

function applyInvocationUsage(run: StoredRun, usage: ModelTokenUsage | undefined): void {
  updateUsage(run, usage === undefined
    ? { unreportedUsageAttempts: run.snapshot.usage.unreportedUsageAttempts + 1 }
    : {
        inputTokens: run.snapshot.usage.inputTokens + usage.inputTokens,
        generationTokens: run.snapshot.usage.generationTokens + (usage.generationTokens ?? 0),
        reasoningTokens: run.snapshot.usage.reasoningTokens + (usage.reasoningTokens ?? 0),
        unreportedReasoningAttempts: run.snapshot.usage.unreportedReasoningAttempts
          + (usage.reasoningTokens === undefined ? 1 : 0),
      });
}

function updateUsage(run: StoredRun, patch: Partial<RunSnapshot["usage"]>): void {
  run.snapshot = freezeSnapshot({
    ...run.snapshot,
    version: run.snapshot.version + 1,
    updatedAt: new Date().toISOString(),
    usage: { ...run.snapshot.usage, ...patch },
  });
}

function freezeSnapshot(snapshot: RunSnapshot): RunSnapshot {
  return Object.freeze({
    ...snapshot,
    budgets: normalizeRunBudgets(snapshot.budgets),
    usage: Object.freeze({ ...snapshot.usage }),
    preset: Object.freeze({ ...snapshot.preset }),
    ...(snapshot.executionCheckpoint === undefined
      ? {}
      : { executionCheckpoint: copyExecutionCheckpoint(snapshot.executionCheckpoint) }),
  });
}

export function normalizeRunSnapshot(snapshot: RunSnapshot): RunSnapshot {
  if (snapshot === null || typeof snapshot !== "object") {
    throw new TypeError("Run snapshot must be an object");
  }
  return freezeSnapshot(snapshot);
}

export function normalizeRunBudgets(value: RunBudgets): RunBudgets {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Run budgets must be an object");
  }
  const record = value as unknown as Readonly<Record<string, unknown>>;
  return Object.freeze({
    maxModelAttempts: nullablePositiveBudget(record.maxModelAttempts, "maxModelAttempts"),
    maxInputTokens: nullablePositiveBudget(record.maxInputTokens, "maxInputTokens"),
    maxRunGenerationTokens: nullablePositiveBudget(
      record.maxRunGenerationTokens,
      "maxRunGenerationTokens",
    ),
    maxReasoningTokens: nullablePositiveBudget(record.maxReasoningTokens, "maxReasoningTokens"),
    maxOutputBytes: nullablePositiveBudget(record.maxOutputBytes, "maxOutputBytes"),
    maxOutputEvents: nullablePositiveBudget(record.maxOutputEvents, "maxOutputEvents"),
  });
}

function nullablePositiveBudget(value: unknown, label: string): number | null {
  if (value === null) return null;
  const normalized = nonNegativeInteger(value, label);
  if (normalized < 1) throw new TypeError(`${label} must be positive or null`);
  return normalized;
}

function isInputCheckpointUpdate(current: AgentExecutionCheckpoint, updated: AgentExecutionCheckpoint): boolean {
  if ((updated.inputRevision ?? 0) !== (current.inputRevision ?? 0) + 1
    || canonicalJson(updated) !== canonicalJson({ ...current, inputRevision: updated.inputRevision, messages: updated.messages })) return false;
  if (updated.messages.length === current.messages.length + 1) {
    return updated.messages.at(-1)?.role === "user" && typeof updated.messages.at(-1)?.attributes?.inputRequestId === "string"
      && canonicalJson(updated.messages.slice(0, -1)) === canonicalJson(current.messages);
  }
  if (updated.messages.length !== current.messages.length) return false;
  const calls = new Set(current.messages.flatMap(m => m.toolCalls ?? []).filter(c => c.name === "delegateToAgents").map(c => c.id));
  let changed = false;
  for (let i = 0; i < current.messages.length; i++) {
    const before = current.messages[i]!, after = updated.messages[i]!;
    if (canonicalJson(before) === canonicalJson(after)) continue;
    if (before.role !== "tool" || !calls.has(before.toolCallId ?? "")
      || canonicalJson(after) !== canonicalJson({ ...before, content: after.content })) return false;
    try {
      const old = typeof before.content === "string" ? JSON.parse(before.content) : before.content;
      const value = typeof after.content === "string" ? JSON.parse(after.content) : after.content;
      const expected = new Set([...old.pendingRunIds, ...old.results.map((row: any) => row.runId)]);
      if (old.state !== "pending" || !old.pendingRunIds.length || !["ready", "blocked"].includes(value.state)
        || value.pendingRunIds.length || value.results.length !== expected.size
        || new Set(value.results.map((row: any) => row.runId)).size !== expected.size
        || value.results.some((row: any) => !expected.has(row.runId))
        || old.results.some((row: any) => !value.results.some((item: any) => canonicalJson(row) === canonicalJson(item)))) return false;
    } catch { return false; }
    changed = true;
  }
  return changed;
}

function copyExecutionCheckpoint(
  checkpoint: AgentExecutionCheckpoint,
): AgentExecutionCheckpoint {
  const copied = copyJsonValue(checkpoint as unknown as JsonValue) as unknown as AgentExecutionCheckpoint;
  nonNegativeInteger(copied.inputRevision ?? 0, "checkpoint input revision");
  if (
    copied.schemaVersion !== 2
    || copied.phase !== "model_ready"
    || !["reactive", "auto", "planned"].includes(copied.executionProfile)
    || typeof copied.initialPlanningOpen !== "boolean"
  ) {
    throw new TypeError("Agent execution checkpoint contract is invalid");
  }
  requiredText(copied.runId, "Agent execution checkpoint Run id");
  if (!Number.isSafeInteger(copied.nextRound) || copied.nextRound < 1) {
    throw new TypeError("Agent execution checkpoint next round must be positive");
  }
  if (!Array.isArray(copied.messages) || copied.messages.length === 0) {
    throw new TypeError("Agent execution checkpoint messages are required");
  }
  nonNegativeInteger(copied.responseAttempts, "checkpoint response attempts");
  if (copied.executionProfile === "planned" && copied.planning === undefined) throw new TypeError("Planned checkpoint requires its coordinator");
  if (copied.roundLimit !== undefined && (!Number.isSafeInteger(copied.roundLimit) || copied.roundLimit < 1)) throw new TypeError("Invalid checkpoint round limit");
  if (!Array.isArray(copied.recoveryAttempts)) {
    throw new TypeError("Agent execution checkpoint recovery attempts are invalid");
  }
  if (!Array.isArray(copied.contextEvidence)) {
    throw new TypeError("Agent execution checkpoint context evidence is invalid");
  }
  return Object.freeze({
    ...copied,
    context: copied.context === null ? null : copyPreparedContextSnapshot(copied.context),
    contextEvidence: copyCheckpointEvidence(copied.contextEvidence),
  });
}

function copyCheckpointEvidence(
  values: readonly import("../context/types.js").ContextEvidenceReceipt[],
): readonly import("../context/types.js").ContextEvidenceReceipt[] {
  const ids = new Set<string>();
  return Object.freeze(values.map((raw) => {
    if (raw === null || typeof raw !== "object") throw new TypeError("Invalid checkpoint evidence");
    const evidenceId = requiredText(raw.evidenceId, "checkpoint evidence id");
    if (ids.has(evidenceId)) throw new TypeError(`Duplicate checkpoint evidence id: ${evidenceId}`);
    ids.add(evidenceId);
    return Object.freeze({
      evidenceId,
      ...(raw.contextBlock === undefined ? {} : { contextBlock: requiredText(raw.contextBlock, "checkpoint evidence contextBlock") }),
      source: requiredText(raw.source, "checkpoint evidence source"),
      ...(raw.itemId === undefined ? {} : { itemId: requiredText(raw.itemId, "checkpoint evidence itemId") }),
      ...(raw.version === undefined ? {} : { version: requiredText(raw.version, "checkpoint evidence version") }),
    });
  }));
}

function byteLength(value: unknown): number {
  return new TextEncoder().encode(canonicalJson(value)).byteLength;
}

function sameInvocationInput(
  receipt: ModelInvocationReceipt,
  input: Omit<ModelInvocationReceipt, "attempt" | "openedAt">,
): boolean {
  const { attempt: _attempt, openedAt: _openedAt, ...existing } = receipt;
  return canonicalJson(existing) === canonicalJson(input);
}

function copySettlement(value: InvocationSettlement): InvocationSettlement {
  return Object.freeze({
    invocationId: value.invocationId,
    status: value.status,
    ...(value.usage === undefined ? {} : { usage: Object.freeze({ ...value.usage }) }),
    ...(value.errorCode === undefined ? {} : { errorCode: value.errorCode }),
  });
}

function sameSettlement(left: InvocationSettlement, right: InvocationSettlement): boolean {
  return canonicalJson(left) === canonicalJson(right);
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(sortJson(value));
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

function normalizeCode(value: unknown): string {
  const code = typeof value === "string" ? value.trim().toLowerCase() : "";
  return /^[a-z0-9][a-z0-9_.-]{0,127}$/.test(code) ? code : "run_failed";
}

function requiredText(value: unknown, label: string): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "") throw new TypeError(`${label} must be non-empty text`);
  return text;
}

function nonNegativeInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new TypeError(`${label} must be a non-negative integer`);
  }
  return Number(value);
}

function validText(value: unknown): boolean {
  return typeof value === "string" && value.trim() !== "";
}


function validateStructuredReceipt(receipt: Pick<ModelInvocationReceipt, "schemaVersion" | "outputContract" | "structuredTask" | "outputProtocol" | "planningScope">): void {
  const output = receipt.outputContract;
  const task = receipt.structuredTask;
  const invalid = () => { throw new AgentError("model_invocation_contract_invalid", "Invalid structured invocation receipt"); };
  if (receipt.schemaVersion !== 3) invalid();
  if (output === undefined) { if (task !== undefined) invalid(); return; }
  if (output === null || typeof output !== "object" || Array.isArray(output)
    || receipt.outputProtocol !== undefined || receipt.planningScope !== undefined) invalid();
  if (output.schemaProfile !== "purra.output-schema/v1" || !["local", "native_required"].includes(output.mode as string)
    || ["schemaId", "schemaVersion"].some(key => typeof output[key] !== "string" || !(output[key] as string).trim() || new TextEncoder().encode(output[key] as string).length > 128)
    || ["schemaDigest", "contractDigest"].some(key => typeof output[key] !== "string" || !/^[0-9a-f]{64}$/.test(output[key] as string))
    || (output.mode === "native_required" && (typeof output.nativeDialect !== "string" || !output.nativeDialect.trim() || output.nativeDialect.length > 128))) invalid();
  if (task !== undefined && (task === null || typeof task.taskId !== "string" || !task.taskId.trim()
    || !Number.isSafeInteger(task.attempt) || task.attempt < 1
    || (task.attempt === 1 ? task.previousInvocationId !== null : typeof task.previousInvocationId !== "string" || !task.previousInvocationId.trim()))) invalid();
}
