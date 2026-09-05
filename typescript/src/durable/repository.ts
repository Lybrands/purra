import { encodeStorageState, decodeStorageState, requireStorageFields } from "../shared/storage-state.js";
import type { JsonValue, ModelTokenUsage } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";
import { copyLongTaskBudgets } from "./contracts.js";
import type {
  LongTaskCheckpoint,
  LongTaskClaim,
  LongTaskCreateCommand,
  LongTaskRecord,
  LongTaskRepository,
  LongTaskRunBinding,
  LongTaskRunRelation,
  LongTaskUnitRecord,
  LongTaskUnitResult,
  LongTaskUsage,
} from "./types.js";

interface TaskState {
  record: LongTaskRecord;
  readonly units: Map<string, LongTaskUnitRecord>;
  readonly checkpoints: Map<string, LongTaskCheckpoint[]>;
  readonly settlements: Map<string, string>;
  readonly bindings: LongTaskRunBinding[];
}

const ZERO_USAGE: LongTaskUsage = Object.freeze({
  invocationCount: 0,
  unreportedUsageAttempts: 0,
  inputTokens: 0,
  generationTokens: 0,
  reasoningTokens: null,
});

export class InMemoryLongTaskRepository implements LongTaskRepository {
  readonly #tasks = new Map<string, TaskState>();
  readonly #idempotency = new Map<string, string>();
  readonly #clockMs: () => number;
  readonly #tokenFactory: () => string;

  /** Opaque version-pinned storage data, never a public output projection. */
  public exportState(): string { return encodeStorageState("purra.task-state/v1", { tasks: new Map([...this.#tasks].map(([id, state]) => [id, { record: state.record, units: state.units, checkpoints: state.checkpoints, settlements: state.settlements, bindings: state.bindings }])), idempotency: this.#idempotency }); }
  public importState(text: string): void {
    const shape = { tasks: this.#tasks, idempotency: this.#idempotency };
    const saved = decodeStorageState(text, "purra.task-state/v1", shape) as typeof shape;
    for (const state of saved.tasks.values()) {
      requireStorageFields(state, ["record", "units", "checkpoints", "settlements", "bindings"]);
      if (![state.units, state.checkpoints, state.settlements].every(v => v instanceof Map) || !Array.isArray(state.bindings)) throw new TypeError("Invalid stored Task");
    }
    this.#tasks.clear(); for (const [key, value] of saved.tasks) this.#tasks.set(key, value);
    this.#idempotency.clear(); for (const [key, value] of saved.idempotency) this.#idempotency.set(key, value);
  }

  public constructor(options: {
    readonly clockMs?: () => number;
    readonly tokenFactory?: () => string;
  } = {}) {
    this.#clockMs = options.clockMs ?? Date.now;
    this.#tokenFactory = options.tokenFactory ?? (() => globalThis.crypto.randomUUID());
  }

  public async create(taskId: string, command: LongTaskCreateCommand): Promise<LongTaskRecord> {
    const id = requiredText(taskId, "long task id");
    if (this.#tasks.has(id)) throw new AgentError("long_task_exists", `Long task already exists: ${id}`);
    const key = idempotencyKey(command.namespace, command.idempotencyKey);
    const previousId = this.#idempotency.get(key);
    if (previousId !== undefined) return this.#require(previousId).record;
    validateCommand(command);
    const units = new Map(command.units.map((spec) => [spec.id, Object.freeze({
      taskId: id,
      id: spec.id,
      position: spec.position,
      semanticKey: spec.semanticKey ?? spec.id,
      dependencies: Object.freeze([...(spec.dependencies ?? [])]),
      required: spec.required ?? true,
      inputRef: spec.inputRef ?? null,
      executor: spec.executor,
      planStepId: spec.planStepId,
      status: "pending" as const,
      attempt: 0,
      maxAttempts: spec.maxAttempts ?? 1,
      workerId: null,
      claimToken: null,
      leaseEpoch: 0,
      leaseExpiresAtMs: null,
      retryReadyAtMs: null,
      outputRef: null,
      artifactDigest: null,
      errorCode: null,
      usage: ZERO_USAGE,
      metadata: copyMapping(spec.metadata ?? {}),
    })]));
    const record: LongTaskRecord = Object.freeze({
      id,
      namespace: requiredText(command.namespace, "long task namespace"),
      kind: requiredText(command.kind, "long task kind"),
      ownerId: requiredText(command.ownerId, "long task ownerId"),
      createdByRunId: requiredText(command.createdByRunId, "long task createdByRunId"),
      idempotencyKey: requiredText(command.idempotencyKey, "long task idempotencyKey"),
      status: "pending",
      revision: 1,
      totalUnits: units.size,
      completedUnits: 0,
      failedUnits: 0,
      maxParallelism: positiveInteger(command.maxParallelism ?? 1, "long task maxParallelism"),
      deadlineAtMs: nullablePositive(command.deadlineAtMs, "long task deadlineAtMs"),
      budgets: copyLongTaskBudgets(command.budgets),
      cancellationRequestedAtMs: null,
      usage: ZERO_USAGE,
      metadata: copyMapping(command.metadata ?? {}),
    });
    const state: TaskState = {
      record,
      units,
      checkpoints: new Map(),
      settlements: new Map(),
      bindings: [],
    };
    this.#tasks.set(id, state);
    this.#idempotency.set(key, id);
    await this.bindRun(id, record.createdByRunId, "created");
    return state.record;
  }

  public async findByIdempotencyKey(
    namespace: string,
    key: string,
  ): Promise<LongTaskRecord | undefined> {
    const taskId = this.#idempotency.get(idempotencyKey(namespace, key));
    return taskId === undefined ? undefined : this.#require(taskId).record;
  }

  public async load(taskId: string): Promise<LongTaskRecord | undefined> {
    return this.#tasks.get(taskId)?.record;
  }

  public async listUnits(taskId: string): Promise<readonly LongTaskUnitRecord[]> {
    return Object.freeze([...this.#require(taskId).units.values()].sort((a, b) => a.position - b.position));
  }

  public async bindRun(
    taskId: string,
    runId: string,
    relation: LongTaskRunRelation,
  ): Promise<LongTaskRunBinding> {
    const state = this.#require(taskId);
    const normalizedRunId = requiredText(runId, "long task binding runId");
    if (relation !== "created" && relation !== "continuation" && relation !== "reference") {
      throw new TypeError("long task binding relation is invalid");
    }
    const existing = state.bindings.find((item) => (
      item.runId === normalizedRunId && item.relation === relation
    ));
    if (existing !== undefined) return existing;
    const binding = Object.freeze({
      taskId: state.record.id,
      runId: normalizedRunId,
      relation,
      sequence: state.bindings.length + 1,
    });
    state.bindings.push(binding);
    this.#touch(state);
    return binding;
  }

  public async listRunBindings(taskId: string): Promise<readonly LongTaskRunBinding[]> {
    return Object.freeze([...this.#require(taskId).bindings]);
  }

  public async start(taskId: string): Promise<LongTaskRecord> {
    const state = this.#require(taskId);
    if (state.record.status === "running") return state.record;
    if (state.record.status !== "pending" && state.record.status !== "paused") {
      throw new AgentError("long_task_not_startable", "Long task cannot start");
    }
    this.#assertDeadline(state);
    this.#setStatus(state, "running");
    return state.record;
  }

  public async claimReadyUnit(
    taskId: string,
    workerId: string,
    leaseDurationMs: number,
  ): Promise<LongTaskUnitRecord | undefined> {
    const state = this.#require(taskId);
    if (state.record.status !== "running") return undefined;
    this.#assertDeadline(state);
    const budgetKind = this.#budgetKind(state, true);
    if (budgetKind !== undefined) {
      this.#failBudget(state, budgetKind);
      return undefined;
    }
    const now = this.#clockMs();
    const duration = positiveInteger(leaseDurationMs, "lease duration");
    let swept = false;
    for (const [id, unit] of state.units) {
      if (
        (unit.status === "claimed" || unit.status === "running")
        && unit.leaseExpiresAtMs !== null
        && unit.leaseExpiresAtMs <= now
      ) {
        const exhausted = unit.attempt >= unit.maxAttempts;
        state.units.set(id, Object.freeze({
          ...clearClaim(unit, exhausted ? "failed" : "pending"),
          errorCode: exhausted ? "long_task_lease_expired" : null,
        }));
        swept = true;
      } else if (
        unit.status === "waiting_retry"
        && unit.retryReadyAtMs !== null
        && unit.retryReadyAtMs <= now
      ) {
        state.units.set(id, Object.freeze({ ...unit, status: "pending", retryReadyAtMs: null }));
        swept = true;
      }
    }
    if (swept) this.#refreshCounts(state);
    const completed = new Set([...state.units.values()]
      .filter((unit) => unit.status === "completed")
      .map((unit) => unit.id));
    const candidate = [...state.units.values()]
      .sort((a, b) => a.position - b.position)
      .find((unit) => (
        unit.status === "pending"
        && unit.attempt < unit.maxAttempts
        && unit.dependencies.every((dependency) => completed.has(dependency))
      ));
    if (candidate === undefined) return undefined;
    const claimed = Object.freeze({
      ...candidate,
      status: "claimed" as const,
      attempt: candidate.attempt + 1,
      workerId: requiredText(workerId, "long task workerId"),
      claimToken: requiredText(this.#tokenFactory(), "long task claim token"),
      leaseEpoch: candidate.leaseEpoch + 1,
      leaseExpiresAtMs: now + duration,
      retryReadyAtMs: null,
    });
    state.units.set(candidate.id, claimed);
    this.#touch(state);
    return claimed;
  }

  public async markUnitRunning(claim: LongTaskClaim): Promise<LongTaskUnitRecord> {
    const { state, unit } = this.#requireClaim(claim);
    const running = Object.freeze({ ...unit, status: "running" as const });
    state.units.set(unit.id, running);
    this.#touch(state);
    return running;
  }

  public async heartbeat(
    claim: LongTaskClaim,
    leaseDurationMs: number,
  ): Promise<LongTaskUnitRecord> {
    const { state, unit } = this.#requireClaim(claim);
    const renewed = Object.freeze({
      ...unit,
      leaseExpiresAtMs: this.#clockMs() + positiveInteger(leaseDurationMs, "lease duration"),
    });
    state.units.set(unit.id, renewed);
    this.#touch(state);
    return renewed;
  }

  public async appendCheckpoint(
    claim: LongTaskClaim,
    payload: JsonValue,
  ): Promise<LongTaskCheckpoint> {
    const { state, unit } = this.#requireClaim(claim);
    const checkpoints = state.checkpoints.get(unit.id) ?? [];
    const checkpoint = Object.freeze({
      taskId: state.record.id,
      unitId: unit.id,
      sequence: checkpoints.length + 1,
      claimToken: claim.claimToken,
      leaseEpoch: claim.leaseEpoch,
      payload: copyJsonValue(payload),
      createdAtMs: this.#clockMs(),
    });
    checkpoints.push(checkpoint);
    state.checkpoints.set(unit.id, checkpoints);
    this.#touch(state);
    return checkpoint;
  }

  public async recordUsage(
    claim: LongTaskClaim,
    usage: ModelTokenUsage | null,
  ): Promise<LongTaskUnitRecord> {
    const { state, unit } = this.#requireClaim(claim);
    const delta = normalizeUsage(usage);
    const updated = Object.freeze({ ...unit, usage: addUsage(unit.usage, delta) });
    state.units.set(unit.id, updated);
    state.record = Object.freeze({ ...state.record, usage: addUsage(state.record.usage, delta) });
    const budgetKind = this.#budgetKind(state, false);
    if (budgetKind !== undefined) {
      this.#failBudget(state, budgetKind);
      throw new AgentError("runtime_budget_exceeded", `Long task ${budgetKind} budget is exhausted`);
    }
    this.#touch(state);
    return updated;
  }

  public async completeUnit(
    claim: LongTaskClaim,
    result: LongTaskUnitResult,
    settlementKey: string,
  ): Promise<LongTaskUnitRecord> {
    const state = this.#require(claim.taskId);
    const key = requiredText(settlementKey, "unit settlement key");
    const settledUnitId = state.settlements.get(key);
    if (settledUnitId !== undefined) {
      if (settledUnitId !== claim.unitId) {
        throw new AgentError(
          "long_task_settlement_conflict",
          "Unit settlement key belongs to another unit",
        );
      }
      return state.units.get(settledUnitId)!;
    }
    const required = this.#requireClaim(claim);
    const outputRef = requiredText(result.outputRef, "long task outputRef");
    const completed = Object.freeze({
      ...required.unit,
      status: "completed" as const,
      workerId: null,
      claimToken: null,
      leaseExpiresAtMs: null,
      outputRef,
      artifactDigest: result.artifactDigest ?? null,
      errorCode: null,
      metadata: copyMapping(result.metadata ?? required.unit.metadata),
    });
    state.units.set(completed.id, completed);
    state.settlements.set(key, completed.id);
    if (result.usage !== undefined) {
      const delta = normalizeUsage(result.usage);
      state.units.set(completed.id, Object.freeze({ ...completed, usage: addUsage(completed.usage, delta) }));
      state.record = Object.freeze({ ...state.record, usage: addUsage(state.record.usage, delta) });
      const budgetKind = this.#budgetKind(state, false);
      if (budgetKind !== undefined) {
        this.#failBudget(state, budgetKind);
        return state.units.get(completed.id)!;
      }
    }
    this.#refreshCounts(state);
    return state.units.get(completed.id)!;
  }

  public async failUnit(
    claim: LongTaskClaim,
    errorCode: string,
    retryable: boolean,
    retryDelayMs: number,
  ): Promise<LongTaskUnitRecord> {
    const { state, unit } = this.#requireClaim(claim);
    const retry = retryable && unit.attempt < unit.maxAttempts;
    const failed = clearClaim(Object.freeze({
      ...unit,
      status: retry ? "waiting_retry" as const : "failed" as const,
      retryReadyAtMs: retry ? this.#clockMs() + nonNegativeInteger(retryDelayMs, "retry delay") : null,
      errorCode: requiredText(errorCode, "unit errorCode"),
    }), retry ? "waiting_retry" : "failed");
    state.units.set(unit.id, failed);
    this.#refreshCounts(state);
    return failed;
  }

  public async listCheckpoints(
    taskId: string,
    unitId: string,
  ): Promise<readonly LongTaskCheckpoint[]> {
    return Object.freeze([...(this.#require(taskId).checkpoints.get(unitId) ?? [])]);
  }

  public async finalizeIfComplete(taskId: string): Promise<LongTaskRecord> {
    const state = this.#require(taskId);
    if (state.record.cancellationRequestedAtMs !== null) return this.cancel(taskId);
    if (state.record.status !== "running") return state.record;
    const required = [...state.units.values()].filter((unit) => unit.required);
    if (required.some((unit) => unit.status === "failed")) this.#setStatus(state, "failed");
    else if (required.every((unit) => unit.status === "completed")) this.#setStatus(state, "completed");
    return state.record;
  }

  public async pause(
    taskId: string,
    options: {
      readonly expectedRevision?: number;
      readonly reasonCode?: string;
    } = {},
  ): Promise<LongTaskRecord> {
    const state = this.#require(taskId);
    if (
      options.expectedRevision !== undefined
      && state.record.revision !== positiveInteger(options.expectedRevision, "expected task revision")
    ) {
      throw new AgentError("stale_long_task_revision", "Long task revision is stale");
    }
    if (options.reasonCode !== undefined) requiredText(options.reasonCode, "pause reasonCode");
    if (state.record.status === "paused") return state.record;
    if (state.record.status !== "pending" && state.record.status !== "running") {
      throw new AgentError("long_task_not_pausable", "Long task cannot pause");
    }
    for (const [id, unit] of state.units) {
      if (unit.status === "claimed" || unit.status === "running") {
        state.units.set(id, Object.freeze({
          ...clearClaim(unit, "pending"),
          attempt: Math.max(0, unit.attempt - 1),
        }));
      }
    }
    this.#setStatus(state, "paused");
    return state.record;
  }

  public async resume(taskId: string, additionalAttempts = 0): Promise<LongTaskRecord> {
    const state = this.#require(taskId);
    if (state.record.status !== "paused" && state.record.status !== "failed") {
      throw new AgentError("long_task_not_resumable", "Long task cannot resume");
    }
    const extra = nonNegativeInteger(additionalAttempts, "additional attempts");
    for (const [id, unit] of state.units) {
      if (unit.status === "failed" || unit.status === "canceled") {
        if (extra < 1) throw new AgentError("durable_retry_requires_attempts", "Resume needs additional attempts");
        state.units.set(id, Object.freeze({
          ...clearClaim(unit, "pending"),
          maxAttempts: unit.maxAttempts + extra,
          errorCode: null,
        }));
      }
    }
    this.#setStatus(state, "running");
    this.#refreshCounts(state);
    return state.record;
  }

  public async requestCancel(taskId: string, requestedAtMs?: number): Promise<LongTaskRecord> {
    const state = this.#require(taskId);
    if (isTerminal(state.record.status)) return state.record;
    if (state.record.cancellationRequestedAtMs === null) {
      state.record = Object.freeze({
        ...state.record,
        cancellationRequestedAtMs: requestedAtMs ?? this.#clockMs(),
      });
      this.#touch(state);
    }
    return state.record;
  }

  public async cancel(taskId: string): Promise<LongTaskRecord> {
    const state = this.#require(taskId);
    if (state.record.status === "canceled") return state.record;
    if (state.record.status === "completed" || state.record.status === "failed") {
      throw new AgentError("long_task_terminal", "Terminal long task cannot be canceled");
    }
    for (const [id, unit] of state.units) {
      if (unit.status !== "completed") state.units.set(id, clearClaim(unit, "canceled"));
    }
    this.#setStatus(state, "canceled");
    return state.record;
  }

  #require(taskId: string): TaskState {
    const state = this.#tasks.get(requiredText(taskId, "long task id"));
    if (state === undefined) throw new AgentError("long_task_not_found", "Long task does not exist");
    return state;
  }

  #requireClaim(claim: LongTaskClaim): { readonly state: TaskState; readonly unit: LongTaskUnitRecord } {
    const state = this.#require(claim.taskId);
    const unit = state.units.get(claim.unitId);
    if (unit === undefined) throw new AgentError("long_task_unit_not_found", "Long task unit does not exist");
    const now = this.#clockMs();
    if (unit.leaseExpiresAtMs !== null && unit.leaseExpiresAtMs <= now) {
      throw new AgentError("long_task_lease_expired", "Long task unit lease has expired");
    }
    if (
      (unit.status !== "claimed" && unit.status !== "running")
      || unit.workerId !== claim.workerId
      || unit.claimToken !== claim.claimToken
      || unit.leaseEpoch !== claim.leaseEpoch
    ) {
      throw new AgentError("long_task_unit_lease_lost", "Long task claim is stale");
    }
    return { state, unit };
  }

  #assertDeadline(state: TaskState): void {
    if (state.record.deadlineAtMs !== null && state.record.deadlineAtMs <= this.#clockMs()) {
      for (const [id, unit] of state.units) {
        if (unit.status !== "completed" && unit.status !== "failed" && unit.status !== "canceled") {
          state.units.set(id, Object.freeze({
            ...clearClaim(unit, "failed"),
            errorCode: "long_task_deadline_exceeded",
          }));
        }
      }
      this.#refreshCounts(state);
      this.#setStatus(state, "failed");
      throw new AgentError("long_task_deadline_exceeded", "Long task deadline has elapsed");
    }
  }

  #budgetKind(state: TaskState, inclusive: boolean): string | undefined {
    const { budgets, usage } = state.record;
    if (
      usage.unreportedUsageAttempts > 0
      && (
        budgets.maxInputTokens !== null
        || budgets.maxRunGenerationTokens !== null
        || budgets.maxReasoningTokens !== null
      )
    ) return "provider_usage_unreported";
    if (
      usage.invocationCount > 0
      && usage.reasoningTokens === null
      && budgets.maxReasoningTokens !== null
    ) return "reasoning_tokens_unreported";
    const rows = [
      ["model_attempts", usage.invocationCount, budgets.maxInvocationAttempts],
      ["input_tokens", usage.inputTokens, budgets.maxInputTokens],
      ["generation_tokens", usage.generationTokens, budgets.maxRunGenerationTokens],
      ["reasoning_tokens", usage.reasoningTokens, budgets.maxReasoningTokens],
    ] as const;
    return rows.find(([, used, maximum]) => (
      maximum !== null
      && used !== null
      && (used > maximum || (inclusive && used >= maximum))
    ))?.[0];
  }

  #failBudget(state: TaskState, budgetKind: string): void {
    for (const [id, unit] of state.units) {
      if (unit.status !== "completed" && unit.status !== "failed" && unit.status !== "canceled") {
        state.units.set(id, Object.freeze({
          ...clearClaim(unit, "failed"),
          errorCode: "runtime_budget_exceeded",
          metadata: copyMapping({ ...unit.metadata, budgetKind }),
        }));
      }
    }
    this.#refreshCounts(state);
    this.#setStatus(state, "failed");
  }

  #refreshCounts(state: TaskState): void {
    const units = [...state.units.values()];
    state.record = Object.freeze({
      ...state.record,
      completedUnits: units.filter((unit) => unit.status === "completed").length,
      failedUnits: units.filter((unit) => unit.status === "failed").length,
    });
    this.#touch(state);
  }

  #setStatus(state: TaskState, status: LongTaskRecord["status"]): void {
    state.record = Object.freeze({ ...state.record, status });
    this.#touch(state);
  }

  #touch(state: TaskState): void {
    state.record = Object.freeze({ ...state.record, revision: state.record.revision + 1 });
  }
}

export function claimFromUnit(unit: LongTaskUnitRecord): LongTaskClaim {
  if (unit.workerId === null || unit.claimToken === null) {
    throw new AgentError("long_task_unit_lease_lost", "Long task unit is not claimed");
  }
  return Object.freeze({
    taskId: unit.taskId,
    unitId: unit.id,
    workerId: unit.workerId,
    claimToken: unit.claimToken,
    leaseEpoch: unit.leaseEpoch,
  });
}

function validateCommand(command: LongTaskCreateCommand): void {
  if (!Array.isArray(command.units) || command.units.length === 0) {
    throw new TypeError("Long task requires units");
  }
  const ids = new Set<string>();
  const positions = new Set<number>();
  for (const unit of command.units) {
    const id = requiredText(unit.id, "long task unit id");
    if (ids.has(id)) throw new TypeError("Long task unit ids must be unique");
    ids.add(id);
    if (!Number.isSafeInteger(unit.position) || unit.position < 0 || positions.has(unit.position)) {
      throw new TypeError("Long task unit positions must be unique non-negative integers");
    }
    positions.add(unit.position);
    positiveInteger(unit.maxAttempts ?? 1, "long task unit maxAttempts");
  }
  const known = new Set<string>();
  for (const unit of [...command.units].sort((a, b) => a.position - b.position)) {
    for (const dependency of unit.dependencies ?? []) {
      if (!ids.has(dependency) || !known.has(dependency)) {
        throw new TypeError("Long task units must be topologically ordered");
      }
    }
    known.add(unit.id);
  }
  if (!command.units.some((unit) => unit.required ?? true)) {
    throw new TypeError("Long task needs at least one required unit");
  }
}

function clearClaim(
  unit: LongTaskUnitRecord,
  status: LongTaskUnitRecord["status"],
): LongTaskUnitRecord {
  return Object.freeze({
    ...unit,
    status,
    workerId: null,
    claimToken: null,
    leaseExpiresAtMs: null,
    ...(status === "waiting_retry" ? {} : { retryReadyAtMs: null }),
  });
}

function normalizeUsage(value: ModelTokenUsage | null): LongTaskUsage {
  if (value === null) return Object.freeze({
    invocationCount: 1,
    unreportedUsageAttempts: 1,
    inputTokens: 0,
    generationTokens: 0,
    reasoningTokens: null,
  });
  if (value === null || typeof value !== "object") throw new TypeError("Usage must be an object");
  return Object.freeze({
    invocationCount: 1,
    unreportedUsageAttempts: 0,
    inputTokens: nonNegativeInteger(value.inputTokens, "usage inputTokens"),
    generationTokens: nonNegativeInteger(value.generationTokens ?? 0, "usage generationTokens"),
    reasoningTokens: value.reasoningTokens === undefined
      ? null
      : nonNegativeInteger(value.reasoningTokens, "usage reasoningTokens"),
  });
}

function addUsage(left: LongTaskUsage, right: LongTaskUsage): LongTaskUsage {
  return Object.freeze({
    invocationCount: left.invocationCount + right.invocationCount,
    unreportedUsageAttempts: left.unreportedUsageAttempts + right.unreportedUsageAttempts,
    inputTokens: left.inputTokens + right.inputTokens,
    generationTokens: left.generationTokens + right.generationTokens,
    reasoningTokens: left.invocationCount === 0
      ? right.reasoningTokens
      : right.invocationCount === 0
      ? left.reasoningTokens
      : left.reasoningTokens === null || right.reasoningTokens === null
      ? null
      : left.reasoningTokens + right.reasoningTokens,
  });
}

function copyMapping(value: Readonly<Record<string, JsonValue>>): Readonly<Record<string, JsonValue>> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Long task metadata must be an object");
  }
  return copyJsonValue(value) as Readonly<Record<string, JsonValue>>;
}

function idempotencyKey(namespace: string, key: string): string {
  return `${requiredText(namespace, "namespace")}\u0000${requiredText(key, "idempotency key")}`;
}

function nullablePositive(value: number | null, label: string): number | null {
  return value === null ? null : positiveInteger(value, label);
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${label} must be positive`);
  return value;
}

function nonNegativeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${label} must be non-negative`);
  return value;
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`${label} is required`);
  return value.trim();
}

function isTerminal(status: LongTaskRecord["status"]): boolean {
  return status === "completed" || status === "failed" || status === "canceled";
}
