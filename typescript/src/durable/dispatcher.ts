import type { JsonValue, ModelTokenUsage } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import { stableFingerprint } from "../shared/fingerprint.js";
import {
  copyAdmissionDecision,
  copyDurableTaskDescriptor,
} from "./contracts.js";
import { claimFromUnit } from "./repository.js";
import type {
  DurableTaskDescriptorResolver,
  DurableUnitExecutionContext,
  DurableUnitExecutor,
  LongTaskClaim,
  LongTaskDispatchReceipt,
  LongTaskDispatcher,
  LongTaskExecutionObserver,
  LongTaskExecutionResult,
  LongTaskRecord,
  LongTaskRepository,
  LongTaskUnitRecord,
  LongTaskUnitResult,
} from "./types.js";

export class DurableExecutorRegistry {
  readonly #executors: ReadonlyMap<string, DurableUnitExecutor>;

  public constructor(executors: Readonly<Record<string, DurableUnitExecutor>>) {
    if (executors === null || typeof executors !== "object" || Array.isArray(executors)) {
      throw new TypeError("Durable executors must be an object");
    }
    const entries = Object.entries(executors).map(([name, executor]) => {
      if (typeof executor?.execute !== "function") {
        throw new TypeError(`Durable executor ${name} must implement execute`);
      }
      return [requiredText(name, "durable executor id"), executor] as const;
    });
    if (entries.length === 0) throw new TypeError("Durable executor registry must not be empty");
    this.#executors = new Map(entries);
  }

  public require(name: string): DurableUnitExecutor {
    const executor = this.#executors.get(requiredText(name, "durable executor id"));
    if (executor === undefined) {
      throw new AgentError("durable_executor_not_found", `Durable executor is not registered: ${name}`);
    }
    return executor;
  }
}

export class RecipeLongTaskDispatcher implements LongTaskDispatcher {
  readonly #repository: LongTaskRepository;
  readonly #descriptors: DurableTaskDescriptorResolver;
  readonly #executors: DurableExecutorRegistry;
  readonly #workerId: string;
  readonly #leaseDurationMs: number;
  readonly #retryBackoffMs: readonly number[];
  readonly #idFactory: () => string;

  public constructor(options: {
    readonly repository: LongTaskRepository;
    readonly descriptorResolver: DurableTaskDescriptorResolver;
    readonly executors: DurableExecutorRegistry;
    readonly workerId: string;
    readonly leaseDurationMs?: number;
    readonly retryBackoffMs?: readonly number[];
    readonly idFactory?: () => string;
  }) {
    if (typeof options.repository?.create !== "function") {
      throw new TypeError("Durable dispatcher requires a LongTaskRepository");
    }
    if (typeof options.descriptorResolver?.resolve !== "function") {
      throw new TypeError("Durable dispatcher requires a descriptor resolver");
    }
    if (!(options.executors instanceof DurableExecutorRegistry)) {
      throw new TypeError("Durable dispatcher requires a DurableExecutorRegistry");
    }
    this.#repository = options.repository;
    this.#descriptors = options.descriptorResolver;
    this.#executors = options.executors;
    this.#workerId = requiredText(options.workerId, "durable workerId");
    this.#leaseDurationMs = positiveInteger(options.leaseDurationMs ?? 300_000, "lease duration");
    this.#retryBackoffMs = Object.freeze((options.retryBackoffMs ?? []).map((value) => (
      nonNegativeInteger(value, "retry backoff")
    )));
    this.#idFactory = options.idFactory ?? (() => globalThis.crypto.randomUUID());
  }

  public async dispatch(input: Parameters<LongTaskDispatcher["dispatch"]>[0]): Promise<LongTaskDispatchReceipt> {
    throwIfCanceled(input.signal);
    const admission = copyAdmissionDecision(input.admission, input.plan);
    if (admission.mode !== "durable" || admission.executionRecipe === undefined) {
      throw new AgentError("invalid_task_admission", "Durable dispatcher requires durable admission");
    }
    const descriptor = copyDurableTaskDescriptor(await this.#descriptors.resolve({
      plan: input.plan,
      admission,
      runId: input.runId,
    }));
    const recipeFingerprint = await stableFingerprint(copyJsonValue(admission.executionRecipe));
    let task = await this.#repository.findByIdempotencyKey(
      descriptor.namespace,
      descriptor.idempotencyKey,
    );
    if (task === undefined) {
      task = await this.#repository.create(requiredText(this.#idFactory(), "durable task id"), {
        namespace: descriptor.namespace,
        kind: admission.executionRecipe.kind,
        ownerId: descriptor.ownerId,
        createdByRunId: input.runId,
        idempotencyKey: descriptor.idempotencyKey,
        units: admission.executionRecipe.steps.map((step, position) => Object.freeze({
          id: step.id,
          position,
          semanticKey: step.id,
          ...(step.dependsOn === undefined ? {} : { dependencies: step.dependsOn }),
          required: true,
          ...(step.inputRef === undefined ? {} : { inputRef: step.inputRef }),
          executor: step.executor,
          planStepId: step.planStepId,
          ...(step.maxAttempts === undefined ? {} : { maxAttempts: step.maxAttempts }),
          ...(step.metadata === undefined ? {} : { metadata: step.metadata }),
        })),
        ...(admission.executionRecipe.maxParallelism === undefined
          ? {}
          : { maxParallelism: admission.executionRecipe.maxParallelism }),
        deadlineAtMs: descriptor.deadlineAtMs === undefined
          ? deadlineMs(input.deadlineAt)
          : descriptor.deadlineAtMs,
        budgets: descriptor.budgets ?? taskBudgets(input.budgets),
        metadata: Object.freeze({
          ...(descriptor.metadata ?? {}),
          recipeFingerprint,
        }),
      });
    } else {
      if (task.metadata.recipeFingerprint !== recipeFingerprint) {
        throw new AgentError(
          "durable_recipe_mismatch",
          "Existing durable task uses a different execution recipe",
        );
      }
      if (task.ownerId !== descriptor.ownerId || task.kind !== admission.executionRecipe.kind) {
        throw new AgentError(
          "durable_idempotency_conflict",
          "Existing durable task belongs to a different authority",
        );
      }
      const expectedDeadline = descriptor.deadlineAtMs === undefined
        ? deadlineMs(input.deadlineAt)
        : descriptor.deadlineAtMs;
      const expectedBudgets = descriptor.budgets ?? taskBudgets(input.budgets);
      if (
        task.deadlineAtMs !== expectedDeadline
        || JSON.stringify(task.budgets) !== JSON.stringify(expectedBudgets)
      ) {
        throw new AgentError(
          "durable_idempotency_conflict",
          "Existing durable task uses different deadline or budget authority",
        );
      }
      await this.#repository.bindRun(task.id, input.runId, "continuation");
    }
    return Object.freeze({
      schemaVersion: 1,
      taskId: task.id,
      message: descriptor.message ?? `Durable task ${task.id} admitted`,
      admission,
      recipeFingerprint,
      metadata: Object.freeze({
        namespace: task.namespace,
        ownerId: task.ownerId,
        reused: task.createdByRunId !== input.runId,
        recipeFingerprint,
      }),
    });
  }

  public async execute(input: Parameters<LongTaskDispatcher["execute"]>[0]): Promise<LongTaskExecutionResult> {
    validateReceipt(input.receipt);
    const taskId = input.receipt.taskId;
    let task = await this.#requireTask(taskId);
    if (task.metadata.recipeFingerprint !== input.receipt.recipeFingerprint) {
      throw new AgentError("durable_recipe_mismatch", "Dispatch receipt does not match stored recipe");
    }
    if (input.runId !== task.createdByRunId) {
      await this.#repository.bindRun(task.id, input.runId, "continuation");
    }
    if (task.status === "completed") return this.#result(task, await this.#repository.listUnits(task.id));
    if (task.status === "canceled" || task.status === "failed") {
      return this.#result(task, await this.#repository.listUnits(task.id));
    }
    if (task.status === "pending") task = await this.#repository.start(task.id);
    else if (task.status === "paused") task = await this.#repository.resume(task.id);

    // ponytail: execute one ready unit at a time. Repository DAG and fencing
    // stay correct; add a bounded worker pool only when parallel throughput is required.
    while (task.status === "running") {
      if (input.signal?.aborted === true) {
        task = await this.#repository.pause(task.id);
        break;
      }
      if (task.cancellationRequestedAtMs !== null) {
        task = await this.#repository.cancel(task.id);
        break;
      }
      let unit: LongTaskUnitRecord | undefined;
      try {
        unit = await this.#repository.claimReadyUnit(
          task.id,
          this.#workerId,
          this.#leaseDurationMs,
        );
      } catch (error) {
        if (error instanceof AgentError && error.code === "long_task_deadline_exceeded") {
          task = await this.#requireTask(task.id);
          break;
        }
        throw error;
      }
      if (unit === undefined) {
        task = await this.#repository.finalizeIfComplete(task.id);
        if (task.status === "running") await delay(1, input.signal);
        continue;
      }
      await this.#runUnit(task, unit, input.observer, input.signal);
      task = await this.#requireTask(task.id);
      await emitProgress(input.observer, task, await this.#repository.listUnits(task.id));
    }
    return this.#result(task, await this.#repository.listUnits(task.id));
  }

  async #runUnit(
    task: LongTaskRecord,
    claimed: LongTaskUnitRecord,
    observer: LongTaskExecutionObserver | undefined,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const claim = claimFromUnit(claimed);
    const unit = await this.#repository.markUnitRunning(claim);
    const units = await this.#repository.listUnits(task.id);
    const dependencyOutputs = Object.freeze(Object.fromEntries(unit.dependencies.map((id) => {
      const dependency = units.find((item) => item.id === id);
      if (dependency?.outputRef === null || dependency?.outputRef === undefined) {
        throw new AgentError("durable_dependency_missing", `Dependency output is missing: ${id}`);
      }
      return [id, dependency.outputRef];
    })));
    const executor = this.#executors.require(unit.executor);
    let heartbeatFailure: unknown;
    let heartbeatPending: Promise<void> = Promise.resolve();
    const executionController = new AbortController();
    const forwardCancellation = () => executionController.abort(signal?.reason);
    signal?.addEventListener("abort", forwardCancellation, { once: true });
    if (signal?.aborted === true) forwardCancellation();
    const heartbeatMs = Math.max(1, Math.floor(this.#leaseDurationMs / 3));
    const timer = globalThis.setInterval(() => {
      heartbeatPending = heartbeatPending.then(async () => {
        if (heartbeatFailure !== undefined) return;
        try {
          await this.#repository.heartbeat(claim, this.#leaseDurationMs);
        } catch (error) {
          heartbeatFailure = error;
          executionController.abort(error);
        }
      });
    }, heartbeatMs);
    try {
      const context: DurableUnitExecutionContext = Object.freeze({
        task,
        unit,
        dependencyOutputs,
        signal: executionController.signal,
        checkpoint: async (payload: JsonValue) => {
          const checkpoint = await this.#repository.appendCheckpoint(claim, payload);
          await observer?.(Object.freeze({
            type: "long_task.checkpoint",
            payload: copyJsonValue({
              taskId: checkpoint.taskId,
              unitId: checkpoint.unitId,
              sequence: checkpoint.sequence,
              leaseEpoch: checkpoint.leaseEpoch,
            }) as Readonly<Record<string, JsonValue>>,
          }));
          return checkpoint;
        },
        recordUsage: (usage: ModelTokenUsage) => (
          this.#repository.recordUsage(claim, usage).then(() => undefined)
        ),
      });
      const result = await abortable(
        Promise.resolve(executor.execute(context)),
        executionController.signal,
      );
      await heartbeatPending;
      if (heartbeatFailure !== undefined) throw heartbeatFailure;
      await this.#repository.completeUnit(
        claim,
        result,
        `${task.id}:${unit.id}:${unit.attempt}:${requiredText(result.outputRef, "durable outputRef")}`,
      );
    } catch (error) {
      if (heartbeatFailure !== undefined) return;
      if (error instanceof AgentError && error.code === "runtime_budget_exceeded") return;
      if (error instanceof AgentCanceledError || signal?.aborted === true) {
        await this.#repository.pause(task.id);
        return;
      }
      const retryable = isRetryable(error);
      const index = Math.min(Math.max(0, unit.attempt - 1), Math.max(0, this.#retryBackoffMs.length - 1));
      await this.#repository.failUnit(
        claim,
        errorCode(error),
        retryable,
        this.#retryBackoffMs[index] ?? 0,
      );
    } finally {
      globalThis.clearInterval(timer);
      await heartbeatPending;
      signal?.removeEventListener("abort", forwardCancellation);
    }
  }

  async #requireTask(taskId: string): Promise<LongTaskRecord> {
    const task = await this.#repository.load(taskId);
    if (task === undefined) throw new AgentError("long_task_not_found", "Long task does not exist");
    return task;
  }

  #result(task: LongTaskRecord, units: readonly LongTaskUnitRecord[]): LongTaskExecutionResult {
    const status = task.status === "completed"
      ? "completed"
      : task.status === "canceled"
        ? "canceled"
        : task.status === "paused"
          ? "paused"
          : "failed";
    const final = [...units]
      .filter((unit) => unit.required && unit.status === "completed" && unit.outputRef !== null)
      .sort((a, b) => b.position - a.position)[0]?.outputRef ?? "";
    return Object.freeze({
      taskId: task.id,
      status,
      finalResponse: final,
      ...(status === "failed"
        ? { errorCode: units.find((unit) => unit.status === "failed")?.errorCode ?? "long_task_failed" }
        : {}),
    });
  }
}

async function emitProgress(
  observer: LongTaskExecutionObserver | undefined,
  task: LongTaskRecord,
  units: readonly LongTaskUnitRecord[],
): Promise<void> {
  await observer?.(Object.freeze({
    type: "long_task.progress",
    payload: copyJsonValue({
      taskId: task.id,
      status: task.status,
      completedUnits: task.completedUnits,
      totalUnits: task.totalUnits,
      units: units.map((unit) => ({
        id: unit.id,
        planStepId: unit.planStepId,
        status: unit.status,
        attempt: unit.attempt,
      })),
    }) as Readonly<Record<string, JsonValue>>,
  }));
}

function validateReceipt(receipt: LongTaskDispatchReceipt): void {
  if (receipt?.schemaVersion !== 1) {
    throw new AgentError("durable_receipt_unsupported", "Durable dispatch receipt is unsupported");
  }
  requiredText(receipt.taskId, "durable receipt taskId");
  requiredText(receipt.recipeFingerprint, "durable receipt recipeFingerprint");
}

function isRetryable(error: unknown): boolean {
  return error !== null
    && typeof error === "object"
    && "retryable" in error
    && (error as { readonly retryable?: unknown }).retryable === true;
}

function errorCode(error: unknown): string {
  if (error instanceof AgentError) return error.code;
  if (error !== null && typeof error === "object" && "code" in error) {
    const code = (error as { readonly code?: unknown }).code;
    if (typeof code === "string" && code.trim() !== "") return code.trim().slice(0, 128);
  }
  return "durable_unit_failed";
}

function taskBudgets(value: import("../run/types.js").RunBudgets): import("./types.js").LongTaskBudgetLimits {
  return Object.freeze({
    maxInvocationAttempts: value.maxModelAttempts,
    maxInputTokens: value.maxInputTokens,
    maxRunOutputTokens: value.maxRunOutputTokens,
    maxReasoningTokens: value.maxReasoningTokens,
  });
}

function deadlineMs(value: string | null): number | null {
  if (value === null) return null;
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) {
    throw new TypeError("Durable deadline must be an ISO date-time");
  }
  return milliseconds;
}

async function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) throw new AgentCanceledError();
  let cancel: (() => void) | undefined;
  const canceled = new Promise<never>((_resolve, reject) => {
    cancel = () => reject(new AgentCanceledError());
    signal.addEventListener("abort", cancel, { once: true });
  });
  try {
    return await Promise.race([promise, canceled]);
  } finally {
    if (cancel !== undefined) signal.removeEventListener("abort", cancel);
    void promise.catch(() => undefined);
  }
}

async function delay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted === true) throw new AgentCanceledError();
  await new Promise<void>((resolve) => globalThis.setTimeout(resolve, milliseconds));
}

function throwIfCanceled(signal?: AbortSignal): void {
  if (signal?.aborted === true) throw new AgentCanceledError();
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
