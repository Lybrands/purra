import { copyJsonValue } from "../model/validation.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import { stableFingerprint } from "../shared/fingerprint.js";
import { DelegationPolicy } from "./policy.js";
import type {
  AgentDelegation,
  DelegatedAgentExecutor,
  DelegatedAgentResult,
  DelegationAggregation,
  DelegationDefinition,
  DelegationEventSink,
  DelegationLifecycleEvent,
  DelegationRepository,
} from "./types.js";

interface ActiveCall {
  readonly digest: string;
  readonly promise: Promise<DelegationAggregation>;
}

export class DelegationCoordinator {
  readonly #repository: DelegationRepository;
  readonly #executor: DelegatedAgentExecutor;
  readonly #policy: DelegationPolicy;
  readonly #capacity: Semaphore;
  readonly #sinks = new Map<string, DelegationEventSink>();
  readonly #activeCalls = new Map<string, ActiveCall>();
  readonly #controllers = new Map<string, Set<AbortController>>();

  public constructor(options: {
    readonly repository: DelegationRepository;
    readonly executor: DelegatedAgentExecutor;
    readonly policy?: DelegationPolicy;
  }) {
    if (typeof options.repository?.createBatch !== "function") {
      throw new TypeError("Delegation coordinator requires a repository");
    }
    if (typeof options.executor?.execute !== "function") {
      throw new TypeError("Delegation coordinator requires an executor");
    }
    this.#repository = options.repository;
    this.#executor = options.executor;
    this.#policy = options.policy ?? new DelegationPolicy();
    this.#capacity = new Semaphore(this.#policy.snapshot().maxParallel);
  }

  public bindRun(runId: string, sink: DelegationEventSink): void {
    const id = requiredText(runId, "delegation Run id");
    if (this.#sinks.has(id)) throw new AgentError("delegation_root_run_already_bound", "Root Run is already bound");
    this.#sinks.set(id, sink);
  }

  public releaseRun(runId: string): void {
    this.#sinks.delete(runId);
  }

  public async executeCall(input: {
    readonly runId: string;
    readonly idempotencyKey: string;
    readonly delegations: unknown;
    readonly enabledTools?: readonly string[];
    readonly signal?: AbortSignal;
  }): Promise<DelegationAggregation> {
    const runId = requiredText(input.runId, "delegation Run id");
    const idempotencyKey = requiredText(input.idempotencyKey, "delegation idempotency key");
    if (!this.#sinks.has(runId)) {
      throw new AgentError("delegation_root_run_not_active", "Delegation requires an active Root Run");
    }
    const definitions = this.#policy.validate(input.delegations);
    const digest = await stableFingerprint(copyJsonValue(definitions));
    const key = `${runId}\u0000${idempotencyKey}`;
    const active = this.#activeCalls.get(key);
    if (active !== undefined) {
      if (active.digest !== digest) {
        throw new AgentError(
          "delegation_idempotency_conflict",
          "Delegation idempotency key was reused with different input",
        );
      }
      return active.promise;
    }
    const promise = this.#executeCall({
      runId,
      idempotencyKey,
      definitions,
      ...(input.enabledTools === undefined ? {} : { enabledTools: Object.freeze([...input.enabledTools]) }),
      ...(input.signal === undefined ? {} : { signal: input.signal }),
    });
    this.#activeCalls.set(key, { digest, promise });
    try {
      return await promise;
    } finally {
      this.#activeCalls.delete(key);
    }
  }

  public async cancelBatch(runId: string, batchId: string): Promise<number> {
    const id = requiredText(runId, "delegation Run id");
    const batch = requiredText(batchId, "delegation batch id");
    const rows = (await this.#repository.listForRun(id)).filter((row) => row.batchId === batch);
    const canceled = await this.#repository.cancelBatch(id, batch);
    for (const controller of this.#controllers.get(batchKey(id, batch)) ?? []) controller.abort();
    const settled = new Map((await this.#repository.listForRun(id))
      .filter((row) => row.batchId === batch)
      .map((row) => [row.id, row]));
    for (const row of rows) {
      if (
        (row.status === "queued" || row.status === "running")
        && settled.get(row.id)?.status === "canceled"
      ) await this.#emit(row, "canceled", "delegation_canceled");
    }
    return canceled;
  }

  public async close(): Promise<void> {
    for (const controllers of this.#controllers.values()) {
      for (const controller of controllers) controller.abort();
    }
    await Promise.allSettled([...this.#activeCalls.values()].map((call) => call.promise));
    this.#controllers.clear();
    this.#activeCalls.clear();
    this.#sinks.clear();
  }

  async #executeCall(input: {
    readonly runId: string;
    readonly idempotencyKey: string;
    readonly definitions: readonly DelegationDefinition[];
    readonly enabledTools?: readonly string[];
    readonly signal?: AbortSignal;
  }): Promise<DelegationAggregation> {
    const batchId = `delegation-batch:${input.idempotencyKey}`;
    const receipt = await this.#repository.createBatch({
      runId: input.runId,
      batchId,
      idempotencyKey: input.idempotencyKey,
      delegations: input.definitions,
    });
    if (!receipt.replayed) {
      for (const row of receipt.delegations) await this.#emit(row, "queued");
    }
    const executable: AgentDelegation[] = [];
    for (const row of receipt.delegations) {
      if (row.status === "queued") executable.push(row);
      if (row.status === "running") {
        const changed = await this.#repository.fail(
          row.id,
          row.runId,
          row.batchId,
          "delegation_interrupted",
        );
        if (changed) await this.#emit(row, "failed", "delegation_interrupted");
      }
    }
    await Promise.all(executable.map((row) => this.#executeOne(
      row,
      input.enabledTools,
      input.signal,
    )));
    return this.#repository.aggregateBatch(input.runId, batchId);
  }

  async #executeOne(
    delegation: AgentDelegation,
    enabledTools: readonly string[] | undefined,
    parentSignal: AbortSignal | undefined,
  ): Promise<DelegatedAgentResult> {
    const release = await this.#capacity.acquire(parentSignal);
    const controller = new AbortController();
    const key = batchKey(delegation.runId, delegation.batchId);
    const controllers = this.#controllers.get(key) ?? new Set<AbortController>();
    controllers.add(controller);
    this.#controllers.set(key, controllers);
    const signal = parentSignal === undefined
      ? controller.signal
      : AbortSignal.any([parentSignal, controller.signal]);
    try {
      const started = await this.#repository.start(delegation.id, delegation.runId, delegation.batchId);
      if (started === undefined) {
        return Object.freeze({ outcome: "failed", errorCode: "delegation_could_not_start" });
      }
      await this.#emit(started, "running");
      const result = validateResult(await this.#executor.execute(Object.freeze({
        runId: started.runId,
        batchId: started.batchId,
        delegationId: started.id,
        agentName: started.agentName,
        agentTitle: started.title,
        agentInstruction: started.instruction,
        objective: started.objective,
        input: started.input,
        contextMode: "isolated",
        ...(enabledTools === undefined ? {} : { enabledTools }),
      }), signal));
      if (result.outcome === "completed") {
        if (!await this.#repository.complete(
          started.id,
          started.runId,
          started.batchId,
          result.content ?? "",
        )) {
          throw new AgentError("delegation_settlement_failed", "Delegation result could not be persisted");
        }
        await this.#emit(started, "done");
        return result;
      }
      const status = result.outcome === "canceled" ? "canceled" : "failed";
      const changed = status === "canceled"
        ? await this.#repository.cancel(started.id, started.runId, started.batchId, result.errorCode!)
        : await this.#repository.fail(started.id, started.runId, started.batchId, result.errorCode!);
      if (changed) await this.#emit(started, status, result.errorCode);
      return result;
    } catch (error) {
      const canceled = signal.aborted || error instanceof AgentCanceledError;
      const errorCode = canceled ? "delegation_canceled" : code(error);
      const changed = canceled
        ? await this.#repository.cancel(delegation.id, delegation.runId, delegation.batchId, errorCode)
        : await this.#repository.fail(delegation.id, delegation.runId, delegation.batchId, errorCode);
      if (changed) await this.#emit(delegation, canceled ? "canceled" : "failed", errorCode);
      if (!canceled && !changed) throw error;
      return Object.freeze({ outcome: canceled ? "canceled" : "failed", errorCode });
    } finally {
      controllers.delete(controller);
      if (controllers.size === 0) this.#controllers.delete(key);
      release();
    }
  }

  async #emit(row: AgentDelegation, status: DelegationLifecycleEvent["status"], errorCode?: string): Promise<void> {
    const sink = this.#sinks.get(row.runId);
    if (sink === undefined) throw new AgentError("delegation_root_run_not_active", "Root Run is not active");
    await sink(Object.freeze({
      runId: row.runId,
      batchId: row.batchId,
      delegationId: row.id,
      agentName: row.agentName,
      agentTitle: row.title,
      status,
      ...(errorCode === undefined ? {} : { errorCode }),
    }));
  }
}

class Semaphore {
  readonly #limit: number;
  #active = 0;
  readonly #waiting: (() => void)[] = [];

  public constructor(limit: number) {
    this.#limit = limit;
  }

  public async acquire(signal?: AbortSignal): Promise<() => void> {
    if (signal?.aborted) throw new AgentCanceledError();
    if (this.#active >= this.#limit) {
      await new Promise<void>((resolve, reject) => {
        const cancel = (): void => {
          const index = this.#waiting.indexOf(resume);
          if (index >= 0) this.#waiting.splice(index, 1);
          reject(new AgentCanceledError());
        };
        const resume = (): void => {
          signal?.removeEventListener("abort", cancel);
          resolve();
        };
        this.#waiting.push(resume);
        signal?.addEventListener("abort", cancel, { once: true });
      });
    }
    this.#active += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.#active -= 1;
      this.#waiting.shift()?.();
    };
  }
}

function validateResult(value: DelegatedAgentResult): DelegatedAgentResult {
  if (value === null || typeof value !== "object") throw new TypeError("Invalid delegated Agent result");
  if (value.outcome !== "completed" && value.outcome !== "failed" && value.outcome !== "canceled") {
    throw new TypeError("Invalid delegated Agent outcome");
  }
  const errorCode = value.errorCode === undefined ? undefined : requiredText(value.errorCode, "delegation error code");
  if (value.outcome === "completed" && errorCode !== undefined) {
    throw new TypeError("Completed delegation cannot carry an error");
  }
  if (value.outcome !== "completed" && errorCode === undefined) {
    throw new TypeError("Unfinished delegation requires an error code");
  }
  return Object.freeze({
    outcome: value.outcome,
    ...(value.content === undefined ? {} : { content: copyJsonValue(value.content) }),
    ...(errorCode === undefined ? {} : { errorCode }),
  });
}

function batchKey(runId: string, batchId: string): string {
  return `${runId}\u0000${batchId}`;
}

function code(error: unknown): string {
  return error instanceof AgentError ? error.code : "delegation_failed";
}

function requiredText(value: unknown, label: string): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "") throw new TypeError(`${label} must be non-empty text`);
  return text;
}
