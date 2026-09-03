import type { JsonValue } from "./model/types.js";
import { AgentError } from "./shared/errors.js";
import { UserInputRequired } from "./interaction.js";
import {
  AgentCapabilityGrant,
  type AgentNode,
  type AgentRunAggregation,
  type AgentTreeRun,
  type BeginRootAgentCommand,
  type ContextCheckpoint,
  type ContinueAgentCommand,
  type ContinueAgentReceipt,
  type RunTreeRepository,
  type SpawnAgentsCommand,
  type SpawnAgentsReceipt,
} from "./agent-tree.js";
import type { DelegationPolicyOptions } from "./delegation/types.js";

export interface AgentTreeOptions {
  readonly repository: RunTreeRepository;
  readonly policy?: DelegationPolicyOptions;
  readonly rootAgentId?: string;
  readonly capabilityGrant?: AgentCapabilityGrant;
  readonly ownerId?: string;
  readonly leaseDurationMs?: number;
}

export interface AgentTreeExecutionResult {
  readonly status: "done" | "failed" | "canceled";
  readonly result?: JsonValue;
  readonly contentRef?: string;
  readonly fingerprint?: string;
  readonly errorCode?: string;
}

export interface AgentTreeRunExecutor {
  execute(
    run: AgentTreeRun,
    agent: AgentNode,
    checkpoint?: ContextCheckpoint,
    signal?: AbortSignal,
  ): Promise<AgentTreeExecutionResult>;
}

export class AgentTreeRunSupervisor {
  readonly #repository: RunTreeRepository;
  readonly #executor: AgentTreeRunExecutor;
  readonly #ownerId: string;
  readonly #leaseDurationMs: number;
  readonly #executionStops = new Map<string, () => void>();

  public constructor(options: {
    readonly repository: RunTreeRepository;
    readonly executor: AgentTreeRunExecutor;
    readonly ownerId?: string;
    readonly leaseDurationMs?: number;
  }) {
    this.#repository = options.repository;
    this.#executor = options.executor;
    this.#ownerId = requiredText(
      options.ownerId ?? `agent-tree-supervisor-${globalThis.crypto.randomUUID()}`,
      "Agent tree supervisor owner id",
    );
    this.#leaseDurationMs = positive(options.leaseDurationMs ?? 30_000, "lease duration");
  }

  public async executeAndJoin(
    requesterRunId: string,
    runIds: readonly string[],
    signal?: AbortSignal,
    claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number } = {},
  ): Promise<AgentRunAggregation> {
    const requesterId = requiredText(requesterRunId, "requester Run id");
    const targets = [...new Set(runIds.map((id) => requiredText(id, "Child Run id")))];
    if (targets.length === 0) {
      return this.#repository.aggregateRuns(requesterId, []);
    }
    let requester = await this.#repository.getRun(requesterId);
    if (requester.status === "waiting") {
      await this.#repository.requireRunClaim(requesterId, claim);
    } else {
      requester = await this.#repository.markWaiting(requesterId, claim);
    }
    const pending = new Set(targets);
    const active = new Map<Promise<void>, string>();
    const attempted = new Set<string>();
    let joined = false;
    try {
      while (pending.size > 0) {
        if (signal?.aborted === true) throw joinCanceledError();
        const aggregate = await this.#repository.aggregateRuns(requesterId, targets);
        pending.clear();
        for (const runId of aggregate.pendingRunIds) pending.add(runId);
        if (pending.size === 0) {
          joined = true;
          return aggregate;
        }
        for (const candidate of await this.#repository.listRunnable(requester.rootRunId)) {
          if (!pending.has(candidate.runId) || attempted.has(candidate.runId)) continue;
          const claimed = await this.#repository.claimRun(candidate.runId, {
            ownerId: this.#ownerId,
            leaseDurationMs: this.#leaseDurationMs,
          });
          if (claimed === undefined) continue;
          const task = this.#executeClaimed(claimed, signal);
          active.set(task, claimed.runId);
          attempted.add(claimed.runId);
        }
        if (active.size === 0) {
          if ([...pending].every(id => attempted.has(id))) { joined = true; return aggregate; }
          throw new AgentError(
            "agent_run_scheduler_stalled",
            "Child Run scheduler made no progress",
          );
        }
        const settled = await raceActive(active, signal);
        active.delete(settled.task);
        pending.delete(settled.runId);
      }
      const aggregate = await this.#repository.aggregateRuns(requesterId, targets);
      joined = true;
      return aggregate;
    } catch (error) {
      if (isAbort(error)) {
        for (const runId of targets) {
          await this.#repository.cancelSubtree(runId);
          this.#executionStops.get(runId)?.();
        }
        for (const task of active.keys()) void task.catch(() => undefined);
        joined = true;
      } else {
        await Promise.allSettled(active.keys());
      }
      if (isAbort(error)) throw joinCanceledError(error);
      throw error;
    } finally {
      const current = await this.#repository.getRun(requesterId);
      if (joined && current.status === "waiting") {
        await this.#repository.releaseWaiting(requesterId, claim);
      }
    }
  }

  async #executeClaimed(run: AgentTreeRun, signal?: AbortSignal): Promise<void> {
    const agent = await this.#repository.getAgent(run.agentId);
    const checkpoint = agent.contextCheckpointId === null
      ? undefined
      : await this.#repository.getCheckpoint(agent.contextCheckpointId);
    const expectedContextVersion = agent.contextVersion;
    let result: AgentTreeExecutionResult;
    try {
      result = validateResult(await this.#executeWithHeartbeat(run, agent, checkpoint, signal));
    } catch (error) {
      if (error instanceof UserInputRequired) {
        await this.#repository.suspendRun(run.runId, { leaseOwnerId: run.leaseOwnerId!, leaseEpoch: run.leaseEpoch });
        return;
      }
      if (isAbort(error)) {
        await this.#repository.cancelSubtree(run.runId);
        throw error;
      }
      await this.#repository.failRun(
        run.runId,
        errorCode(error),
        leaseClaim(run),
      );
      return;
    }
    if (result.status === "done") {
      await this.#repository.completeRun(run.runId, {
        expectedContextVersion,
        result: result.result ?? null,
        contentRef: requiredText(result.contentRef, "Agent Run content reference"),
        fingerprint: requiredText(result.fingerprint, "Agent Run context fingerprint"),
        ...leaseClaim(run),
      });
    } else if (result.status === "canceled") {
      await this.#repository.cancelSubtree(run.runId);
    } else {
      await this.#repository.failRun(
        run.runId,
        requiredText(result.errorCode, "Agent Run error code"),
        leaseClaim(run),
      );
    }
  }

  async #executeWithHeartbeat(
    run: AgentTreeRun,
    agent: AgentNode,
    checkpoint?: ContextCheckpoint,
    signal?: AbortSignal,
  ): Promise<AgentTreeExecutionResult> {
    let stopped = false;
    let wake: (() => void) | undefined;
    let stopExecution: (() => void) | undefined;
    const stoppedExecution = new Promise<never>((_resolve, reject) => {
      stopExecution = () => reject(abortError());
    });
    const requestStop = () => stopExecution?.();
    this.#executionStops.set(run.runId, requestStop);
    const sleep = () => new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        wake = undefined;
        resolve();
      }, Math.max(1, Math.floor(this.#leaseDurationMs / 3)));
      wake = () => {
        clearTimeout(timer);
        wake = undefined;
        resolve();
      };
    });
    const heartbeat = (async () => {
      while (!stopped) {
        await sleep();
        if (stopped) return new Promise<never>(() => undefined);
        await this.#repository.renewRunLease(run.runId, {
          ownerId: requiredText(run.leaseOwnerId, "Agent Run lease owner"),
          leaseEpoch: run.leaseEpoch,
          leaseDurationMs: this.#leaseDurationMs,
        });
      }
      return new Promise<never>(() => undefined);
    })();
    try {
      return await Promise.race([
        this.#executor.execute(run, agent, checkpoint, signal),
        heartbeat,
        stoppedExecution,
      ]);
    } finally {
      stopped = true;
      wake?.();
      if (this.#executionStops.get(run.runId) === requestStop) {
        this.#executionStops.delete(run.runId);
      }
    }
  }
}

export class RunCommandService {
  readonly #repository: RunTreeRepository;
  readonly #supervisor: AgentTreeRunSupervisor | undefined;

  public constructor(
    repository: RunTreeRepository,
    supervisor?: AgentTreeRunSupervisor,
  ) {
    this.#repository = repository;
    this.#supervisor = supervisor;
  }

  public beginRoot(command: BeginRootAgentCommand): Promise<AgentTreeRun> {
    return this.#repository.beginRoot(command);
  }

  public spawnAgents(command: SpawnAgentsCommand): Promise<SpawnAgentsReceipt> {
    return this.#repository.spawnAgents(command);
  }

  public continueAgent(command: ContinueAgentCommand): Promise<ContinueAgentReceipt> {
    return this.#repository.continueAgent(command);
  }

  public async compileChildGrant(
    parentRunId: string,
    options: {
      readonly canSpawnAgents: boolean;
      readonly allowedTools?: readonly string[];
    },
  ): Promise<AgentCapabilityGrant> {
    if (typeof options.canSpawnAgents !== "boolean") {
      throw new TypeError("Child spawn authority must be boolean");
    }
    const run = await this.#repository.getRun(parentRunId);
    const parent = await this.#repository.getAgent(run.agentId);
    const parentGrant = parent.capabilityGrant;
    const allowedTools = options.allowedTools === undefined
      ? parentGrant.allowedTools
      : options.allowedTools.filter((name) => parentGrant.allowedTools.includes(name));
    return new AgentCapabilityGrant({
      canSpawnAgents: parentGrant.canSpawnAgents && options.canSpawnAgents,
      maxDepth: parentGrant.maxDepth,
      maxChildrenPerCall: parentGrant.maxChildrenPerCall,
      maxAgentsPerRoot: parentGrant.maxAgentsPerRoot,
      maxParallelRuns: parentGrant.maxParallelRuns,
      allowedTools,
      allowedModels: parentGrant.allowedModels,
    });
  }

  public joinRuns(
    requesterRunId: string,
    runIds: readonly string[],
    signal?: AbortSignal,
    claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number } = {},
  ): Promise<AgentRunAggregation> {
    if (this.#supervisor === undefined) {
      return this.#repository.aggregateRuns(requesterRunId, runIds);
    }
    return this.#supervisor.executeAndJoin(requesterRunId, runIds, signal, claim);
  }

  public cancelRun(runId: string): Promise<readonly string[]> {
    return this.#repository.cancelSubtree(runId);
  }

  public closeAgent(agentId: string): Promise<AgentNode> {
    return this.#repository.closeAgent(agentId);
  }
}

function validateResult(value: AgentTreeExecutionResult): AgentTreeExecutionResult {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Agent tree executor returned an invalid result");
  }
  if (value.status !== "done" && value.status !== "failed" && value.status !== "canceled") {
    throw new TypeError("Agent tree executor must return a terminal status");
  }
  if (value.status === "done") {
    requiredText(value.contentRef, "Agent Run content reference");
    requiredText(value.fingerprint, "Agent Run context fingerprint");
    if (value.errorCode !== undefined) {
      throw new TypeError("Completed Agent Run cannot carry an error");
    }
  } else {
    requiredText(value.errorCode, "Agent Run error code");
  }
  return value;
}

function requiredText(value: unknown, label: string): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "") throw new TypeError(`${label} must be non-empty text`);
  return text;
}

function positive(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 1) {
    throw new TypeError(`${label} must be a positive integer`);
  }
  return Number(value);
}

function leaseClaim(run: AgentTreeRun): {
  readonly leaseOwnerId?: string;
  readonly leaseEpoch?: number;
} {
  return run.leaseOwnerId === null
    ? Object.freeze({})
    : Object.freeze({
        leaseOwnerId: run.leaseOwnerId,
        leaseEpoch: run.leaseEpoch,
      });
}

function abortError(): Error {
  return new DOMException("Agent Run was canceled", "AbortError");
}

function isAbort(error: unknown): boolean {
  return (
    error instanceof DOMException && error.name === "AbortError"
  ) || (
    error instanceof AgentError && error.code === "child_run_join_canceled"
  );
}

function joinCanceledError(cause?: unknown): AgentError {
  return new AgentError(
    "child_run_join_canceled",
    "Child Run join was canceled",
    cause === undefined ? undefined : { cause },
  );
}

function errorCode(error: unknown): string {
  if (error instanceof AgentError) return error.code;
  if (error instanceof Error && error.name !== "") return error.name;
  return "agent_run_failed";
}

async function raceActive(
  active: ReadonlyMap<Promise<void>, string>,
  signal?: AbortSignal,
): Promise<{ readonly task: Promise<void>; readonly runId: string }> {
  const completions = [...active].map(async ([task, runId]) => {
    await task;
    return { task, runId };
  });
  if (signal === undefined) return Promise.race(completions);
  let onAbort: (() => void) | undefined;
  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => reject(joinCanceledError());
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
  try {
    return await Promise.race([...completions, aborted]);
  } finally {
    if (onAbort !== undefined) signal.removeEventListener("abort", onAbort);
  }
}
