import type { JsonValue } from "./model/types.js";
import { copyJsonValue } from "./model/validation.js";
import { AgentError } from "./shared/errors.js";
import { stableFingerprint } from "./shared/fingerprint.js";

export type AgentNodeState = "active" | "closed";
export type AgentTreeRunStatus =
  | "queued"
  | "running"
  | "waiting"
  | "done"
  | "failed"
  | "canceled";

const ACTIVE_RUN_STATUSES = new Set<AgentTreeRunStatus>([
  "queued",
  "running",
  "waiting",
]);

export interface AgentCapabilityGrantOptions {
  readonly canSpawnAgents?: boolean;
  readonly maxDepth?: number;
  readonly maxChildrenPerCall?: number;
  readonly maxAgentsPerRoot?: number;
  readonly maxParallelRuns?: number;
  readonly allowedTools?: readonly string[];
  readonly allowedModels?: readonly string[];
}

export class AgentCapabilityGrant {
  public readonly canSpawnAgents: boolean;
  public readonly maxDepth: number;
  public readonly maxChildrenPerCall: number;
  public readonly maxAgentsPerRoot: number;
  public readonly maxParallelRuns: number;
  public readonly allowedTools: readonly string[];
  public readonly allowedModels: readonly string[];

  public constructor(options: AgentCapabilityGrantOptions = {}) {
    if (options.canSpawnAgents !== undefined && typeof options.canSpawnAgents !== "boolean") {
      throw new TypeError("canSpawnAgents must be boolean");
    }
    this.canSpawnAgents = options.canSpawnAgents ?? false;
    this.maxDepth = positive(options.maxDepth ?? 3, "maxDepth");
    this.maxChildrenPerCall = positive(
      options.maxChildrenPerCall ?? 3,
      "maxChildrenPerCall",
    );
    this.maxAgentsPerRoot = positive(
      options.maxAgentsPerRoot ?? 16,
      "maxAgentsPerRoot",
    );
    this.maxParallelRuns = positive(
      options.maxParallelRuns ?? 3,
      "maxParallelRuns",
    );
    this.allowedTools = uniqueText(options.allowedTools ?? [], "allowed tool");
    this.allowedModels = uniqueText(options.allowedModels ?? [], "allowed model");
    Object.freeze(this);
  }

  public authorizeChild(
    requested?: AgentCapabilityGrant,
  ): AgentCapabilityGrant {
    const child = requested ?? this;
    if (!(child instanceof AgentCapabilityGrant)) {
      throw new TypeError("Child Agent capability grant is invalid");
    }
    if (child.canSpawnAgents && !this.canSpawnAgents) {
      fail("agent_capability_escalation", "Child cannot gain spawn authority");
    }
    for (const name of [
      "maxDepth",
      "maxChildrenPerCall",
      "maxAgentsPerRoot",
      "maxParallelRuns",
    ] as const) {
      if (child[name] > this[name]) {
        fail("agent_capability_escalation", `Child cannot increase ${name}`);
      }
    }
    if (child.allowedTools.some((name) => !this.allowedTools.includes(name))) {
      fail("agent_capability_escalation", "Child cannot gain tools");
    }
    if (child.allowedModels.some((name) => !this.allowedModels.includes(name))) {
      fail("agent_capability_escalation", "Child cannot gain models");
    }
    return child;
  }

  public toJSON(): Readonly<Record<string, JsonValue>> {
    return Object.freeze({
      canSpawnAgents: this.canSpawnAgents,
      maxDepth: this.maxDepth,
      maxChildrenPerCall: this.maxChildrenPerCall,
      maxAgentsPerRoot: this.maxAgentsPerRoot,
      maxParallelRuns: this.maxParallelRuns,
      allowedTools: Object.freeze([...this.allowedTools]),
      allowedModels: Object.freeze([...this.allowedModels]),
    });
  }
}

export interface AgentNode {
  readonly agentId: string;
  readonly rootAgentId: string;
  readonly parentAgentId: string | null;
  readonly depth: number;
  readonly createdByRunId: string;
  readonly createdByCallId: string;
  readonly name: string;
  readonly title: string;
  readonly instruction: string;
  readonly capabilityGrant: AgentCapabilityGrant;
  readonly contextVersion: number;
  readonly contextCheckpointId: string | null;
  readonly latestRunId: string | null;
  readonly state: AgentNodeState;
}

export interface AgentTreeRun {
  readonly runId: string;
  readonly agentId: string;
  readonly rootRunId: string;
  readonly parentRunId: string | null;
  readonly previousRunId: string | null;
  readonly spawnBatchId: string | null;
  readonly objective: string;
  readonly input: Readonly<Record<string, JsonValue>>;
  readonly required: boolean;
  readonly priority: number;
  readonly status: AgentTreeRunStatus;
  readonly result: JsonValue | null;
  readonly errorCode: string | null;
  readonly createdSequence: number;
  readonly leaseOwnerId: string | null;
  readonly leaseEpoch: number;
  readonly leaseExpiresAtMs: number | null;
}

export interface ContextCheckpoint {
  readonly checkpointId: string;
  readonly agentId: string;
  readonly version: number;
  readonly previousCheckpointId: string | null;
  readonly sourceRunId: string;
  readonly contentRef: string;
  readonly fingerprint: string;
}

export interface ChildAgentSpec {
  readonly name: string;
  readonly title: string;
  readonly instruction: string;
  readonly objective: string;
  readonly input?: Readonly<Record<string, JsonValue>>;
  readonly required?: boolean;
  readonly priority?: number;
  readonly capabilityGrant?: AgentCapabilityGrant;
}

export interface BeginRootAgentCommand {
  readonly runId: string;
  readonly agentId: string;
  readonly name: string;
  readonly title: string;
  readonly instruction: string;
  readonly objective: string;
  readonly capabilityGrant: AgentCapabilityGrant;
  readonly idempotencyKey: string;
}

export interface SpawnAgentsCommand {
  readonly parentRunId: string;
  readonly idempotencyKey: string;
  readonly children: readonly ChildAgentSpec[];
  readonly leaseOwnerId?: string;
  readonly leaseEpoch?: number;
}

export interface ContinueAgentCommand {
  readonly requesterRunId: string;
  readonly idempotencyKey: string;
  readonly agentId: string;
  readonly expectedContextVersion: number;
  readonly message: string;
  readonly required?: boolean;
  readonly priority?: number;
  readonly leaseOwnerId?: string;
  readonly leaseEpoch?: number;
}

export interface SpawnedAgent {
  readonly agent: AgentNode;
  readonly run: AgentTreeRun;
}

export interface SpawnAgentsReceipt {
  readonly batchId: string;
  readonly items: readonly SpawnedAgent[];
  readonly replayed: boolean;
}

export interface ContinueAgentReceipt {
  readonly agent: AgentNode;
  readonly run: AgentTreeRun;
  readonly replayed: boolean;
}

export interface AgentRunAggregation {
  readonly state: "pending" | "ready" | "blocked";
  readonly pendingRunIds: readonly string[];
  readonly requiredFailures: readonly string[];
  readonly results: readonly Readonly<Record<string, JsonValue>>[];
}

export interface RunTreeRepository {
  beginRoot(command: BeginRootAgentCommand): Promise<AgentTreeRun>;
  spawnAgents(command: SpawnAgentsCommand): Promise<SpawnAgentsReceipt>;
  continueAgent(command: ContinueAgentCommand): Promise<ContinueAgentReceipt>;
  claimRun(
    runId: string,
    options?: { readonly ownerId?: string; readonly leaseDurationMs?: number },
  ): Promise<AgentTreeRun | undefined>;
  renewRunLease(
    runId: string,
    input: { readonly ownerId: string; readonly leaseEpoch: number; readonly leaseDurationMs: number },
  ): Promise<AgentTreeRun>;
  markWaiting(
    runId: string,
    claim?: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number },
  ): Promise<AgentTreeRun>;
  releaseWaiting(
    runId: string,
    claim?: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number },
  ): Promise<AgentTreeRun>;
  completeRun(
    runId: string,
    input: {
      readonly expectedContextVersion: number;
      readonly result: JsonValue;
      readonly contentRef: string;
      readonly fingerprint: string;
      readonly leaseOwnerId?: string;
      readonly leaseEpoch?: number;
    },
  ): Promise<AgentTreeRun>;
  failRun(
    runId: string,
    errorCode: string,
    claim?: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number },
  ): Promise<AgentTreeRun>;
  cancelSubtree(runId: string): Promise<readonly string[]>;
  aggregateRuns(
    requesterRunId: string,
    runIds: readonly string[],
  ): Promise<AgentRunAggregation>;
  closeAgent(agentId: string): Promise<AgentNode>;
  getAgent(agentId: string): Promise<AgentNode>;
  getRun(runId: string): Promise<AgentTreeRun>;
  getCheckpoint(checkpointId: string): Promise<ContextCheckpoint>;
  listRunnable(rootRunId: string): Promise<readonly AgentTreeRun[]>;
  listDescendants(runId: string): Promise<readonly AgentTreeRun[]>;
  requireRunClaim(
    runId: string,
    claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number },
  ): void;
}

interface StoredSpawnReceipt {
  readonly digest: string;
  readonly receipt: SpawnAgentsReceipt;
}

interface StoredContinueReceipt {
  readonly digest: string;
  readonly receipt: ContinueAgentReceipt;
}

export class InMemoryRunTreeRepository implements RunTreeRepository {
  readonly #agents = new Map<string, AgentNode>();
  readonly #runs = new Map<string, AgentTreeRun>();
  readonly #checkpoints = new Map<string, ContextCheckpoint>();
  readonly #spawnReceipts = new Map<string, StoredSpawnReceipt>();
  readonly #continueReceipts = new Map<string, StoredContinueReceipt>();
  readonly #rootDigests = new Map<string, string>();
  #sequence = 0;
  #agentSequence = 0;
  #runSequence = 0;
  #batchSequence = 0;
  #checkpointSequence = 0;
  readonly #clockMs: () => number;

  public constructor(options: { readonly clockMs?: () => number } = {}) {
    this.#clockMs = options.clockMs ?? Date.now;
  }

  public async beginRoot(command: BeginRootAgentCommand): Promise<AgentTreeRun> {
    const runId = requiredText(command.runId, "root Run id");
    const agentId = requiredText(command.agentId, "root Agent id");
    if (!(command.capabilityGrant instanceof AgentCapabilityGrant)) {
      throw new TypeError("Root Agent capability grant is required");
    }
    const digest = await stableFingerprint({
      agentId,
      name: requiredText(command.name, "root Agent name"),
      title: requiredText(command.title, "root Agent title"),
      instruction: requiredText(command.instruction, "root Agent instruction"),
      objective: requiredText(command.objective, "root Run objective"),
      capabilityGrant: command.capabilityGrant.toJSON(),
      idempotencyKey: requiredText(command.idempotencyKey, "root idempotency key"),
    });
    const existingRun = this.#runs.get(runId);
    if (existingRun !== undefined) {
      if (this.#rootDigests.get(runId) !== digest) {
        fail(
          "child_spawn_idempotency_conflict",
          "Root Run id was reused with different input",
        );
      }
      return existingRun;
    }
    let agent = this.#agents.get(agentId);
    if (agent === undefined) {
      agent = freezeAgent({
        agentId,
        rootAgentId: agentId,
        parentAgentId: null,
        depth: 0,
        createdByRunId: runId,
        createdByCallId: requiredText(command.idempotencyKey, "root idempotency key"),
        name: requiredText(command.name, "root Agent name"),
        title: requiredText(command.title, "root Agent title"),
        instruction: requiredText(command.instruction, "root Agent instruction"),
        capabilityGrant: command.capabilityGrant,
        contextVersion: 0,
        contextCheckpointId: null,
        latestRunId: null,
        state: "active",
      });
    } else {
      if (
        agent.parentAgentId !== null
        || agent.state === "closed"
        || !sameGrant(agent.capabilityGrant, command.capabilityGrant)
      ) {
        fail("agent_scope_violation", "Root Agent cannot be rebound");
      }
      if (this.#hasActiveRun(agent.agentId)) {
        fail("agent_busy", "Root Agent already has an active Run");
      }
    }
    const run = freezeRun({
      runId,
      agentId,
      rootRunId: runId,
      parentRunId: null,
      previousRunId: agent.latestRunId,
      spawnBatchId: null,
      objective: requiredText(command.objective, "root Run objective"),
      input: Object.freeze({}),
      required: true,
      priority: 0,
      status: "running",
      result: null,
      errorCode: null,
      createdSequence: this.#nextSequence(),
      leaseOwnerId: null,
      leaseEpoch: 0,
      leaseExpiresAtMs: null,
    });
    this.#runs.set(runId, run);
    this.#rootDigests.set(runId, digest);
    this.#agents.set(agentId, freezeAgent({ ...agent, latestRunId: runId }));
    return run;
  }

  public async spawnAgents(
    command: SpawnAgentsCommand,
  ): Promise<SpawnAgentsReceipt> {
    const parentRunId = requiredText(command.parentRunId, "parent Run id");
    if (!Array.isArray(command.children) || command.children.length === 0) {
      fail("agent_capacity_exceeded", "Invalid child Agent count");
    }
    const children = Object.freeze(command.children.map(copyChildSpec));
    if (new Set(children.map((child) => child.name)).size !== children.length) {
      throw new TypeError("Spawned Agent names must be unique");
    }
    const idempotencyKey = requiredText(command.idempotencyKey, "spawn idempotency key");
    const digest = await stableFingerprint(children.map(childSpecJson));
    const key = keyOf(parentRunId, idempotencyKey);
    const replay = this.#spawnReceipts.get(key);
    if (replay !== undefined) {
      if (replay.digest !== digest) {
        fail(
          "child_spawn_idempotency_conflict",
          "Spawn key was reused with different input",
        );
      }
      return Object.freeze({ ...replay.receipt, replayed: true });
    }
    const parentRun = this.#requireActiveRun(parentRunId);
    this.#requireClaim(parentRun, command);
    const parentAgent = this.#requireActiveAgent(parentRun.agentId);
    const grant = parentAgent.capabilityGrant;
    if (!grant.canSpawnAgents) {
      fail("agent_capability_escalation", "Agent cannot create children");
    }
    if (parentAgent.depth >= grant.maxDepth) {
      fail("agent_depth_exceeded", "Maximum Agent depth was reached");
    }
    if (children.length > grant.maxChildrenPerCall) {
      fail("agent_capacity_exceeded", "Invalid child Agent count");
    }
    const participating = new Set(
      [...this.#runs.values()]
        .filter((run) => run.rootRunId === parentRun.rootRunId)
        .map((run) => run.agentId),
    );
    if (participating.size + children.length > grant.maxAgentsPerRoot) {
      fail("agent_capacity_exceeded", "Root Agent capacity was exceeded");
    }
    this.#batchSequence += 1;
    const batchId = `agent-batch-${this.#batchSequence}`;
    const items = children.map((spec) => {
      const childGrant = grant.authorizeChild(spec.capabilityGrant);
      this.#agentSequence += 1;
      this.#runSequence += 1;
      const agentId = `agent-${this.#agentSequence}`;
      const runId = `agent-run-${this.#runSequence}`;
      const agent = freezeAgent({
        agentId,
        rootAgentId: parentAgent.rootAgentId,
        parentAgentId: parentAgent.agentId,
        depth: parentAgent.depth + 1,
        createdByRunId: parentRun.runId,
        createdByCallId: idempotencyKey,
        name: spec.name,
        title: spec.title,
        instruction: spec.instruction,
        capabilityGrant: childGrant,
        contextVersion: 0,
        contextCheckpointId: null,
        latestRunId: runId,
        state: "active",
      });
      const run = freezeRun({
        runId,
        agentId,
        rootRunId: parentRun.rootRunId,
        parentRunId: parentRun.runId,
        previousRunId: null,
        spawnBatchId: batchId,
        objective: spec.objective,
        input: spec.input,
        required: spec.required,
        priority: spec.priority,
        status: "queued",
        result: null,
        errorCode: null,
        createdSequence: this.#nextSequence(),
        leaseOwnerId: null,
        leaseEpoch: 0,
        leaseExpiresAtMs: null,
      });
      this.#agents.set(agentId, agent);
      this.#runs.set(runId, run);
      return Object.freeze({ agent, run });
    });
    const receipt = Object.freeze({
      batchId,
      items: Object.freeze(items),
      replayed: false,
    });
    this.#spawnReceipts.set(key, { digest, receipt });
    return receipt;
  }

  public async continueAgent(
    command: ContinueAgentCommand,
  ): Promise<ContinueAgentReceipt> {
    const requesterRunId = requiredText(command.requesterRunId, "requester Run id");
    const agentId = requiredText(command.agentId, "continued Agent id");
    const contextVersion = nonNegative(
      command.expectedContextVersion,
      "expected context version",
    );
    if (command.required !== undefined && typeof command.required !== "boolean") {
      throw new TypeError("Continuation required must be boolean");
    }
    const input = Object.freeze({
      agentId,
      contextVersion,
      message: requiredText(command.message, "continuation message"),
      required: command.required ?? true,
      priority: integer(command.priority ?? 0, "continuation priority"),
    });
    const idempotencyKey = requiredText(
      command.idempotencyKey,
      "continuation idempotency key",
    );
    const digest = await stableFingerprint(input);
    const key = keyOf(requesterRunId, idempotencyKey);
    const replay = this.#continueReceipts.get(key);
    if (replay !== undefined) {
      if (replay.digest !== digest) {
        fail(
          "child_spawn_idempotency_conflict",
          "Continuation key was reused with different input",
        );
      }
      return Object.freeze({ ...replay.receipt, replayed: true });
    }
    const requester = this.#requireActiveRun(requesterRunId);
    this.#requireClaim(requester, command);
    const target = this.#requireActiveAgent(agentId);
    const requesterAgent = this.#requireActiveAgent(requester.agentId);
    if (
      requesterAgent.rootAgentId !== target.rootAgentId
      || !this.#isAncestor(requesterAgent.agentId, target.agentId)
    ) {
      fail("agent_scope_violation", "Requester is not an Agent ancestor");
    }
    if (target.contextVersion !== contextVersion) {
      fail("agent_context_conflict", "Agent context version changed");
    }
    if (this.#hasActiveRun(target.agentId)) {
      fail("agent_busy", "Agent already has an active Run");
    }
    const participating = new Set(
      [...this.#runs.values()]
        .filter((run) => run.rootRunId === requester.rootRunId)
        .map((run) => run.agentId),
    );
    const root = this.#requireActiveAgent(requesterAgent.rootAgentId);
    if (
      !participating.has(target.agentId)
      && participating.size >= root.capabilityGrant.maxAgentsPerRoot
    ) {
      fail("agent_capacity_exceeded", "Root Agent capacity was exceeded");
    }
    this.#runSequence += 1;
    const run = freezeRun({
      runId: `agent-run-${this.#runSequence}`,
      agentId: target.agentId,
      rootRunId: requester.rootRunId,
      parentRunId: requester.runId,
      previousRunId: target.latestRunId,
      spawnBatchId: null,
      objective: input.message,
      input: Object.freeze({}),
      required: input.required,
      priority: input.priority,
      status: "queued",
      result: null,
      errorCode: null,
      createdSequence: this.#nextSequence(),
      leaseOwnerId: null,
      leaseEpoch: 0,
      leaseExpiresAtMs: null,
    });
    const agent = freezeAgent({ ...target, latestRunId: run.runId });
    this.#agents.set(agent.agentId, agent);
    this.#runs.set(run.runId, run);
    const receipt = Object.freeze({ agent, run, replayed: false });
    this.#continueReceipts.set(key, { digest, receipt });
    return receipt;
  }

  public async claimRun(
    runId: string,
    options: { readonly ownerId?: string; readonly leaseDurationMs?: number } = {},
  ): Promise<AgentTreeRun | undefined> {
    const run = this.#requireRun(runId);
    const now = this.#nowMs();
    const reclaimable = run.status === "running" && this.#leaseExpired(run, now);
    if (run.status !== "queued" && !reclaimable) return undefined;
    const ownerId = requiredText(options.ownerId ?? "run-tree-supervisor", "lease owner id");
    const leaseDurationMs = positive(options.leaseDurationMs ?? 30_000, "lease duration");
    const root = this.#requireActiveAgent(this.#requireRun(run.rootRunId).agentId);
    const active = [...this.#runs.values()].filter((item) => (
      item.rootRunId === run.rootRunId
      && item.status === "running"
      && !this.#leaseExpired(item, now)
    )).length;
    if (active >= root.capabilityGrant.maxParallelRuns) return undefined;
    const claimed = freezeRun({
      ...run,
      status: "running",
      leaseOwnerId: ownerId,
      leaseEpoch: run.leaseEpoch + 1,
      leaseExpiresAtMs: now + leaseDurationMs,
    });
    this.#runs.set(run.runId, claimed);
    return claimed;
  }

  public async renewRunLease(
    runId: string,
    input: { readonly ownerId: string; readonly leaseEpoch: number; readonly leaseDurationMs: number },
  ): Promise<AgentTreeRun> {
    const run = this.#requireActiveRun(runId);
    this.#requireClaim(run, {
      leaseOwnerId: input.ownerId,
      leaseEpoch: input.leaseEpoch,
    });
    const renewed = freezeRun({
      ...run,
      leaseExpiresAtMs: this.#nowMs() + positive(input.leaseDurationMs, "lease duration"),
    });
    this.#runs.set(run.runId, renewed);
    return renewed;
  }

  public async markWaiting(
    runId: string,
    claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number } = {},
  ): Promise<AgentTreeRun> {
    const run = this.#requireRun(runId);
    if (run.status !== "running") fail("agent_run_state_conflict", "Agent Run state changed");
    this.#requireClaim(run, claim);
    const waiting = freezeRun({ ...run, status: "waiting" });
    this.#runs.set(run.runId, waiting);
    return waiting;
  }

  public async releaseWaiting(
    runId: string,
    claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number } = {},
  ): Promise<AgentTreeRun> {
    const run = this.#requireRun(runId);
    if (run.status !== "waiting") {
      fail("agent_run_state_conflict", "Agent Run state changed");
    }
    this.#requireClaim(run, claim);
    const root = this.#requireActiveAgent(this.#requireRun(run.rootRunId).agentId);
    const active = [...this.#runs.values()].filter((item) => (
      item.rootRunId === run.rootRunId && item.status === "running"
    )).length;
    if (active >= root.capabilityGrant.maxParallelRuns) {
      fail("agent_capacity_exceeded", "No root execution slot is available");
    }
    const resumed = freezeRun({ ...run, status: "running" });
    this.#runs.set(run.runId, resumed);
    return resumed;
  }

  public async completeRun(
    runId: string,
    input: {
      readonly expectedContextVersion: number;
      readonly result: JsonValue;
      readonly contentRef: string;
      readonly fingerprint: string;
      readonly leaseOwnerId?: string;
      readonly leaseEpoch?: number;
    },
  ): Promise<AgentTreeRun> {
    const run = this.#requireRun(runId);
    if (run.status !== "running") {
      fail("agent_run_state_conflict", "Only a running Run may complete");
    }
    this.#requireClaim(run, input);
    if ([...this.#runs.values()].some((item) => (
      item.rootRunId === run.rootRunId
      && ACTIVE_RUN_STATUSES.has(item.status)
      && this.#isCausalDescendant(run.runId, item.runId)
    ))) {
      fail("root_run_not_quiescent", "Run has unfinished children");
    }
    const agent = this.#requireActiveAgent(run.agentId);
    if (agent.contextVersion !== input.expectedContextVersion) {
      fail("agent_context_conflict", "Agent context version changed");
    }
    this.#checkpointSequence += 1;
    const checkpoint = Object.freeze({
      checkpointId: `context-${this.#checkpointSequence}`,
      agentId: agent.agentId,
      version: agent.contextVersion + 1,
      previousCheckpointId: agent.contextCheckpointId,
      sourceRunId: run.runId,
      contentRef: requiredText(input.contentRef, "context content reference"),
      fingerprint: requiredText(input.fingerprint, "context fingerprint"),
    });
    const completed = freezeRun({
      ...run,
      status: "done",
      result: copyJsonValue(input.result),
      leaseOwnerId: null,
      leaseExpiresAtMs: null,
    });
    this.#checkpoints.set(checkpoint.checkpointId, checkpoint);
    this.#runs.set(run.runId, completed);
    this.#agents.set(agent.agentId, freezeAgent({
      ...agent,
      contextVersion: checkpoint.version,
      contextCheckpointId: checkpoint.checkpointId,
    }));
    return completed;
  }

  public async failRun(
    runId: string,
    errorCode: string,
    claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number } = {},
  ): Promise<AgentTreeRun> {
    const run = this.#requireRun(runId);
    if (!ACTIVE_RUN_STATUSES.has(run.status)) return run;
    this.#requireClaim(run, claim);
    const failed = freezeRun({
      ...run,
      status: "failed",
      errorCode: requiredText(errorCode, "Agent Run error code"),
      leaseOwnerId: null,
      leaseExpiresAtMs: null,
    });
    this.#runs.set(run.runId, failed);
    for (const item of this.#runs.values()) {
      if (
        ACTIVE_RUN_STATUSES.has(item.status)
        && this.#isCausalDescendant(run.runId, item.runId)
      ) {
        this.#runs.set(item.runId, freezeRun({
          ...item,
          status: "canceled",
          errorCode: "ancestor_run_failed",
          leaseOwnerId: null,
          leaseExpiresAtMs: null,
        }));
      }
    }
    return failed;
  }

  public async cancelSubtree(runId: string): Promise<readonly string[]> {
    const root = this.#requireRun(runId);
    const pending = [root.runId];
    const ordered: string[] = [];
    while (pending.length > 0) {
      const current = pending.pop()!;
      ordered.push(current);
      pending.push(...[...this.#runs.values()]
        .filter((item) => (
          item.rootRunId === root.rootRunId && item.parentRunId === current
        ))
        .map((item) => item.runId));
    }
    const canceled: string[] = [];
    for (const current of ordered) {
      const run = this.#requireRun(current);
      if (ACTIVE_RUN_STATUSES.has(run.status)) {
        this.#runs.set(current, freezeRun({
          ...run,
          status: "canceled",
          errorCode: "agent_run_canceled",
          leaseOwnerId: null,
          leaseExpiresAtMs: null,
        }));
        canceled.push(current);
      }
    }
    return Object.freeze(canceled);
  }

  public async aggregateRuns(
    requesterRunId: string,
    runIds: readonly string[],
  ): Promise<AgentRunAggregation> {
    const requester = this.#requireRun(requesterRunId);
    const ids = [...new Set(runIds.map((id) => requiredText(id, "joined Run id")))];
    const rows = ids.map((id) => this.#requireRun(id));
    if (rows.some((row) => (
      row.rootRunId !== requester.rootRunId
      || !this.#isCausalDescendant(requester.runId, row.runId)
    ))) {
      fail("child_run_scope_violation", "Joined Run escaped requester");
    }
    const pending = rows
      .filter((row) => ACTIVE_RUN_STATUSES.has(row.status))
      .map((row) => row.runId);
    const failures = rows
      .filter((row) => (
        row.required && (row.status === "failed" || row.status === "canceled")
      ))
      .map((row) => row.runId);
    return Object.freeze({
      state: pending.length > 0 ? "pending" : failures.length > 0 ? "blocked" : "ready",
      pendingRunIds: Object.freeze(pending),
      requiredFailures: Object.freeze(failures),
      results: Object.freeze(rows
        .filter((row) => !ACTIVE_RUN_STATUSES.has(row.status))
        .map((row) => Object.freeze({
          agentId: row.agentId,
          runId: row.runId,
          status: row.status,
          result: row.result,
          errorCode: row.errorCode,
        }))),
    });
  }

  public async closeAgent(agentId: string): Promise<AgentNode> {
    const agent = this.#requireActiveAgent(agentId);
    if (this.#hasActiveRun(agent.agentId)) {
      fail("agent_busy", "Agent has an active Run");
    }
    const closed = freezeAgent({ ...agent, state: "closed" });
    this.#agents.set(agent.agentId, closed);
    return closed;
  }

  public async getAgent(agentId: string): Promise<AgentNode> {
    return this.#requireAgent(agentId);
  }

  public async getRun(runId: string): Promise<AgentTreeRun> {
    return this.#requireRun(runId);
  }

  public async getCheckpoint(checkpointId: string): Promise<ContextCheckpoint> {
    const checkpoint = this.#checkpoints.get(requiredText(checkpointId, "checkpoint id"));
    if (checkpoint === undefined) {
      fail("agent_context_checkpoint_not_found", "Context checkpoint does not exist");
    }
    return checkpoint;
  }

  public async listRunnable(rootRunId: string): Promise<readonly AgentTreeRun[]> {
    const root = requiredText(rootRunId, "root Run id");
    const now = this.#nowMs();
    return Object.freeze([...this.#runs.values()]
      .filter((run) => (
        run.rootRunId === root
        && (
          run.status === "queued"
          || (run.status === "running" && this.#leaseExpired(run, now))
        )
      ))
      .sort((left, right) => (
        right.priority - left.priority
        || left.createdSequence - right.createdSequence
      )));
  }

  public async listDescendants(runId: string): Promise<readonly AgentTreeRun[]> {
    const parent = requiredText(runId, "ancestor Run id");
    this.#requireRun(parent);
    return Object.freeze([...this.#runs.values()]
      .filter((run) => this.#isCausalDescendant(parent, run.runId))
      .sort((left, right) => left.createdSequence - right.createdSequence));
  }

  public requireRunClaim(
    runId: string,
    claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number },
  ): void {
    this.#requireClaim(this.#requireRun(runId), claim);
  }

  #transition(
    runId: string,
    expected: ReadonlySet<AgentTreeRunStatus>,
    status: AgentTreeRunStatus,
  ): AgentTreeRun {
    const run = this.#requireRun(runId);
    if (!expected.has(run.status)) {
      fail("agent_run_state_conflict", "Agent Run state changed");
    }
    const changed = freezeRun({ ...run, status });
    this.#runs.set(run.runId, changed);
    return changed;
  }

  #requireAgent(agentId: string): AgentNode {
    const agent = this.#agents.get(requiredText(agentId, "Agent id"));
    if (agent === undefined) fail("agent_not_found", "Agent does not exist");
    return agent;
  }

  #requireActiveAgent(agentId: string): AgentNode {
    const agent = this.#requireAgent(agentId);
    if (agent.state === "closed") fail("agent_closed", "Agent is closed");
    return agent;
  }

  #requireRun(runId: string): AgentTreeRun {
    const run = this.#runs.get(requiredText(runId, "Agent Run id"));
    if (run === undefined) fail("child_run_not_found", "Agent Run does not exist");
    return run;
  }

  #requireClaim(
    run: AgentTreeRun,
    claim: { readonly leaseOwnerId?: string; readonly leaseEpoch?: number },
  ): void {
    const owner = claim.leaseOwnerId === undefined
      ? null
      : requiredText(claim.leaseOwnerId, "lease owner id");
    const epoch = claim.leaseEpoch === undefined
      ? null
      : nonNegative(claim.leaseEpoch, "lease epoch");
    if (run.leaseOwnerId === null) {
      if (owner === null && epoch === null) return;
      fail("agent_run_lease_lost", "Agent Run has no matching lease");
    }
    if (
      owner !== run.leaseOwnerId
      || epoch !== run.leaseEpoch
      || this.#leaseExpired(run)
    ) {
      fail("agent_run_lease_lost", "Agent Run lease is stale or expired");
    }
  }

  #leaseExpired(run: AgentTreeRun, now = this.#nowMs()): boolean {
    return (
      (run.status === "running" || run.status === "waiting")
      && run.leaseExpiresAtMs !== null
      && now >= run.leaseExpiresAtMs
    );
  }

  #nowMs(): number {
    return nonNegative(this.#clockMs(), "Agent tree clock");
  }

  #requireActiveRun(runId: string): AgentTreeRun {
    const run = this.#requireRun(runId);
    if (run.status !== "running" && run.status !== "waiting") {
      fail("root_run_not_active", "Agent Run is not active");
    }
    return run;
  }

  #hasActiveRun(agentId: string): boolean {
    return [...this.#runs.values()].some((run) => (
      run.agentId === agentId && ACTIVE_RUN_STATUSES.has(run.status)
    ));
  }

  #isAncestor(ancestorId: string, descendantId: string): boolean {
    let current = this.#requireAgent(descendantId);
    while (true) {
      if (current.agentId === ancestorId) return true;
      if (current.parentAgentId === null) return false;
      current = this.#requireAgent(current.parentAgentId);
    }
  }

  #isCausalDescendant(parentRunId: string, runId: string): boolean {
    let current = this.#requireRun(runId);
    while (current.parentRunId !== null) {
      if (current.parentRunId === parentRunId) return true;
      current = this.#requireRun(current.parentRunId);
    }
    return false;
  }

  #nextSequence(): number {
    this.#sequence += 1;
    return this.#sequence;
  }
}

interface NormalizedChildAgentSpec {
  readonly name: string;
  readonly title: string;
  readonly instruction: string;
  readonly objective: string;
  readonly input: Readonly<Record<string, JsonValue>>;
  readonly required: boolean;
  readonly priority: number;
  readonly capabilityGrant?: AgentCapabilityGrant;
}

function copyChildSpec(value: ChildAgentSpec): NormalizedChildAgentSpec {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Child Agent spec is invalid");
  }
  const input = value.input === undefined ? Object.freeze({}) : copyJsonValue(value.input);
  if (input === null || typeof input !== "object" || Array.isArray(input)) {
    throw new TypeError("Child Agent input must be an object");
  }
  if (
    value.capabilityGrant !== undefined
    && !(value.capabilityGrant instanceof AgentCapabilityGrant)
  ) {
    throw new TypeError("Child Agent capability grant is invalid");
  }
  if (value.required !== undefined && typeof value.required !== "boolean") {
    throw new TypeError("Child Agent required must be boolean");
  }
  return Object.freeze({
    name: requiredText(value.name, "child Agent name"),
    title: requiredText(value.title, "child Agent title"),
    instruction: requiredText(value.instruction, "child Agent instruction"),
    objective: requiredText(value.objective, "child Agent objective"),
    input: input as Readonly<Record<string, JsonValue>>,
    required: value.required ?? true,
    priority: integer(value.priority ?? 0, "child Agent priority"),
    ...(value.capabilityGrant === undefined
      ? {}
      : { capabilityGrant: value.capabilityGrant }),
  });
}

function childSpecJson(value: NormalizedChildAgentSpec): JsonValue {
  return {
    name: value.name,
    title: value.title,
    instruction: value.instruction,
    objective: value.objective,
    input: value.input,
    required: value.required,
    priority: value.priority,
    capabilityGrant: value.capabilityGrant?.toJSON() ?? null,
  };
}

function freezeAgent(value: AgentNode): AgentNode {
  return Object.freeze(value);
}

function freezeRun(value: AgentTreeRun): AgentTreeRun {
  return Object.freeze(value);
}

function sameGrant(left: AgentCapabilityGrant, right: AgentCapabilityGrant): boolean {
  return JSON.stringify(left.toJSON()) === JSON.stringify(right.toJSON());
}

function uniqueText(values: readonly string[], label: string): readonly string[] {
  const normalized = values.map((value) => requiredText(value, label));
  if (new Set(normalized).size !== normalized.length) {
    throw new TypeError(`${label}s must be unique`);
  }
  return Object.freeze(normalized);
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

function nonNegative(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new TypeError(`${label} must be a non-negative integer`);
  }
  return Number(value);
}

function integer(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value)) throw new TypeError(`${label} must be an integer`);
  return Number(value);
}

function keyOf(left: string, right: string): string {
  return `${left}\u0000${right}`;
}

function fail(code: string, message: string): never {
  throw new AgentError(code, message);
}
