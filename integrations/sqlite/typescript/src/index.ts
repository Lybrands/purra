import { SqliteRecoverySchedule, wakeRecoverySchedule, removeRecoverySchedule } from "./recovery-schedule.js";
export { SqliteRecoverySchedule } from "./recovery-schedule.js";
import { OutputJournal } from "./journal.js";
export type { ApprovalUpgradeInspection } from "./approval-format.js";
import { storageVersion, enableApprovals, inspectApprovalUpgrade, type ApprovalUpgradeInspection } from "./approval-format.js";
import { checkApprovalReconciliation, inspectApprovalState, checkApprovalDispatch, requireApprovalOwner, SqliteApprovalStore, type ApprovalAuthorizer } from "./approvals.js";
export { SqliteApprovalStore } from "./approvals.js";
export type { ApprovalAuthorizer, ApprovalDecisionReceipt } from "./approvals.js";
import { DatabaseSync } from "node:sqlite";
import { AsyncLocalStorage } from "node:async_hooks";
import { openSync, closeSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import {
  AgentError, copyToolHandlerResult, StorageSession, STORAGE_PORT_METHODS,
  buildRecoveryInspection, jsonIdentityDigest, type AgentPresetSnapshot, type RecoveryInspection,
  type StorageStores, type StoragePorts, type StorageSelection,
  type OutputPublisher, type ToolHandlerResult, type ToolIdempotencyGateway, type AgentExecutionCheckpoint, type AgentToolExecutionCheckpoint, type RunRepository,
} from "purra";

type Stores = StorageStores;
type StateSelection = StorageSelection;
const RUN_READ_METHODS = new Set(["get", "listEvents", "listRootEvents"]);
const ROOT_BOUND_METHODS = new Set(["get", "openInvocation", "appendEvent", "appendBatch", "saveExecutionCheckpoint", "saveToolExecutionCheckpoint", "settleInvocation", "settleRun", "cancel"]);

export class SqliteAgentAdapters {
  readonly #db: DatabaseSync;
  readonly #scope: string;
  readonly #journal: OutputJournal;
  #tail: Promise<unknown> = Promise.resolve();
  #active = false;
  #approvalRuntimeClock: (() => number) | undefined;
  readonly #owner = new AsyncLocalStorage<string>();
  readonly #epoch = new AsyncLocalStorage<number>();
  readonly runs = this.#port("runs") as StoragePorts["runs"] & Pick<RunRepository, "executeOwned" | "executeToolOwned" | "saveToolExecutionCheckpoint">;
  readonly runTree = this.#port("runTree");
  readonly artifacts = this.#port("artifacts");
  readonly artifactClaims = this.artifacts;
  readonly artifactMaintenance = this.artifacts;
  readonly longTasks = this.#port("longTasks");
  readonly publisher: OutputPublisher = {
    publishCommitted: async (event) => {
      const rows = await this.runs.listEvents(event.runId, event.sequence - 1, 1);
      if (JSON.stringify(rows[0]) !== JSON.stringify(event)) throw new Error("only persisted output can be published");
    },
    waitForSequence: async (id, after, signal) => {
      while (!(await this.runs.listEvents(id, after, 1)).length) await sleep(50, undefined, { signal });
    },
  };
  readonly idempotency: ToolIdempotencyGateway = {
    executeOnce: async (key, operation, dispatch) => {
      const outcome = await this.#transaction(async (all, extra) => {
        const approval = dispatch === undefined ? undefined : await checkApprovalDispatch(
          this.#db, this.#scope, all, extra, dispatch, this.#owner.getStore(), (this.#approvalRuntimeClock ?? Date.now)(), this.#epoch.getStore());
        if (approval?.error !== undefined) return { error: approval.error };
        const prior = extra.tools[key];
        if (prior) {
          if (dispatch !== undefined && (prior.runId !== dispatch.runId || prior.callId !== dispatch.call.id
            || prior.approvalId !== approval!.record.approvalId || prior.intentDigest !== approval!.record.intentDigest)) throw new AgentError("approval_intent_conflict", "Tool receipt identity conflicts");
          if (prior.state === "claimed") {
            if (dispatch === undefined) throw new Error("tool_effect_unknown");
            throw new AgentError("tool_effect_unknown", "Tool outcome requires reconciliation");
          }
          return { saved: prior.result as ToolHandlerResult };
        }
        if (dispatch !== undefined && Object.entries(extra.tools).some(([other, entry]: [string, any]) => other !== key && entry.approvalId === approval!.record.approvalId)) {
          throw new AgentError("approval_intent_conflict", "Approval is already associated with another receipt key");
        }
        extra.tools[key] = { state: "claimed", ...(dispatch === undefined ? {} : {
          runId: dispatch.runId, callId: dispatch.call.id, approvalId: approval!.record.approvalId,
          intentDigest: approval!.record.intentDigest, approvalRevision: approval!.record.revision,
          leaseOwnerId: this.#owner.getStore(), leaseEpoch: extra.leases[dispatch.runId].epoch,
        }) };
        return {};
      }, false, dispatch === undefined ? "extra" : "all");
      if (outcome.error !== undefined) throw new AgentError(outcome.error, "Approval dispatch rejected");
      if (outcome.saved !== undefined) return outcome.saved;
      const result = await operation();
      if (dispatch !== undefined && !["committed", "not_started"].includes(result?.effectState)) throw new AgentError("tool_effect_unknown", "Tool outcome requires reconciliation");
      await this.#extraTransaction(async (extra) => {
        if (dispatch !== undefined) {
          const lease = requireApprovalOwner(extra, dispatch.runId, this.#owner.getStore(), this.#epoch.getStore());
          const claim = extra.tools[key];
          if (claim?.state !== "claimed" || claim.leaseOwnerId !== lease.owner || claim.leaseEpoch !== lease.epoch) throw new AgentError("run_lease_lost", "Tool claim ownership changed");
        }
        extra.tools[key] = { ...extra.tools[key], state: "complete", result };
      });
      return result;
    },
  };

  constructor(path: string, options: { scope: string }) {
    if (!options.scope.trim()) throw new TypeError("scope is required");
    this.#scope = options.scope;
    if (path !== ":memory:") {
      try { closeSync(openSync(path, "ax", 0o600)); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    }
    this.#db = new DatabaseSync(path);
    try { storageVersion(this.#db); } catch (error) {
      this.#db.close();
      throw error;
    }
    this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=0; PRAGMA foreign_keys=ON;");
    this.#db.exec("CREATE TABLE IF NOT EXISTS purra_state (scope TEXT NOT NULL, sdk TEXT NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(scope,sdk))");
    this.#journal = new OutputJournal(this.#db, this.#scope);
  }

  #port<K extends keyof Stores>(name: K): StoragePorts[K] {
    const methods = new Set<string>(STORAGE_PORT_METHODS[name]);
    return new Proxy({} as StoragePorts[K], { get: (_target, method: string) => {
      if (name === "runs" && (method === "executeOwned" || method === "executeToolOwned")) return this.#executeOwned.bind(this);
      if (name === "runs" && (method === "listEvents" || method === "listRootEvents")) {
        return (id: string, after: number, limit?: number) => this.#withConnection(
          async () => this.#journal.read(id, after, limit, method === "listRootEvents"), true,
        );
      }
      if (!(methods.has(method) || name === "runs" && method === "saveToolExecutionCheckpoint") || method === "constructor") return undefined;
      return (...args: unknown[]) => this.#transaction(async (all, extra) => {
        if (name === "runs" && !["get", "listEvents", "listRootEvents"].includes(method)) {
          const lease = extra.leases[String(args[0])];
          const owner = this.#owner.getStore();
          if (owner !== undefined && lease && (lease.owner !== owner || lease.expires <= Date.now() || lease.epoch !== this.#epoch.getStore())) throw new Error("run_lease_lost");
        }
        return (all[name] as any)[method](...args);
      }, name === "runs" && RUN_READ_METHODS.has(method), name === "runs" ? "all" : name,
      name === "runs" && ROOT_BOUND_METHODS.has(method) && typeof args[0] === "string" ? args[0] : undefined);
    } });
  }

  async #executeOwned<T>(runId: string, operation: () => Promise<T>, checkpoint?: AgentExecutionCheckpoint | AgentToolExecutionCheckpoint): Promise<T> {
    const owner = globalThis.crypto.randomUUID();
    const epoch = await this.transaction(async (all, extra) => {
      const saved = await all.runs.get(runId);
      if (saved.status !== "running") throw new AgentError("run_terminal", "Run is terminal");
      const old = extra.leases[runId];
      if (old && old.owner !== null && old.expires > Date.now()) throw new AgentError("run_lease_conflict", "Run execution lease could not be acquired");
      if (saved.toolExecutionCheckpoint !== undefined && checkpoint?.phase !== "tool_ready") throw new AgentError("approval_runtime_required", "Pending tool checkpoint requires tool recovery");
      if (checkpoint?.phase === "tool_ready" && Object.values(extra.tools).some((entry: any) => entry.runId === runId && entry.state === "claimed")) throw new AgentError("run_recovery_requires_reconciliation", "Tool effect requires reconciliation");
      if (checkpoint !== undefined && JSON.stringify(saved.toolExecutionCheckpoint ?? saved.executionCheckpoint) !== JSON.stringify(checkpoint)) throw new AgentError("agent_execution_checkpoint_conflict", "Selected checkpoint is not canonical");
      if (checkpoint !== undefined) {
        const events = await all.runs.listEvents(runId, 0, Number.MAX_SAFE_INTEGER);
        const lastCheckpoint = events.reduce((last, event, index) => event.kind === "agent.execution_checkpoint" ? index : last, -1);
        if (events.slice(lastCheckpoint + 1).some(event => event.kind === "invocation.started")) throw new AgentError("run_recovery_requires_reconciliation", "The last model/tool attempt needs reconciliation");
      }
      extra.leases[runId] = { owner, epoch: (old?.epoch ?? 0) + 1, expires: Date.now() + 30000 };
      return extra.leases[runId].epoch;
    });
    let stopped = false;
    const heartbeat = async () => {
      while (!stopped) {
        await sleep(1000, undefined, { signal: stop.signal }).catch(() => {});
        if (stopped) break;
        await this.#extraTransaction(async (extra) => {
          const lease = extra.leases[runId];
          if (lease?.owner !== owner || lease.epoch !== epoch || lease.expires <= Date.now()) throw new AgentError("run_lease_lost", "Execution lease was lost");
          extra.leases[runId].expires = Date.now() + 30000;
        });
      }
    };
    const stop = new AbortController();
    let heartbeatError: unknown;
    const monitor = heartbeat().catch((error) => { heartbeatError = error; });
    try {
      const result = await this.#epoch.run(epoch, () => this.#owner.run(owner, operation));
      if (heartbeatError) throw heartbeatError;
      return result;
    } finally {
      stopped = true; stop.abort(); await monitor;
      await this.#extraTransaction(async (extra) => {
        if (extra.leases[runId]?.owner === owner && extra.leases[runId].epoch === epoch) extra.leases[runId] = { ...extra.leases[runId], owner: null, expires: 0 };
      });
    }
  }

  async transaction<T>(operation: (all: Stores, extra: { tools: Record<string, any>; [key: string]: any }) => Promise<T>): Promise<T> {
    return this.#transaction(operation);
  }

  async inspectApprovalUpgrade(): Promise<ApprovalUpgradeInspection> {
    return this.#withConnection(async () => inspectApprovalUpgrade(this.#db), true);
  }

  async enableApprovals(): Promise<void> {
    await this.#withConnection(async () => enableApprovals(this.#db));
  }

  approvalStore(options: { authorize: ApprovalAuthorizer; clockMs?: () => number }): SqliteApprovalStore {
    return new SqliteApprovalStore({
      bindRuntimeClock: clock => {
        if (this.#approvalRuntimeClock !== undefined && this.#approvalRuntimeClock !== clock) throw new AgentError("approval_runtime_clock_conflict", "Approval gateways on one adapter must share a clock");
        this.#approvalRuntimeClock = clock;
      },
      owner: () => this.#owner.getStore(),
      epoch: () => this.#epoch.getStore(),
      idempotency: this.idempotency,
      read: operation => this.#withConnection(() => operation(this.#db, this.#scope), true),
      write: operation => this.#transaction((all, extra) => operation(this.#db, this.#scope, all, extra)),
    }, options.authorize, options.clockMs);
  }

  async #withConnection<T>(operation: () => Promise<T>, readOnly = false): Promise<T> {
    const previous = this.#tail;
    let release!: () => void;
    this.#tail = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    this.#active = true;
    let begun = false;
    try {
      const deadline = Date.now() + 5000;
      for (;;) {
        try { this.#db.exec(readOnly ? "BEGIN" : "BEGIN IMMEDIATE"); begun = true; break; }
        catch (error) {
          if (!(error instanceof Error) || !error.message.includes("locked") || Date.now() >= deadline) throw error;
          await sleep(10);
        }
      }
      const result = await operation();
      this.#db.exec("COMMIT"); begun = false;
      return result;
    } catch (error) {
      if (begun) this.#db.exec("ROLLBACK");
      throw error;
    } finally { this.#active = false; release(); }
  }

  async #extraTransaction<T>(operation: (extra: { tools: Record<string, any>; [key: string]: any }) => Promise<T>): Promise<T> {
    return this.#transaction(async (_all, extra) => operation(extra), false, "extra");
  }

  async #transaction<T>(operation: (all: Stores, extra: { tools: Record<string, any>; [key: string]: any }) => Promise<T>, readOnly = false, selection: StateSelection = "all", journalRunId?: string): Promise<T> {
    return this.#withConnection(async () => {
      const version = storageVersion(this.#db);
      const row = this.#db.prepare("SELECT version,body FROM purra_state WHERE scope=? AND sdk='typescript'").get(this.#scope);
      if (row && row.version !== version) throw new Error("unsupported SQLite storage version");
      const rootRunId = row && journalRunId !== undefined ? this.#journal.rootForRun(journalRunId) : undefined;
      const deferredJournal = row && selection === "all" && !readOnly && rootRunId !== undefined ? this.#journal.deferred(rootRunId) : undefined;
      const events = row && selection === "all" && deferredJournal === undefined ? this.#journal.restore(rootRunId) : [];
      const prior = new Map<string, number>(deferredJournal?.counts);
      for (const event of events) prior.set(event.runId, event.sequence);
      const session = new StorageSession(row ? String(row.body) : undefined, selection, events,
        { ...(rootRunId === undefined ? {} : { rootRunId }), ...(deferredJournal === undefined ? {} : { deferredJournal }) });
      const result = await operation(session.stores, session.extra);
      if (!readOnly) {
        if (version !== 5 && selection === "all" && session.hasToolExecutionCheckpoint()) throw new AgentError("approval_storage_not_enabled", "Tool checkpoints require explicit approval storage activation");
        const checkpoint = session.exportSnapshot();
        if (!row || row.body !== checkpoint.body) this.#db.prepare("INSERT INTO purra_state VALUES(?, 'typescript', ?, ?) ON CONFLICT(scope,sdk) DO UPDATE SET version=excluded.version,body=excluded.body").run(this.#scope, version, checkpoint.body);
        if (selection === "all") this.#journal.append(checkpoint.journals, prior);
      }
      return result;
    }, readOnly);
  }

  async reconcileTool(key: string, proof: { result: ToolHandlerResult } | { notExecuted: true }): Promise<void> {
    await this.#extraTransaction(async (extra) => {
      const claim = extra.tools[key];
      if (claim?.state !== "claimed") throw new Error("tool_claim_conflict");
      const wakeRunId = claim.runId;
      if ("approvalId" in claim || "intentDigest" in claim || "approvalRevision" in claim) {
        await checkApprovalReconciliation(this.#db, this.#scope, claim);
        const lease = extra.leases[claim.runId];
        if (lease?.owner && lease.expires > Date.now()) throw new AgentError("run_lease_conflict", "Reconciliation requires an idle Run");
        if ("result" in proof) {
          const result = copyToolHandlerResult(proof.result, "confirm");
          extra.tools[key] = { ...claim, state: "complete", result };
        } else if (proof.notExecuted === true) delete extra.tools[key];
        else throw new TypeError("invalid tool reconciliation proof");
      } else if ("result" in proof) extra.tools[key] = { state: "complete", result: proof.result };
      else if (proof.notExecuted === true) delete extra.tools[key];
      else throw new TypeError("invalid tool reconciliation proof");
      if (typeof wakeRunId === "string") wakeRecoverySchedule(extra, wakeRunId, true);
    });
  }

  async pruneRecoverySchedule(runIds: readonly string[]): Promise<readonly string[]> {
    return this.#transaction(async (all, extra) => {
      const removed: string[] = [];
      for (const runId of new Set(runIds)) {
        const saved = await all.runs.get(runId);
        if (saved.status !== "running" && removeRecoverySchedule(extra, runId)) removed.push(runId);
      }
      return Object.freeze(removed);
    });
  }

  recoverySchedule(options: { intervalMs?: number; maxBackoffMs?: number; clockMs?: () => number } = {}): SqliteRecoverySchedule {
    return new SqliteRecoverySchedule((operation, readOnly) => this.#transaction((_all, extra) => operation(extra), readOnly, "extra"), options);
  }

  async listRunCandidates(options: { afterRunId?: string; limit?: number } = {}) {
    const limit = options.limit ?? 100, after = options.afterRunId;
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) throw new TypeError("candidate page limit must be between 1 and 1000");
    if (after !== undefined && (typeof after !== "string" || !after)) throw new TypeError("candidate cursor must be a nonempty Run id");
    return this.#withConnection(async () => {
      storageVersion(this.#db);
      const query = this.#db.prepare("SELECT run_id FROM purra_journal_runs WHERE scope=? AND sdk='typescript'"
        + (after === undefined ? "" : " AND run_id>?") + " ORDER BY run_id LIMIT ?");
      const rows = after === undefined ? query.all(this.#scope, limit + 1) : query.all(this.#scope, after, limit + 1);
      if (rows.some(row => typeof row.run_id !== "string" || !row.run_id)) throw new TypeError("invalid Run candidate identity");
      const runIds = Object.freeze(rows.slice(0, limit).map(row => row.run_id as string));
      return Object.freeze({ authority: "candidate_only" as const, runIds,
        nextAfterRunId: rows.length > limit ? runIds[runIds.length - 1]! : null });
    }, true);
  }

  async listRunning(): Promise<readonly string[]> {
    return this.#transaction(async all => all.runs.runningRunIds(), true);
  }

  /** Read one committed snapshot. No lease claim, reconciliation or external execution. */
  async inspectRecovery(runId: string, options: { expectedPreset?: AgentPresetSnapshot } = {}): Promise<RecoveryInspection> {
    return this.#transaction((all, extra) => this.#inspectRecoverySnapshot(all, extra, runId, options), true, "all", runId);
  }

  async inspectRecoveryMany(runIds: readonly string[]): Promise<Readonly<Record<string, RecoveryInspection>>> {
    if (!Array.isArray(runIds) || runIds.length > 100 || runIds.some(id => typeof id !== "string" || !id)) throw new TypeError("inspection batch requires at most 100 nonempty Run ids");
    if (runIds.length === 0) return Object.freeze({});
    return this.#transaction(async (all, extra) => {
      const result: Record<string, RecoveryInspection> = Object.create(null);
      for (const id of new Set(runIds)) result[id] = await this.#inspectRecoverySnapshot(all, extra, id, {});
      return Object.freeze(result);
    }, true);
  }

  async #inspectRecoverySnapshot(all: Stores, extra: { tools: Record<string, any>; [key: string]: any }, runId: string, options: { expectedPreset?: AgentPresetSnapshot }): Promise<RecoveryInspection> {
    const saved = await all.runs.get(runId);
    const events = await all.runs.listEvents(runId, 0, Number.MAX_SAFE_INTEGER);
    const lastCheckpoint = events.reduce((last, event, index) => event.kind === "agent.execution_checkpoint" ? index : last, -1);
    const lease = extra.leases[runId];
    const approval = await inspectApprovalState(this.#db, this.#scope, saved, extra, (this.#approvalRuntimeClock ?? Date.now)());
    return buildRecoveryInspection({
      ...approval,
      status: saved.status === "running" ? "running" : "terminal",
      checkpoint: saved.executionCheckpoint === undefined && saved.toolExecutionCheckpoint === undefined ? "missing" : "present",
      attemptsAfterCheckpoint: saved.executionCheckpoint === undefined && saved.toolExecutionCheckpoint === undefined ? null
        : events.slice(lastCheckpoint + 1).filter(event => event.kind === "invocation.started").length,
      // Legacy opaque claims remain storage-wide; proven approval claims are reported separately.
      unknownToolReceipts: Object.values(extra.tools).filter(receipt => receipt.state === "claimed").length - (approval.approvalUnknownReceipts ?? 0),
      receiptScope: "storage",
      lease: lease?.owner && lease.expires > Date.now() ? "active" : "inactive",
      configuration: options.expectedPreset === undefined ? "unknown"
        : await jsonIdentityDigest(saved.preset) === await jsonIdentityDigest(options.expectedPreset) ? "matched" : "mismatch",
      cancellation: saved.status === "canceled" ? "requested" : "clear",
      deadline: saved.deadlineAt !== null && Date.parse(saved.deadlineAt) <= Date.now() ? "expired" : "open",
    });
  }

  close(): void { if (this.#active) throw new Error("storage transaction is active"); this.#db.close(); }
}
