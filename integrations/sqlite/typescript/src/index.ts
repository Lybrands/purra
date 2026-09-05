import { OutputJournal, STORAGE_VERSION } from "./journal.js";
import { DatabaseSync } from "node:sqlite";
import { AsyncLocalStorage } from "node:async_hooks";
import { openSync, closeSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import {
  AgentError, StorageSession, STORAGE_PORT_METHODS,
  type StorageStores, type StoragePorts, type StorageSelection,
  type OutputPublisher, type ToolHandlerResult, type ToolIdempotencyGateway, type AgentExecutionCheckpoint,
} from "purra";

type Stores = StorageStores;
type StateSelection = StorageSelection;
const RUN_READ_METHODS = new Set(["get", "listEvents", "listRootEvents"]);
const ROOT_BOUND_METHODS = new Set(["get", "openInvocation", "appendEvent", "appendBatch", "saveExecutionCheckpoint", "settleInvocation", "settleRun", "cancel"]);

export class SqliteAgentAdapters {
  readonly #db: DatabaseSync;
  readonly #scope: string;
  readonly #journal: OutputJournal;
  #tail: Promise<unknown> = Promise.resolve();
  #active = false;
  readonly #owner = new AsyncLocalStorage<string>();
  readonly runs = this.#port("runs");
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
    executeOnce: async (key, operation) => {
      const saved = await this.#extraTransaction(async (extra) => {
        if (extra.tools[key]) {
          if (extra.tools[key].state === "claimed") throw new Error("tool_effect_unknown");
          return extra.tools[key].result as ToolHandlerResult;
        }
        extra.tools[key] = { state: "claimed" };
        return undefined;
      });
      if (saved !== undefined) return saved;
      const result = await operation();
      await this.#extraTransaction(async (extra) => { extra.tools[key] = { state: "complete", result }; });
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
    if (this.#db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='purra_state'").get()
      && this.#db.prepare("SELECT 1 FROM purra_state WHERE version != ? LIMIT 1").get(STORAGE_VERSION)) {
      this.#db.close();
      throw new Error("unsupported SQLite storage version");
    }
    this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=0; PRAGMA foreign_keys=ON;");
    this.#db.exec("CREATE TABLE IF NOT EXISTS purra_state (scope TEXT NOT NULL, sdk TEXT NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(scope,sdk))");
    this.#journal = new OutputJournal(this.#db, this.#scope);
  }

  #port<K extends keyof Stores>(name: K): StoragePorts[K] {
    const methods = new Set<string>(STORAGE_PORT_METHODS[name]);
    return new Proxy({} as StoragePorts[K], { get: (_target, method: string) => {
      if (name === "runs" && method === "executeOwned") return this.#executeOwned.bind(this);
      if (name === "runs" && (method === "listEvents" || method === "listRootEvents")) {
        return (id: string, after: number, limit?: number) => this.#withConnection(
          async () => this.#journal.read(id, after, limit, method === "listRootEvents"), true,
        );
      }
      if (!methods.has(method) || method === "constructor") return undefined;
      return (...args: unknown[]) => this.#transaction(async (all, extra) => {
        if (name === "runs" && !["get", "listEvents", "listRootEvents"].includes(method)) {
          const lease = extra.leases[String(args[0])];
          const owner = this.#owner.getStore();
          if (owner !== undefined && lease && (lease.owner !== owner || lease.expires <= Date.now())) throw new Error("run_lease_lost");
        }
        return (all[name] as any)[method](...args);
      }, name === "runs" && RUN_READ_METHODS.has(method), name === "runs" ? "all" : name,
      name === "runs" && ROOT_BOUND_METHODS.has(method) && typeof args[0] === "string" ? args[0] : undefined);
    } });
  }

  async #executeOwned<T>(runId: string, operation: () => Promise<T>, checkpoint?: AgentExecutionCheckpoint): Promise<T> {
    const owner = globalThis.crypto.randomUUID();
    await this.transaction(async (all, extra) => {
      const saved = await all.runs.get(runId);
      if (saved.status !== "running") throw new AgentError("run_terminal", "Run is terminal");
      const old = extra.leases[runId];
      if (old && old.owner !== null && old.expires > Date.now()) throw new AgentError("run_lease_conflict", "Run execution lease could not be acquired");
      if (checkpoint !== undefined && JSON.stringify(saved.executionCheckpoint) !== JSON.stringify(checkpoint)) throw new AgentError("agent_execution_checkpoint_conflict", "Selected checkpoint is not canonical");
      if (checkpoint !== undefined) {
        const events = await all.runs.listEvents(runId, 0, Number.MAX_SAFE_INTEGER);
        const lastCheckpoint = events.reduce((last, event, index) => event.kind === "agent.execution_checkpoint" ? index : last, -1);
        if (events.slice(lastCheckpoint + 1).some(event => event.kind === "invocation.started")) throw new AgentError("run_recovery_requires_reconciliation", "The last model/tool attempt needs reconciliation");
      }
      extra.leases[runId] = { owner, expires: Date.now() + 30000 };
    });
    let stopped = false;
    const heartbeat = async () => {
      while (!stopped) {
        await sleep(1000, undefined, { signal: stop.signal }).catch(() => {});
        if (stopped) break;
        await this.#extraTransaction(async (extra) => {
          if (extra.leases[runId]?.owner !== owner) throw new Error("run_lease_lost");
          extra.leases[runId].expires = Date.now() + 30000;
        });
      }
    };
    const stop = new AbortController();
    let heartbeatError: unknown;
    const monitor = heartbeat().catch((error) => { heartbeatError = error; });
    try {
      const result = await this.#owner.run(owner, operation);
      if (heartbeatError) throw heartbeatError;
      return result;
    } finally {
      stopped = true; stop.abort(); await monitor;
      await this.#extraTransaction(async (extra) => {
        if (extra.leases[runId]?.owner === owner) extra.leases[runId] = { owner: null, expires: 0 };
      });
    }
  }

  async transaction<T>(operation: (all: Stores, extra: { tools: Record<string, any>; [key: string]: any }) => Promise<T>): Promise<T> {
    return this.#transaction(operation);
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
      const row = this.#db.prepare("SELECT version,body FROM purra_state WHERE scope=? AND sdk='typescript'").get(this.#scope);
      if (row && row.version !== STORAGE_VERSION) throw new Error("unsupported SQLite storage version");
      const rootRunId = row && journalRunId !== undefined ? this.#journal.rootForRun(journalRunId) : undefined;
      const deferredJournal = row && selection === "all" && !readOnly && rootRunId !== undefined ? this.#journal.deferred(rootRunId) : undefined;
      const events = row && selection === "all" && deferredJournal === undefined ? this.#journal.restore(rootRunId) : [];
      const prior = new Map<string, number>(deferredJournal?.counts);
      for (const event of events) prior.set(event.runId, event.sequence);
      const session = new StorageSession(row ? String(row.body) : undefined, selection, events,
        { ...(rootRunId === undefined ? {} : { rootRunId }), ...(deferredJournal === undefined ? {} : { deferredJournal }) });
      const result = await operation(session.stores, session.extra);
      if (!readOnly) {
        const checkpoint = session.exportSnapshot();
        if (!row || row.body !== checkpoint.body) this.#db.prepare("INSERT INTO purra_state VALUES(?, 'typescript', ?, ?) ON CONFLICT(scope,sdk) DO UPDATE SET version=excluded.version,body=excluded.body").run(this.#scope, STORAGE_VERSION, checkpoint.body);
        if (selection === "all") this.#journal.append(checkpoint.journals, prior);
      }
      return result;
    }, readOnly);
  }

  async reconcileTool(key: string, proof: { result: ToolHandlerResult } | { notExecuted: true }): Promise<void> {
    await this.#extraTransaction(async (extra) => {
      if (extra.tools[key]?.state !== "claimed") throw new Error("tool_claim_conflict");
      if ("result" in proof) extra.tools[key] = { state: "complete", result: proof.result };
      else if (proof.notExecuted === true) delete extra.tools[key];
      else throw new TypeError("invalid tool reconciliation proof");
    });
  }

  close(): void { if (this.#active) throw new Error("storage transaction is active"); this.#db.close(); }
}
