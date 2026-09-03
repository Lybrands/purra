import { DatabaseSync } from "node:sqlite";
import { AsyncLocalStorage } from "node:async_hooks";
import { openSync, closeSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import {
  InMemoryRunRepository, InMemoryRunTreeRepository, InMemoryArtifactStore,
  InMemoryLongTaskRepository, InMemoryDelegationRepository,
  type OutputPublisher, type ToolHandlerResult, type ToolIdempotencyGateway, type AgentExecutionCheckpoint,
} from "purra";

function stores() {
  const runTree = new InMemoryRunTreeRepository();
  const runs = new InMemoryRunRepository({ leaseValidator: (id, claim) => runTree.requireRunClaim(id, claim) });
  return { runs, runTree, artifacts: new InMemoryArtifactStore(), longTasks: new InMemoryLongTaskRepository(), delegations: new InMemoryDelegationRepository() };
}
type Stores = ReturnType<typeof stores>;

export class SqliteAgentAdapters {
  readonly #db: DatabaseSync;
  readonly #scope: string;
  #tail: Promise<unknown> = Promise.resolve();
  #active = false;
  readonly #owner = new AsyncLocalStorage<string>();
  readonly runs = this.#port("runs");
  readonly runTree = this.#port("runTree");
  readonly artifacts = this.#port("artifacts");
  readonly artifactClaims = this.artifacts;
  readonly artifactMaintenance = this.artifacts;
  readonly longTasks = this.#port("longTasks");
  readonly delegations = this.#port("delegations");
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
      const saved = await this.transaction(async (_stores, extra) => {
        if (extra.tools[key]) {
          if (extra.tools[key].state === "claimed") throw new Error("tool_effect_unknown");
          return extra.tools[key].result as ToolHandlerResult;
        }
        extra.tools[key] = { state: "claimed" };
        return undefined;
      });
      if (saved !== undefined) return saved;
      const result = await operation();
      await this.transaction(async (_stores, extra) => { extra.tools[key] = { state: "complete", result }; });
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
    this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=0;");
    this.#db.exec("CREATE TABLE IF NOT EXISTS purra_state (scope TEXT NOT NULL, sdk TEXT NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(scope,sdk))");
  }

  #port<K extends keyof Stores>(name: K): Stores[K] {
    const methods = new Set(Object.getOwnPropertyNames(Object.getPrototypeOf(stores()[name])));
    return new Proxy({} as Stores[K], { get: (_target, method: string) => {
      if (name === "runs" && method === "executeOwned") return this.#executeOwned.bind(this);
      if (!methods.has(method) || method === "constructor") return undefined;
      return (...args: unknown[]) => this.transaction(async (all, extra) => {
        if (name === "runs" && !["get", "listEvents", "listRootEvents"].includes(method)) {
          const lease = extra.leases[String(args[0])];
          const owner = this.#owner.getStore();
          if (owner !== undefined && lease && (lease.owner !== owner || lease.expires <= Date.now())) throw new Error("run_lease_lost");
        }
        return (all[name] as any)[method](...args);
      });
    } });
  }

  async #executeOwned<T>(runId: string, operation: () => Promise<T>, checkpoint?: AgentExecutionCheckpoint): Promise<T> {
    const owner = globalThis.crypto.randomUUID();
    await this.transaction(async (all, extra) => {
      const saved = await all.runs.get(runId);
      if (saved.status !== "running") throw new Error("run_terminal");
      const old = extra.leases[runId];
      if (old && old.owner !== null && old.expires > Date.now()) throw new Error("run_lease_conflict");
      if (checkpoint !== undefined && JSON.stringify(saved.executionCheckpoint) !== JSON.stringify(checkpoint)) throw new Error("checkpoint_conflict");
      if (checkpoint !== undefined) {
        const events = await all.runs.listEvents(runId, 0, Number.MAX_SAFE_INTEGER);
        const lastCheckpoint = events.reduce((last, event, index) => event.kind === "agent.execution_checkpoint" ? index : last, -1);
        if (events.slice(lastCheckpoint + 1).some(event => event.kind === "invocation.started")) throw new Error("run_recovery_requires_reconciliation");
      }
      extra.leases[runId] = { owner, expires: Date.now() + 30000 };
    });
    let stopped = false;
    const heartbeat = async () => {
      while (!stopped) {
        await sleep(1000, undefined, { signal: stop.signal }).catch(() => {});
        if (stopped) break;
        await this.transaction(async (_all, extra) => {
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
      await this.transaction(async (_all, extra) => {
        if (extra.leases[runId]?.owner === owner) extra.leases[runId] = { owner: null, expires: 0 };
      });
    }
  }

  async transaction<T>(operation: (all: Stores, extra: { tools: Record<string, any>; [key: string]: any }) => Promise<T>): Promise<T> {
    const previous = this.#tail;
    let release!: () => void;
    this.#tail = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    this.#active = true;
    let begun = false;
    try {
      const deadline = Date.now() + 5000;
      for (;;) {
        try { this.#db.exec("BEGIN IMMEDIATE"); begun = true; break; }
        catch (error) {
          if (!(error instanceof Error) || !error.message.includes("locked") || Date.now() >= deadline) throw error;
          await sleep(10);
        }
      }
      const all = stores();
      const row = this.#db.prepare("SELECT version,body FROM purra_state WHERE scope=? AND sdk='typescript'").get(this.#scope);
      if (row && row.version !== 1) throw new Error("unsupported SQLite storage version");
      const saved = row ? JSON.parse(String(row.body)) : { extra: { tools: Object.create(null) } };
      saved.extra.tools = Object.assign(Object.create(null), saved.extra.tools);
      saved.extra.leases = Object.assign(Object.create(null), saved.extra.leases);
      for (const key of Object.keys(all) as (keyof Stores)[]) if (saved[key]) all[key].importState(saved[key]);
      const result = await operation(all, saved.extra);
      // ponytail: O(project history) snapshots; use indexed rows for large journals.
      const body = JSON.stringify({ ...Object.fromEntries(Object.entries(all).map(([k, v]) => [k, v.exportState()])), extra: saved.extra });
      if (!row || row.body !== body) this.#db.prepare("INSERT INTO purra_state VALUES(?, 'typescript', 1, ?) ON CONFLICT(scope,sdk) DO UPDATE SET body=excluded.body").run(this.#scope, body);
      this.#db.exec("COMMIT"); begun = false;
      return result;
    } catch (error) {
      if (begun) this.#db.exec("ROLLBACK");
      throw error;
    } finally { this.#active = false; release(); }
  }

  async reconcileTool(key: string, proof: { result: ToolHandlerResult } | { notExecuted: true }): Promise<void> {
    await this.transaction(async (_all, extra) => {
      if (extra.tools[key]?.state !== "claimed") throw new Error("tool_claim_conflict");
      if ("result" in proof) extra.tools[key] = { state: "complete", result: proof.result };
      else if (proof.notExecuted === true) delete extra.tools[key];
      else throw new TypeError("invalid tool reconciliation proof");
    });
  }

  close(): void { if (this.#active) throw new Error("storage transaction is active"); this.#db.close(); }
}
