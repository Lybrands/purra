/** Transaction-scoped Core state bridge for durable adapter packages. */
import { InMemoryRunRepository } from "../run/store.js";
import { InMemoryRunTreeRepository } from "../agent-tree.js";
import { InMemoryArtifactStore } from "../artifacts/repository.js";
import { InMemoryLongTaskRepository } from "../durable/repository.js";
import type { OutputEvent } from "../output/types.js";

export const STORAGE_STATE_SCHEMA = "purra.storage-state/typescript/v1";

function createStorageStores() {
  const runTree = new InMemoryRunTreeRepository();
  return { runTree, runs: new InMemoryRunRepository({ leaseValidator: (id, claim) => runTree.requireRunClaim(id, claim) }),
    artifacts: new InMemoryArtifactStore(), longTasks: new InMemoryLongTaskRepository() };
}
export type StorageStores = ReturnType<typeof createStorageStores>;
export type StorageSelection = "all" | "extra" | Exclude<keyof StorageStores, "runs">;
export type StorageJournalOptions = NonNullable<Parameters<InMemoryRunRepository["importState"]>[2]>;

type StorageExtra = { tools: Record<string, any>; leases: Record<string, any>; [key: string]: any };

type Saved = { schema: string; stores: Partial<Record<keyof StorageStores, string>>; extra: Record<string, any> };

export class StorageSession {
  readonly stores = createStorageStores();
  readonly extra: StorageExtra;
  readonly #saved: Saved;
  readonly #selection: StorageSelection;

  constructor(body?: string, selection: StorageSelection = "all", events: readonly OutputEvent[] = [], options: StorageJournalOptions = {}) {
    this.#selection = selection;
    const saved: Saved = body === undefined ? { schema: STORAGE_STATE_SCHEMA, stores: {}, extra: {} } : JSON.parse(body);
    if (saved === null || typeof saved !== "object" || saved.schema !== STORAGE_STATE_SCHEMA
      || Object.keys(saved).sort().join() !== "extra,schema,stores"
      || saved.stores === null || typeof saved.stores !== "object" || Array.isArray(saved.stores)
      || saved.extra === null || typeof saved.extra !== "object" || Array.isArray(saved.extra)) throw new TypeError("Unsupported Core storage state");
    const keys = ["runs", "runTree", "artifacts", "longTasks"] as const;
    if (body !== undefined && (Object.keys(saved.stores).length !== keys.length || keys.some(key => typeof saved.stores[key] !== "string"))) throw new TypeError("Invalid storage groups");
    this.#saved = saved;
    this.extra = saved.extra as StorageExtra;
    for (const key of ["tools", "leases"]) {
      if (this.extra[key] !== undefined && (this.extra[key] === null || typeof this.extra[key] !== "object" || Array.isArray(this.extra[key]))) throw new TypeError("Invalid storage extension");
      this.extra[key] = Object.assign(Object.create(null), this.extra[key]);
    }
    if (saved.stores.runs !== undefined && selection === "all") {
      this.stores.runs.importState(saved.stores.runs, options.deferredJournal === undefined ? events : undefined, options);
    }
    for (const key of ["runTree", "artifacts", "longTasks"] as const) {
      if (saved.stores[key] !== undefined && (selection === "all" || selection === key)) this.stores[key].importState(saved.stores[key]);
    }
  }

  exportSnapshot() {
    const checkpoint = this.#selection === "all" || this.#saved.stores.runs === undefined
      ? this.stores.runs.exportJournalState({ incremental: true }) : undefined;
    if (checkpoint !== undefined) this.#saved.stores.runs = checkpoint.state;
    for (const key of ["runTree", "artifacts", "longTasks"] as const) {
      if (this.#selection === "all" || this.#selection === key || this.#saved.stores[key] === undefined) this.#saved.stores[key] = this.stores[key].exportState();
    }
    return { body: JSON.stringify(this.#saved), journals: checkpoint?.journals ?? [] };
  }
}

export const STORAGE_PORT_METHODS = {
  runs: ["begin", "openInvocation", "appendEvent", "appendBatch", "saveExecutionCheckpoint", "settleInvocation", "settleRun", "cancel", "get", "listEvents", "listRootEvents"],
  runTree: ["beginRoot", "spawnAgents", "continueAgent", "claimRun", "suspendRun", "renewRunLease", "markWaiting", "releaseWaiting", "completeRun", "failRun", "cancelSubtree", "aggregateRuns", "closeAgent", "getAgent", "getRun", "getCheckpoint", "listRunnable", "listDescendants", "requireRunClaim"],
  artifacts: ["create", "load", "findForOwner", "replayReceipt", "append", "listBatches", "finalize", "abort", "acquire", "loadActive", "renew", "release", "releaseForRun", "maintain", "inspect"],
  longTasks: ["create", "findByIdempotencyKey", "load", "listUnits", "bindRun", "listRunBindings", "start", "claimReadyUnit", "markUnitRunning", "heartbeat", "appendCheckpoint", "recordUsage", "completeUnit", "failUnit", "listCheckpoints", "finalizeIfComplete", "pause", "resume", "requestCancel", "cancel"],
} as const;

export type StoragePorts = {
  [K in keyof StorageStores]: {
    [M in (typeof STORAGE_PORT_METHODS)[K][number] & keyof StorageStores[K]]:
      StorageStores[K][M] extends (...args: infer A) => infer R ? (...args: A) => Promise<Awaited<R>> : never;
  };
};
