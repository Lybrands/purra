import { encodeStorageState, decodeStorageState } from "../shared/storage-state.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";
import { stableFingerprint } from "../shared/fingerprint.js";
import type {
  AgentDelegation,
  DelegationAggregation,
  DelegationBatchCommand,
  DelegationBatchReceipt,
  DelegationDefinition,
  DelegationRepository,
  DelegationStatus,
} from "./types.js";

interface StoredBatch {
  readonly digest: string;
  readonly batchId: string;
  readonly delegationIds: readonly string[];
}

export class InMemoryDelegationRepository implements DelegationRepository {
  readonly #rows = new Map<string, AgentDelegation>();
  readonly #batches = new Map<string, StoredBatch>();
  readonly #idFactory: () => string;

  /** Opaque version-pinned storage data, never a public output projection. */
  public exportState(): string { return encodeStorageState({ rows: this.#rows, batches: this.#batches }); }
  public importState(text: string): void {
    const shape = { rows: this.#rows, batches: this.#batches };
    const saved = decodeStorageState(text) as typeof shape;
    this.#rows.clear(); for (const [key, value] of saved.rows) this.#rows.set(key, value);
    this.#batches.clear(); for (const [key, value] of saved.batches) this.#batches.set(key, value);
  }

  public constructor(idFactory: () => string = () => globalThis.crypto.randomUUID()) {
    this.#idFactory = idFactory;
  }

  public async createBatch(command: DelegationBatchCommand): Promise<DelegationBatchReceipt> {
    const runId = requiredText(command.runId, "delegation Run id");
    const batchId = requiredText(command.batchId, "delegation batch id");
    const idempotencyKey = requiredText(command.idempotencyKey, "delegation idempotency key");
    if (!Array.isArray(command.delegations) || command.delegations.length === 0) {
      throw new TypeError("Delegation batch must not be empty");
    }
    const definitions = Object.freeze(command.delegations.map(copyDefinition));
    const digest = await stableFingerprint(copyJsonValue(definitions));
    const replayKey = `${runId}\u0000${idempotencyKey}`;
    const existing = this.#batches.get(replayKey);
    if (existing !== undefined) {
      if (existing.digest !== digest || existing.batchId !== batchId) {
        throw new AgentError(
          "delegation_idempotency_conflict",
          "Delegation idempotency key was reused with different input",
        );
      }
      return Object.freeze({
        delegations: Object.freeze(existing.delegationIds.map((id) => this.#rows.get(id)!)),
        replayed: true,
      });
    }

    const now = new Date().toISOString();
    const newIds = new Set<string>();
    const rows = definitions.map((definition) => {
      const row: AgentDelegation = Object.freeze({
        ...definition,
        id: requiredText(this.#idFactory(), "delegation id"),
        batchId,
        runId,
        idempotencyKey,
        input: definition.input ?? Object.freeze({}),
        contextMode: "isolated",
        status: "queued",
        required: definition.required ?? true,
        priority: definition.priority ?? 0,
        createdAt: now,
        updatedAt: now,
      });
      if (this.#rows.has(row.id) || newIds.has(row.id)) {
        throw new AgentError("duplicate_delegation", "Delegation id already exists");
      }
      newIds.add(row.id);
      return row;
    });
    for (const row of rows) this.#rows.set(row.id, row);
    this.#batches.set(replayKey, Object.freeze({
      digest,
      batchId,
      delegationIds: Object.freeze(rows.map((row) => row.id)),
    }));
    return Object.freeze({ delegations: Object.freeze(rows), replayed: false });
  }

  public async start(
    delegationId: string,
    runId: string,
    batchId: string,
  ): Promise<AgentDelegation | undefined> {
    const row = this.#owned(delegationId, runId, batchId);
    if (row.status !== "queued") return undefined;
    return this.#replace(row, { status: "running" });
  }

  public async complete(
    delegationId: string,
    runId: string,
    batchId: string,
    resultSummary: import("../model/types.js").JsonValue,
  ): Promise<boolean> {
    const row = this.#owned(delegationId, runId, batchId);
    if (row.status !== "running") return false;
    this.#replace(row, { status: "done", resultSummary: copyJsonValue(resultSummary) });
    return true;
  }

  public async fail(
    delegationId: string,
    runId: string,
    batchId: string,
    errorCode: string,
  ): Promise<boolean> {
    return this.#finish(delegationId, runId, batchId, "failed", errorCode);
  }

  public async cancel(
    delegationId: string,
    runId: string,
    batchId: string,
    reason: string,
  ): Promise<boolean> {
    return this.#finish(delegationId, runId, batchId, "canceled", reason);
  }

  public async listForRun(runId: string): Promise<readonly AgentDelegation[]> {
    const required = requiredText(runId, "delegation Run id");
    return Object.freeze([...this.#rows.values()].filter((row) => row.runId === required));
  }

  public async aggregateBatch(runId: string, batchId: string): Promise<DelegationAggregation> {
    const requiredRun = requiredText(runId, "delegation Run id");
    const requiredBatch = requiredText(batchId, "delegation batch id");
    const rows = [...this.#rows.values()].filter((row) => (
      row.runId === requiredRun && row.batchId === requiredBatch
    ));
    const counts: Record<DelegationStatus, number> = {
      queued: 0,
      running: 0,
      done: 0,
      failed: 0,
      canceled: 0,
    };
    for (const row of rows) counts[row.status] += 1;
    const requiredFailures = rows
      .filter((row) => row.required && (row.status === "failed" || row.status === "canceled"))
      .map((row) => row.id);
    const pending = counts.queued + counts.running;
    return Object.freeze({
      state: pending > 0 ? "pending" : requiredFailures.length > 0 ? "blocked" : "ready",
      counts: Object.freeze(counts),
      requiredFailures: Object.freeze(requiredFailures),
      results: Object.freeze(rows
        .filter((row) => row.status === "done")
        .map((row) => Object.freeze({
          delegationId: row.id,
          agentName: row.agentName,
          agentTitle: row.title,
          summary: row.resultSummary ?? "",
        }))),
    });
  }

  public async cancelBatch(runId: string, batchId: string, reason = "delegation_canceled"): Promise<number> {
    const rows = await this.listForRun(runId);
    let canceled = 0;
    for (const row of rows) {
      if (row.batchId === batchId && await this.cancel(row.id, runId, batchId, reason)) canceled += 1;
    }
    return canceled;
  }

  #finish(
    delegationId: string,
    runId: string,
    batchId: string,
    status: "failed" | "canceled",
    errorCode: string,
  ): boolean {
    const row = this.#owned(delegationId, runId, batchId);
    if (row.status !== "queued" && row.status !== "running") return false;
    this.#replace(row, { status, errorCode: requiredText(errorCode, "delegation error code") });
    return true;
  }

  #owned(delegationId: string, runId: string, batchId: string): AgentDelegation {
    const id = requiredText(delegationId, "delegation id");
    const row = this.#rows.get(id);
    if (row === undefined) throw new AgentError("delegation_not_found", "Delegation does not exist");
    if (row.runId !== runId || row.batchId !== batchId) {
      throw new AgentError("delegation_scope_violation", "Delegation escaped its Root Run batch");
    }
    return row;
  }

  #replace(
    row: AgentDelegation,
    update: { readonly status: DelegationStatus; readonly resultSummary?: import("../model/types.js").JsonValue; readonly errorCode?: string },
  ): AgentDelegation {
    const next = Object.freeze({
      ...row,
      ...update,
      updatedAt: new Date().toISOString(),
    });
    this.#rows.set(row.id, next);
    return next;
  }
}

function copyDefinition(value: DelegationDefinition): DelegationDefinition {
  if (value === null || typeof value !== "object") throw new TypeError("Invalid delegation definition");
  const input = value.input === undefined ? Object.freeze({}) : copyJsonValue(value.input);
  if (input === null || typeof input !== "object" || Array.isArray(input)) {
    throw new TypeError("Delegation input must be an object");
  }
  if (value.required !== undefined && typeof value.required !== "boolean") {
    throw new TypeError("Delegation required must be a boolean");
  }
  if (value.priority !== undefined && !Number.isSafeInteger(value.priority)) {
    throw new TypeError("Delegation priority must be an integer");
  }
  return Object.freeze({
    agentName: requiredText(value.agentName, "delegated Agent name"),
    title: requiredText(value.title, "delegated Agent title"),
    instruction: requiredText(value.instruction, "delegated Agent instruction"),
    objective: requiredText(value.objective, "delegation objective"),
    input,
    required: value.required ?? true,
    priority: value.priority ?? 0,
  });
}

function requiredText(value: unknown, label: string): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "") throw new TypeError(`${label} must be non-empty text`);
  return text;
}
