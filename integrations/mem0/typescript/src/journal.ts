import { DatabaseSync } from "node:sqlite";
import { randomUUID } from "node:crypto";
import type { MemoryUsage } from "./providers.js";
import type { MemoryMetadata, MemoryResolution, MemoryRef, MemoryReview, MemoryLink } from "./memory.js";

export class MemoryError extends Error {
  constructor(public readonly code: string) { super(code); this.name = "MemoryError"; }
}

export interface Metadata {
  purra_scope: string;
  purra_store: string;
  purra_operation: string;
  purra_version: number;
  purra_state: "active" | "pending" | "disabled";
  purra_source: string;
  purra_revision: string;
  purra_inferred: boolean;
  purra_expires: string | null;
  purra_metadata: MemoryMetadata;
  purra_reason: string | null;
  purra_created: string;
  purra_updated: string;
}
export interface ItemView {
  version: number; state: Metadata["purra_state"]; metadata: MemoryMetadata;
  reason: string | null; createdAt: string; updatedAt: string; resolution?: string | null;
}
export interface Item { id: string; meta: Metadata; hash: string; deleted: boolean; view?: ItemView }
export function itemView(row: Item): ItemView {
  return { version: row.meta.purra_version, state: row.meta.purra_state,
    metadata: row.meta.purra_metadata, reason: row.meta.purra_reason,
    createdAt: row.meta.purra_created, updatedAt: row.meta.purra_updated, ...row.view };
}
export interface Plan {
  kind: string; target: string | null; meta: Metadata | null; hash?: string | null;
  discarding?: boolean; budget?: string; provider_error?: string; providers_verified?: boolean;
  resolution?: MemoryResolution; review_key?: string; review_epoch?: number;
  review_refs?: readonly MemoryRef[]; review?: Omit<MemoryReview, "proposal" | "key">; policy_hash?: string;
  changes?: Partial<ItemView>;
  link?: Omit<MemoryLink, "key" | "valid">;
}
export interface OperationRow {
  key: string; fingerprint: string; state: "running" | "unknown" | "failed" | "complete" | "discarded";
  plan: Plan; ids: string[] | null;
}

/** Only control metadata is stored here. Content and vectors remain in Mem0. */
export class Journal {
  private readonly db: DatabaseSync;
  readonly store: string;
  constructor(path: string, private readonly scope: string) {
    if (typeof path !== "string" || !path.trim() || path === ":memory:") {
      throw new TypeError("journalPath must be a persistent SQLite path");
    }
    this.db = new DatabaseSync(path);
    this.db.exec(`
      PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS purra_mem0_info (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS purra_mem0_epochs (scope TEXT PRIMARY KEY, epoch INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS purra_mem0_ops (
        scope TEXT NOT NULL, key TEXT NOT NULL, fingerprint TEXT NOT NULL,
        state TEXT NOT NULL, plan TEXT NOT NULL, ids TEXT, PRIMARY KEY(scope,key));
      CREATE UNIQUE INDEX IF NOT EXISTS purra_mem0_writer ON purra_mem0_ops(scope)
        WHERE state IN ('running','unknown');
      CREATE TABLE IF NOT EXISTS purra_mem0_items (
        scope TEXT NOT NULL, id TEXT NOT NULL, record TEXT NOT NULL, PRIMARY KEY(scope,id));
      CREATE TABLE IF NOT EXISTS purra_mem0_budgets (
        scope TEXT NOT NULL, key TEXT NOT NULL, limits TEXT NOT NULL, PRIMARY KEY(scope,key));
      CREATE TABLE IF NOT EXISTS purra_mem0_calls (
        scope TEXT NOT NULL, id TEXT NOT NULL, budget TEXT NOT NULL, operation TEXT,
        kind TEXT NOT NULL, state TEXT NOT NULL, input_chars INTEGER NOT NULL,
        reserved_output INTEGER NOT NULL, input_tokens INTEGER, output_tokens INTEGER,
        PRIMARY KEY(scope,id));
      CREATE INDEX IF NOT EXISTS purra_mem0_calls_budget ON purra_mem0_calls(scope,budget);
      CREATE INDEX IF NOT EXISTS purra_mem0_calls_operation ON purra_mem0_calls(scope,operation);
      CREATE TABLE IF NOT EXISTS purra_mem0_revocations (
        scope TEXT NOT NULL, source TEXT NOT NULL, revision TEXT NOT NULL,
        PRIMARY KEY(scope,source,revision));
    `);
    this.transaction(() => {
      this.db.prepare("INSERT OR IGNORE INTO purra_mem0_info VALUES ('store',?)").run(randomUUID().replaceAll("-", ""));
      this.db.prepare("INSERT OR IGNORE INTO purra_mem0_epochs VALUES (?,0)").run(scope);
    });
    this.store = this.db.prepare("SELECT value FROM purra_mem0_info WHERE key='store'").get()!.value as string;
  }
  private transaction<T>(work: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try { const result = work(); this.db.exec("COMMIT"); return result; }
    catch (error) { this.db.exec("ROLLBACK"); throw error; }
  }
  operation(key: string): OperationRow | undefined {
    const row = this.db.prepare("SELECT * FROM purra_mem0_ops WHERE scope=? AND key=?").get(this.scope, key);
    if (!row) return undefined;
    return { key, fingerprint: row.fingerprint as string, state: row.state as OperationRow["state"],
      plan: JSON.parse(row.plan as string) as Plan, ids: row.ids === null ? null : JSON.parse(row.ids as string) as string[] };
  }
  begin(key: string, fingerprint: string, plan: Plan): OperationRow | undefined {
    return this.transaction(() => {
      const previous = this.operation(key);
      if (previous) {
        if (previous.fingerprint !== fingerprint) throw new MemoryError("memory_idempotency_conflict");
        return previous;
      }
      if (this.db.prepare("SELECT 1 FROM purra_mem0_ops WHERE scope=? AND state IN ('running','unknown')").get(this.scope)) {
        throw new MemoryError("memory_write_busy");
      }
      this.db.prepare("INSERT INTO purra_mem0_ops VALUES (?,?,?,'running',?,NULL)")
        .run(this.scope, key, fingerprint, JSON.stringify(plan));
      return undefined;
    });
  }
  savePlan(key: string, plan: Plan): void {
    this.db.prepare("UPDATE purra_mem0_ops SET plan=? WHERE scope=? AND key=?").run(JSON.stringify(plan), this.scope, key);
  }
  saveIds(key: string, ids: string[]): void {
    this.db.prepare("UPDATE purra_mem0_ops SET ids=? WHERE scope=? AND key=?").run(JSON.stringify(ids), this.scope, key);
  }
  budget(key: string, limits: Record<string, number>): void {
    this.transaction(() => {
      const row = this.db.prepare("SELECT limits FROM purra_mem0_budgets WHERE scope=? AND key=?").get(this.scope, key);
      if (row) {
        const previous = JSON.parse(row.limits as string) as Record<string, number>;
        if (Object.keys(previous).length !== Object.keys(limits).length || Object.entries(limits).some(([k, v]) => previous[k] !== v)) {
          throw new MemoryError("memory_budget_conflict");
        }
      }
      this.db.prepare("INSERT OR IGNORE INTO purra_mem0_budgets VALUES (?,?,?)").run(this.scope, key, JSON.stringify(limits));
    });
  }
  usage(column: "budget" | "operation", key: string): MemoryUsage {
    const row = this.db.prepare(`SELECT
      COALESCE(SUM(kind='llm'),0) AS llmCalls,
      COALESCE(SUM(kind='embedding'),0) AS embeddingCalls,
      COALESCE(SUM(input_chars),0) AS inputChars,
      COALESCE(SUM(reserved_output),0) AS reservedOutputTokens,
      COALESCE(SUM(input_tokens),0) AS reportedInputTokens,
      COALESCE(SUM(output_tokens),0) AS reportedOutputTokens,
      COALESCE(SUM(input_tokens IS NULL OR output_tokens IS NULL),0) AS unreportedCalls,
      COALESCE(SUM(state='started'),0) AS unsettledCalls
      FROM purra_mem0_calls WHERE scope=? AND ${column}=?`).get(this.scope, key)!;
    return Object.freeze(row) as unknown as MemoryUsage;
  }
  admit(budget: string, operation: string | undefined, kind: "llm" | "embedding", chars: number, reserve: number): string {
    return this.transaction(() => {
      const op = operation === undefined ? undefined : this.operation(operation);
      if (op?.plan.kind === "review") this.assertSnapshot(op.plan);
      if (op && op.plan.kind !== "delete" && op.plan.meta) this.assertSource(op.plan.meta);
      const limits = JSON.parse(this.db.prepare("SELECT limits FROM purra_mem0_budgets WHERE scope=? AND key=?").get(this.scope, budget)!.limits as string) as Record<string, number>;
      const used = this.usage("budget", budget);
      if (used[kind === "llm" ? "llmCalls" : "embeddingCalls"] + 1 > limits["max_" + kind + "_calls"]!
          || used.inputChars + chars > limits.max_input_chars!
          || used.reservedOutputTokens + reserve > limits.max_output_tokens!) throw new MemoryError("memory_budget_exceeded");
      const id = randomUUID().replaceAll("-", "");
      this.db.prepare("INSERT INTO purra_mem0_calls VALUES (?,?,?,?,?,'started',?,?,NULL,NULL)")
        .run(this.scope, id, budget, operation ?? null, kind, chars, reserve);
      return id;
    });
  }
  settle(id: string, state: string, inputTokens: number | null = null, outputTokens: number | null = null): void {
    this.db.prepare("UPDATE purra_mem0_calls SET state=?,input_tokens=?,output_tokens=? WHERE scope=? AND id=?")
      .run(state, inputTokens, outputTokens, this.scope, id);
  }
  providerError(key: string, code: string): void {
    this.transaction(() => {
      const op = this.operation(key);
      if (op) this.savePlan(key, { ...op.plan, provider_error: code });
    });
  }
  verifyProviders(key: string): void {
    this.transaction(() => {
      const op = this.operation(key)!;
      this.savePlan(key, { ...op.plan, providers_verified: true });
    });
  }
  fail(key: string, dispatched: boolean): void {
    this.db.prepare("UPDATE purra_mem0_ops SET state=? WHERE scope=? AND key=?").run(dispatched ? "unknown" : "failed", this.scope, key);
  }
  discard(key: string): void {
    this.db.prepare("UPDATE purra_mem0_ops SET state='discarded' WHERE scope=? AND key=?").run(this.scope, key);
  }
  revoked(source: string, revision: string): boolean {
    return !!this.db.prepare("SELECT 1 FROM purra_mem0_revocations WHERE scope=? AND source=? AND revision IN ('',?)")
      .get(this.scope, source, revision);
  }
  assertSource(meta: Pick<Metadata, "purra_source" | "purra_revision">): void {
    if (this.revoked(meta.purra_source, meta.purra_revision)) throw new MemoryError("memory_source_revoked");
  }
  revokeSource(key: string, fingerprint: string, source: string, revision: string | null): void {
    // An uncertain SDK writer must not block withdrawal; its fence stays intact.
    this.transaction(() => {
      const previous = this.operation(key);
      if (previous) {
        if (previous.fingerprint !== fingerprint) throw new MemoryError("memory_idempotency_conflict");
        return;
      }
      const plan = { kind: "revoke_source", target: null, meta: null, source, revision };
      this.db.prepare("INSERT INTO purra_mem0_ops VALUES (?,?,?,'complete',?,'[]')").run(this.scope, key, fingerprint, JSON.stringify(plan));
      if (!this.revoked(source, revision ?? "")) {
        this.db.prepare("INSERT INTO purra_mem0_revocations VALUES (?,?,?)").run(this.scope, source, revision ?? "");
        this.db.prepare("UPDATE purra_mem0_epochs SET epoch=epoch+1 WHERE scope=?").run(this.scope);
      }
    });
  }
  item(id: string): Item | undefined {
    const row = this.db.prepare("SELECT record FROM purra_mem0_items WHERE scope=? AND id=?").get(this.scope, id);
    return row ? JSON.parse(row.record as string) as Item : undefined;
  }
  resolve(key: string, fingerprint: string, plan: Plan, epoch: number): void {
    const resolution = plan.resolution!;
    this.control(key, fingerprint, plan, epoch, resolution.items, resolution.items.map(ref => ({
      state: ref.id === resolution.keep ? "active" : "disabled",
      reason: ref.id === resolution.keep ? null : resolution.kind, resolution: key,
    })));
  }
  control(key: string, fingerprint: string, plan: Plan, epoch: number,
      refs: readonly MemoryRef[], changes: readonly Partial<ItemView>[]): void {
    this.transaction(() => {
      const previous = this.operation(key);
      if (previous) {
        if (previous.fingerprint !== fingerprint) throw new MemoryError("memory_idempotency_conflict");
        return;
      }
      if (this.db.prepare("SELECT 1 FROM purra_mem0_ops WHERE scope=? AND state IN ('running','unknown')").get(this.scope)) throw new MemoryError("memory_write_busy");
      if (this.epoch !== epoch) throw new MemoryError("memory_context_stale");
      if (plan.review_key !== undefined) this.assertSnapshot(this.reviewPlan(plan.review_key));
      const records: Item[] = [], now = Date.now();
      for (const [index, ref] of refs.entries()) {
        const row = this.item(ref.id);
        if (!row || row.deleted) throw new MemoryError("memory_not_found");
        if (itemView(row).version !== ref.version) throw new MemoryError("memory_version_conflict");
        this.assertSource(row.meta);
        if ((["resolve", "link"].includes(plan.kind) || changes[index]?.state === "active")
            && row.meta.purra_expires !== null && Date.parse(row.meta.purra_expires) <= now) throw new MemoryError("memory_context_stale");
        if (changes[index]) row.view = { ...itemView(row), ...changes[index], version: ref.version + 1, updatedAt: new Date(now).toISOString() };
        records.push(row);
      }
      this.db.prepare("INSERT INTO purra_mem0_ops VALUES (?,?,?,'complete',?,?)")
        .run(this.scope, key, fingerprint, JSON.stringify(plan), JSON.stringify(records.map(r => r.id)));
      for (const row of records) this.db.prepare("UPDATE purra_mem0_items SET record=? WHERE scope=? AND id=?").run(JSON.stringify(row), this.scope, row.id);
      this.db.prepare("UPDATE purra_mem0_epochs SET epoch=epoch+1 WHERE scope=?").run(this.scope);
    });
  }
  assertSnapshot(plan: Plan): void {
    if (plan.review_epoch !== this.epoch || !plan.review_refs?.length) throw new MemoryError("memory_context_stale");
    const now = Date.now();
    for (const ref of plan.review_refs) {
      const row = this.item(ref.id);
      if (!row || row.deleted || itemView(row).version !== ref.version) throw new MemoryError("memory_context_stale");
      this.assertSource(row.meta);
      if (row.meta.purra_expires !== null && Date.parse(row.meta.purra_expires) <= now) throw new MemoryError("memory_context_stale");
    }
  }
  links(id: string, after: string | undefined, limit: number): { key: string; data: NonNullable<Plan["link"]> }[] {
    return this.db.prepare(`SELECT key,plan FROM purra_mem0_ops WHERE scope=? AND key>? AND state='complete'
      AND json_extract(plan,'$.kind')='link'
      AND (json_extract(plan,'$.link.from.id')=? OR json_extract(plan,'$.link.to.id')=?)
      ORDER BY key LIMIT ?`).all(this.scope, after ?? "", id, id, limit)
      .map(row => ({ key: row.key as string, data: (JSON.parse(row.plan as string) as Plan).link! }));
  }
  reviewPlan(key: string): Plan {
    const op = this.operation(key);
    if (!op || op.state !== "complete" || op.plan.kind !== "review" || !op.plan.review) throw new MemoryError("memory_review_unavailable");
    return op.plan;
  }
  finishReview(key: string, plan: Plan): void {
    this.transaction(() => {
      if (this.operation(key)?.state !== "running") throw new MemoryError("memory_operation_unresolved");
      this.assertSnapshot(plan);
      this.savePlan(key, plan);
      this.db.prepare("UPDATE purra_mem0_ops SET state='complete',ids='[]' WHERE scope=? AND key=?").run(this.scope, key);
    });
  }
  items(state: string | null, after: string | undefined, limit: number): Item[] {
    return this.db.prepare(`SELECT record FROM purra_mem0_items AS item WHERE scope=? AND id>?
      AND json_extract(record,'$.deleted')=0
      AND (? IS NULL OR COALESCE(json_extract(record,'$.view.state'),json_extract(record,'$.meta.purra_state'))=?)
      AND NOT EXISTS (SELECT 1 FROM purra_mem0_revocations AS r
        WHERE r.scope=item.scope AND r.source=json_extract(item.record,'$.meta.purra_source')
        AND r.revision IN ('',json_extract(item.record,'$.meta.purra_revision')))
      AND (? IS NULL OR ? != 'active' OR json_extract(record,'$.meta.purra_expires') IS NULL
           OR json_extract(record,'$.meta.purra_expires')>?) ORDER BY id LIMIT ?`)
      .all(this.scope, after ?? "", state, state, state, state, new Date().toISOString(), limit)
      .map(row => JSON.parse(row.record as string) as Item);
  }
  writing(id: string): boolean {
    return this.db.prepare("SELECT plan FROM purra_mem0_ops WHERE scope=? AND state IN ('running','unknown')").all(this.scope)
      .some(row => (JSON.parse(row.plan as string) as Plan).target === id);
  }
  commit(key: string, records: Item[]): void {
    this.transaction(() => {
      for (const record of records) this.db.prepare("INSERT OR REPLACE INTO purra_mem0_items VALUES (?,?,?)").run(this.scope, record.id, JSON.stringify(record));
      this.db.prepare("UPDATE purra_mem0_ops SET state='complete' WHERE scope=? AND key=?").run(this.scope, key);
      if (records.length) this.db.prepare("UPDATE purra_mem0_epochs SET epoch=epoch+1 WHERE scope=?").run(this.scope);
    });
  }
  get epoch(): number {
    return this.db.prepare("SELECT epoch FROM purra_mem0_epochs WHERE scope=?").get(this.scope)!.epoch as number;
  }
  close(): void { this.db.close(); }
}
