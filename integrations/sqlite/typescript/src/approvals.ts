import type { DatabaseSync } from "node:sqlite";
import { AgentError, ApprovalIntent, copyApprovalDecisionCommand, copyApprovalRecord, jsonIdentityDigest,
  type ApprovalRecord, type ApprovalDecisionCommand, type ApprovalDecisionAudit, type StorageStores } from "purra";
import { storageVersion } from "./approval-format.js";

type Operation<T> = (db: DatabaseSync, scope: string, all: StorageStores, extra: Record<string, any>) => Promise<T>;
export interface ApprovalStorageAccess {
  read<T>(operation: (db: DatabaseSync, scope: string) => Promise<T>): Promise<T>;
  write<T>(operation: Operation<T>): Promise<T>;
}
export type ApprovalAuthorizer = (principalId: string, record: ApprovalRecord, command: ApprovalDecisionCommand) => boolean | Promise<boolean>;
export interface ApprovalDecisionReceipt {
  readonly approvalId: string;
  readonly intentDigest: string;
  readonly commandKey: string;
  readonly status: "approved" | "rejected";
  readonly revision: number;
  readonly decidedAtMs: number;
}
function fail(code: string): never { throw new AgentError(code, "Approval operation was rejected"); }
function text(value: string): void {
  if (typeof value !== "string" || !value || value.trim() !== value || [...value].length > 1024) throw new TypeError("Invalid approval text");
}
function integer(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError("Invalid approval time");
  return value;
}
function enabled(db: DatabaseSync): void { if (storageVersion(db) !== 5) fail("approval_storage_not_enabled"); }
async function load(db: DatabaseSync, scope: string, id: string): Promise<ApprovalRecord> {
  text(id); enabled(db);
  const row = db.prepare("SELECT run_id,call_id,body FROM purra_approvals WHERE scope=? AND sdk='typescript' AND approval_id=?").get(scope, id);
  if (!row) fail("approval_not_found");
  const record = await copyApprovalRecord(JSON.parse(String(row.body)));
  if (record.approvalId !== id || record.intent.runId !== row.run_id || record.intent.toolCallId !== row.call_id) fail("approval_record_conflict");
  return record;
}
function save(db: DatabaseSync, scope: string, record: ApprovalRecord): void {
  db.prepare("INSERT INTO purra_approvals VALUES(?, 'typescript', ?, ?, ?, ?) ON CONFLICT(scope,sdk,approval_id) DO UPDATE SET body=excluded.body")
    .run(scope, record.approvalId, record.intent.runId, record.intent.toolCallId, JSON.stringify(record));
}
async function runState(db: DatabaseSync, scope: string, all: StorageStores, extra: Record<string, any>, intent: ApprovalRecord["intent"]) {
  const binding = db.prepare("SELECT root_run_id FROM purra_journal_runs WHERE scope=? AND sdk='typescript' AND run_id=?").get(scope, intent.runId);
  if (binding?.root_run_id !== intent.rootRunId) fail("approval_run_conflict");
  const run = await all.runs.get(intent.runId), root = await all.runs.get(intent.rootRunId);
  const canceled = [run, root].some(row => row.status !== "running");
  const deadlines = [run, root].flatMap(row => row.deadlineAt === null ? [] : [Date.parse(row.deadlineAt)]);
  return { canceled, deadline: deadlines.length ? Math.min(...deadlines) : undefined, fingerprint: await jsonIdentityDigest(run.preset) };
}
function receipt(record: ApprovalRecord): ApprovalDecisionReceipt {
  const audit = record.decisionAudit as ApprovalDecisionAudit;
  return Object.freeze({ approvalId: record.approvalId, intentDigest: record.intentDigest, commandKey: audit.command.commandKey,
    status: audit.command.decision === "approve" ? "approved" : "rejected", revision: audit.revision, decidedAtMs: audit.decidedAtMs });
}

/** Host-only decisions. No method suspends a Run or permits tool dispatch. */
export class SqliteApprovalStore {
  constructor(private readonly access: ApprovalStorageAccess, private readonly authorize: ApprovalAuthorizer, private readonly clock: () => number = Date.now) {
    if (typeof authorize !== "function") throw new TypeError("Approval authorizer is required");
  }

  async create(intent: ApprovalIntent, options: { expiresAtMs: number }): Promise<ApprovalRecord> {
    if (!(intent instanceof ApprovalIntent)) throw new TypeError("Approval intent is required");
    const requestedExpiry = integer(options.expiresAtMs);
    const id = await jsonIdentityDigest({ profile: "purra.approval-key/v1", runId: intent.value.runId, toolCallId: intent.value.toolCallId });
    return this.access.write(async (db, scope, all, extra) => {
      enabled(db);
      const state = await runState(db, scope, all, extra, intent.value);
      if (state.canceled) fail("approval_run_terminal");
      if (state.fingerprint !== intent.value.presetFingerprint) fail("approval_configuration_mismatch");
      const expiresAtMs = Math.min(requestedExpiry, state.deadline ?? requestedExpiry);
      if (db.prepare("SELECT 1 FROM purra_approvals WHERE scope=? AND sdk='typescript' AND approval_id=?").get(scope, id)) {
        const record = await load(db, scope, id);
        if (record.intentDigest !== intent.digest || record.expiresAtMs !== expiresAtMs) fail("approval_intent_conflict");
        return record;
      }
      const now = integer(this.clock());
      if (now >= expiresAtMs) fail("approval_expired");
      const record = await copyApprovalRecord({ approvalId: id, intent: intent.value, intentDigest: intent.digest,
        revision: 1, status: "pending", createdAtMs: now, expiresAtMs, decisionAudit: {} });
      save(db, scope, record);
      return record;
    });
  }

  async get(approvalId: string): Promise<ApprovalRecord> {
    return this.access.read((db, scope) => load(db, scope, approvalId));
  }

  async listPending(options: { runId?: string } = {}): Promise<readonly ApprovalRecord[]> {
    const runId = options.runId;
    if (runId !== undefined) text(runId);
    return this.access.read(async (db, scope) => {
      enabled(db);
      const query = db.prepare("SELECT approval_id FROM purra_approvals WHERE scope=? AND sdk='typescript'"
        + (runId === undefined ? "" : " AND run_id=?") + " ORDER BY approval_id");
      const rows = runId === undefined ? query.all(scope) : query.all(scope, runId);
      const records = await Promise.all(rows.map(row => load(db, scope, String(row.approval_id))));
      return Object.freeze(records.filter(record => ["pending", "approved"].includes(record.status)));
    });
  }

  async #refresh(db: DatabaseSync, scope: string, all: StorageStores, extra: Record<string, any>, record: ApprovalRecord, now: number) {
    const state = await runState(db, scope, all, extra, record.intent);
    const status = state.canceled ? "canceled" : now >= Math.min(record.expiresAtMs, state.deadline ?? record.expiresAtMs) ? "expired" : undefined;
    if (status !== undefined && ["pending", "approved"].includes(record.status)) {
      record = await copyApprovalRecord({ ...record, status, revision: record.revision + 1 });
      save(db, scope, record);
    }
    return { record, fingerprint: state.fingerprint };
  }

  async refresh(approvalId: string): Promise<ApprovalRecord> {
    return this.access.write(async (db, scope, all, extra) =>
      (await this.#refresh(db, scope, all, extra, await load(db, scope, approvalId), integer(this.clock()))).record);
  }

  async decide(input: ApprovalDecisionCommand, options: { principalId: string }): Promise<ApprovalDecisionReceipt> {
    const command = copyApprovalDecisionCommand(input), principalId = options.principalId;
    text(principalId);
    const observed = await this.get(command.approvalId);
    let allowed: boolean;
    try { allowed = await this.authorize(principalId, observed, command); }
    catch { return fail("approval_authorization_failed"); }
    if (allowed !== true) fail("approval_authorization_denied");
    const result = await this.access.write(async (db, scope, all, extra) => {
      let record = await load(db, scope, command.approvalId);
      const audit = record.decisionAudit as ApprovalDecisionAudit;
      if (audit.command?.commandKey === command.commandKey) {
        if (audit.principalId !== principalId || await jsonIdentityDigest(audit.command) !== await jsonIdentityDigest(command)) fail("approval_command_conflict");
        return { receipt: receipt(record) };
      }
      if (record.revision !== command.expectedRevision || record.intentDigest !== command.intentDigest || record.status !== "pending") fail("approval_revision_conflict");
      const now = integer(this.clock());
      const refreshed = await this.#refresh(db, scope, all, extra, record, now);
      record = refreshed.record;
      if (["expired", "canceled"].includes(record.status)) return { error: "approval_" + record.status };
      if (refreshed.fingerprint !== record.intent.presetFingerprint) return { error: "approval_configuration_mismatch" };
      record = await copyApprovalRecord({ ...record, status: command.decision === "approve" ? "approved" : "rejected", revision: record.revision + 1,
        decisionAudit: { command, principalId, decidedAtMs: now, revision: record.revision + 1 } });
      save(db, scope, record);
      return { receipt: receipt(record) };
    });
    if (result.error) fail(result.error);
    return result.receipt!;
  }
}
