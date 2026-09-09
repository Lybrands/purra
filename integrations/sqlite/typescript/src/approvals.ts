import { wakeRecoverySchedule } from "./recovery-schedule.js";
import type { DatabaseSync } from "node:sqlite";
import { AgentError, ApprovalIntent, ApprovalRequired, copyToolExecutionCheckpoint, copyApprovalDecisionCommand, copyApprovalRecord, jsonIdentityDigest,
  type AgentToolExecutionCheckpoint, type ToolApprovalGateway, type ToolIdempotencyGateway, type ToolDispatchContext, type ApprovalRecord, type ApprovalDecisionCommand, type ApprovalDecisionAudit, type StorageStores } from "purra";
import { storageVersion } from "./approval-format.js";

type Operation<T> = (db: DatabaseSync, scope: string, all: StorageStores, extra: Record<string, any>) => Promise<T>;
export interface ApprovalStorageAccess {
  readonly bindRuntimeClock?: (clock: () => number) => void;
  readonly owner?: () => string | undefined;
  readonly epoch?: () => number | undefined;
  readonly idempotency?: ToolIdempotencyGateway;
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
  return decode(id, row);
}
async function decode(id: string, row: Record<string, unknown>): Promise<ApprovalRecord> {
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

/** Host-authorized decisions and atomic durable waits. Dispatch is revalidated separately. */
export class SqliteApprovalStore {
  constructor(private readonly access: ApprovalStorageAccess, private readonly authorize: ApprovalAuthorizer, private readonly clock: () => number = Date.now) {
    if (typeof authorize !== "function") throw new TypeError("Approval authorizer is required");
  }

  async create(intent: ApprovalIntent, options: { expiresAtMs: number }): Promise<ApprovalRecord> {
    if (!(intent instanceof ApprovalIntent)) throw new TypeError("Approval intent is required");
    const requestedExpiry = integer(options.expiresAtMs);
    return this.access.write((db, scope, all, extra) => this.#create(db, scope, all, extra, intent, requestedExpiry));
  }

  async #create(db: DatabaseSync, scope: string, all: StorageStores, extra: Record<string, any>, intent: ApprovalIntent, requestedExpiry: number): Promise<ApprovalRecord> {
    const id = await jsonIdentityDigest({ profile: "purra.approval-key/v1", runId: intent.value.runId, toolCallId: intent.value.toolCallId });
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
  }

  async prepare(checkpoint: AgentToolExecutionCheckpoint, intent: ApprovalIntent, options: { expiresAtMs: number }): Promise<void> {
    const copied = copyToolExecutionCheckpoint(checkpoint), expiry = integer(options.expiresAtMs);
    if (!(intent instanceof ApprovalIntent)) throw new TypeError("Approval intent is required");
    const call = copied.assistant.toolCalls![0]!;
    if (copied.runId !== intent.value.runId || call.id !== intent.value.toolCallId || call.name !== intent.value.toolName
      || await jsonIdentityDigest(call.arguments) !== await jsonIdentityDigest(intent.value.arguments)) fail("approval_intent_conflict");
    const outcome = await this.access.write(async (db, scope, all, extra) => {
      requireApprovalOwner(extra, copied.runId, this.access.owner?.(), this.access.epoch?.());
      let record = await this.#create(db, scope, all, extra, intent, expiry);
      record = (await this.#refresh(db, scope, all, extra, record, integer(this.clock()))).record;
      await all.runs.saveToolExecutionCheckpoint(copied.runId, copied);
      await all.runs.appendEvent(copied.runId, { sourceKey: `approval-required:${record.approvalId}`, kind: "approval.required",
        channel: "lifecycle", visibility: "private", payload: { approvalId: record.approvalId } });
      const completed = Object.values(extra.tools).some((entry: any) => entry.approvalId === record.approvalId && entry.intentDigest === record.intentDigest
          && entry.runId === record.intent.runId && entry.callId === record.intent.toolCallId && entry.state === "complete");
      return { record, completed };
    });
    if (outcome.completed) return;
    if (outcome.record.status === "pending") throw new ApprovalRequired(copied.runId, outcome.record.approvalId);
    if (outcome.record.status !== "approved") fail(`approval_${outcome.record.status}`);
  }

  gateway(): ToolApprovalGateway {
    if (this.access.idempotency === undefined || this.access.owner === undefined) fail("approval_runtime_required");
    this.access.bindRuntimeClock?.(this.clock);
    return Object.freeze({ requiresDurableIdempotency: true, idempotencyGateway: this.access.idempotency,
      request: async (request: Parameters<ToolApprovalGateway["request"]>[0]) => {
        if (request.dispatch === undefined) fail("approval_run_conflict");
        const outcome = await this.access.write((db, scope, all, extra) => checkApprovalDispatch(
          db, scope, all, extra, request.dispatch!, this.access.owner!(), integer(this.clock()), this.access.epoch?.()));
        if (outcome.error !== undefined) fail(outcome.error);
        return "approved" as const;
      },
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
      const query = db.prepare("SELECT approval_id,run_id,call_id,body FROM purra_approvals WHERE scope=? AND sdk='typescript'"
        + (runId === undefined ? "" : " AND run_id=?") + " ORDER BY approval_id");
      const rows = runId === undefined ? query.all(scope) : query.all(scope, runId);
      const records = await Promise.all(rows.map(row => decode(String(row.approval_id), row)));
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
      wakeRecoverySchedule(extra, record.intent.runId, true);
      return { receipt: receipt(record) };
    });
    if (result.error) fail(result.error);
    return result.receipt!;
  }
}

export function requireApprovalOwner(extra: Record<string, any>, runId: string, owner: string | undefined, epoch?: number) {
  const lease = extra.leases[runId];
  if (owner === undefined || lease?.owner !== owner || lease.expires <= Date.now()
    || !Number.isSafeInteger(lease.epoch) || lease.epoch < 1 || epoch !== undefined && epoch !== lease.epoch) fail("run_lease_lost");
  return lease;
}

export async function checkApprovalDispatch(db: DatabaseSync, scope: string, all: StorageStores, extra: Record<string, any>,
  dispatch: ToolDispatchContext, owner: string | undefined, now: number, epoch?: number) {
  enabled(db);
  requireApprovalOwner(extra, dispatch.runId, owner, epoch);
  const row = db.prepare("SELECT approval_id FROM purra_approvals WHERE scope=? AND sdk='typescript' AND run_id=? AND call_id=?")
    .get(scope, dispatch.runId, dispatch.call.id);
  if (!row) fail("approval_not_found");
  let record = await load(db, scope, String(row.approval_id));
  if (dispatch.approvalBinding !== undefined && Object.entries(dispatch.approvalBinding).some(([key, value]) =>
    record.intent[key as keyof typeof record.intent] !== value)) fail("approval_intent_conflict");
  const run = await all.runs.get(dispatch.runId), checkpoint = run.toolExecutionCheckpoint;
  if (checkpoint === undefined || await jsonIdentityDigest(checkpoint.assistant.toolCalls![0]!) !== await jsonIdentityDigest(dispatch.call)
    || record.intent.toolName !== dispatch.call.name
    || await jsonIdentityDigest(record.intent.arguments) !== await jsonIdentityDigest(dispatch.call.arguments)) fail("approval_intent_conflict");
  if (!all.runs.hasSettledToolInvocation(dispatch.runId, checkpoint.invocationId)) fail("approval_invocation_unsettled");
  const state = await runState(db, scope, all, extra, record.intent);
  if (state.canceled) return { record, error: "approval_canceled" };
  if (state.fingerprint !== record.intent.presetFingerprint) return { record, error: "approval_configuration_mismatch" };
  const completed = Object.values(extra.tools).some((entry: any) => entry.approvalId === record.approvalId && entry.intentDigest === record.intentDigest
          && entry.runId === record.intent.runId && entry.callId === record.intent.toolCallId && entry.state === "complete");
  if (completed) return { record };
  if (now >= Math.min(record.expiresAtMs, state.deadline ?? record.expiresAtMs) && ["pending", "approved"].includes(record.status)) {
    record = await copyApprovalRecord({ ...record, status: "expired", revision: record.revision + 1 });
    save(db, scope, record);
  }
  return { record, ...(record.status === "approved" ? {} : { error: `approval_${record.status}` }) };
}


/** Bind host reconciliation to the persisted approval; it grants no new dispatch. */
export async function checkApprovalReconciliation(db: DatabaseSync, scope: string, claim: Record<string, any>): Promise<void> {
  enabled(db);
  if (typeof claim.approvalId !== "string" || !claim.approvalId) fail("approval_claim_conflict");
  const row = db.prepare("SELECT approval_id,run_id,call_id,body FROM purra_approvals WHERE scope=? AND sdk='typescript' AND approval_id=?")
    .get(scope, claim.approvalId);
  if (!row || row.run_id !== claim.runId || row.call_id !== claim.callId) fail("approval_claim_conflict");
  const record = await decode(String(row.approval_id), row);
  const audit = record.decisionAudit as Partial<ApprovalDecisionAudit>;
  if (claim.state !== "claimed" || claim.intentDigest !== record.intentDigest
    || claim.approvalRevision !== audit.revision || audit.command?.decision !== "approve") fail("approval_claim_conflict");
}

/** Snapshot-only observations. Never refreshes approval state or acquires ownership. */
export async function inspectApprovalState(db: DatabaseSync, scope: string, saved: Awaited<ReturnType<StorageStores["runs"]["get"]>>, extra: Record<string, any>, now: number) {
  if (storageVersion(db) !== 5) return {};
  const rows = db.prepare("SELECT approval_id FROM purra_approvals WHERE scope=? AND sdk='typescript' AND run_id=?").all(scope, saved.runId);
  const records = await Promise.all(rows.map(row => load(db, scope, String(row.approval_id))));
  const unknown = Object.values(extra.tools).filter((entry: any) => entry.state === "claimed" && entry.runId === saved.runId
    && records.some(record => record.approvalId === entry.approvalId && record.intentDigest === entry.intentDigest && record.intent.toolCallId === entry.callId)).length;
  const observation: import("purra").RecoveryObservations = {approvalState: records.length ? "unknown" : "none", approvalRecords: records.length,
    approvalUnknownReceipts: unknown, approvalCheckpointIntent: "unknown", approvalReceipt: "unknown"};
  const call = saved.toolExecutionCheckpoint?.assistant.toolCalls?.[0];
  if (call === undefined) return observation;
  const record = records.find(record => record.intent.toolCallId === call.id);
  if (record === undefined) return {...observation, approvalState: "missing" as const, approvalReceipt: "absent" as const};
  const matches = record.intent.toolName === call.name && await jsonIdentityDigest(record.intent.arguments) === await jsonIdentityDigest(call.arguments)
    && record.intent.presetFingerprint === await jsonIdentityDigest(saved.preset);
  const complete = matches && Object.values(extra.tools).some((entry: any) => entry.state === "complete" && entry.runId === saved.runId
    && entry.callId === call.id && entry.approvalId === record.approvalId && entry.intentDigest === record.intentDigest
    && ["not_started", "committed"].includes(entry.result?.effectState)
    && entry.approvalRevision === record.decisionAudit?.revision && typeof entry.leaseOwnerId === "string" && entry.leaseOwnerId.length > 0
    && Number.isSafeInteger(entry.leaseEpoch) && entry.leaseEpoch > 0);
  let state = record.status;
  if (state === "pending" || state === "approved") {
    if (saved.status !== "running") state = "canceled";
    else if (now >= Math.min(record.expiresAtMs, saved.deadlineAt === null ? record.expiresAtMs : Date.parse(saved.deadlineAt))) state = "expired";
  }
  return {...observation, approvalState: state, approvalCheckpointIntent: matches ? "matched" as const : "mismatch" as const,
    approvalReceipt: complete ? "complete" as const : "absent" as const};
}
