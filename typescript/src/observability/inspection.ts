import type { RunSnapshot } from "../run/types.js";

export interface RecoveryObservations {
  readonly status?: "running" | "terminal" | "unknown";
  readonly checkpoint?: "present" | "missing" | "unknown";
  readonly lease?: "active" | "inactive" | "unknown";
  readonly configuration?: "matched" | "mismatch" | "unknown";
  readonly permissions?: "allowed" | "denied" | "unknown";
  readonly usage?: "recorded" | "unknown";
  readonly receiptScope?: "run" | "storage" | "unknown";
  readonly cancellation?: "requested" | "clear" | "unknown";
  readonly deadline?: "expired" | "open" | "unknown";
  readonly attemptsAfterCheckpoint?: number | null;
  readonly unknownToolReceipts?: number | null;
}

/** Pure normalization of evidence supplied by a host/storage observer; never a resume permit. */
export function buildRecoveryInspection(state: RecoveryObservations) {
  const enums = {
    status: ["running", "terminal", "unknown"], checkpoint: ["present", "missing", "unknown"],
    lease: ["active", "inactive", "unknown"], configuration: ["matched", "mismatch", "unknown"],
    permissions: ["allowed", "denied", "unknown"], usage: ["recorded", "unknown"],
    receiptScope: ["run", "storage", "unknown"], cancellation: ["requested", "clear", "unknown"],
    deadline: ["expired", "open", "unknown"],
  } as const;
  const observed: Record<string, string | number | null> = {};
  for (const key of Object.keys(enums) as (keyof typeof enums)[]) {
    const value = state[key] === undefined ? "unknown" : state[key];
    if (!(enums[key] as readonly unknown[]).includes(value)) throw new TypeError("invalid recovery observation");
    observed[key] = value;
  }
  for (const key of ["attemptsAfterCheckpoint", "unknownToolReceipts"] as const) {
    const value = state[key] ?? null;
    if (value !== null && (!Number.isSafeInteger(value) || value < 0)) throw new TypeError("invalid recovery count");
    observed[key] = value;
  }
  const blockers: string[] = [], cautions: string[] = [], unknown: string[] = [], actions: string[] = [];
  const rules = [
    ["status", "terminal", "run_terminal", "inspect_run"],
    ["checkpoint", "missing", "checkpoint_missing", "inspect_run"],
    ["lease", "active", "run_lease_conflict", "wait_for_lease"],
    ["configuration", "mismatch", "configuration_mismatch", "verify_configuration"],
    ["permissions", "denied", "permission_denied", "verify_permissions"],
    ["cancellation", "requested", "run_canceled", "inspect_run"],
    ["deadline", "expired", "run_deadline_exceeded", "inspect_run"],
  ] as const;
  for (const [key, blocked, code, action] of rules) {
    if (observed[key] === blocked) { blockers.push(code); actions.push(action); }
  }
  if (typeof observed.attemptsAfterCheckpoint === "number" && observed.attemptsAfterCheckpoint > 0) {
    blockers.push("run_recovery_requires_reconciliation"); actions.push("reconcile_attempt");
  }
  if (typeof observed.unknownToolReceipts === "number" && observed.unknownToolReceipts > 0) {
    if (observed.receiptScope === "run") blockers.push("tool_effect_unknown");
    else cautions.push("unattributed_tool_effect_unknown");
    actions.push("reconcile_tools");
  }
  for (const [key, value] of Object.entries(observed)) if (value === null || value === "unknown") unknown.push(key);
  if (observed.receiptScope !== "run") unknown.push("runToolEffects");
  unknown.push("externalToolEffects", "agentTreeOwnership");
  for (const [key, action] of [["configuration", "verify_configuration"], ["permissions", "verify_permissions"], ["usage", "verify_usage"]] as const) {
    if (observed[key] === "unknown") actions.push(action);
  }
  actions.push("revalidate_execution");
  return Object.freeze({ schemaVersion: 1 as const, authority: "diagnosis_only" as const,
    observations: Object.freeze(observed), blockers: Object.freeze(blockers), cautions: Object.freeze(cautions), unknown: Object.freeze(unknown),
    suggestedActions: Object.freeze([...new Set(actions)]) });
}

export type RecoveryInspection = ReturnType<typeof buildRecoveryInspection>;

/** Only calls get; missing execution proofs remain unknown. */
export async function inspectRecovery(repository: { get(runId: string): Promise<RunSnapshot> }, runId: string): Promise<RecoveryInspection> {
  const saved = await repository.get(runId);
  return buildRecoveryInspection({ status: saved.status === "running" ? "running" : "terminal",
    checkpoint: saved.executionCheckpoint === undefined ? "missing" : "present" });
}
