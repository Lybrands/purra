/** Immutable approval data; no record or digest is a dispatch permit. */
import { AgentError } from "./shared/errors.js";
import { StructuredOutputContract, jsonIdentityDigest } from "./structured.js";
import { copyJsonValue } from "./model/validation.js";
import type { JsonValue } from "./model/types.js";

export const APPROVAL_INTENT_PROFILE = "purra.approval-intent/v1";
export type ApprovalStatus = "pending" | "approved" | "rejected" | "expired" | "canceled";
export interface ApprovalIntentValue {
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly rootRunId: string;
  readonly toolCallId: string;
  readonly toolName: string;
  readonly arguments: Readonly<Record<string, JsonValue>>;
  readonly presetFingerprint: string;
  readonly bindingId: string;
  readonly bindingRevision: string;
  readonly scopeId: string;
  readonly scopeRevision: string;
  readonly effect: "write" | "destructive";
}
const names = ["runId", "rootRunId", "toolCallId", "toolName", "presetFingerprint", "bindingId", "bindingRevision", "scopeId", "scopeRevision", "effect"] as const;
function text(value: unknown): asserts value is string {
  if (typeof value !== "string" || !value || value.trim() !== value || [...value].length > 1024) throw new TypeError("Invalid approval text");
}
function integer(value: unknown, minimum = 0): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum) throw new TypeError("Invalid approval integer");
}
function keys(value: unknown, expected: readonly string[]): asserts value is Record<string, any> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("Invalid approval fields");
  const actual = Object.keys(value).sort(), sorted = [...expected].sort();
  if (actual.length !== sorted.length || actual.some((key, index) => key !== sorted[index])) throw new TypeError("Invalid approval fields");
}

export class ApprovalIntent {
  private constructor(readonly value: ApprovalIntentValue, readonly digest: string) { Object.freeze(this); }
  static async create(input: ApprovalIntentValue): Promise<ApprovalIntent> {
    keys(input, ["schemaVersion", "arguments", ...names]);
    if (input.schemaVersion !== 1) throw new TypeError("Invalid approval intent version");
    for (const name of names) text(input[name]);
    if (!["write", "destructive"].includes(input.effect)) throw new TypeError("Invalid approval effect");
    // Snapshot before the first await; caller mutations cannot change the hashed intent.
    const copied = copyJsonValue(input as unknown as JsonValue) as unknown as ApprovalIntentValue;
    const contract = await StructuredOutputContract.create({ schemaId: "purra.approval-arguments", schemaVersion: "1", schema: { type: "object" } });
    contract.validateValue(copied.arguments);
    return new ApprovalIntent(copied, await jsonIdentityDigest({ profile: APPROVAL_INTENT_PROFILE, intent: copied }));
  }
}

export interface ApprovalDecisionCommand {
  readonly approvalId: string;
  readonly expectedRevision: number;
  readonly intentDigest: string;
  readonly commandKey: string;
  readonly decision: "approve" | "reject";
}
export function copyApprovalDecisionCommand(value: ApprovalDecisionCommand): ApprovalDecisionCommand {
  keys(value, ["approvalId", "expectedRevision", "intentDigest", "commandKey", "decision"]);
  text(value.approvalId); text(value.intentDigest); text(value.commandKey); integer(value.expectedRevision, 1);
  if (!["approve", "reject"].includes(value.decision)) throw new TypeError("Invalid approval decision");
  return Object.freeze({ ...value });
}
export interface ApprovalDecisionAudit {
  readonly command: ApprovalDecisionCommand;
  readonly principalId: string;
  readonly decidedAtMs: number;
  readonly revision: number;
}
export interface ApprovalRecord {
  readonly approvalId: string;
  readonly intent: ApprovalIntentValue;
  readonly intentDigest: string;
  readonly revision: number;
  readonly status: ApprovalStatus;
  readonly createdAtMs: number;
  readonly expiresAtMs: number;
  readonly decisionAudit: ApprovalDecisionAudit | Readonly<Record<string, never>>;
}
export async function copyApprovalRecord(input: ApprovalRecord): Promise<ApprovalRecord> {
  keys(input, ["approvalId", "intent", "intentDigest", "revision", "status", "createdAtMs", "expiresAtMs", "decisionAudit"]);
  const value = copyJsonValue(input as unknown as JsonValue) as unknown as ApprovalRecord;
  text(value.approvalId); integer(value.revision, 1); integer(value.createdAtMs); integer(value.expiresAtMs, value.createdAtMs + 1);
  if (!["pending", "approved", "rejected", "expired", "canceled"].includes(value.status)) throw new TypeError("Invalid approval status");
  const intent = await ApprovalIntent.create(value.intent);
  if (intent.digest !== value.intentDigest) throw new TypeError("Conflicting approval digest");
  const audit = value.decisionAudit;
  if (Object.keys(audit).length) {
    keys(audit, ["command", "principalId", "decidedAtMs", "revision"]);
    const command = copyApprovalDecisionCommand(audit.command);
    text(audit.principalId); integer(audit.decidedAtMs, value.createdAtMs); integer(audit.revision, 2);
    if (command.approvalId !== value.approvalId || command.intentDigest !== value.intentDigest
      || command.expectedRevision + 1 !== audit.revision || audit.revision > value.revision
      || audit.decidedAtMs >= value.expiresAtMs || value.status === "pending"
      || ["approved", "rejected"].includes(value.status) && value.status !== (command.decision === "approve" ? "approved" : "rejected")) throw new TypeError("Conflicting approval audit");
  } else {
    keys(audit, []);
    if (["approved", "rejected"].includes(value.status)) throw new TypeError("Approval decision audit is required");
  }
  return value;
}

/** Durable wait control flow, distinct from tool failure and user clarification. */
export class ApprovalRequired extends AgentError {
  constructor(readonly runId: string, readonly approvalId: string) {
    super("approval_required", "Run is waiting for approval");
    this.name = "ApprovalRequired";
  }
}
