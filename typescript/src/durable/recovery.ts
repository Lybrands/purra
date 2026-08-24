import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";
import type { AgentPresetSnapshot, RunBudgets, RunSnapshot } from "../run/types.js";
import type {
  DurableContinuation,
  DurableRecoveryPayload,
  DurableRecoverySnapshot,
  LongTaskDispatchReceipt,
  RecoveryAuthenticator,
} from "./types.js";

export class HmacRecoveryAuthenticator implements RecoveryAuthenticator {
  readonly #key: Promise<CryptoKey>;

  public constructor(secret: string | Uint8Array) {
    const bytes = typeof secret === "string" ? new TextEncoder().encode(secret) : new Uint8Array(secret);
    if (bytes.byteLength < 32) throw new TypeError("Recovery HMAC secret must contain at least 32 bytes");
    this.#key = globalThis.crypto.subtle.importKey(
      "raw",
      toArrayBuffer(bytes),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign", "verify"],
    );
  }

  public async sign(payload: DurableRecoveryPayload): Promise<string> {
    const signature = await globalThis.crypto.subtle.sign(
      "HMAC",
      await this.#key,
      new TextEncoder().encode(canonicalJson(payload)),
    );
    return bytesToHex(new Uint8Array(signature));
  }

  public async verify(payload: DurableRecoveryPayload, proof: string): Promise<boolean> {
    if (!/^[a-f0-9]{64}$/.test(proof)) return false;
    return globalThis.crypto.subtle.verify(
      "HMAC",
      await this.#key,
      toArrayBuffer(hexToBytes(proof)),
      new TextEncoder().encode(canonicalJson(payload)),
    );
  }
}

export async function createRecoverySnapshot(input: {
  readonly run: RunSnapshot;
  readonly plan: import("../planning/types.js").ExecutionPlan;
  readonly receipt: LongTaskDispatchReceipt;
  readonly authenticator: RecoveryAuthenticator;
}): Promise<DurableRecoverySnapshot> {
  const payload: DurableRecoveryPayload = Object.freeze({
    schemaVersion: 1,
    sourceRunId: input.run.runId,
    plan: input.plan,
    receipt: input.receipt,
    preset: input.run.preset,
    deadlineAt: input.run.deadlineAt,
    remainingBudgets: remainingBudgets(input.run),
  });
  const authorityProof = await input.authenticator.sign(payload);
  if (typeof authorityProof !== "string" || authorityProof.trim() === "") {
    throw new AgentError("durable_recovery_authentication_failed", "Recovery signer returned no proof");
  }
  return Object.freeze({ ...payload, authorityProof });
}

export async function validateContinuation(input: {
  readonly continuation: DurableContinuation;
  readonly currentPreset: AgentPresetSnapshot;
  readonly authenticator: RecoveryAuthenticator;
  readonly nowMs?: number;
}): Promise<DurableRecoverySnapshot> {
  const continuation = input.continuation;
  if (continuation === null || typeof continuation !== "object") {
    throw new AgentError("durable_continuation_invalid", "Durable continuation is invalid");
  }
  if (typeof continuation.command !== "string" || continuation.command.trim() === "") {
    throw new AgentError("durable_continuation_invalid", "Durable continuation command is required");
  }
  const snapshot = continuation.snapshot;
  if (snapshot?.schemaVersion !== 1) {
    throw new AgentError("durable_recovery_snapshot_unsupported", "Recovery snapshot is unsupported");
  }
  if (snapshot.preset.schemaVersion !== 2 || input.currentPreset.schemaVersion !== 2) {
    throw new AgentError("agent_preset_snapshot_unsupported", "Durable continuation requires preset snapshot v2");
  }
  if (canonicalJson(snapshot.preset) !== canonicalJson(input.currentPreset)) {
    throw new AgentError("agent_preset_snapshot_mismatch", "Durable continuation preset does not match");
  }
  if (snapshot.deadlineAt !== null && Date.parse(snapshot.deadlineAt) <= (input.nowMs ?? Date.now())) {
    throw new AgentError("run_deadline_exceeded", "Durable continuation deadline has elapsed");
  }
  const payload: DurableRecoveryPayload = Object.freeze({
    schemaVersion: 1,
    sourceRunId: requiredText(snapshot.sourceRunId, "source Run id"),
    plan: snapshot.plan,
    receipt: snapshot.receipt,
    preset: snapshot.preset,
    deadlineAt: snapshot.deadlineAt,
    remainingBudgets: copyBudgets(snapshot.remainingBudgets),
  });
  if (!await input.authenticator.verify(payload, snapshot.authorityProof)) {
    throw new AgentError(
      "durable_recovery_authentication_failed",
      "Durable continuation authority proof is invalid",
    );
  }
  if (snapshot.receipt.recipeFingerprint !== snapshot.receipt.metadata.recipeFingerprint
      && snapshot.receipt.metadata.recipeFingerprint !== undefined) {
    throw new AgentError("durable_recipe_mismatch", "Recovery receipt recipe fingerprint is inconsistent");
  }
  return Object.freeze({ ...payload, authorityProof: snapshot.authorityProof });
}

function remainingBudgets(run: RunSnapshot): RunBudgets {
  return Object.freeze({
    maxModelAttempts: remaining(run.budgets.maxModelAttempts, run.usage.modelAttempts),
    maxTotalTokens: remaining(run.budgets.maxTotalTokens, run.usage.knownTokens),
    maxOutputBytes: remaining(run.budgets.maxOutputBytes, run.usage.outputBytes),
    maxOutputEvents: remaining(run.budgets.maxOutputEvents, run.usage.outputEvents),
  });
}

function remaining(maximum: number | null, used: number): number | null {
  if (maximum === null) return null;
  const value = maximum - used;
  if (value < 1) {
    throw new AgentError("run_budget_exhausted", "Durable continuation has no remaining allowance");
  }
  return value;
}

function copyBudgets(value: RunBudgets): RunBudgets {
  if (value === null || typeof value !== "object") throw new TypeError("Recovery budgets are invalid");
  return Object.freeze({
    maxModelAttempts: nullablePositive(value.maxModelAttempts, "maxModelAttempts"),
    maxTotalTokens: nullablePositive(value.maxTotalTokens, "maxTotalTokens"),
    maxOutputBytes: nullablePositive(value.maxOutputBytes, "maxOutputBytes"),
    maxOutputEvents: nullablePositive(value.maxOutputEvents, "maxOutputEvents"),
  });
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(sortValue(copyJsonValue(value as import("../model/types.js").JsonValue)));
}

function sortValue(value: import("../model/types.js").JsonValue): import("../model/types.js").JsonValue {
  if (Array.isArray(value)) return value.map(sortValue);
  if (value !== null && typeof value === "object") {
    const record = value as Readonly<Record<string, import("../model/types.js").JsonValue>>;
    return Object.fromEntries(Object.keys(record).sort().map((key) => [key, sortValue(record[key]!)]));
  }
  return value;
}

function bytesToHex(bytes: Uint8Array): string {
  return [...bytes].map((value) => value.toString(16).padStart(2, "0")).join("");
}

function hexToBytes(value: string): Uint8Array {
  return new Uint8Array(value.match(/.{2}/g)!.map((byte) => Number.parseInt(byte, 16)));
}

function toArrayBuffer(value: Uint8Array): ArrayBuffer {
  const buffer = new ArrayBuffer(value.byteLength);
  new Uint8Array(buffer).set(value);
  return buffer;
}

function nullablePositive(value: number | null, label: string): number | null {
  if (value === null) return null;
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${label} must be positive`);
  return value;
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`${label} is required`);
  return value.trim();
}
