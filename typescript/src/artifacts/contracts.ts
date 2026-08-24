import type { JsonValue } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";
import { stableFingerprint } from "../shared/fingerprint.js";
import type {
  ArtifactAccessRequest,
  ArtifactAppendCommand,
  ArtifactAppendOperation,
  ArtifactClaimLeaseCommand,
  ArtifactCreateCommand,
  ArtifactFinalizeCommand,
  ArtifactFinalizeOperation,
  ArtifactMaintenancePolicy,
  ArtifactMutationLease,
  ArtifactOwnerRef,
  ArtifactResumeCandidate,
  ArtifactValidationResult,
  ArtifactWriteClaimCommand,
} from "./types.js";

export function copyArtifactOwnerRef(value: ArtifactOwnerRef): ArtifactOwnerRef {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Artifact ownerRef must be an object");
  }
  return Object.freeze({
    kind: requiredText(value.kind, "Artifact owner kind"),
    id: requiredText(value.id, "Artifact owner id"),
  });
}

export function copyArtifactCreateCommand(value: ArtifactCreateCommand): Required<ArtifactCreateCommand> {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Artifact create command must be an object");
  }
  return Object.freeze({
    namespace: requiredText(value.namespace, "Artifact namespace"),
    kind: requiredText(value.kind, "Artifact kind"),
    ownerId: requiredText(value.ownerId, "Artifact ownerId"),
    ownerRef: copyArtifactOwnerRef(value.ownerRef),
    createdByRunId: requiredText(value.createdByRunId, "Artifact createdByRunId"),
    schemaVersion: positiveInteger(value.schemaVersion ?? 1, "Artifact schemaVersion"),
    expectedItemCount: nullableNonNegative(value.expectedItemCount ?? null, "Artifact expectedItemCount"),
    metadata: copyMapping(value.metadata ?? {}, "Artifact metadata"),
  });
}

export function copyArtifactMutationLease(
  value: ArtifactMutationLease,
): Required<ArtifactMutationLease> {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Artifact mutation lease must be an object");
  }
  return Object.freeze({
    runId: requiredText(value.runId, "Artifact mutation Run id"),
    claimToken: requiredText(value.claimToken, "Artifact mutation claim token"),
    leaseDurationMs: positiveInteger(
      value.leaseDurationMs ?? 300_000,
      "Artifact mutation lease duration",
    ),
  });
}

export async function prepareArtifactAppend(
  value: ArtifactAppendCommand,
): Promise<ArtifactAppendOperation> {
  if (value === null || typeof value !== "object" || !Array.isArray(value.items)) {
    throw new TypeError("Artifact append command must contain items");
  }
  if (value.items.length === 0) throw new AgentError("artifact_batch_empty", "Artifact batch is empty");
  const artifactId = requiredText(value.artifactId, "Artifact id");
  const sequence = positiveInteger(value.sequence, "Artifact sequence");
  const batchId = requiredText(value.batchId, "Artifact batch id");
  const items = Object.freeze(value.items.map((item) => (
    copyMapping(item, "Artifact batch item")
  )));
  const coverageKeys = copyTextList(value.coverageKeys ?? [], "Artifact coverage key", false);
  const operation = {
    artifactId,
    expectedRevision: positiveInteger(value.expectedRevision, "Artifact expected revision"),
    sequence,
    batchId,
    idempotencyKey: requiredText(value.idempotencyKey, "Artifact idempotency key"),
    items,
    writeLease: copyArtifactMutationLease(value.writeLease),
    coverageKeys,
  };
  const contentDigest = await stableFingerprint(copyJsonValue({
    artifactId,
    sequence,
    batchId,
    items,
    coverageKeys,
  }));
  return Object.freeze({ ...operation, contentDigest });
}

export function copyArtifactFinalizeCommand(
  value: ArtifactFinalizeCommand,
): ArtifactFinalizeOperation {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Artifact finalize command must be an object");
  }
  return Object.freeze({
    artifactId: requiredText(value.artifactId, "Artifact id"),
    expectedRevision: positiveInteger(value.expectedRevision, "Artifact expected revision"),
    writeLease: copyArtifactMutationLease(value.writeLease),
    expectedItemCount: nullableNonNegative(
      value.expectedItemCount ?? null,
      "Artifact expected item count",
    ),
    expectedCoverageKeys: copyTextList(
      value.expectedCoverageKeys ?? [],
      "Artifact expected coverage key",
      true,
    ),
    resourceRef: optionalText(value.resourceRef ?? null),
  });
}

export function copyArtifactResumeCandidate(
  value: ArtifactResumeCandidate,
): Required<ArtifactResumeCandidate> {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Artifact resume candidate must be an object");
  }
  const status = artifactStatus(value.status);
  const committedItemCount = nonNegativeInteger(
    value.committedItemCount ?? 0,
    "Artifact committed item count",
  );
  const expectedItemCount = nullableNonNegative(
    value.expectedItemCount ?? null,
    "Artifact expected item count",
  );
  if (expectedItemCount !== null && committedItemCount > expectedItemCount) {
    throw new TypeError("Artifact committed item count exceeds its manifest");
  }
  return Object.freeze({
    artifactId: requiredText(value.artifactId, "Artifact candidate id"),
    namespace: requiredText(value.namespace, "Artifact candidate namespace"),
    kind: requiredText(value.kind, "Artifact candidate kind"),
    ownerId: requiredText(value.ownerId, "Artifact candidate ownerId"),
    ownerRef: copyArtifactOwnerRef(value.ownerRef),
    createdByRunId: requiredText(value.createdByRunId, "Artifact candidate createdByRunId"),
    status,
    revision: positiveInteger(value.revision, "Artifact candidate revision"),
    committedItemCount,
    expectedItemCount,
  });
}

export function copyArtifactAccessRequest(value: ArtifactAccessRequest): ArtifactAccessRequest {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Artifact access request must be an object");
  }
  if (value.mode !== "read" && value.mode !== "write") {
    throw new TypeError("Artifact access mode must be read or write");
  }
  return Object.freeze({
    artifactId: requiredText(value.artifactId, "Artifact access id"),
    runId: requiredText(value.runId, "Artifact access Run id"),
    mode: value.mode,
    expectedRevision: positiveInteger(value.expectedRevision, "Artifact access revision"),
  });
}

export function copyArtifactWriteClaimCommand(
  value: ArtifactWriteClaimCommand,
): ArtifactWriteClaimCommand {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Artifact write claim command must be an object");
  }
  return Object.freeze({
    artifactId: requiredText(value.artifactId, "Artifact claim id"),
    runId: requiredText(value.runId, "Artifact claim Run id"),
    expectedRevision: positiveInteger(value.expectedRevision, "Artifact claim revision"),
    leaseDurationMs: positiveInteger(value.leaseDurationMs, "Artifact claim lease duration"),
  });
}

export function copyArtifactClaimLeaseCommand(
  value: ArtifactClaimLeaseCommand,
): ArtifactClaimLeaseCommand {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Artifact claim lease command must be an object");
  }
  return Object.freeze({
    artifactId: requiredText(value.artifactId, "Artifact claim id"),
    runId: requiredText(value.runId, "Artifact claim Run id"),
    claimToken: requiredText(value.claimToken, "Artifact claim token"),
    ...(value.leaseDurationMs === undefined
      ? {}
      : { leaseDurationMs: positiveInteger(value.leaseDurationMs, "Artifact claim lease duration") }),
  });
}

export function copyArtifactMaintenancePolicy(
  value: ArtifactMaintenancePolicy = {},
): Required<ArtifactMaintenancePolicy> {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Artifact maintenance policy must be an object");
  }
  return Object.freeze({
    terminalRetentionMs: nullableNonNegative(
      value.terminalRetentionMs ?? null,
      "Artifact terminal retention",
    ),
    maxPurgeArtifacts: positiveInteger(
      value.maxPurgeArtifacts ?? 100,
      "Artifact maximum purge count",
    ),
  });
}

export function copyArtifactValidationResult(
  value: ArtifactValidationResult,
): Required<ArtifactValidationResult> {
  if (value === null || typeof value !== "object" || typeof value.accepted !== "boolean") {
    throw new TypeError("Artifact validator returned an invalid result");
  }
  const code = value.code === undefined ? "" : requiredText(value.code, "Artifact validation code");
  if (!value.accepted && code === "") {
    throw new TypeError("Rejected Artifact validation requires a code");
  }
  return Object.freeze({
    accepted: value.accepted,
    code,
    details: copyMapping(value.details ?? {}, "Artifact validation details"),
  });
}

export async function artifactCoverageDigest(keys: readonly string[]): Promise<string> {
  return stableFingerprint(Object.freeze([...new Set(keys)].sort()));
}

export function artifactOwnerKey(input: {
  readonly namespace: string;
  readonly kind: string;
  readonly ownerId: string;
  readonly ownerRef: ArtifactOwnerRef;
}): string {
  const ownerRef = copyArtifactOwnerRef(input.ownerRef);
  return [
    requiredText(input.namespace, "Artifact namespace"),
    requiredText(input.kind, "Artifact kind"),
    requiredText(input.ownerId, "Artifact ownerId"),
    ownerRef.kind,
    ownerRef.id,
  ].join("\u0000");
}

export function artifactStatus(value: unknown): "open" | "finalized" | "aborted" {
  if (value !== "open" && value !== "finalized" && value !== "aborted") {
    throw new TypeError("Artifact status is invalid");
  }
  return value;
}

export function copyMapping(
  value: Readonly<Record<string, JsonValue>>,
  label: string,
): Readonly<Record<string, JsonValue>> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  return copyJsonValue(value) as Readonly<Record<string, JsonValue>>;
}

export function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`${label} is required`);
  return value.trim();
}

export function optionalText(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  return requiredText(value, "Optional text");
}

export function positiveInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${label} must be positive`);
  return value;
}

export function nonNegativeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${label} must be non-negative`);
  return value;
}

function nullableNonNegative(value: number | null, label: string): number | null {
  return value === null ? null : nonNegativeInteger(value, label);
}

function copyTextList(
  value: readonly string[],
  label: string,
  unique: boolean,
): readonly string[] {
  if (!Array.isArray(value)) throw new TypeError(`${label}s must be an array`);
  const copied = value.map((item) => requiredText(item, label));
  if (unique && new Set(copied).size !== copied.length) {
    throw new TypeError(`${label}s must be unique`);
  }
  return Object.freeze(copied);
}
