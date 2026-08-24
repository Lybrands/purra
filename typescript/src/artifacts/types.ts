import type { JsonValue } from "../model/types.js";

export type ArtifactStatus = "open" | "finalized" | "aborted";
export type ArtifactAccessMode = "read" | "write";
export type ArtifactAccessReason =
  | "allowed"
  | "artifact_id_mismatch"
  | "artifact_aborted"
  | "artifact_finalized"
  | "artifact_revision_conflict"
  | "cross_run_not_authorized";

export interface ArtifactOwnerRef {
  readonly kind: string;
  readonly id: string;
}

export interface ArtifactCreateCommand {
  readonly namespace: string;
  readonly kind: string;
  readonly ownerId: string;
  readonly ownerRef: ArtifactOwnerRef;
  readonly createdByRunId: string;
  readonly schemaVersion?: number;
  readonly expectedItemCount?: number | null;
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface ArtifactRecord {
  readonly id: string;
  readonly namespace: string;
  readonly kind: string;
  readonly ownerId: string;
  readonly ownerRef: ArtifactOwnerRef;
  readonly createdByRunId: string;
  readonly schemaVersion: number;
  readonly status: ArtifactStatus;
  readonly revision: number;
  readonly nextSequence: number;
  readonly committedItemCount: number;
  readonly expectedItemCount: number | null;
  readonly metadata: Readonly<Record<string, JsonValue>>;
  readonly resourceRef: string | null;
  readonly coverageDigest: string | null;
}

export interface ArtifactMutationLease {
  readonly runId: string;
  readonly claimToken: string;
  readonly leaseDurationMs?: number;
}

export interface ArtifactAppendCommand {
  readonly artifactId: string;
  readonly expectedRevision: number;
  readonly sequence: number;
  readonly batchId: string;
  readonly idempotencyKey: string;
  readonly items: readonly Readonly<Record<string, JsonValue>>[];
  readonly writeLease: ArtifactMutationLease;
  readonly coverageKeys?: readonly string[];
}

export interface ArtifactAppendOperation extends ArtifactAppendCommand {
  readonly writeLease: Required<ArtifactMutationLease>;
  readonly coverageKeys: readonly string[];
  readonly contentDigest: string;
}

export interface ArtifactBatch {
  readonly artifactId: string;
  readonly batchId: string;
  readonly idempotencyKey: string;
  readonly sequence: number;
  readonly committedRevision: number;
  readonly items: readonly Readonly<Record<string, JsonValue>>[];
  readonly coverageKeys: readonly string[];
  readonly contentDigest: string;
}

export interface ArtifactBatchReceipt {
  readonly artifactId: string;
  readonly batchId: string;
  readonly sequence: number;
  readonly committedRevision: number;
  readonly nextSequence: number;
  readonly acceptedCount: number;
  readonly replayed: boolean;
}

export interface ArtifactFinalizeCommand {
  readonly artifactId: string;
  readonly expectedRevision: number;
  readonly writeLease: ArtifactMutationLease;
  readonly expectedItemCount?: number | null;
  readonly expectedCoverageKeys?: readonly string[];
  readonly resourceRef?: string | null;
}

export interface ArtifactFinalizeOperation extends ArtifactFinalizeCommand {
  readonly writeLease: Required<ArtifactMutationLease>;
  readonly expectedItemCount: number | null;
  readonly expectedCoverageKeys: readonly string[];
  readonly resourceRef: string | null;
}

export interface ArtifactValidationResult {
  readonly accepted: boolean;
  readonly code?: string;
  readonly details?: Readonly<Record<string, JsonValue>>;
}

export interface ArtifactValidator {
  validateBatch(
    artifact: ArtifactRecord,
    command: ArtifactAppendOperation,
  ): Promise<ArtifactValidationResult> | ArtifactValidationResult;
  validateFinalization(
    artifact: ArtifactRecord,
    batches: readonly ArtifactBatch[],
    command: ArtifactFinalizeOperation,
  ): Promise<ArtifactValidationResult> | ArtifactValidationResult;
}

export interface ArtifactResumeCandidate {
  readonly artifactId: string;
  readonly namespace: string;
  readonly kind: string;
  readonly ownerId: string;
  readonly ownerRef: ArtifactOwnerRef;
  readonly createdByRunId: string;
  readonly status: ArtifactStatus;
  readonly revision: number;
  readonly committedItemCount?: number;
  readonly expectedItemCount?: number | null;
}

export interface ArtifactAccessRequest {
  readonly artifactId: string;
  readonly runId: string;
  readonly mode: ArtifactAccessMode;
  readonly expectedRevision: number;
}

export interface ArtifactAccessDecision {
  readonly artifactId: string;
  readonly runId: string;
  readonly mode: ArtifactAccessMode;
  readonly allowed: boolean;
  readonly reason: ArtifactAccessReason;
  readonly artifactRevision: number;
  readonly requiresWriteClaim: boolean;
}

export interface ArtifactWriteClaimCommand {
  readonly artifactId: string;
  readonly runId: string;
  readonly expectedRevision: number;
  readonly leaseDurationMs: number;
}

export interface ArtifactWriteClaim {
  readonly artifactId: string;
  readonly runId: string;
  readonly claimToken: string;
  readonly acquiredRevision: number;
  readonly expiresAtMs: number;
}

export interface ArtifactClaimLeaseCommand {
  readonly artifactId: string;
  readonly runId: string;
  readonly claimToken: string;
  readonly leaseDurationMs?: number;
}

export interface ArtifactAccessGrant {
  readonly decision: ArtifactAccessDecision;
  readonly writeClaim?: ArtifactWriteClaim;
}

export interface ArtifactAccessAuthorizer {
  authorize(
    candidate: ArtifactResumeCandidate,
    request: ArtifactAccessRequest,
  ): Promise<boolean> | boolean;
}

export interface ArtifactRepository {
  create(artifactId: string, command: ArtifactCreateCommand): Promise<ArtifactRecord>;
  load(artifactId: string): Promise<ArtifactRecord | undefined>;
  findForOwner(input: {
    readonly namespace: string;
    readonly kind: string;
    readonly ownerId: string;
    readonly ownerRef: ArtifactOwnerRef;
  }): Promise<ArtifactRecord | undefined>;
  replayReceipt(command: ArtifactAppendOperation): Promise<ArtifactBatchReceipt | undefined>;
  append(command: ArtifactAppendOperation): Promise<ArtifactBatchReceipt>;
  listBatches(artifactId: string): Promise<readonly ArtifactBatch[]>;
  finalize(command: ArtifactFinalizeOperation, coverageDigest: string): Promise<ArtifactRecord>;
  abort(input: {
    readonly artifactId: string;
    readonly expectedRevision: number;
    readonly writeLease: Required<ArtifactMutationLease>;
  }): Promise<ArtifactRecord>;
}

export interface ArtifactClaimRepository {
  acquire(command: ArtifactWriteClaimCommand): Promise<ArtifactWriteClaim>;
  loadActive(artifactId: string): Promise<ArtifactWriteClaim | undefined>;
  renew(command: Required<ArtifactClaimLeaseCommand>): Promise<ArtifactWriteClaim>;
  release(command: ArtifactClaimLeaseCommand): Promise<boolean>;
  releaseForRun(runId: string): Promise<number>;
}

export interface ArtifactMaintenancePolicy {
  readonly terminalRetentionMs?: number | null;
  readonly maxPurgeArtifacts?: number;
}

export interface ArtifactMaintenanceReport {
  readonly expiredClaimsReleased: number;
  readonly unavailableRunClaimsReleased: number;
  readonly invalidTargetClaimsReleased: number;
  readonly purgedArtifacts: number;
  readonly consistencyIssues: number;
}

export interface ArtifactMaintenanceSnapshot {
  readonly checkedAtMs: number;
  readonly scopeRunId: string | null;
  readonly openArtifacts: number;
  readonly finalizedArtifacts: number;
  readonly abortedArtifacts: number;
  readonly unknownArtifacts: number;
  readonly activeClaims: number;
  readonly expiredClaims: number;
  readonly unavailableRunClaims: number;
  readonly invalidTargetClaims: number;
  readonly consistencyIssues: number;
}

export interface ArtifactMaintenanceRepository {
  maintain(
    policy: ArtifactMaintenancePolicy,
    timestampMs?: number,
  ): Promise<ArtifactMaintenanceReport>;
  inspect(input?: {
    readonly runId?: string;
    readonly timestampMs?: number;
  }): Promise<ArtifactMaintenanceSnapshot>;
}
