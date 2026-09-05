import { encodeStorageState, decodeStorageState, requireStorageFields } from "../shared/storage-state.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";
import { stableFingerprint } from "../shared/fingerprint.js";
import {
  artifactOwnerKey,
  copyArtifactClaimLeaseCommand,
  copyArtifactCreateCommand,
  copyArtifactFinalizeCommand,
  copyArtifactMaintenancePolicy,
  copyArtifactMutationLease,
  copyArtifactOwnerRef,
  copyArtifactWriteClaimCommand,
  nonNegativeInteger,
  prepareArtifactAppend,
  requiredText,
} from "./contracts.js";
import type {
  ArtifactAppendOperation,
  ArtifactBatch,
  ArtifactBatchReceipt,
  ArtifactClaimLeaseCommand,
  ArtifactClaimRepository,
  ArtifactCreateCommand,
  ArtifactFinalizeOperation,
  ArtifactMaintenancePolicy,
  ArtifactMaintenanceReport,
  ArtifactMaintenanceRepository,
  ArtifactMaintenanceSnapshot,
  ArtifactMutationLease,
  ArtifactRecord,
  ArtifactRepository,
  ArtifactWriteClaim,
  ArtifactWriteClaimCommand,
} from "./types.js";

interface ArtifactState {
  record: ArtifactRecord;
  readonly createFingerprint: string;
  readonly batches: ArtifactBatch[];
  readonly receipts: Map<string, {
    readonly contentDigest: string;
    readonly receipt: ArtifactBatchReceipt;
  }>;
  updatedAtMs: number;
}

export class InMemoryArtifactStore implements
  ArtifactRepository,
  ArtifactClaimRepository,
  ArtifactMaintenanceRepository {
  readonly #artifacts = new Map<string, ArtifactState>();
  readonly #owners = new Map<string, string>();
  readonly #claims = new Map<string, ArtifactWriteClaim>();
  readonly #clockMs: () => number;
  readonly #tokenFactory: () => string;
  readonly #runIsAvailable: (runId: string) => boolean;

  /** Opaque version-pinned storage data, never a public output projection. */
  public exportState(): string { return encodeStorageState("purra.artifact-state/v1", { artifacts: new Map([...this.#artifacts].map(([id, state]) => [id, { record: state.record, createFingerprint: state.createFingerprint, batches: state.batches, receipts: new Map([...state.receipts].map(([key, value]) => [key, { contentDigest: value.contentDigest, receipt: value.receipt }])), updatedAtMs: state.updatedAtMs }])), owners: this.#owners, claims: this.#claims }); }
  public importState(text: string): void {
    const shape = { artifacts: this.#artifacts, owners: this.#owners, claims: this.#claims };
    const saved = decodeStorageState(text, "purra.artifact-state/v1", shape) as typeof shape;
    for (const state of saved.artifacts.values()) {
      requireStorageFields(state, ["record", "createFingerprint", "batches", "receipts", "updatedAtMs"]);
      if (!(state.receipts instanceof Map) || !Array.isArray(state.batches)) throw new TypeError("Invalid stored Artifact");
      for (const value of state.receipts.values()) requireStorageFields(value, ["contentDigest", "receipt"]);
    }
    this.#artifacts.clear(); for (const [key, value] of saved.artifacts) this.#artifacts.set(key, value);
    this.#owners.clear(); for (const [key, value] of saved.owners) this.#owners.set(key, value);
    this.#claims.clear(); for (const [key, value] of saved.claims) this.#claims.set(key, value);
  }

  public constructor(options: {
    readonly clockMs?: () => number;
    readonly tokenFactory?: () => string;
    readonly runIsAvailable?: (runId: string) => boolean;
  } = {}) {
    this.#clockMs = options.clockMs ?? Date.now;
    this.#tokenFactory = options.tokenFactory ?? (() => globalThis.crypto.randomUUID());
    this.#runIsAvailable = options.runIsAvailable ?? (() => true);
  }

  public async create(
    artifactId: string,
    raw: ArtifactCreateCommand,
  ): Promise<ArtifactRecord> {
    const id = requiredText(artifactId, "Artifact id");
    const command = copyArtifactCreateCommand(raw);
    const createFingerprint = await stableFingerprint(copyJsonValue(command));
    const existing = this.#artifacts.get(id);
    if (existing !== undefined) {
      if (existing.createFingerprint === createFingerprint) return existing.record;
      throw new AgentError("artifact_id_conflict", "Artifact id conflicts");
    }
    const ownerKey = artifactOwnerKey(command);
    const ownedId = this.#owners.get(ownerKey);
    if (ownedId !== undefined) {
      const owned = this.#require(ownedId);
      if (owned.createFingerprint === createFingerprint) return owned.record;
      throw new AgentError("artifact_owner_conflict", "Artifact owner identity conflicts");
    }
    const record: ArtifactRecord = Object.freeze({
      id,
      namespace: command.namespace,
      kind: command.kind,
      ownerId: command.ownerId,
      ownerRef: command.ownerRef,
      createdByRunId: command.createdByRunId,
      schemaVersion: command.schemaVersion,
      status: "open",
      revision: 1,
      nextSequence: 1,
      committedItemCount: 0,
      expectedItemCount: command.expectedItemCount,
      metadata: command.metadata,
      resourceRef: null,
      coverageDigest: null,
    });
    this.#artifacts.set(id, {
      record,
      createFingerprint,
      batches: [],
      receipts: new Map(),
      updatedAtMs: this.#clockMs(),
    });
    this.#owners.set(ownerKey, id);
    return record;
  }

  public async load(artifactId: string): Promise<ArtifactRecord | undefined> {
    return this.#artifacts.get(requiredText(artifactId, "Artifact id"))?.record;
  }

  public async findForOwner(input: {
    readonly namespace: string;
    readonly kind: string;
    readonly ownerId: string;
    readonly ownerRef: import("./types.js").ArtifactOwnerRef;
  }): Promise<ArtifactRecord | undefined> {
    const artifactId = this.#owners.get(artifactOwnerKey({
      namespace: input.namespace,
      kind: input.kind,
      ownerId: input.ownerId,
      ownerRef: copyArtifactOwnerRef(input.ownerRef),
    }));
    return artifactId === undefined ? undefined : this.#require(artifactId).record;
  }

  public async replayReceipt(
    raw: ArtifactAppendOperation,
  ): Promise<ArtifactBatchReceipt | undefined> {
    const command = await validateAppendOperation(raw);
    return this.#replay(command);
  }

  public async append(raw: ArtifactAppendOperation): Promise<ArtifactBatchReceipt> {
    const command = await validateAppendOperation(raw);
    const replayed = this.#replay(command);
    if (replayed !== undefined) return replayed;
    const state = this.#require(command.artifactId);
    requireOpen(state.record);
    requireRevision(state.record, command.expectedRevision);
    if (state.record.nextSequence !== command.sequence) {
      throw new AgentError("artifact_sequence_conflict", "Artifact sequence conflicts");
    }
    const claim = this.#requireClaim(state.record, command.writeLease);
    const committedRevision = state.record.revision + 1;
    const batch: ArtifactBatch = Object.freeze({
      artifactId: state.record.id,
      batchId: command.batchId,
      idempotencyKey: command.idempotencyKey,
      sequence: command.sequence,
      committedRevision,
      items: command.items,
      coverageKeys: command.coverageKeys,
      contentDigest: command.contentDigest,
    });
    const receipt: ArtifactBatchReceipt = Object.freeze({
      artifactId: state.record.id,
      batchId: command.batchId,
      sequence: command.sequence,
      committedRevision,
      nextSequence: command.sequence + 1,
      acceptedCount: command.items.length,
      replayed: false,
    });
    state.batches.push(batch);
    state.receipts.set(command.idempotencyKey, {
      contentDigest: command.contentDigest,
      receipt,
    });
    state.record = Object.freeze({
      ...state.record,
      revision: committedRevision,
      nextSequence: command.sequence + 1,
      committedItemCount: state.record.committedItemCount + command.items.length,
    });
    this.#claims.set(state.record.id, Object.freeze({
      ...claim,
      acquiredRevision: committedRevision,
      expiresAtMs: this.#clockMs() + command.writeLease.leaseDurationMs,
    }));
    state.updatedAtMs = this.#clockMs();
    return receipt;
  }

  public async listBatches(artifactId: string): Promise<readonly ArtifactBatch[]> {
    return Object.freeze([...this.#require(artifactId).batches]);
  }

  public async finalize(
    raw: ArtifactFinalizeOperation,
    coverageDigest: string,
  ): Promise<ArtifactRecord> {
    const command = copyArtifactFinalizeCommand(raw);
    const state = this.#require(command.artifactId);
    requireOpen(state.record);
    requireRevision(state.record, command.expectedRevision);
    this.#requireClaim(state.record, command.writeLease);
    state.record = Object.freeze({
      ...state.record,
      status: "finalized",
      revision: state.record.revision + 1,
      resourceRef: command.resourceRef,
      coverageDigest: requiredText(coverageDigest, "Artifact coverage digest"),
    });
    this.#claims.delete(state.record.id);
    state.updatedAtMs = this.#clockMs();
    return state.record;
  }

  public async abort(input: {
    readonly artifactId: string;
    readonly expectedRevision: number;
    readonly writeLease: Required<ArtifactMutationLease>;
  }): Promise<ArtifactRecord> {
    const artifactId = requiredText(input.artifactId, "Artifact id");
    const writeLease = copyArtifactMutationLease(input.writeLease);
    const state = this.#require(artifactId);
    requireOpen(state.record);
    requireRevision(state.record, input.expectedRevision);
    this.#requireClaim(state.record, writeLease);
    state.record = Object.freeze({
      ...state.record,
      status: "aborted",
      revision: state.record.revision + 1,
    });
    this.#claims.delete(artifactId);
    state.updatedAtMs = this.#clockMs();
    return state.record;
  }

  public async acquire(raw: ArtifactWriteClaimCommand): Promise<ArtifactWriteClaim> {
    const command = copyArtifactWriteClaimCommand(raw);
    const state = this.#require(command.artifactId);
    requireOpen(state.record);
    requireRevision(state.record, command.expectedRevision);
    const current = this.#activeClaim(state.record.id);
    if (current !== undefined) {
      if (
        current.runId === command.runId
        && current.acquiredRevision === command.expectedRevision
      ) {
        return current;
      }
      throw new AgentError(
        "artifact_write_claim_conflict",
        "Artifact already has an active writer",
      );
    }
    const claim = Object.freeze({
      artifactId: state.record.id,
      runId: command.runId,
      claimToken: requiredText(this.#tokenFactory(), "Artifact claim token"),
      acquiredRevision: state.record.revision,
      expiresAtMs: this.#clockMs() + command.leaseDurationMs,
    });
    this.#claims.set(state.record.id, claim);
    return claim;
  }

  public async loadActive(artifactId: string): Promise<ArtifactWriteClaim | undefined> {
    return this.#activeClaim(requiredText(artifactId, "Artifact id"));
  }

  public async renew(
    raw: Required<ArtifactClaimLeaseCommand>,
  ): Promise<ArtifactWriteClaim> {
    const copied = copyArtifactClaimLeaseCommand(raw);
    if (copied.leaseDurationMs === undefined) {
      throw new TypeError("Artifact claim renewal requires a lease duration");
    }
    const state = this.#require(copied.artifactId);
    const claim = this.#requireClaim(state.record, {
      runId: copied.runId,
      claimToken: copied.claimToken,
      leaseDurationMs: copied.leaseDurationMs,
    });
    const renewed = Object.freeze({
      ...claim,
      expiresAtMs: this.#clockMs() + copied.leaseDurationMs,
    });
    this.#claims.set(state.record.id, renewed);
    return renewed;
  }

  public async release(raw: ArtifactClaimLeaseCommand): Promise<boolean> {
    const command = copyArtifactClaimLeaseCommand(raw);
    if (command.leaseDurationMs !== undefined) {
      throw new TypeError("Artifact claim release cannot include a lease duration");
    }
    const claim = this.#claims.get(command.artifactId);
    if (
      claim === undefined
      || claim.runId !== command.runId
      || claim.claimToken !== command.claimToken
    ) {
      return false;
    }
    this.#claims.delete(command.artifactId);
    return true;
  }

  public async releaseForRun(runId: string): Promise<number> {
    const normalized = requiredText(runId, "Artifact claim Run id");
    const targets = [...this.#claims]
      .filter(([, claim]) => claim.runId === normalized)
      .map(([artifactId]) => artifactId);
    for (const artifactId of targets) this.#claims.delete(artifactId);
    return targets.length;
  }

  public async maintain(
    rawPolicy: ArtifactMaintenancePolicy,
    timestampMs?: number,
  ): Promise<ArtifactMaintenanceReport> {
    const policy = copyArtifactMaintenancePolicy(rawPolicy);
    const now = timestampMs === undefined
      ? nonNegativeInteger(this.#clockMs(), "Artifact maintenance timestamp")
      : nonNegativeInteger(timestampMs, "Artifact maintenance timestamp");
    let expiredClaimsReleased = 0;
    let unavailableRunClaimsReleased = 0;
    let invalidTargetClaimsReleased = 0;
    for (const [artifactId, claim] of [...this.#claims]) {
      const artifact = this.#artifacts.get(artifactId)?.record;
      if (claim.expiresAtMs <= now) expiredClaimsReleased += 1;
      else if (!this.#runIsAvailable(claim.runId)) unavailableRunClaimsReleased += 1;
      else if (
        artifact === undefined
        || artifact.status !== "open"
        || artifact.revision !== claim.acquiredRevision
      ) {
        invalidTargetClaimsReleased += 1;
      } else {
        continue;
      }
      this.#claims.delete(artifactId);
    }
    let purgedArtifacts = 0;
    if (policy.terminalRetentionMs !== null) {
      const cutoff = now - policy.terminalRetentionMs;
      const candidates = [...this.#artifacts]
        .filter(([, state]) => state.record.status !== "open" && state.updatedAtMs <= cutoff)
        .sort((left, right) => (
          left[1].updatedAtMs - right[1].updatedAtMs
          || left[0].localeCompare(right[0])
        ))
        .slice(0, policy.maxPurgeArtifacts);
      for (const [artifactId] of candidates) {
        this.#purge(artifactId);
        purgedArtifacts += 1;
      }
    }
    return Object.freeze({
      expiredClaimsReleased,
      unavailableRunClaimsReleased,
      invalidTargetClaimsReleased,
      purgedArtifacts,
      consistencyIssues: 0,
    });
  }

  public async inspect(input: {
    readonly runId?: string;
    readonly timestampMs?: number;
  } = {}): Promise<ArtifactMaintenanceSnapshot> {
    const now = input.timestampMs === undefined
      ? nonNegativeInteger(this.#clockMs(), "Artifact inspection timestamp")
      : nonNegativeInteger(input.timestampMs, "Artifact inspection timestamp");
    const runId = input.runId === undefined ? null : requiredText(input.runId, "Artifact scope Run id");
    const artifacts = [...this.#artifacts.values()].map((state) => state.record)
      .filter((artifact) => (
        runId === null
        || artifact.createdByRunId === runId
        || this.#claims.get(artifact.id)?.runId === runId
      ));
    const claims = [...this.#claims.values()]
      .filter((claim) => runId === null || claim.runId === runId);
    const expiredClaims = claims.filter((claim) => claim.expiresAtMs <= now).length;
    const unavailableRunClaims = claims.filter((claim) => (
      claim.expiresAtMs > now && !this.#runIsAvailable(claim.runId)
    )).length;
    const invalidTargetClaims = claims.filter((claim) => {
      if (claim.expiresAtMs <= now || !this.#runIsAvailable(claim.runId)) return false;
      const artifact = this.#artifacts.get(claim.artifactId)?.record;
      return artifact === undefined
        || artifact.status !== "open"
        || artifact.revision !== claim.acquiredRevision;
    }).length;
    return Object.freeze({
      checkedAtMs: now,
      scopeRunId: runId,
      openArtifacts: artifacts.filter((item) => item.status === "open").length,
      finalizedArtifacts: artifacts.filter((item) => item.status === "finalized").length,
      abortedArtifacts: artifacts.filter((item) => item.status === "aborted").length,
      unknownArtifacts: 0,
      activeClaims: claims.length - expiredClaims - unavailableRunClaims - invalidTargetClaims,
      expiredClaims,
      unavailableRunClaims,
      invalidTargetClaims,
      consistencyIssues: 0,
    });
  }

  #require(artifactId: string): ArtifactState {
    const state = this.#artifacts.get(requiredText(artifactId, "Artifact id"));
    if (state === undefined) throw new AgentError("artifact_not_found", "Artifact does not exist");
    return state;
  }

  #activeClaim(artifactId: string): ArtifactWriteClaim | undefined {
    const claim = this.#claims.get(artifactId);
    return claim !== undefined && claim.expiresAtMs > this.#clockMs() ? claim : undefined;
  }

  #replay(command: ArtifactAppendOperation): ArtifactBatchReceipt | undefined {
    const replay = this.#artifacts.get(command.artifactId)?.receipts.get(command.idempotencyKey);
    if (replay === undefined) return undefined;
    if (replay.contentDigest !== command.contentDigest) {
      throw new AgentError("artifact_idempotency_conflict", "Artifact idempotency key conflicts");
    }
    return Object.freeze({ ...replay.receipt, replayed: true });
  }

  #requireClaim(
    artifact: ArtifactRecord,
    lease: Required<ArtifactMutationLease>,
  ): ArtifactWriteClaim {
    const claim = this.#activeClaim(artifact.id);
    if (
      claim === undefined
      || claim.runId !== lease.runId
      || claim.claimToken !== lease.claimToken
      || claim.acquiredRevision !== artifact.revision
    ) {
      throw new AgentError("artifact_write_claim_invalid", "Artifact write claim is invalid");
    }
    return claim;
  }

  #purge(artifactId: string): void {
    const state = this.#require(artifactId);
    this.#owners.delete(artifactOwnerKey(state.record));
    this.#artifacts.delete(artifactId);
    this.#claims.delete(artifactId);
  }
}

async function validateAppendOperation(
  raw: ArtifactAppendOperation,
): Promise<ArtifactAppendOperation> {
  const copied = await prepareArtifactAppend(raw);
  if (copied.contentDigest !== raw.contentDigest) {
    throw new AgentError("artifact_content_digest_invalid", "Artifact content digest is invalid");
  }
  return copied;
}

function requireOpen(artifact: ArtifactRecord): void {
  if (artifact.status !== "open") {
    throw new AgentError("artifact_not_open", "Artifact is not open");
  }
}

function requireRevision(artifact: ArtifactRecord, expectedRevision: number): void {
  if (artifact.revision !== expectedRevision) {
    throw new AgentError("artifact_revision_conflict", "Artifact revision conflicts");
  }
}
