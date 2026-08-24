import { AgentError } from "../shared/errors.js";
import {
  copyArtifactAccessRequest,
  copyArtifactClaimLeaseCommand,
  copyArtifactResumeCandidate,
} from "./contracts.js";
import type {
  ArtifactAccessAuthorizer,
  ArtifactAccessDecision,
  ArtifactAccessGrant,
  ArtifactAccessReason,
  ArtifactAccessRequest,
  ArtifactClaimLeaseCommand,
  ArtifactClaimRepository,
  ArtifactResumeCandidate,
  ArtifactWriteClaim,
} from "./types.js";

export class ArtifactAccessPolicy {
  public decide(
    rawCandidate: ArtifactResumeCandidate,
    rawRequest: ArtifactAccessRequest,
    crossRunAuthorized = false,
  ): ArtifactAccessDecision {
    const candidate = copyArtifactResumeCandidate(rawCandidate);
    const request = copyArtifactAccessRequest(rawRequest);
    const reason = denialReason(candidate, request, crossRunAuthorized);
    const allowed = reason === undefined;
    return Object.freeze({
      artifactId: request.artifactId,
      runId: request.runId,
      mode: request.mode,
      allowed,
      reason: reason ?? "allowed",
      artifactRevision: candidate.revision,
      requiresWriteClaim: allowed && request.mode === "write",
    });
  }
}

export class ArtifactAccessController {
  readonly #claims: ArtifactClaimRepository;
  readonly #authorizer: ArtifactAccessAuthorizer | undefined;
  readonly #policy: ArtifactAccessPolicy;

  public constructor(
    claims: ArtifactClaimRepository,
    options: {
      readonly authorizer?: ArtifactAccessAuthorizer;
      readonly policy?: ArtifactAccessPolicy;
    } = {},
  ) {
    if (
      typeof claims?.acquire !== "function"
      || typeof claims.renew !== "function"
      || typeof claims.release !== "function"
    ) {
      throw new TypeError("Artifact access requires a claim repository");
    }
    if (
      options.authorizer !== undefined
      && typeof options.authorizer.authorize !== "function"
    ) {
      throw new TypeError("Artifact authorizer must implement authorize");
    }
    this.#claims = claims;
    this.#authorizer = options.authorizer;
    this.#policy = options.policy ?? new ArtifactAccessPolicy();
  }

  public async authorize(
    rawCandidate: ArtifactResumeCandidate,
    rawRequest: ArtifactAccessRequest,
    leaseDurationMs?: number,
  ): Promise<ArtifactAccessGrant> {
    const candidate = copyArtifactResumeCandidate(rawCandidate);
    const request = copyArtifactAccessRequest(rawRequest);
    const crossRunAuthorized = request.runId !== candidate.createdByRunId
      && this.#authorizer !== undefined
      && await this.#authorizer.authorize(candidate, request) === true;
    const decision = this.#policy.decide(candidate, request, crossRunAuthorized);
    if (!decision.allowed) {
      throw new AgentError(decision.reason, "Artifact access was denied");
    }
    if (!decision.requiresWriteClaim) {
      if (leaseDurationMs !== undefined) {
        throw new TypeError("Artifact read access cannot include a write lease");
      }
      return Object.freeze({ decision });
    }
    if (leaseDurationMs === undefined) {
      throw new TypeError("Artifact write access requires a lease duration");
    }
    const claim = await this.#claims.acquire({
      artifactId: candidate.artifactId,
      runId: request.runId,
      expectedRevision: request.expectedRevision,
      leaseDurationMs,
    });
    validateClaim(claim, decision);
    return Object.freeze({ decision, writeClaim: claim });
  }

  public async renew(raw: ArtifactClaimLeaseCommand): Promise<ArtifactWriteClaim> {
    const command = copyArtifactClaimLeaseCommand(raw);
    if (command.leaseDurationMs === undefined) {
      throw new TypeError("Artifact claim renewal requires a lease duration");
    }
    return this.#claims.renew({
      ...command,
      leaseDurationMs: command.leaseDurationMs,
    });
  }

  public async release(raw: ArtifactClaimLeaseCommand): Promise<boolean> {
    const command = copyArtifactClaimLeaseCommand(raw);
    if (command.leaseDurationMs !== undefined) {
      throw new TypeError("Artifact claim release cannot include a lease duration");
    }
    return this.#claims.release(command);
  }
}

function denialReason(
  candidate: Required<ArtifactResumeCandidate>,
  request: ArtifactAccessRequest,
  crossRunAuthorized: boolean,
): ArtifactAccessReason | undefined {
  if (candidate.artifactId !== request.artifactId) return "artifact_id_mismatch";
  if (candidate.createdByRunId !== request.runId && !crossRunAuthorized) {
    return "cross_run_not_authorized";
  }
  if (candidate.status === "aborted") return "artifact_aborted";
  if (candidate.revision !== request.expectedRevision) return "artifact_revision_conflict";
  if (request.mode === "write" && candidate.status === "finalized") {
    return "artifact_finalized";
  }
  return undefined;
}

function validateClaim(
  claim: ArtifactWriteClaim,
  decision: ArtifactAccessDecision,
): void {
  if (
    claim?.artifactId !== decision.artifactId
    || claim.runId !== decision.runId
    || claim.acquiredRevision !== decision.artifactRevision
    || typeof claim.claimToken !== "string"
    || claim.claimToken.trim() === ""
  ) {
    throw new TypeError("Artifact claim repository returned a mismatched claim");
  }
}
