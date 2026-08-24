import { AgentError } from "../shared/errors.js";
import {
  artifactCoverageDigest,
  copyArtifactCreateCommand,
  copyArtifactFinalizeCommand,
  copyArtifactMutationLease,
  copyArtifactValidationResult,
  positiveInteger,
  prepareArtifactAppend,
  requiredText,
} from "./contracts.js";
import type {
  ArtifactAppendCommand,
  ArtifactBatch,
  ArtifactBatchReceipt,
  ArtifactCreateCommand,
  ArtifactFinalizeCommand,
  ArtifactRecord,
  ArtifactRepository,
  ArtifactValidator,
} from "./types.js";

export class ArtifactLifecycle {
  readonly #repository: ArtifactRepository;
  readonly #validator: ArtifactValidator | undefined;
  readonly #idFactory: () => string;

  public constructor(options: {
    readonly repository: ArtifactRepository;
    readonly validator?: ArtifactValidator;
    readonly idFactory?: () => string;
  }) {
    if (
      typeof options.repository?.create !== "function"
      || typeof options.repository.append !== "function"
      || typeof options.repository.finalize !== "function"
    ) {
      throw new TypeError("Artifact lifecycle requires a repository");
    }
    if (
      options.validator !== undefined
      && (
        typeof options.validator.validateBatch !== "function"
        || typeof options.validator.validateFinalization !== "function"
      )
    ) {
      throw new TypeError("Artifact validator is incomplete");
    }
    this.#repository = options.repository;
    this.#validator = options.validator;
    this.#idFactory = options.idFactory ?? (() => globalThis.crypto.randomUUID());
  }

  public async begin(raw: ArtifactCreateCommand): Promise<ArtifactRecord> {
    const command = copyArtifactCreateCommand(raw);
    const artifactId = requiredText(this.#idFactory(), "Artifact id");
    return this.#repository.create(artifactId, command);
  }

  public get(artifactId: string): Promise<ArtifactRecord> {
    return this.#require(artifactId);
  }

  public async append(raw: ArtifactAppendCommand): Promise<ArtifactBatchReceipt> {
    const command = await prepareArtifactAppend(raw);
    const replayed = await this.#repository.replayReceipt(command);
    if (replayed !== undefined) return replayed;
    const artifact = await this.#require(command.artifactId);
    requireOpen(artifact);
    requireRevision(artifact, command.expectedRevision);
    if (artifact.nextSequence !== command.sequence) {
      throw new AgentError("artifact_sequence_conflict", "Artifact batch is not next");
    }
    const duplicateInBatch = duplicates(command.coverageKeys);
    if (duplicateInBatch.length > 0) {
      throw new AgentError("artifact_coverage_duplicate", "Artifact batch repeats coverage keys");
    }
    const batches = await this.#repository.listBatches(artifact.id);
    const committedCoverage = new Set(batches.flatMap((batch) => batch.coverageKeys));
    if (command.coverageKeys.some((key) => committedCoverage.has(key))) {
      throw new AgentError("artifact_coverage_duplicate", "Artifact coverage was already committed");
    }
    await this.#validateBatch(artifact, command);
    return this.#repository.append(command);
  }

  public async finalize(raw: ArtifactFinalizeCommand): Promise<ArtifactRecord> {
    const command = copyArtifactFinalizeCommand(raw);
    const artifact = await this.#require(command.artifactId);
    requireOpen(artifact);
    requireRevision(artifact, command.expectedRevision);
    if (
      artifact.expectedItemCount !== null
      && command.expectedItemCount !== null
      && artifact.expectedItemCount !== command.expectedItemCount
    ) {
      throw new AgentError(
        "artifact_manifest_count_mismatch",
        "Artifact final item count conflicts with its manifest",
      );
    }
    const expectedCount = command.expectedItemCount ?? artifact.expectedItemCount;
    if (expectedCount !== null && artifact.committedItemCount !== expectedCount) {
      throw new AgentError("artifact_item_count_incomplete", "Artifact item count is incomplete");
    }
    const batches = await this.#repository.listBatches(artifact.id);
    validateSequence(batches);
    const coverage = batches.flatMap((batch) => batch.coverageKeys);
    if (duplicates(coverage).length > 0) {
      throw new AgentError("artifact_coverage_duplicate", "Artifact coverage contains duplicates");
    }
    if (command.expectedCoverageKeys.length > 0) {
      const expected = new Set(command.expectedCoverageKeys);
      const observed = new Set(coverage);
      if (
        expected.size !== observed.size
        || [...expected].some((key) => !observed.has(key))
      ) {
        throw new AgentError("artifact_coverage_mismatch", "Artifact coverage is incomplete");
      }
    }
    await this.#validateFinalization(artifact, batches, command);
    return this.#repository.finalize(command, await artifactCoverageDigest(coverage));
  }

  public async abort(input: {
    readonly artifactId: string;
    readonly expectedRevision: number;
    readonly writeLease: import("./types.js").ArtifactMutationLease;
  }): Promise<ArtifactRecord> {
    const artifactId = requiredText(input.artifactId, "Artifact id");
    const expectedRevision = positiveInteger(input.expectedRevision, "Artifact expected revision");
    const writeLease = copyArtifactMutationLease(input.writeLease);
    const artifact = await this.#require(artifactId);
    requireOpen(artifact);
    requireRevision(artifact, expectedRevision);
    return this.#repository.abort({ artifactId, expectedRevision, writeLease });
  }

  async #require(artifactId: string): Promise<ArtifactRecord> {
    const normalized = requiredText(artifactId, "Artifact id");
    const artifact = await this.#repository.load(normalized);
    if (artifact === undefined) throw new AgentError("artifact_not_found", "Artifact does not exist");
    return artifact;
  }

  async #validateBatch(
    artifact: ArtifactRecord,
    command: import("./types.js").ArtifactAppendOperation,
  ): Promise<void> {
    if (this.#validator === undefined) return;
    const result = copyArtifactValidationResult(
      await this.#validator.validateBatch(artifact, command),
    );
    if (!result.accepted) {
      throw new AgentError(result.code, "Artifact batch validation failed");
    }
  }

  async #validateFinalization(
    artifact: ArtifactRecord,
    batches: readonly ArtifactBatch[],
    command: import("./types.js").ArtifactFinalizeOperation,
  ): Promise<void> {
    if (this.#validator === undefined) return;
    const result = copyArtifactValidationResult(
      await this.#validator.validateFinalization(artifact, batches, command),
    );
    if (!result.accepted) {
      throw new AgentError(result.code, "Artifact finalization validation failed");
    }
  }
}

function requireOpen(artifact: ArtifactRecord): void {
  if (artifact.status !== "open") {
    throw new AgentError("artifact_not_open", "Artifact is not open");
  }
}

function requireRevision(artifact: ArtifactRecord, expected: number): void {
  if (artifact.revision !== expected) {
    throw new AgentError("artifact_revision_conflict", "Artifact revision conflicts");
  }
}

function validateSequence(batches: readonly ArtifactBatch[]): void {
  if (batches.some((batch, index) => batch.sequence !== index + 1)) {
    throw new AgentError("artifact_batch_sequence_invalid", "Artifact batch sequence is invalid");
  }
}

function duplicates(values: readonly string[]): readonly string[] {
  const seen = new Set<string>();
  const repeated = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) repeated.add(value);
    seen.add(value);
  }
  return Object.freeze([...repeated].sort());
}
