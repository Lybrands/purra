"""Core-owned state machine for recoverable artifact commits."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence

from purra.artifacts.contracts import (
    ArtifactAppendCommand,
    ArtifactBatch,
    ArtifactBatchReceipt,
    ArtifactCreateCommand,
    ArtifactFinalizeCommand,
    ArtifactMutationLease,
    ArtifactRecord,
    ArtifactStatus,
    ArtifactValidationResult,
    coverage_digest,
)
from purra.artifacts.errors import (
    ArtifactConflictError,
    ArtifactNotFoundError,
    ArtifactStateError,
    ArtifactValidationError,
)
from purra.artifacts.ports import ArtifactRepository, ArtifactValidator


class ArtifactLifecycle:
    """Coordinate validation while repositories guarantee atomic CAS writes."""

    def __init__(
        self,
        repository: ArtifactRepository,
        *,
        validator: ArtifactValidator | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._repository = repository
        self._validator = validator
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)

    async def begin(self, command: ArtifactCreateCommand) -> ArtifactRecord:
        artifact_id = str(self._id_factory() or "").strip()
        if not artifact_id:
            raise RuntimeError("artifact id factory returned an empty id")
        return await self._repository.create(artifact_id, command)

    async def get(self, artifact_id: str) -> ArtifactRecord:
        """Load an artifact or raise the same typed error as mutations."""

        return await self._require(artifact_id)

    async def append(
        self,
        command: ArtifactAppendCommand,
    ) -> ArtifactBatchReceipt:
        replayed = await self._repository.replay_receipt(command)
        if replayed is not None:
            return replayed
        artifact = await self._require(command.artifact_id)
        _require_open(artifact)
        if artifact.revision != command.expected_revision:
            raise ArtifactConflictError(
                "artifact revision does not match",
                code="artifact_revision_conflict",
                details={
                    "expectedRevision": command.expected_revision,
                    "actualRevision": artifact.revision,
                },
            )
        if artifact.next_sequence != command.sequence:
            raise ArtifactConflictError(
                "artifact batch sequence is not next",
                code="artifact_sequence_conflict",
                details={
                    "expectedSequence": artifact.next_sequence,
                    "actualSequence": command.sequence,
                },
            )
        duplicate_coverage = _duplicates(command.coverage_keys)
        if duplicate_coverage:
            raise ArtifactValidationError(
                "artifact batch contains duplicate coverage keys",
                code="artifact_coverage_duplicate",
                details={"duplicateKeys": sorted(duplicate_coverage)},
            )
        committed_coverage = {
            key
            for batch in await self._repository.list_batches(artifact.id)
            for key in batch.coverage_keys
        }
        repeated_coverage = committed_coverage.intersection(
            command.coverage_keys
        )
        if repeated_coverage:
            raise ArtifactValidationError(
                "artifact batch repeats committed coverage keys",
                code="artifact_coverage_duplicate",
                details={"duplicateKeys": sorted(repeated_coverage)},
            )
        await self._validate_batch(artifact, command)
        return await self._repository.append(command)

    async def finalize(
        self,
        command: ArtifactFinalizeCommand,
    ) -> ArtifactRecord:
        artifact = await self._require(command.artifact_id)
        _require_open(artifact)
        if artifact.revision != command.expected_revision:
            raise ArtifactConflictError(
                "artifact revision does not match",
                code="artifact_revision_conflict",
                details={
                    "expectedRevision": command.expected_revision,
                    "actualRevision": artifact.revision,
                },
            )
        expected_count = (
            command.expected_item_count
            if command.expected_item_count is not None
            else artifact.expected_item_count
        )
        if (
            artifact.expected_item_count is not None
            and command.expected_item_count is not None
            and artifact.expected_item_count != command.expected_item_count
        ):
            raise ArtifactValidationError(
                "final item count conflicts with the artifact manifest",
                code="artifact_manifest_count_mismatch",
                details={
                    "manifestCount": artifact.expected_item_count,
                    "finalizeCount": command.expected_item_count,
                },
            )
        if (
            expected_count is not None
            and artifact.committed_item_count != expected_count
        ):
            raise ArtifactValidationError(
                "artifact item count is incomplete",
                code="artifact_item_count_incomplete",
                details={
                    "expectedCount": expected_count,
                    "actualCount": artifact.committed_item_count,
                },
            )
        batches = tuple(await self._repository.list_batches(artifact.id))
        _validate_batch_sequence(batches)
        observed_coverage = tuple(
            key
            for batch in batches
            for key in batch.coverage_keys
        )
        duplicates = _duplicates(observed_coverage)
        if duplicates:
            raise ArtifactValidationError(
                "artifact coverage contains duplicate keys",
                code="artifact_coverage_duplicate",
                details={"duplicateKeys": sorted(duplicates)},
            )
        expected_coverage = set(command.expected_coverage_keys)
        observed_coverage_set = set(observed_coverage)
        if expected_coverage and observed_coverage_set != expected_coverage:
            raise ArtifactValidationError(
                "artifact coverage does not match the final manifest",
                code="artifact_coverage_mismatch",
                details={
                    "missingKeys": sorted(expected_coverage - observed_coverage_set),
                    "unexpectedKeys": sorted(
                        observed_coverage_set - expected_coverage
                    ),
                },
            )
        await self._validate_finalization(artifact, batches, command)
        return await self._repository.finalize(
            command,
            coverage_digest=coverage_digest(tuple(observed_coverage)),
        )

    async def abort(
        self,
        artifact_id: str,
        *,
        expected_revision: int,
        write_lease: ArtifactMutationLease,
    ) -> ArtifactRecord:
        if not isinstance(write_lease, ArtifactMutationLease):
            raise TypeError("artifact abort write_lease is invalid")
        artifact = await self._require(artifact_id)
        _require_open(artifact)
        if artifact.revision != int(expected_revision):
            raise ArtifactConflictError(
                "artifact revision does not match",
                code="artifact_revision_conflict",
                details={
                    "expectedRevision": int(expected_revision),
                    "actualRevision": artifact.revision,
                },
            )
        return await self._repository.abort(
            artifact.id,
            expected_revision=artifact.revision,
            write_lease=write_lease,
        )

    async def _require(self, artifact_id: str) -> ArtifactRecord:
        normalized = str(artifact_id or "").strip()
        artifact = await self._repository.load(normalized)
        if artifact is None:
            raise ArtifactNotFoundError(
                "artifact does not exist",
                code="artifact_not_found",
                details={"artifactId": normalized},
            )
        return artifact

    async def _validate_batch(
        self,
        artifact: ArtifactRecord,
        command: ArtifactAppendCommand,
    ) -> None:
        if self._validator is None:
            return
        result = await self._validator.validate_batch(artifact, command)
        _require_validation_result(result)

    async def _validate_finalization(
        self,
        artifact: ArtifactRecord,
        batches: Sequence[ArtifactBatch],
        command: ArtifactFinalizeCommand,
    ) -> None:
        if self._validator is None:
            return
        result = await self._validator.validate_finalization(
            artifact,
            batches,
            command,
        )
        _require_validation_result(result)


def _require_open(artifact: ArtifactRecord) -> None:
    if artifact.status is not ArtifactStatus.OPEN:
        raise ArtifactStateError(
            "artifact is not open",
            code="artifact_not_open",
            details={"status": artifact.status.value},
        )


def _require_validation_result(result: ArtifactValidationResult) -> None:
    if not isinstance(result, ArtifactValidationResult):
        raise TypeError("artifact validator returned an invalid result")
    if not result.accepted:
        raise ArtifactValidationError(
            "artifact domain validation rejected the operation",
            code=result.code or "artifact_validation_failed",
            details=result.details,
        )


def _validate_batch_sequence(batches: Sequence[ArtifactBatch]) -> None:
    observed = tuple(batch.sequence for batch in batches)
    expected = tuple(range(1, len(batches) + 1))
    if observed != expected:
        raise ArtifactValidationError(
            "artifact batch sequence contains a gap or reordering",
            code="artifact_batch_sequence_invalid",
            details={"expected": expected, "actual": observed},
        )


def _duplicates(values: Sequence[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return duplicates


__all__ = ["ArtifactLifecycle"]
