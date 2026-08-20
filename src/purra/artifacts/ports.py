"""Dependency-inversion ports for artifact persistence and domain validation."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from purra.artifacts.contracts import (
    ArtifactAppendCommand,
    ArtifactBatch,
    ArtifactBatchReceipt,
    ArtifactCreateCommand,
    ArtifactFinalizeCommand,
    ArtifactMutationLease,
    ArtifactRecord,
    ArtifactValidationResult,
)
from purra.artifacts.continuity import (
    ArtifactAccessRequest,
    ArtifactClaimLeaseCommand,
    ArtifactResumeCandidate,
    ArtifactWriteClaim,
    ArtifactWriteClaimCommand,
)
from purra.artifacts.maintenance import (
    ArtifactMaintenancePolicy,
    ArtifactMaintenanceReport,
    ArtifactMaintenanceSnapshot,
)
from purra.artifacts.ownership import ArtifactOwnerRef


@runtime_checkable
class ArtifactRepository(Protocol):
    async def create(
        self,
        artifact_id: str,
        command: ArtifactCreateCommand,
    ) -> ArtifactRecord: ...

    async def load(self, artifact_id: str) -> ArtifactRecord | None: ...

    async def find_for_owner(
        self,
        *,
        namespace: str,
        kind: str,
        owner_id: str,
        owner_ref: ArtifactOwnerRef,
    ) -> ArtifactRecord | None: ...

    async def replay_receipt(
        self,
        command: ArtifactAppendCommand,
    ) -> ArtifactBatchReceipt | None: ...

    async def append(
        self,
        command: ArtifactAppendCommand,
    ) -> ArtifactBatchReceipt: ...

    async def list_batches(self, artifact_id: str) -> Sequence[ArtifactBatch]: ...

    async def finalize(
        self,
        command: ArtifactFinalizeCommand,
        *,
        coverage_digest: str,
    ) -> ArtifactRecord: ...

    async def abort(
        self,
        artifact_id: str,
        *,
        expected_revision: int,
        write_lease: ArtifactMutationLease,
    ) -> ArtifactRecord: ...


@runtime_checkable
class ArtifactValidator(Protocol):
    async def validate_batch(
        self,
        artifact: ArtifactRecord,
        command: ArtifactAppendCommand,
    ) -> ArtifactValidationResult: ...

    async def validate_finalization(
        self,
        artifact: ArtifactRecord,
        batches: Sequence[ArtifactBatch],
        command: ArtifactFinalizeCommand,
    ) -> ArtifactValidationResult: ...


@runtime_checkable
class ArtifactClaimRepository(Protocol):
    """Atomic, exclusive writer claims for Artifacts.

    ``acquire`` must verify the expected Artifact revision and either return
    the one active claim for the same Run or reject a competing unexpired
    owner atomically. A durable claim must be returned even if cancellation
    arrives after commit; cancellation before commit must not leave a claim.
    """

    async def acquire(
        self,
        command: ArtifactWriteClaimCommand,
    ) -> ArtifactWriteClaim: ...

    async def load_active(
        self,
        artifact_id: str,
    ) -> ArtifactWriteClaim | None: ...

    async def renew(
        self,
        command: ArtifactClaimLeaseCommand,
    ) -> ArtifactWriteClaim: ...

    async def release(self, command: ArtifactClaimLeaseCommand) -> bool: ...

    async def release_for_run(self, run_id: str) -> int:
        """Release every live claim owned by a terminal/disconnected Run."""
        ...


@runtime_checkable
class ArtifactAccessAuthorizer(Protocol):
    """Host policy for granting Artifact access to a different Run."""

    async def authorize(
        self,
        candidate: ArtifactResumeCandidate,
        request: ArtifactAccessRequest,
    ) -> bool: ...


@runtime_checkable
class ArtifactMaintenanceRepository(Protocol):
    """Atomic storage adapter for lease cleanup and explicit retention GC."""

    async def maintain(
        self,
        policy: ArtifactMaintenancePolicy,
        *,
        timestamp_ms: int | None = None,
    ) -> ArtifactMaintenanceReport: ...

    async def inspect(
        self,
        *,
        run_id: str | None = None,
        timestamp_ms: int | None = None,
    ) -> ArtifactMaintenanceSnapshot: ...


__all__ = [
    "ArtifactAccessAuthorizer",
    "ArtifactClaimRepository",
    "ArtifactMaintenanceRepository",
    "ArtifactRepository",
    "ArtifactValidator",
]
