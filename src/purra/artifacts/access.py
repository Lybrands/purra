"""Core coordinator for Artifact authorization and exclusive write claims."""

from __future__ import annotations

from purra.artifacts.continuity import (
    ArtifactAccessGrant,
    ArtifactAccessMode,
    ArtifactAccessPolicy,
    ArtifactAccessReason,
    ArtifactAccessRequest,
    ArtifactClaimLeaseCommand,
    ArtifactResumeCandidate,
    ArtifactWriteClaim,
    ArtifactWriteClaimCommand,
)
from purra.artifacts.errors import ArtifactAccessDeniedError
from purra.artifacts.ports import ArtifactAccessAuthorizer, ArtifactClaimRepository


class ArtifactAccessController:
    """Authorize access and atomically claim every writer."""

    def __init__(
        self,
        claim_repository: ArtifactClaimRepository,
        *,
        authorizer: ArtifactAccessAuthorizer | None = None,
        policy: ArtifactAccessPolicy | None = None,
    ) -> None:
        self._claim_repository = claim_repository
        self._authorizer = authorizer
        self._policy = policy or ArtifactAccessPolicy()

    async def authorize(
        self,
        candidate: ArtifactResumeCandidate,
        request: ArtifactAccessRequest,
        *,
        lease_duration_ms: int | None = None,
    ) -> ArtifactAccessGrant:
        cross_run_authorized = bool(
            request.run_id != candidate.created_by_run_id
            and self._authorizer is not None
            and await self._authorizer.authorize(candidate, request)
        )
        decision = self._policy.decide(
            candidate,
            request,
            cross_run_authorized=cross_run_authorized,
        )
        if not decision.allowed:
            self._raise_denied(candidate, request, decision.reason)
        claim: ArtifactWriteClaim | None = None
        if decision.requires_write_claim:
            if lease_duration_ms is None:
                raise ValueError(
                    "artifact write access requires lease_duration_ms"
                )
            claim = await self._claim_repository.acquire(ArtifactWriteClaimCommand(
                artifact_id=candidate.artifact_id,
                run_id=request.run_id,
                expected_revision=request.expected_revision,
                lease_duration_ms=lease_duration_ms,
            ))
            if (
                claim.artifact_id != candidate.artifact_id
                or claim.run_id != request.run_id
                or claim.acquired_revision != request.expected_revision
            ):
                raise TypeError(
                    "artifact claim repository returned a mismatched claim"
                )
        elif lease_duration_ms is not None:
            raise ValueError("this artifact access does not require a write lease")
        return ArtifactAccessGrant(decision=decision, write_claim=claim)

    @staticmethod
    def _raise_denied(
        candidate: ArtifactResumeCandidate,
        request: ArtifactAccessRequest,
        reason: ArtifactAccessReason,
    ) -> None:
        raise ArtifactAccessDeniedError(
            "artifact access was denied",
            code=reason.value,
            details={
                "artifactId": request.artifact_id,
                "runId": request.run_id,
                "mode": request.mode.value,
                "artifactRevision": candidate.revision,
            },
        )

    async def renew(
        self,
        command: ArtifactClaimLeaseCommand,
    ) -> ArtifactWriteClaim:
        if command.lease_duration_ms is None:
            raise ValueError("renew requires lease_duration_ms")
        return await self._claim_repository.renew(command)

    async def release(self, command: ArtifactClaimLeaseCommand) -> bool:
        if command.lease_duration_ms is not None:
            raise ValueError("release cannot include lease_duration_ms")
        return await self._claim_repository.release(command)


__all__ = ["ArtifactAccessController"]
