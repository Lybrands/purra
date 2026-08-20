"""Content-free contracts and policy for Artifact access."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from purra.artifacts.contracts import ArtifactStatus
from purra.artifacts.ownership import ArtifactOwnerRef
from purra.normalization import (
    non_negative_int,
    optional_non_negative_int,
    optional_positive_int,
    positive_int,
    required_text,
)


class ArtifactAccessMode(StrEnum):
    READ = "read"
    WRITE = "write"


class ArtifactAccessReason(StrEnum):
    ALLOWED = "allowed"
    ARTIFACT_ID_MISMATCH = "artifact_id_mismatch"
    ARTIFACT_ABORTED = "artifact_aborted"
    ARTIFACT_FINALIZED = "artifact_finalized"
    ARTIFACT_REVISION_CONFLICT = "artifact_revision_conflict"
    CROSS_RUN_NOT_AUTHORIZED = "cross_run_not_authorized"


@dataclass(frozen=True, slots=True)
class ArtifactResumeCandidate:
    """Content-free view used to authorize a possible continuation."""

    artifact_id: str
    namespace: str
    kind: str
    owner_id: str
    owner_ref: ArtifactOwnerRef
    created_by_run_id: str
    status: ArtifactStatus
    revision: int
    committed_item_count: int = 0
    expected_item_count: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "artifact_id",
            "namespace",
            "kind",
            "owner_id",
            "created_by_run_id",
        ):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"artifact candidate {name}"
            ))
        if not isinstance(self.owner_ref, ArtifactOwnerRef):
            raise TypeError("artifact candidate owner_ref is invalid")
        object.__setattr__(self, "status", ArtifactStatus(self.status))
        object.__setattr__(self, "revision", positive_int(
            self.revision, "artifact candidate revision"
        ))
        object.__setattr__(self, "committed_item_count", non_negative_int(
            self.committed_item_count, "committed_item_count"
        ))
        object.__setattr__(self, "expected_item_count", optional_non_negative_int(
            self.expected_item_count, "expected_item_count"
        ))
        if (
            self.expected_item_count is not None
            and self.committed_item_count > self.expected_item_count
        ):
            raise ValueError(
                "committed_item_count cannot exceed expected_item_count"
            )


@dataclass(frozen=True, slots=True)
class ArtifactAccessRequest:
    artifact_id: str
    run_id: str
    mode: ArtifactAccessMode
    expected_revision: int

    def __post_init__(self) -> None:
        for name in ("artifact_id", "run_id"):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"artifact access {name}"
            ))
        object.__setattr__(self, "mode", ArtifactAccessMode(self.mode))
        object.__setattr__(self, "expected_revision", positive_int(
            self.expected_revision, "expected_revision"
        ))


@dataclass(frozen=True, slots=True)
class ArtifactAccessDecision:
    artifact_id: str
    run_id: str
    mode: ArtifactAccessMode
    allowed: bool
    reason: ArtifactAccessReason
    artifact_revision: int
    requires_write_claim: bool = False

    def __post_init__(self) -> None:
        for name in ("artifact_id", "run_id"):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"artifact decision {name}"
            ))
        object.__setattr__(self, "mode", ArtifactAccessMode(self.mode))
        object.__setattr__(self, "allowed", bool(self.allowed))
        object.__setattr__(self, "reason", ArtifactAccessReason(self.reason))
        if self.allowed != (self.reason is ArtifactAccessReason.ALLOWED):
            raise ValueError(
                "artifact access decision reason does not match its verdict"
            )
        object.__setattr__(self, "artifact_revision", positive_int(
            self.artifact_revision, "artifact_revision"
        ))
        requires_claim = bool(self.requires_write_claim)
        if requires_claim and (
            not self.allowed or self.mode is not ArtifactAccessMode.WRITE
        ):
            raise ValueError(
                "only allowed write decisions may require a write claim"
            )
        object.__setattr__(self, "requires_write_claim", requires_claim)


@dataclass(frozen=True, slots=True)
class ArtifactWriteClaimCommand:
    artifact_id: str
    run_id: str
    expected_revision: int
    lease_duration_ms: int

    def __post_init__(self) -> None:
        for name in ("artifact_id", "run_id"):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"artifact claim {name}"
            ))
        object.__setattr__(self, "expected_revision", positive_int(
            self.expected_revision, "expected_revision"
        ))
        object.__setattr__(self, "lease_duration_ms", positive_int(
            self.lease_duration_ms, "lease_duration_ms"
        ))


@dataclass(frozen=True, slots=True)
class ArtifactWriteClaim:
    artifact_id: str
    run_id: str
    claim_token: str
    acquired_revision: int
    expires_at_ms: int

    def __post_init__(self) -> None:
        for name in ("artifact_id", "run_id", "claim_token"):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"artifact write claim {name}"
            ))
        object.__setattr__(self, "acquired_revision", positive_int(
            self.acquired_revision, "acquired_revision"
        ))
        object.__setattr__(self, "expires_at_ms", positive_int(
            self.expires_at_ms, "expires_at_ms"
        ))


@dataclass(frozen=True, slots=True)
class ArtifactClaimLeaseCommand:
    artifact_id: str
    run_id: str
    claim_token: str
    lease_duration_ms: int | None = None

    def __post_init__(self) -> None:
        for name in ("artifact_id", "run_id", "claim_token"):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"artifact claim lease {name}"
            ))
        object.__setattr__(self, "lease_duration_ms", optional_positive_int(
            self.lease_duration_ms, "lease_duration_ms"
        ))


@dataclass(frozen=True, slots=True)
class ArtifactAccessGrant:
    decision: ArtifactAccessDecision
    write_claim: ArtifactWriteClaim | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.decision, ArtifactAccessDecision):
            raise TypeError("artifact access grant decision is invalid")
        if not self.decision.allowed:
            raise ValueError("denied artifact access cannot become a grant")
        if self.decision.requires_write_claim != (self.write_claim is not None):
            raise ValueError("artifact access grant write claim does not match")
        if self.write_claim is not None and (
            self.write_claim.artifact_id != self.decision.artifact_id
            or self.write_claim.run_id != self.decision.run_id
            or self.write_claim.acquired_revision
            != self.decision.artifact_revision
        ):
            raise ValueError("artifact write claim does not match the decision")


class ArtifactAccessPolicy:
    """Generic lifecycle and optimistic-concurrency checks."""

    def decide(
        self,
        candidate: ArtifactResumeCandidate,
        request: ArtifactAccessRequest,
        *,
        cross_run_authorized: bool = False,
    ) -> ArtifactAccessDecision:
        reason = self._denial_reason(
            candidate,
            request,
            cross_run_authorized=bool(cross_run_authorized),
        )
        allowed = reason is None
        return ArtifactAccessDecision(
            artifact_id=request.artifact_id,
            run_id=request.run_id,
            mode=request.mode,
            allowed=allowed,
            reason=reason or ArtifactAccessReason.ALLOWED,
            artifact_revision=candidate.revision,
            requires_write_claim=(
                allowed and request.mode is ArtifactAccessMode.WRITE
            ),
        )

    @staticmethod
    def _denial_reason(
        candidate: ArtifactResumeCandidate,
        request: ArtifactAccessRequest,
        *,
        cross_run_authorized: bool,
    ) -> ArtifactAccessReason | None:
        if candidate.artifact_id != request.artifact_id:
            return ArtifactAccessReason.ARTIFACT_ID_MISMATCH
        if (
            candidate.created_by_run_id != request.run_id
            and not cross_run_authorized
        ):
            return ArtifactAccessReason.CROSS_RUN_NOT_AUTHORIZED
        if candidate.status is ArtifactStatus.ABORTED:
            return ArtifactAccessReason.ARTIFACT_ABORTED
        if candidate.revision != request.expected_revision:
            return ArtifactAccessReason.ARTIFACT_REVISION_CONFLICT
        if (
            request.mode is ArtifactAccessMode.WRITE
            and candidate.status is ArtifactStatus.FINALIZED
        ):
            return ArtifactAccessReason.ARTIFACT_FINALIZED
        return None


__all__ = [
    "ArtifactAccessDecision",
    "ArtifactAccessGrant",
    "ArtifactAccessMode",
    "ArtifactAccessPolicy",
    "ArtifactAccessReason",
    "ArtifactAccessRequest",
    "ArtifactClaimLeaseCommand",
    "ArtifactResumeCandidate",
    "ArtifactWriteClaim",
    "ArtifactWriteClaimCommand",
]
