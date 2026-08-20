"""Typed failures for artifact lifecycle and persistence boundaries."""

from __future__ import annotations

from purra.errors import CodedAgentCoreError


class ArtifactError(CodedAgentCoreError):
    default_code = "artifact_error"


class ArtifactNotFoundError(ArtifactError):
    pass


class ArtifactConflictError(ArtifactError):
    pass


class ArtifactStateError(ArtifactError):
    pass


class ArtifactValidationError(ArtifactError):
    pass


class ArtifactAccessDeniedError(ArtifactError):
    pass


__all__ = [
    "ArtifactAccessDeniedError",
    "ArtifactConflictError",
    "ArtifactError",
    "ArtifactNotFoundError",
    "ArtifactStateError",
    "ArtifactValidationError",
]
