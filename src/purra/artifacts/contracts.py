"""Storage-neutral contracts for recoverable model-generated artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping

from purra.artifacts.ownership import ArtifactOwnerRef
from purra.normalization import (
    non_negative_int,
    optional_non_negative_int,
    optional_text,
    positive_int,
    required_text,
    text_tuple,
    unique_text_tuple,
)
from purra.json_values import (
    canonical_json_digest,
    freeze_json_mapping,
    thaw_json_mapping,
)


class ArtifactStatus(StrEnum):
    OPEN = "open"
    FINALIZED = "finalized"
    ABORTED = "aborted"


@dataclass(frozen=True, slots=True)
class ArtifactCreateCommand:
    namespace: str
    kind: str
    owner_id: str
    owner_ref: ArtifactOwnerRef
    created_by_run_id: str
    schema_version: int = 1
    expected_item_count: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("namespace", "kind", "owner_id"):
            object.__setattr__(
                self,
                name,
                required_text(getattr(self, name), f"artifact {name}"),
            )
        if not isinstance(self.owner_ref, ArtifactOwnerRef):
            raise TypeError("artifact owner_ref must be an ArtifactOwnerRef")
        object.__setattr__(self, "created_by_run_id", required_text(
            self.created_by_run_id,
            "artifact created_by_run_id",
        ))
        object.__setattr__(
            self,
            "schema_version",
            positive_int(self.schema_version, "artifact schema_version"),
        )
        object.__setattr__(
            self,
            "expected_item_count",
            optional_non_negative_int(
                self.expected_item_count,
                "artifact expected_item_count",
            ),
        )
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    id: str
    namespace: str
    kind: str
    owner_id: str
    owner_ref: ArtifactOwnerRef
    created_by_run_id: str
    schema_version: int
    status: ArtifactStatus = ArtifactStatus.OPEN
    revision: int = 1
    next_sequence: int = 1
    committed_item_count: int = 0
    expected_item_count: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    resource_ref: str | None = None
    coverage_digest: str | None = None

    def __post_init__(self) -> None:
        for name in ("id", "namespace", "kind", "owner_id"):
            object.__setattr__(
                self,
                name,
                required_text(getattr(self, name), f"artifact {name}"),
            )
        if not isinstance(self.owner_ref, ArtifactOwnerRef):
            raise TypeError("artifact owner_ref must be an ArtifactOwnerRef")
        object.__setattr__(self, "created_by_run_id", required_text(
            self.created_by_run_id,
            "artifact created_by_run_id",
        ))
        object.__setattr__(self, "status", ArtifactStatus(self.status))
        for name in ("schema_version", "revision", "next_sequence"):
            object.__setattr__(
                self,
                name,
                positive_int(getattr(self, name), f"artifact {name}"),
            )
        object.__setattr__(
            self,
            "committed_item_count",
            non_negative_int(
                self.committed_item_count,
                "committed_item_count",
            ),
        )
        object.__setattr__(
            self,
            "expected_item_count",
            optional_non_negative_int(
                self.expected_item_count,
                "artifact expected_item_count",
            ),
        )
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))
        object.__setattr__(
            self,
            "resource_ref",
            optional_text(self.resource_ref),
        )
        object.__setattr__(
            self,
            "coverage_digest",
            optional_text(self.coverage_digest),
        )

@dataclass(frozen=True, slots=True)
class ArtifactMutationLease:
    """Opaque proof that one Run currently owns an Artifact write.

    The repository, rather than a caller, decides whether the token is live and
    belongs to the Artifact.  ``lease_duration_ms`` is only a bounded renewal
    request applied atomically with a successful mutation.
    """

    run_id: str
    claim_token: str
    lease_duration_ms: int = 300_000

    def __post_init__(self) -> None:
        for name in ("run_id", "claim_token"):
            object.__setattr__(
                self,
                name,
                required_text(
                    getattr(self, name),
                    f"artifact mutation lease {name}",
                ),
            )
        object.__setattr__(
            self,
            "lease_duration_ms",
            positive_int(
                self.lease_duration_ms,
                "artifact mutation lease duration",
            ),
        )


@dataclass(frozen=True, slots=True)
class ArtifactAppendCommand:
    artifact_id: str
    expected_revision: int
    sequence: int
    batch_id: str
    idempotency_key: str
    items: tuple[Mapping[str, Any], ...]
    write_lease: ArtifactMutationLease
    coverage_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("artifact_id", "batch_id", "idempotency_key"):
            object.__setattr__(
                self,
                name,
                required_text(getattr(self, name), f"artifact batch {name}"),
            )
        for name in ("expected_revision", "sequence"):
            object.__setattr__(
                self,
                name,
                positive_int(getattr(self, name), f"artifact batch {name}"),
            )
        items = tuple(freeze_json_mapping(item) for item in self.items)
        if not items:
            raise ValueError("artifact batch must contain at least one item")
        object.__setattr__(self, "items", items)
        coverage = text_tuple(
            str(value or "").strip() for value in self.coverage_keys
        )
        object.__setattr__(self, "coverage_keys", coverage)
        if not isinstance(self.write_lease, ArtifactMutationLease):
            raise TypeError("artifact append write_lease is invalid")

    @property
    def content_digest(self) -> str:
        payload = {
            "artifactId": self.artifact_id,
            "sequence": self.sequence,
            "batchId": self.batch_id,
            "items": [thaw_json_mapping(item) for item in self.items],
            "coverageKeys": list(self.coverage_keys),
        }
        return canonical_json_digest(payload)


@dataclass(frozen=True, slots=True)
class ArtifactBatch:
    artifact_id: str
    batch_id: str
    idempotency_key: str
    sequence: int
    committed_revision: int
    items: tuple[Mapping[str, Any], ...]
    coverage_keys: tuple[str, ...] = ()
    content_digest: str = ""

    def __post_init__(self) -> None:
        for name in ("artifact_id", "batch_id", "idempotency_key"):
            object.__setattr__(
                self,
                name,
                required_text(getattr(self, name), f"artifact batch {name}"),
            )
        for name in ("sequence", "committed_revision"):
            object.__setattr__(
                self,
                name,
                positive_int(getattr(self, name), f"artifact batch {name}"),
            )
        object.__setattr__(
            self,
            "items",
            tuple(freeze_json_mapping(item) for item in self.items),
        )
        object.__setattr__(self, "coverage_keys", tuple(self.coverage_keys))
        object.__setattr__(
            self,
            "content_digest",
            required_text(
                self.content_digest,
                "artifact batch content_digest",
            ),
        )


@dataclass(frozen=True, slots=True)
class ArtifactBatchReceipt:
    artifact_id: str
    batch_id: str
    sequence: int
    committed_revision: int
    next_sequence: int
    accepted_count: int
    replayed: bool = False

    def __post_init__(self) -> None:
        for name in ("artifact_id", "batch_id"):
            object.__setattr__(
                self,
                name,
                required_text(getattr(self, name), f"artifact receipt {name}"),
            )
        for name in ("sequence", "committed_revision", "next_sequence"):
            object.__setattr__(
                self,
                name,
                positive_int(getattr(self, name), f"artifact receipt {name}"),
            )
        object.__setattr__(
            self,
            "accepted_count",
            non_negative_int(
                self.accepted_count,
                "artifact accepted_count",
            ),
        )
        object.__setattr__(self, "replayed", bool(self.replayed))


@dataclass(frozen=True, slots=True)
class ArtifactFinalizeCommand:
    artifact_id: str
    expected_revision: int
    write_lease: ArtifactMutationLease
    expected_item_count: int | None = None
    expected_coverage_keys: tuple[str, ...] = ()
    resource_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_id",
            required_text(self.artifact_id, "artifact_id"),
        )
        object.__setattr__(
            self,
            "expected_revision",
            positive_int(self.expected_revision, "expected_revision"),
        )
        object.__setattr__(
            self,
            "expected_item_count",
            optional_non_negative_int(
                self.expected_item_count,
                "artifact expected_item_count",
            ),
        )
        object.__setattr__(
            self,
            "expected_coverage_keys",
            unique_text_tuple(
                str(value or "").strip()
                for value in self.expected_coverage_keys
            ),
        )
        object.__setattr__(
            self,
            "resource_ref",
            optional_text(self.resource_ref),
        )
        if not isinstance(self.write_lease, ArtifactMutationLease):
            raise TypeError("artifact finalize write_lease is invalid")


@dataclass(frozen=True, slots=True)
class ArtifactValidationResult:
    accepted: bool
    code: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "accepted", bool(self.accepted))
        code = optional_text(self.code)
        if not self.accepted and not code:
            raise ValueError("rejected artifact validation requires a code")
        object.__setattr__(self, "code", code)
        object.__setattr__(self, "details", freeze_json_mapping(self.details))


def coverage_digest(keys: tuple[str, ...]) -> str:
    return canonical_json_digest(sorted(set(keys)))


__all__ = [
    "ArtifactAppendCommand",
    "ArtifactBatch",
    "ArtifactBatchReceipt",
    "ArtifactCreateCommand",
    "ArtifactFinalizeCommand",
    "ArtifactMutationLease",
    "ArtifactRecord",
    "ArtifactStatus",
    "ArtifactValidationResult",
    "coverage_digest",
]
