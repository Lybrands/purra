"""Storage-neutral contracts for durable execution spanning many Runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from graphlib import CycleError, TopologicalSorter
from typing import Any, Mapping

from purra.normalization import (
    non_negative_int,
    optional_non_negative_int,
    optional_positive_int,
    optional_text,
    positive_int,
    required_text,
    unique_text_tuple,
)
from purra.json_values import freeze_json_mapping
from purra.recovery import FailureDisposition


class LongTaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    @property
    def terminal(self) -> bool:
        return self in {
            LongTaskStatus.COMPLETED,
            LongTaskStatus.FAILED,
            LongTaskStatus.CANCELED,
        }


class LongTaskRunRelation(StrEnum):
    """Why a Run is linked to a durable task."""

    CREATED = "created"
    CONTINUATION = "continuation"
    REFERENCE = "reference"


@dataclass(frozen=True, slots=True)
class LongTaskRunBinding:
    task_id: str
    run_id: str
    relation: LongTaskRunRelation

    def __post_init__(self) -> None:
        for name in ("task_id", "run_id"):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"long task Run binding {name}"
            ))
        object.__setattr__(self, "relation", LongTaskRunRelation(self.relation))


class LongTaskUnitStatus(StrEnum):
    PENDING = "pending"
    WAITING_RETRY = "waiting_retry"
    CLAIMED = "claimed"
    RUNNING = "running"
    NEEDS_SPLIT = "needs_split"
    BLOCKED = "blocked"
    EXPANDED = "expanded"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"

    @property
    def terminal(self) -> bool:
        return self in {
            LongTaskUnitStatus.COMPLETED,
            LongTaskUnitStatus.EXPANDED,
            LongTaskUnitStatus.FAILED,
            LongTaskUnitStatus.CANCELED,
        }


@dataclass(frozen=True, slots=True)
class LongTaskUsage:
    """Authoritative linked-Run usage accumulated exactly once per Run."""

    invocation_count: int = 0
    unreported_usage_attempts: int = 0
    input_tokens: int = 0
    generation_tokens: int = 0
    reasoning_tokens: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "invocation_count",
            "unreported_usage_attempts",
            "input_tokens",
            "generation_tokens",
        ):
            object.__setattr__(
                self,
                name,
                non_negative_int(getattr(self, name), f"long task usage {name}"),
            )
        if self.reasoning_tokens is not None:
            object.__setattr__(
                self,
                "reasoning_tokens",
                non_negative_int(
                    self.reasoning_tokens,
                    "long task usage reasoning_tokens",
                ),
            )
        if self.unreported_usage_attempts > self.invocation_count:
            raise ValueError(
                "unreported usage attempts cannot exceed invocation count"
            )

    def to_mapping(self) -> dict[str, int | None]:
        return {
            "invocationCount": self.invocation_count,
            "unreportedUsageAttempts": self.unreported_usage_attempts,
            "inputTokens": self.input_tokens,
            "generationTokens": self.generation_tokens,
            "reasoningTokens": self.reasoning_tokens,
        }


@dataclass(frozen=True, slots=True)
class LongTaskBudgetLimits:
    """Persisted task maxima; ``None`` is the explicit unlimited value."""

    max_invocation_attempts: int | None = None
    max_input_tokens: int | None = None
    max_run_generation_tokens: int | None = None
    max_reasoning_tokens: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "max_invocation_attempts",
            "max_input_tokens",
            "max_run_generation_tokens",
            "max_reasoning_tokens",
        ):
            object.__setattr__(
                self,
                name,
                optional_non_negative_int(
                    getattr(self, name),
                    f"long task budget {name}",
                ),
            )

    def to_mapping(self) -> dict[str, int | None]:
        return {
            "maxInvocationAttempts": self.max_invocation_attempts,
            "maxInputTokens": self.max_input_tokens,
            "maxRunGenerationTokens": (
                self.max_run_generation_tokens
            ),
            "maxReasoningTokens": self.max_reasoning_tokens,
        }


@dataclass(frozen=True, slots=True)
class LongTaskUnitSpec:
    id: str
    position: int
    semantic_key: str | None = None
    dependencies: tuple[str, ...] = ()
    parent_unit_id: str | None = None
    required: bool = True
    input_ref: str | None = None
    max_attempts: int = 3
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "id",
            required_text(self.id, "long task unit id"),
        )
        object.__setattr__(
            self,
            "position",
            non_negative_int(self.position, "long task unit position"),
        )
        object.__setattr__(
            self,
            "semantic_key",
            required_text(
                self.semantic_key or self.id,
                "long task unit semantic_key",
            ),
        )
        dependencies = unique_text_tuple(
            str(item or "").strip() for item in self.dependencies
        )
        if self.id in dependencies:
            raise ValueError("long task unit cannot depend on itself")
        object.__setattr__(self, "dependencies", dependencies)
        object.__setattr__(
            self,
            "parent_unit_id",
            optional_text(self.parent_unit_id),
        )
        object.__setattr__(self, "required", bool(self.required))
        object.__setattr__(self, "input_ref", optional_text(self.input_ref))
        object.__setattr__(
            self,
            "max_attempts",
            positive_int(self.max_attempts, "long task unit max_attempts"),
        )
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class LongTaskCreateCommand:
    namespace: str
    kind: str
    owner_id: str
    created_by_run_id: str
    units: tuple[LongTaskUnitSpec, ...]
    max_parallelism: int = 1
    deadline_at_ms: int | None = None
    budget_limits: LongTaskBudgetLimits = field(
        default_factory=LongTaskBudgetLimits
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "namespace",
            "kind",
            "owner_id",
            "created_by_run_id",
        ):
            object.__setattr__(
                self,
                name,
                required_text(getattr(self, name), f"long task {name}"),
            )
        units = tuple(self.units)
        if not units:
            raise ValueError("long task requires at least one execution unit")
        if not any(unit.required for unit in units):
            raise ValueError("long task requires at least one required execution unit")
        ids = tuple(item.id for item in units)
        semantic_keys = tuple(item.semantic_key for item in units)
        positions = tuple(item.position for item in units)
        if len(ids) != len(set(ids)):
            raise ValueError("long task unit ids must be unique")
        if len(semantic_keys) != len(set(semantic_keys)):
            raise ValueError("long task unit semantic keys must be unique")
        if len(positions) != len(set(positions)):
            raise ValueError("long task unit positions must be unique")
        known = set(ids)
        missing = {
            dependency
            for unit in units
            for dependency in unit.dependencies
            if dependency not in known
        }
        if missing:
            raise ValueError(
                "long task unit dependencies are unknown: "
                + ", ".join(sorted(missing))
            )
        _require_acyclic(units)
        object.__setattr__(self, "units", units)
        object.__setattr__(
            self,
            "max_parallelism",
            positive_int(self.max_parallelism, "long task max_parallelism"),
        )
        object.__setattr__(
            self,
            "deadline_at_ms",
            optional_positive_int(self.deadline_at_ms, "long task deadline_at_ms"),
        )
        if not isinstance(self.budget_limits, LongTaskBudgetLimits):
            raise TypeError("long task budget_limits must be LongTaskBudgetLimits")
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class LongTaskRecord:
    id: str
    namespace: str
    kind: str
    owner_id: str
    created_by_run_id: str
    status: LongTaskStatus
    revision: int
    total_units: int
    completed_units: int
    failed_units: int
    max_parallelism: int
    deadline_at_ms: int | None = None
    budget_limits: LongTaskBudgetLimits = field(
        default_factory=LongTaskBudgetLimits
    )
    cancellation_requested_at_ms: int | None = None
    usage: LongTaskUsage = field(default_factory=LongTaskUsage)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    create_time: str | None = None
    update_time: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "id",
            "namespace",
            "kind",
            "owner_id",
            "created_by_run_id",
        ):
            object.__setattr__(
                self,
                name,
                required_text(getattr(self, name), f"long task record {name}"),
            )
        object.__setattr__(self, "status", LongTaskStatus(self.status))
        for name in ("revision", "total_units", "max_parallelism"):
            object.__setattr__(
                self,
                name,
                positive_int(getattr(self, name), f"long task record {name}"),
            )
        for name in ("completed_units", "failed_units"):
            object.__setattr__(
                self,
                name,
                non_negative_int(
                    getattr(self, name),
                    f"long task record {name}",
                ),
            )
        if self.completed_units + self.failed_units > self.total_units:
            raise ValueError("long task progress exceeds total units")
        object.__setattr__(
            self,
            "deadline_at_ms",
            optional_positive_int(self.deadline_at_ms, "long task deadline_at_ms"),
        )
        if not isinstance(self.budget_limits, LongTaskBudgetLimits):
            raise TypeError("long task budget_limits must be LongTaskBudgetLimits")
        if self.cancellation_requested_at_ms is not None:
            requested_at = int(self.cancellation_requested_at_ms)
            if requested_at < 0:
                raise ValueError("long task cancellation timestamp is invalid")
            object.__setattr__(
                self,
                "cancellation_requested_at_ms",
                requested_at,
            )
        if not isinstance(self.usage, LongTaskUsage):
            raise TypeError("long task usage must be LongTaskUsage")
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))
        for name in ("create_time", "update_time"):
            object.__setattr__(
                self,
                name,
                optional_text(getattr(self, name)),
            )


@dataclass(frozen=True, slots=True)
class LongTaskUnitRecord:
    task_id: str
    id: str
    position: int
    status: LongTaskUnitStatus
    semantic_key: str | None = None
    dependencies: tuple[str, ...] = ()
    parent_unit_id: str | None = None
    required: bool = True
    attempt: int = 0
    max_attempts: int = 3
    worker_id: str | None = None
    lease_epoch: int = 0
    lease_expires_at_ms: int | None = None
    settled_by_worker_id: str | None = None
    run_id: str | None = None
    input_ref: str | None = None
    output_ref: str | None = None
    artifact_digest: str | None = None
    validation_receipt: Mapping[str, Any] = field(default_factory=dict)
    failure: Mapping[str, Any] = field(default_factory=dict)
    disposition: FailureDisposition | None = None
    error_code: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    create_time: str | None = None
    update_time: str | None = None

    def __post_init__(self) -> None:
        for name in ("task_id", "id"):
            object.__setattr__(
                self,
                name,
                required_text(
                    getattr(self, name),
                    f"long task unit record {name}",
                ),
            )
        object.__setattr__(self, "status", LongTaskUnitStatus(self.status))
        object.__setattr__(
            self,
            "semantic_key",
            required_text(
                self.semantic_key or self.id,
                "long task unit record semantic_key",
            ),
        )
        object.__setattr__(self, "dependencies", tuple(self.dependencies))
        object.__setattr__(
            self,
            "parent_unit_id",
            optional_text(self.parent_unit_id),
        )
        object.__setattr__(self, "required", bool(self.required))
        for name in ("position", "attempt", "max_attempts", "lease_epoch"):
            normalizer = positive_int if name == "max_attempts" else non_negative_int
            object.__setattr__(
                self,
                name,
                normalizer(
                    getattr(self, name),
                    f"long task unit record {name}",
                ),
            )
        for name in (
            "worker_id",
            "settled_by_worker_id",
            "run_id",
            "input_ref",
            "output_ref",
            "artifact_digest",
            "error_code",
        ):
            object.__setattr__(
                self,
                name,
                optional_text(getattr(self, name)),
            )
        if self.lease_expires_at_ms is not None:
            object.__setattr__(
                self,
                "lease_expires_at_ms",
                int(self.lease_expires_at_ms),
            )
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))
        object.__setattr__(
            self,
            "validation_receipt",
            freeze_json_mapping(self.validation_receipt),
        )
        object.__setattr__(self, "failure", freeze_json_mapping(self.failure))
        if self.disposition is not None:
            object.__setattr__(
                self,
                "disposition",
                FailureDisposition(self.disposition),
            )
        for name in ("create_time", "update_time"):
            object.__setattr__(
                self,
                name,
                optional_text(getattr(self, name)),
            )


@dataclass(frozen=True, slots=True)
class LongTaskUnitResult:
    output_ref: str
    run_id: str | None = None
    artifact_digest: str | None = None
    validation_receipt: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "output_ref",
            required_text(self.output_ref, "long task unit result output_ref"),
        )
        object.__setattr__(self, "run_id", optional_text(self.run_id))
        object.__setattr__(
            self,
            "artifact_digest",
            optional_text(self.artifact_digest),
        )
        object.__setattr__(
            self,
            "validation_receipt",
            freeze_json_mapping(self.validation_receipt),
        )
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class LongTaskSplitResult:
    children: tuple[LongTaskUnitSpec, ...]
    replacement_dependency_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        children = tuple(self.children)
        ids = tuple(child.id for child in children)
        semantic_keys = tuple(child.semantic_key for child in children)
        if len(ids) != len(set(ids)):
            raise ValueError("split child ids must be unique")
        if len(semantic_keys) != len(set(semantic_keys)):
            raise ValueError("split child semantic keys must be unique")
        replacement_ids = unique_text_tuple(self.replacement_dependency_ids)
        unknown = set(replacement_ids) - set(ids)
        if unknown:
            raise ValueError("replacement dependencies must reference split children")
        if children:
            _require_acyclic(children)
        object.__setattr__(self, "children", children)
        object.__setattr__(self, "replacement_dependency_ids", replacement_ids)


def _require_acyclic(units: tuple[LongTaskUnitSpec, ...]) -> None:
    try:
        tuple(TopologicalSorter({
            unit.id: unit.dependencies for unit in units
        }).static_order())
    except CycleError as error:
        raise ValueError(
            "long task unit dependencies contain a cycle"
        ) from error


__all__ = [
    "LongTaskBudgetLimits",
    "LongTaskCreateCommand",
    "LongTaskRecord",
    "LongTaskRunBinding",
    "LongTaskRunRelation",
    "LongTaskStatus",
    "LongTaskSplitResult",
    "LongTaskUnitRecord",
    "LongTaskUnitResult",
    "LongTaskUnitSpec",
    "LongTaskUnitStatus",
    "LongTaskUsage",
]
