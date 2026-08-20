"""Business-neutral decisions between one Run and durable execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Sequence

from purra.contracts import ExecutionRecipe, ExecutionPlan
from purra.normalization import (
    non_negative_int,
    optional_text,
    required_text,
)
from purra.events import AgentEvent

from purra.json_values import freeze_json_mapping


class ExecutionMode(StrEnum):
    INLINE = "inline"
    DURABLE = "durable"
    CLARIFY = "clarify"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class TaskAdmissionDecision:
    mode: ExecutionMode = ExecutionMode.INLINE
    reason_code: str = "inline_default"
    estimated_units: int = 1
    estimated_model_calls: int = 1
    requires_confirmation: bool = False
    message: str | None = None
    covered_step_ids: Sequence[str] = ()
    execution_recipe: ExecutionRecipe | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "mode", ExecutionMode(self.mode))
        object.__setattr__(self, "reason_code", required_text(
            self.reason_code, "task admission reason_code"
        ))
        for name in ("estimated_units", "estimated_model_calls"):
            object.__setattr__(self, name, non_negative_int(
                getattr(self, name), f"task admission {name}"
            ))
        object.__setattr__(
            self,
            "requires_confirmation",
            bool(self.requires_confirmation),
        )
        object.__setattr__(self, "message", optional_text(self.message))
        if isinstance(self.covered_step_ids, (str, bytes, bytearray)):
            raise ValueError("covered task admission step ids must be a sequence")
        covered_step_ids = tuple(
            str(step_id or "").strip()
            for step_id in self.covered_step_ids
        )
        if any(not step_id for step_id in covered_step_ids):
            raise ValueError("covered task admission step ids must be non-empty")
        if len(covered_step_ids) != len(set(covered_step_ids)):
            raise ValueError("covered task admission step ids must be unique")
        if self.mode is ExecutionMode.DURABLE and not covered_step_ids:
            raise ValueError("durable task admission requires covered step ids")
        if self.mode is not ExecutionMode.DURABLE and covered_step_ids:
            raise ValueError(
                "only durable task admission can cover planned steps"
            )
        object.__setattr__(self, "covered_step_ids", covered_step_ids)
        if self.execution_recipe is not None:
            if self.mode is not ExecutionMode.DURABLE:
                raise ValueError(
                    "only durable task admission can carry an execution recipe"
                )
            if not isinstance(self.execution_recipe, ExecutionRecipe):
                raise TypeError(
                    "task admission execution_recipe must be an ExecutionRecipe"
                )
        elif self.mode is ExecutionMode.DURABLE:
            raise ValueError(
                "durable task admission requires an execution recipe"
            )
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))

    def to_event_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "mode": self.mode.value,
            "reasonCode": self.reason_code,
            "estimatedUnits": self.estimated_units,
            "estimatedModelCalls": self.estimated_model_calls,
            "requiresConfirmation": self.requires_confirmation,
            "coveredStepIds": list(self.covered_step_ids),
        }
        if self.message:
            payload["message"] = self.message
        if self.execution_recipe is not None:
            payload["executionRecipe"] = {
                "kind": self.execution_recipe.kind,
                "stepCount": len(self.execution_recipe.steps),
                "maxParallelism": self.execution_recipe.max_parallelism,
            }
        return payload


@dataclass(frozen=True, slots=True)
class LongTaskDispatchReceipt:
    task_id: str
    message: str
    admission: TaskAdmissionDecision
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "task_id", required_text(
            self.task_id, "long task dispatch task_id"
        ))
        object.__setattr__(self, "message", required_text(
            self.message, "long task dispatch message"
        ))
        if (
            not isinstance(self.admission, TaskAdmissionDecision)
            or self.admission.mode is not ExecutionMode.DURABLE
        ):
            raise ValueError("long task dispatch receipt requires durable admission")
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))


class LongTaskExecutionStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    PAUSED = "paused"


@dataclass(frozen=True, slots=True)
class LongTaskExecutionUpdate:
    """One canonical parent-stream event emitted while a durable task runs."""

    event: AgentEvent
    persist: bool = True
    plan_revision: ExecutionPlan | None = None
    plan_revision_metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.plan_revision is not None and not isinstance(
            self.plan_revision,
            ExecutionPlan,
        ):
            raise TypeError("long task plan revision must be an ExecutionPlan")
        metadata = freeze_json_mapping(self.plan_revision_metadata)
        if metadata and self.plan_revision is None:
            raise ValueError(
                "long task plan revision metadata requires a plan revision"
            )
        object.__setattr__(self, "plan_revision_metadata", metadata)


@dataclass(frozen=True, slots=True)
class LongTaskExecutionResult:
    task_id: str
    status: LongTaskExecutionStatus
    final_response: str = ""
    error: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "task_id", required_text(
            self.task_id, "long task execution task_id"
        ))
        object.__setattr__(self, "status", LongTaskExecutionStatus(self.status))
        object.__setattr__(self, "final_response", str(self.final_response or ""))
        object.__setattr__(self, "error", optional_text(self.error))
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))


__all__ = [
    "ExecutionMode",
    "LongTaskDispatchReceipt",
    "LongTaskExecutionResult",
    "LongTaskExecutionStatus",
    "LongTaskExecutionUpdate",
    "TaskAdmissionDecision",
]
