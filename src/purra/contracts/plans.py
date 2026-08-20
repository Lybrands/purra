"""Semantic planning and bounded runtime-transition contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from purra.contracts.enums import (
    StepExecutor,
    StepStatus,
    StepType,
    ToolRiskLevel,
)
from purra.json_values import freeze_json_mapping, thaw_json_mapping
from purra.normalization import (
    optional_text,
    required_text,
    text_frozenset,
    unique_text_tuple,
)


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """Normalized semantic intent; it never grants tools or data access."""

    goal: str
    target: Mapping[str, Any] = field(default_factory=dict)
    operation: str | None = None
    instruction: str | None = None
    constraints: tuple[str, ...] = ()
    preserve: tuple[str, ...] = ()
    deliverable: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "goal", required_text(self.goal, "task spec goal"))
        object.__setattr__(self, "target", freeze_json_mapping(self.target))
        object.__setattr__(self, "operation", optional_text(self.operation))
        object.__setattr__(self, "instruction", optional_text(self.instruction))
        object.__setattr__(
            self,
            "constraints",
            unique_text_tuple(self.constraints),
        )
        object.__setattr__(self, "preserve", unique_text_tuple(self.preserve))
        object.__setattr__(self, "deliverable", optional_text(self.deliverable))

    def to_mapping(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "goal": self.goal,
                "target": thaw_json_mapping(self.target),
                "operation": self.operation,
                "instruction": self.instruction,
                "constraints": list(self.constraints),
                "preserve": list(self.preserve),
                "deliverable": self.deliverable,
            }.items()
            if value not in (None, "", [], {})
        }


@dataclass(frozen=True, slots=True)
class WorkStep:
    """One Planner-authored semantic step without runtime state or authority."""

    id: str
    title: str
    type: StepType
    executor: StepExecutor
    risk_level: ToolRiskLevel | None = None
    capability_names: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    description: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", required_text(self.id, "work step id"))
        object.__setattr__(
            self,
            "title",
            required_text(self.title, "work step title"),
        )
        object.__setattr__(self, "type", StepType(self.type))
        object.__setattr__(self, "executor", StepExecutor(self.executor))
        object.__setattr__(
            self,
            "risk_level",
            ToolRiskLevel(self.risk_level) if self.risk_level is not None else None,
        )
        object.__setattr__(
            self,
            "capability_names",
            unique_text_tuple(self.capability_names),
        )
        object.__setattr__(
            self,
            "depends_on",
            unique_text_tuple(self.depends_on),
        )
        object.__setattr__(self, "description", optional_text(self.description))


@dataclass(frozen=True, slots=True)
class WorkPlan:
    """Planner-authored semantic intent; never executable by itself."""

    title: str
    steps: tuple[WorkStep, ...]
    goal: str | None = None
    task_spec: TaskSpec | None = None

    def __post_init__(self) -> None:
        title = required_text(self.title, "work plan title")
        steps = tuple(self.steps)
        if not steps:
            raise ValueError("work plan requires at least one step")
        if not all(isinstance(step, WorkStep) for step in steps):
            raise TypeError("work plan steps must be WorkStep values")
        _validate_dependencies(steps, "work plan")
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "steps", steps)
        object.__setattr__(self, "goal", optional_text(self.goal))
        if self.task_spec is not None and not isinstance(self.task_spec, TaskSpec):
            raise TypeError("work plan task_spec must be TaskSpec")


@dataclass(frozen=True, slots=True)
class TaskStep:
    """Persisted execution step compiled from a WorkStep or host protocol."""

    id: str
    title: str
    type: StepType
    executor: StepExecutor
    status: StepStatus = StepStatus.PENDING
    risk_level: ToolRiskLevel | None = None
    suggested_tools: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    description: str | None = None
    result_summary: str | None = None
    error: str | None = None
    protocol_private: bool = False
    planning_capability: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", required_text(self.id, "task step id"))
        object.__setattr__(self, "title", required_text(self.title, "task step title"))
        object.__setattr__(self, "type", StepType(self.type))
        object.__setattr__(self, "executor", StepExecutor(self.executor))
        object.__setattr__(self, "status", StepStatus(self.status))
        object.__setattr__(
            self,
            "risk_level",
            ToolRiskLevel(self.risk_level) if self.risk_level is not None else None,
        )
        object.__setattr__(
            self,
            "suggested_tools",
            unique_text_tuple(self.suggested_tools),
        )
        object.__setattr__(self, "depends_on", unique_text_tuple(self.depends_on))
        object.__setattr__(self, "description", optional_text(self.description))
        object.__setattr__(self, "result_summary", optional_text(self.result_summary))
        object.__setattr__(self, "error", optional_text(self.error))
        if not isinstance(self.protocol_private, bool):
            raise TypeError("task step protocol_private must be a boolean")
        if self.protocol_private and self.executor is not StepExecutor.TOOL:
            raise ValueError("only tool steps may be protocol-private")
        object.__setattr__(
            self,
            "planning_capability",
            optional_text(self.planning_capability),
        )
        if (
            self.planning_capability is not None
            and self.executor is not StepExecutor.TOOL
        ):
            raise ValueError("only tool steps may reference a planning capability")


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """Core-compiled plan whose TaskSteps drive persisted runtime authority."""

    title: str
    steps: tuple[TaskStep, ...]
    goal: str | None = None
    task_spec: TaskSpec | None = None
    work_step_ids: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        title = required_text(self.title, "execution plan title")
        steps = tuple(self.steps)
        if not steps:
            raise ValueError("execution plan requires at least one step")
        if not all(isinstance(step, TaskStep) for step in steps):
            raise TypeError("execution plan steps must be TaskStep values")
        _validate_dependencies(steps, "execution plan")
        work_step_ids = (
            tuple(step.id for step in steps if not step.protocol_private)
            if self.work_step_ids is None
            else unique_text_tuple(self.work_step_ids)
        )
        unknown_work_steps = set(work_step_ids) - {step.id for step in steps}
        if unknown_work_steps:
            raise ValueError(
                "execution plan work_step_ids name unknown steps: "
                + ", ".join(sorted(unknown_work_steps))
            )
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "steps", steps)
        object.__setattr__(self, "work_step_ids", work_step_ids)
        object.__setattr__(self, "goal", optional_text(self.goal))
        if self.task_spec is not None and not isinstance(self.task_spec, TaskSpec):
            raise TypeError("execution plan task_spec must be TaskSpec")


@dataclass(frozen=True, slots=True)
class ExecutionTransition:
    """The single active runtime step and its compiled tool authorization."""

    step_id: str
    executor: StepExecutor
    allowed_tool_names: frozenset[str] = frozenset()
    future_tool_names: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "step_id",
            required_text(self.step_id, "execution transition step id"),
        )
        object.__setattr__(self, "executor", StepExecutor(self.executor))
        object.__setattr__(
            self,
            "allowed_tool_names",
            text_frozenset(self.allowed_tool_names),
        )
        object.__setattr__(
            self,
            "future_tool_names",
            text_frozenset(self.future_tool_names),
        )


def _validate_dependencies(steps: tuple[Any, ...], label: str) -> None:
    ids = tuple(step.id for step in steps)
    if len(ids) != len(set(ids)):
        raise ValueError(f"{label} step ids must be unique")
    known: set[str] = set()
    for step in steps:
        unknown = set(step.depends_on) - known
        if unknown:
            raise ValueError(
                f"{label} dependencies must reference earlier steps: "
                + ", ".join(sorted(unknown))
            )
        if step.id in step.depends_on:
            raise ValueError(f"{label} step cannot depend on itself")
        known.add(step.id)


__all__ = [
    "ExecutionPlan",
    "ExecutionTransition",
    "TaskSpec",
    "TaskStep",
    "WorkPlan",
    "WorkStep",
]
