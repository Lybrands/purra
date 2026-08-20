"""Immutable composition-time choices for one Agent execution style."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from purra.context_strategies import ContextStrategy
from purra.planning_policies import ReactivePlanningPolicy
from purra.ports import PlanningPolicy, WorkPlanner
from purra.task_admission import LongTaskDispatcher, TaskAdmissionEvaluator


@dataclass(frozen=True, slots=True)
class ExecutionProfile:
    """Bundle optional orchestration capabilities without Kernel services."""

    planner: WorkPlanner | None = None
    planning_policy: PlanningPolicy = field(
        default_factory=ReactivePlanningPolicy
    )
    context_strategy: ContextStrategy = ContextStrategy.SINGLE_PASS
    task_admission_evaluator: TaskAdmissionEvaluator | None = None
    long_task_dispatcher: LongTaskDispatcher | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "context_strategy",
            ContextStrategy(self.context_strategy),
        )

        if self.planning_enabled and self.planner is None:
            raise ValueError(
                "planned execution profile requires an explicit planner"
            )
        if not self.planning_enabled and self.planner is not None:
            raise ValueError(
                "reactive execution profile cannot configure an unused planner"
            )

    @property
    def planning_enabled(self) -> bool:
        return not isinstance(self.planning_policy, ReactivePlanningPolicy)

    def snapshot_mapping(self) -> dict[str, Any]:
        """Return the deterministic orchestration surface owned by the host."""

        return {
            "planningEnabled": self.planning_enabled,
            "plannerType": _component_type(self.planner),
            "planningPolicyType": _component_type(self.planning_policy),
            "contextStrategy": self.context_strategy.value,
            "taskAdmissionType": _component_type(self.task_admission_evaluator),
            "longTaskDispatcherType": _component_type(self.long_task_dispatcher),
        }


def _component_type(value: object | None) -> str | None:
    if value is None:
        return None
    kind = type(value)
    return f"{kind.__module__}.{kind.__qualname__}"


__all__ = ["ExecutionProfile"]
