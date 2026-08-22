"""Immutable composition-time choices for one Agent execution style."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from purra.context_strategies import ContextStrategy
from purra.planning_policies import ReactivePlanningPolicy
from purra.ports import PlanningPolicy, WorkPlanner
from purra.task_admission import LongTaskDispatcher, TaskAdmissionEvaluator


ComponentBindingResolver = Callable[[str, object | None], Mapping[str, Any]]


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

    def snapshot_mapping(
        self,
        binding_resolver: ComponentBindingResolver | None = None,
    ) -> dict[str, Any]:
        """Return the deterministic orchestration surface owned by the host."""

        bind = binding_resolver or _legacy_component_binding
        return {
            "planningEnabled": self.planning_enabled,
            "planner": bind("planner", self.planner),
            "planningPolicy": bind("planningPolicy", self.planning_policy),
            "contextStrategy": self.context_strategy.value,
            "taskAdmission": bind(
                "taskAdmissionEvaluator",
                self.task_admission_evaluator,
            ),
            "longTaskDispatcher": bind(
                "longTaskDispatcher",
                self.long_task_dispatcher,
            ),
        }


def _component_type(value: object | None) -> str | None:
    if value is None:
        return None
    kind = type(value)
    return f"{kind.__module__}.{kind.__qualname__}"


def _legacy_component_binding(
    role: str,
    value: object | None,
) -> Mapping[str, Any]:
    del role
    return {"type": _component_type(value)} if value is not None else {"kind": "none"}


__all__ = ["ExecutionProfile"]
