"""Immutable composition-time choices for one Agent execution style."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from purra.context_strategies import ContextStrategy
from purra.ports import PlanningPolicy, WorkPlanner
from purra.task_admission import LongTaskDispatcher, TaskAdmissionEvaluator


ComponentBindingResolver = Callable[[str, object | None], Mapping[str, Any]]


@dataclass(frozen=True, slots=True)
class ExecutionProfile:
    """Bundle optional orchestration capabilities without Kernel services."""

    planner: WorkPlanner | None = None
    planning_policy: PlanningPolicy | None = None
    context_strategy: ContextStrategy = ContextStrategy.SINGLE_PASS
    task_admission_evaluator: TaskAdmissionEvaluator | None = None
    long_task_dispatcher: LongTaskDispatcher | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "context_strategy",
            ContextStrategy(self.context_strategy),
        )

        if self.planner is None and self.planning_policy is not None:
            raise ValueError(
                "planning policy requires a configured planner"
            )

    @property
    def planning_enabled(self) -> bool:
        return self.planner is not None

    def snapshot_mapping(
        self,
        binding_resolver: ComponentBindingResolver,
    ) -> dict[str, Any]:
        """Return the deterministic orchestration surface owned by the host."""

        return {
            "planningEnabled": self.planning_enabled,
            "planner": binding_resolver("planner", self.planner),
            "planningPolicy": binding_resolver(
                "planningPolicy", self.planning_policy
            ),
            "contextStrategy": self.context_strategy.value,
            "taskAdmission": binding_resolver(
                "taskAdmissionEvaluator",
                self.task_admission_evaluator,
            ),
            "longTaskDispatcher": binding_resolver(
                "longTaskDispatcher",
                self.long_task_dispatcher,
            ),
        }

__all__ = ["ExecutionProfile"]
