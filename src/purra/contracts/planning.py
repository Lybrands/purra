"""Planning contracts: constraints, capabilities, results, and turns."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from purra.contracts.enums import PlanningKind, StepExecutor, ToolBatchOutcome
from purra.contracts.context import ContextBlock
from purra.contracts.model import AgentMessage
from purra.contracts.plans import (
    ExecutionPlan,
    ExecutionTransition,
    TaskSpec,
    TaskStep,
    WorkPlan,
    WorkStep,
)
from purra.json_values import freeze_json_mapping
from purra.normalization import (
    non_negative_int,
    optional_positive_int,
    optional_text as _optional_text,
    positive_int,
    text_frozenset,
)

@dataclass(frozen=True, slots=True)
class PlanningConstraints:
    """Request-scoped limits on tools a planner may select.

    ``context_satisfied_tool_names`` is node-wide: the named tool's result is
    already present in trusted context, so the planner must not call it.
    ``planning_excluded_tool_names`` names tools that are valid runtime
    capabilities but outside this request's explicitly bounded evidence or
    action scope.  Exclusion is not a claim that their results are present.
    ``satisfied_tool_dependency_edges`` is deliberately narrower: only the
    named consumer's dependency is already satisfied, while the dependency
    tool remains available for explicit use and for every other consumer.
    ``required_any_tool_names`` is a generic completion guard: at least one of
    the named tools must be selected (or already completed during replanning).
    ``execution_satisfied_tool_names`` is host-only lowering state.  It names
    private runtime tools whose durable work is already present, without
    advertising those protocol details to the Planner.
    Delegated Agents are exposed through an ordinary host-authorized tool, so
    this contract does not define a separate Agent execution topology.
    ``planning_excluded_executors`` removes an
    execution mechanism that the request's admitted runtime cannot support.
    ``allow_model_only_fallback`` controls whether invalid Planner output may
    degrade to a side-effect-free response for this request.
    ``min_initial_visible_steps`` is the built-in model Planner's initial plan
    preference, not a permission boundary. Revisions and custom WorkPlanners
    retain their own cardinality rules; Core still validates executable plans.
    """

    context_satisfied_tool_names: frozenset[str] = frozenset()
    planning_excluded_tool_names: frozenset[str] = frozenset()
    satisfied_tool_dependency_edges: frozenset[tuple[str, str]] = frozenset()
    required_any_tool_names: frozenset[str] = frozenset()
    execution_satisfied_tool_names: frozenset[str] = frozenset()
    planning_excluded_executors: frozenset[StepExecutor] = frozenset()
    allow_model_only_fallback: bool = True
    min_initial_visible_steps: int = 3

    def __post_init__(self) -> None:
        if type(self.min_initial_visible_steps) is not int:
            raise TypeError("min_initial_visible_steps must be an integer")
        if not 1 <= self.min_initial_visible_steps <= 9_007_199_254_740_991:
            raise ValueError("min_initial_visible_steps must be a positive safe integer")
        for field_name in (
            "context_satisfied_tool_names",
            "planning_excluded_tool_names",
            "required_any_tool_names",
            "execution_satisfied_tool_names",
        ):
            object.__setattr__(
                self,
                field_name,
                text_frozenset(getattr(self, field_name)),
            )
        try:
            excluded_executors = frozenset(
                StepExecutor(value)
                for value in self.planning_excluded_executors
            )
        except ValueError as error:
            raise ValueError(
                "planning_excluded_executors contains an unsupported executor"
            ) from error
        object.__setattr__(
            self,
            "planning_excluded_executors",
            excluded_executors,
        )
        if not isinstance(self.allow_model_only_fallback, bool):
            raise TypeError("allow_model_only_fallback must be a boolean")
        edges: set[tuple[str, str]] = set()
        for edge in self.satisfied_tool_dependency_edges:
            if not isinstance(edge, (tuple, list)) or len(edge) != 2:
                raise TypeError(
                    "satisfied tool dependency edges must be tool/dependency pairs"
                )
            tool_name = str(edge[0]).strip()
            dependency_name = str(edge[1]).strip()
            if not tool_name or not dependency_name:
                raise ValueError(
                    "satisfied tool dependency edge names must be non-empty"
                )
            edges.add((tool_name, dependency_name))
        object.__setattr__(
            self,
            "satisfied_tool_dependency_edges",
            frozenset(edges),
        )


@dataclass(frozen=True, slots=True)
class ResponseConstraints:
    """Host-owned structural limits for a model's final response."""

    exact_top_level_item_count: int | None = None

    def __post_init__(self) -> None:
        value = self.exact_top_level_item_count
        if value is None:
            return
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("exact response item count must be an integer")
        if not 1 <= value <= 100:
            raise ValueError("exact response item count must be between 1 and 100")


@dataclass(frozen=True, slots=True)
class ResponseValidationResult:
    """Business-agnostic result returned by an injected response validator.

    An empty result accepts the response.  A rejected result carries a stable
    machine-readable code plus trusted repair guidance supplied by the host
    adapter; Core only orchestrates withholding and one bounded retry.
    """

    violation_code: str | None = None
    repair_guidance: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        code = _optional_text(self.violation_code)
        guidance = _optional_text(self.repair_guidance)
        if (code is None) != (guidance is None):
            raise ValueError(
                "response validation rejection requires both a violation "
                "code and repair guidance"
            )
        object.__setattr__(self, "violation_code", code)
        object.__setattr__(self, "repair_guidance", guidance)
        object.__setattr__(self, "details", freeze_json_mapping(self.details))

    @property
    def accepted(self) -> bool:
        return self.violation_code is None


@dataclass(frozen=True, slots=True)
class PlanningCapabilities:
    available_tool_names: frozenset[str] = frozenset()
    model_supports_tools: bool = True
    planning_context_blocks: tuple[ContextBlock, ...] = ()
    tool_guidance: Mapping[str, Any] = field(default_factory=dict)
    constraints: PlanningConstraints = field(default_factory=PlanningConstraints)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "available_tool_names",
            text_frozenset(self.available_tool_names),
        )
        object.__setattr__(self, "model_supports_tools", bool(self.model_supports_tools))
        planning_context = tuple(self.planning_context_blocks)
        if any(
            not isinstance(block, ContextBlock)
            for block in planning_context
        ):
            raise TypeError(
                "planning context blocks must contain ContextBlock values"
            )
        names = tuple(block.name for block in planning_context)
        if len(names) != len(set(names)):
            raise ValueError("planning context block names must be unique")
        object.__setattr__(
            self,
            "planning_context_blocks",
            planning_context,
        )
        object.__setattr__(
            self,
            "tool_guidance",
            freeze_json_mapping(self.tool_guidance),
        )
        if not isinstance(self.constraints, PlanningConstraints):
            raise TypeError("planning constraints must be PlanningConstraints")



@dataclass(frozen=True, slots=True)
class PlannerLimits:
    max_steps: int | None = None
    max_step_id_chars: int = 48
    max_title_chars: int = 48
    max_goal_chars: int = 160
    max_tool_steps: int = 4
    max_repair_attempts: int = 1
    result_capacity_target_tokens: int | None = None
    attempt_timeout_ms: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "max_step_id_chars",
            "max_title_chars",
            "max_goal_chars",
        ):
            object.__setattr__(self, name, positive_int(
                getattr(self, name), name
            ))
        object.__setattr__(self, "max_steps", optional_positive_int(
            self.max_steps, "max_steps"
        ))
        object.__setattr__(
            self,
            "result_capacity_target_tokens",
            optional_positive_int(
                self.result_capacity_target_tokens,
                "result_capacity_target_tokens",
            ),
        )
        object.__setattr__(
            self,
            "attempt_timeout_ms",
            optional_positive_int(
                self.attempt_timeout_ms,
                "attempt_timeout_ms",
            ),
        )
        max_tool_steps = int(self.max_tool_steps)
        if max_tool_steps < 0 or (
            self.max_steps is not None
            and max_tool_steps > self.max_steps
        ):
            raise ValueError(
                "max_tool_steps must be non-negative and must not exceed "
                "max_steps when configured"
            )
        object.__setattr__(self, "max_tool_steps", max_tool_steps)
        max_repair_attempts = int(self.max_repair_attempts)
        if max_repair_attempts < 0 or max_repair_attempts > 3:
            raise ValueError(
                "max_repair_attempts must be between zero and three"
            )
        object.__setattr__(
            self,
            "max_repair_attempts",
            max_repair_attempts,
        )


@dataclass(frozen=True, slots=True)
class PlanningResult:
    kind: PlanningKind
    work_plan: WorkPlan
    reason: str | None = None
    model: str | None = None
    model_call_count: int = 0
    model_call_parameters: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", PlanningKind(self.kind))
        if not isinstance(self.work_plan, WorkPlan):
            raise TypeError("planning result work_plan must be WorkPlan")
        object.__setattr__(self, "reason", _optional_text(self.reason))
        object.__setattr__(self, "model", _optional_text(self.model))
        call_count = non_negative_int(
            self.model_call_count, "planning model call count"
        )
        parameters = tuple(
            freeze_json_mapping(item)
            for item in self.model_call_parameters
        )
        if parameters:
            call_count = len(parameters)
        object.__setattr__(self, "model_call_count", call_count)
        object.__setattr__(self, "model_call_parameters", parameters)


@dataclass(frozen=True, slots=True)
class PlanningTurn:
    """Runtime evidence supplied when the planner revises future work.

    Completed steps are immutable history. ``messages`` includes the trusted
    tool-result continuation accumulated by the runtime, allowing the planner
    to choose the next transition from observed results instead of committing
    the whole run before execution starts.
    """

    revision: int
    round_number: int
    remaining_model_rounds: int
    messages: tuple[AgentMessage, ...]
    completed_steps: tuple[TaskStep, ...] = ()
    last_tool_outcome: ToolBatchOutcome = ToolBatchOutcome.COMPLETED

    def __post_init__(self) -> None:
        object.__setattr__(self, "revision", positive_int(
            self.revision, "planning turn revision"
        ))
        object.__setattr__(self, "round_number", positive_int(
            self.round_number, "planning turn round number"
        ))
        object.__setattr__(self, "remaining_model_rounds", non_negative_int(
            self.remaining_model_rounds, "remaining model rounds"
        ))
        object.__setattr__(self, "messages", tuple(self.messages))
        object.__setattr__(self, "completed_steps", tuple(self.completed_steps))
        object.__setattr__(
            self,
            "last_tool_outcome",
            ToolBatchOutcome(self.last_tool_outcome),
        )
