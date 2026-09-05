"""Private control protocol for adaptive Planner activation."""

from __future__ import annotations

from dataclasses import dataclass

import json
from collections.abc import Sequence

from purra.contracts import PlanningMode, RunId, ToolCall, ToolSchema


AUTO_PLANNING_TOOL_NAME = "request_plan"
AUTO_REMAINING_PLANNING_TOOL_NAME = "request_remaining_plan"
AUTO_PLANNING_TOOL_SCHEMA = ToolSchema(
    name=AUTO_PLANNING_TOOL_NAME,
    description=(
        "Request a governed execution plan before any business tool runs. "
        "Use only when the task needs at least three distinct user-visible "
        "semantic steps or must be validated for authority, budget, approval, "
        "or durable execution. Do not use for direct answers, work with fewer "
        "than three real steps, or ordinary read-only tool use."
    ),
    parameters={
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    display_names={"en": "Plan execution", "zh-CN": "制定执行计划"},
)
AUTO_REMAINING_PLANNING_TOOL_SCHEMA = ToolSchema(
    name=AUTO_REMAINING_PLANNING_TOOL_NAME,
    description=(
        "Request a governed plan for the remaining work after earlier business "
        "tool results changed what must happen next. Use only when the rest of "
        "the task now needs multiple dependent actions or a planning-required "
        "tool. Do not repeat completed work."
    ),
    parameters={
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    display_names={"en": "Plan remaining work", "zh-CN": "规划剩余工作"},
)
AUTO_PLANNING_TOOL_SCHEMAS = (
    AUTO_PLANNING_TOOL_SCHEMA,
    AUTO_REMAINING_PLANNING_TOOL_SCHEMA,
)


@dataclass(frozen=True, slots=True)
class AutoPlanningRequest:
    """Private pause receipt emitted before any requested tool is executed."""

    run_id: RunId | None
    model: str
    round_count: int
    trigger: str
    requested_tool_names: tuple[str, ...] = ()
    phase: str = "initial"
    resume_checkpoint: object | None = None


@dataclass(frozen=True, slots=True)
class PlanningActivationResolution:
    trigger: str | None = None
    requested_tool_names: tuple[str, ...] = ()
    error_code: str | None = None


def resolve_planning_activation(
    calls: Sequence[ToolCall],
    *,
    mode: PlanningMode,
    planning_available: bool,
    planning_required_tool_names: frozenset[str],
    initial_planning_open: bool = True,
) -> PlanningActivationResolution:
    """Resolve the private pre-effect control protocol deterministically."""

    mode = PlanningMode(mode)
    calls = tuple(calls)
    if not calls:
        return PlanningActivationResolution()
    initial_control_calls = tuple(
        call for call in calls if call.name == AUTO_PLANNING_TOOL_NAME
    )
    remaining_control_calls = tuple(
        call for call in calls
        if call.name == AUTO_REMAINING_PLANNING_TOOL_NAME
    )
    control_calls = (*initial_control_calls, *remaining_control_calls)
    required_names = tuple(sorted(
        {call.name for call in calls} & planning_required_tool_names
    ))
    if mode is PlanningMode.PLANNED:
        return PlanningActivationResolution(
            error_code=(
                "invalid_planning_control_call" if control_calls else None
            )
        )
    if mode is PlanningMode.REACTIVE:
        return PlanningActivationResolution(
            error_code=(
                "planning_required"
                if control_calls or required_names
                else None
            )
        )
    if not control_calls and not required_names:
        return PlanningActivationResolution()
    if not planning_available:
        return PlanningActivationResolution(error_code="planning_unavailable")
    if control_calls and (
        len(control_calls) != 1
        or len(calls) != 1
        or not _empty_arguments(control_calls[0].arguments_json)
    ):
        return PlanningActivationResolution(
            error_code="invalid_planning_control_call"
        )
    if (
        initial_control_calls and not initial_planning_open
    ) or (
        remaining_control_calls and initial_planning_open
    ):
        return PlanningActivationResolution(
            error_code="invalid_planning_control_call"
        )
    return PlanningActivationResolution(
        trigger=(
            "model_requested"
            if initial_control_calls
            else "remaining_model_requested"
            if remaining_control_calls
            else "tool_required"
            if initial_planning_open
            else "remaining_tool_required"
        ),
        requested_tool_names=required_names,
    )


def _empty_arguments(value: str) -> bool:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return False
    return isinstance(parsed, dict) and not parsed


__all__ = [
    "AUTO_PLANNING_TOOL_NAME",
    "AUTO_PLANNING_TOOL_SCHEMA",
    "AUTO_REMAINING_PLANNING_TOOL_NAME",
    "AUTO_REMAINING_PLANNING_TOOL_SCHEMA",
    "AUTO_PLANNING_TOOL_SCHEMAS",
    "AutoPlanningRequest",
    "PlanningActivationResolution",
    "resolve_planning_activation",
]
