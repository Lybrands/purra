"""Deterministically expand planner actions from host-owned tool contracts."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Sequence

from purra.contracts import (
    PlanningConstraints,
    StepExecutor,
    StepStatus,
    StepType,
    ExecutionPlan,
    TaskStep,
    ToolExecutionMode,
    ToolSchema,
    WorkPlan,
    WorkStep,
)
from purra.errors import ContractViolationError
from purra.ports import ToolRegistration


@dataclass(frozen=True, slots=True)
class CompiledExecutionPlan:
    execution_plan: ExecutionPlan
    inserted_tool_names: tuple[str, ...] = ()
    lowered_tool_names: tuple[str, ...] = ()


def projected_planning_tool_names(
    registrations: Sequence[ToolRegistration],
    enabled_tool_names: frozenset[str],
) -> frozenset[str]:
    """Project runtime registrations onto stable Planner capabilities."""

    _planning_capabilities(registrations, enabled_tool_names)
    return frozenset(
        (
            registration.planning_capability.name
            if registration.planning_capability is not None
            else registration.schema.name
        )
        for registration in registrations
        if registration.schema.name in enabled_tool_names
    )


def projected_planning_tool_schemas(
    registrations: Sequence[ToolRegistration],
    enabled_tool_names: frozenset[str],
) -> dict[str, ToolSchema]:
    """Return one canonical schema per public planning capability."""

    capabilities = _planning_capabilities(
        registrations,
        enabled_tool_names,
    )
    return {
        name: members[0].planning_capability
        for name, members in capabilities.items()
        if members[0].planning_capability is not None
    }


def runtime_tool_names_for_planning_names(
    registrations: Sequence[ToolRegistration],
    enabled_tool_names: frozenset[str],
    planning_tool_names: frozenset[str],
) -> frozenset[str]:
    """Resolve selected public capabilities to executable runtime schemas."""

    capabilities = _planning_capabilities(
        registrations,
        enabled_tool_names,
    )
    by_name = {
        registration.schema.name: registration
        for registration in registrations
        if registration.schema.name in enabled_tool_names
    }
    available = frozenset(by_name) | frozenset(capabilities)
    unknown = planning_tool_names - available
    if unknown:
        raise ContractViolationError(
            "planning selection names unavailable capabilities: "
            + ", ".join(sorted(unknown))
        )
    resolved: set[str] = set()
    for name in planning_tool_names:
        if name in capabilities:
            resolved.update(
                registration.schema.name
                for registration in capabilities[name]
            )
        elif name in by_name:
            resolved.add(name)
    return frozenset(resolved)


def project_completed_steps_for_planning(
    steps: Sequence[TaskStep],
    registrations: Sequence[ToolRegistration],
    enabled_tool_names: frozenset[str],
) -> tuple[TaskStep, ...]:
    """Hide successful private protocol nodes from dynamic Planner turns.

    A business capability becomes completed only when one of its terminal
    runtime tools completed.  A failed private node is surfaced immediately as
    a failed capability so recovery still has the evidence it needs.
    """

    capabilities = _planning_capabilities(
        registrations,
        enabled_tool_names,
    )
    by_runtime = {
        registration.schema.name: capability_name
        for capability_name, members in capabilities.items()
        for registration in members
    }
    terminal_names = {
        capability_name: _terminal_runtime_names(members)
        for capability_name, members in capabilities.items()
    }
    projected: list[TaskStep] = []
    for step in steps:
        runtime_name = next(iter(step.suggested_tools), "")
        capability_name = by_runtime.get(runtime_name)
        if capability_name is None:
            projected.append(step)
            continue
        if (
            step.status is StepStatus.DONE
            and runtime_name not in terminal_names[capability_name]
        ):
            continue
        projected.append(replace(
            step,
            suggested_tools=(capability_name,),
            protocol_private=False,
            planning_capability=capability_name,
        ))
    return tuple(projected)


def compile_work_plan(
    plan: WorkPlan,
    registrations: Sequence[ToolRegistration],
    *,
    constraints: PlanningConstraints = PlanningConstraints(),
    satisfied_tool_names: frozenset[str] = frozenset(),
    enabled_tool_names: frozenset[str] | None = None,
) -> CompiledExecutionPlan:
    """Lower public capabilities and insert runtime prerequisites.

    Public runtime tools without a planning capability may be named directly.
    Registrations with a ``planning_capability`` must be selected through that
    public capability, which Core deterministically expands into its private
    runtime protocol. Every lowered or synthesized step remains subject to
    normal runtime authority.
    """

    by_name = {item.schema.name: item for item in registrations}
    enabled = (
        frozenset(by_name)
        if enabled_tool_names is None
        else frozenset(enabled_tool_names)
    )
    capabilities = _planning_capabilities(registrations, enabled)
    existing_ids = {step.id for step in plan.steps}
    completed = set(satisfied_tool_names)
    completed.update(constraints.execution_satisfied_tool_names)
    completed.update(constraints.context_satisfied_tool_names)
    inserted: list[str] = []
    lowered: list[str] = []
    expanded: list[TaskStep] = []
    prerequisite_sequence = 0

    def append_prerequisites(tool_name: str, path: tuple[str, ...]) -> None:
        nonlocal prerequisite_sequence
        if tool_name in path:
            raise ContractViolationError(
                "tool context contract contains a prerequisite cycle: "
                + " -> ".join((*path, tool_name))
            )
        registration = by_name.get(tool_name)
        if registration is None:
            raise ContractViolationError(
                f"tool context contract names unavailable tool {tool_name!r}"
            )
        for dependency in registration.prerequisite_tools:
            if dependency in completed or (
                tool_name,
                dependency,
            ) in constraints.satisfied_tool_dependency_edges:
                continue
            if dependency in constraints.planning_excluded_tool_names:
                raise ContractViolationError(
                    f"tool {tool_name!r} requires planning-excluded tool "
                    f"{dependency!r}"
                )
            append_prerequisites(dependency, (*path, tool_name))
            if dependency in completed:
                continue
            dependency_registration = by_name[dependency]
            prerequisite_sequence += 1
            step_id = _unique_step_id(
                f"host-prerequisite-{dependency}-{prerequisite_sequence}",
                existing_ids,
            )
            existing_ids.add(step_id)
            expanded.append(TaskStep(
                id=step_id,
                # ToolPolicy.title is the host-owned, product-localized label.
                # Showing the internal tool name here leaked English protocol
                # identifiers such as ``Prepare getSourceCoveragePlan`` into
                # the user-facing execution progress UI.
                title=dependency_registration.policy.title,
                type=(
                    StepType.READ
                    if dependency_registration.policy.mode is ToolExecutionMode.READ
                    else StepType.ANALYZE
                ),
                executor=StepExecutor.TOOL,
                status=StepStatus.PENDING,
                risk_level=dependency_registration.policy.risk_level,
                suggested_tools=(dependency,),
                protocol_private=True,
                description=(
                    f"Host-inserted prerequisite for {tool_name}; derived from "
                    "the registered tool context contract."
                ),
            ))
            completed.add(dependency)
            inserted.append(dependency)

    for step in plan.steps:
        execution_step = _to_execution_step(step)
        if step.executor is not StepExecutor.TOOL:
            expanded.append(execution_step)
            continue
        if len(step.capability_names) != 1:
            raise ContractViolationError(
                "host plan compilation requires exactly one tool per step"
            )
        selected_name = step.capability_names[0]
        registration = by_name.get(selected_name)
        if registration is not None:
            if selected_name not in enabled:
                raise ContractViolationError(
                    f"plan names disabled tool {selected_name!r}"
                )
            if registration.planning_capability is not None:
                raise ContractViolationError(
                    "plan must select public planning capability "
                    f"{registration.planning_capability.name!r} instead of "
                    f"private runtime tool {selected_name!r}"
                )
            append_prerequisites(selected_name, ())
            completed.add(selected_name)
            expanded.append(execution_step)
            continue
        members = capabilities.get(selected_name)
        if members is None:
            raise ContractViolationError(
                f"plan names unavailable capability {selected_name!r}"
            )
        runtime_sequence = tuple(
            name
            for name in _ordered_runtime_names(members)
            if name not in completed
        )
        if not runtime_sequence:
            raise ContractViolationError(
                f"plan redundantly selects completed capability {selected_name!r}"
            )
        previous_step_id: str | None = None
        for index, runtime_name in enumerate(runtime_sequence):
            append_prerequisites(runtime_name, ())
            is_terminal = index == len(runtime_sequence) - 1
            runtime_registration = by_name[runtime_name]
            runtime_step_id = (
                step.id
                if is_terminal
                else _unique_step_id(
                    f"{step.id}-protocol-{index + 1}",
                    existing_ids,
                )
            )
            existing_ids.add(runtime_step_id)
            expanded.append(replace(
                execution_step,
                id=runtime_step_id,
                title=(
                    step.title
                    if is_terminal
                    else runtime_registration.policy.title
                ),
                risk_level=runtime_registration.policy.risk_level,
                suggested_tools=(runtime_name,),
                depends_on=(
                    (previous_step_id,)
                    if previous_step_id is not None
                    else step.depends_on
                ),
                description=(
                    step.description
                    if is_terminal
                    else "Host-private execution protocol for " + selected_name
                ),
                protocol_private=not is_terminal,
                planning_capability=selected_name,
            ))
            completed.add(runtime_name)
            lowered.append(runtime_name)
            previous_step_id = runtime_step_id

    return CompiledExecutionPlan(
        execution_plan=ExecutionPlan(
            title=plan.title,
            goal=plan.goal,
            task_spec=plan.task_spec,
            steps=tuple(expanded),
            work_step_ids=tuple(step.id for step in plan.steps),
        ),
        inserted_tool_names=tuple(inserted),
        lowered_tool_names=tuple(lowered),
    )


def _to_execution_step(step: WorkStep) -> TaskStep:
    return TaskStep(
        id=step.id,
        title=step.title,
        type=step.type,
        executor=step.executor,
        risk_level=step.risk_level,
        suggested_tools=step.capability_names,
        depends_on=step.depends_on,
        description=step.description,
    )


def _planning_capabilities(
    registrations: Sequence[ToolRegistration],
    enabled_tool_names: frozenset[str],
) -> dict[str, tuple[ToolRegistration, ...]]:
    runtime_names = {
        registration.schema.name
        for registration in registrations
        if registration.schema.name in enabled_tool_names
    }
    grouped: dict[str, list[ToolRegistration]] = {}
    schemas = {}
    for registration in registrations:
        capability = registration.planning_capability
        if (
            registration.schema.name not in enabled_tool_names
            or capability is None
        ):
            continue
        if capability.name in runtime_names:
            raise ContractViolationError(
                "planning capability collides with a runtime tool: "
                + capability.name
            )
        previous = schemas.get(capability.name)
        if previous is not None and previous != capability:
            raise ContractViolationError(
                "runtime tools declare conflicting planning capabilities: "
                + capability.name
            )
        schemas[capability.name] = capability
        grouped.setdefault(capability.name, []).append(registration)
    return {name: tuple(items) for name, items in grouped.items()}


def _ordered_runtime_names(
    registrations: Sequence[ToolRegistration],
) -> tuple[str, ...]:
    by_name = {item.schema.name: item for item in registrations}
    ordered: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visited:
            return
        if name in visiting:
            raise ContractViolationError(
                "planning capability contains a runtime dependency cycle"
            )
        visiting.add(name)
        for dependency in by_name[name].prerequisite_tools:
            if dependency in by_name:
                visit(dependency)
        visiting.remove(name)
        visited.add(name)
        ordered.append(name)

    for registration in registrations:
        visit(registration.schema.name)
    return tuple(ordered)


def _terminal_runtime_names(
    registrations: Sequence[ToolRegistration],
) -> frozenset[str]:
    names = {item.schema.name for item in registrations}
    prerequisites = {
        dependency
        for item in registrations
        for dependency in item.prerequisite_tools
        if dependency in names
    }
    return frozenset(names - prerequisites)


def _unique_step_id(candidate: str, existing: set[str]) -> str:
    normalized = candidate[:48]
    if normalized not in existing:
        return normalized
    index = 2
    while True:
        suffix = f"-{index}"
        value = normalized[: 48 - len(suffix)] + suffix
        if value not in existing:
            return value
        index += 1


__all__ = [
    "CompiledExecutionPlan",
    "compile_work_plan",
    "project_completed_steps_for_planning",
    "projected_planning_tool_names",
    "projected_planning_tool_schemas",
    "runtime_tool_names_for_planning_names",
]
