"""Compile an ExecutionRecipe into durable task unit specs."""

from __future__ import annotations

from collections.abc import Sequence

from purra.contracts import ExecutionRecipe
from purra.json_values import thaw_json_mapping
from purra.long_tasks.contracts import LongTaskRecord, LongTaskUnitSpec


def compile_recipe_units(
    recipe: ExecutionRecipe,
    covered_step_ids: Sequence[str],
) -> tuple[LongTaskUnitSpec, ...]:
    covered = frozenset(covered_step_ids)
    mapped: set[str] = set()
    units: list[LongTaskUnitSpec] = []
    for position, step in enumerate(recipe.steps):
        plan_step_id = step.plan_step_id or step.id
        if plan_step_id not in covered:
            raise ValueError(
                "execution recipe maps to an unadmitted plan step: "
                + plan_step_id
            )
        mapped.add(plan_step_id)
        units.append(LongTaskUnitSpec(
            id=step.id,
            position=position,
            dependencies=step.depends_on,
            input_ref=step.input_ref,
            max_attempts=step.max_attempts,
            metadata={
                **thaw_json_mapping(step.metadata),
                "unitKind": step.kind,
                "executor": step.executor or step.kind,
                "plannerStepId": plan_step_id,
            },
        ))
    missing = covered - mapped
    if missing:
        raise ValueError(
            "execution recipe does not implement admitted plan steps: "
            + ", ".join(sorted(missing))
        )
    return tuple(units)


def require_same_recipe(
    task: LongTaskRecord,
    recipe: ExecutionRecipe,
) -> None:
    if thaw_json_mapping(task.metadata.get("recipe") or {}) != recipe.to_metadata():
        raise RuntimeError("durable_task_recipe_conflict")


def same_optional_text(left: object, right: object) -> bool:
    return (
        None if left is None else str(left).strip()
    ) == (
        None if right is None else str(right).strip()
    )
