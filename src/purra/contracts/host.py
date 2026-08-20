"""Opaque host association and deterministic execution-recipe contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from purra.normalization import (
    optional_text,
    positive_int,
    required_text,
)
from purra.json_values import freeze_json_mapping


@dataclass(frozen=True, slots=True)
class RunBinding:
    """Opaque host ownership attached to a Run without Core interpretation."""

    namespace: str
    aggregate_id: str
    command_id: str
    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("namespace", "aggregate_id", "command_id"):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"run binding {name}"
            ))
        object.__setattr__(
            self,
            "attributes",
            freeze_json_mapping(self.attributes),
        )


@dataclass(frozen=True, slots=True)
class ExecutionRecipeStep:
    """One host-authored mechanical step; its metadata remains opaque to Core."""

    id: str
    kind: str
    depends_on: tuple[str, ...] = ()
    input_ref: str | None = None
    executor: str | None = None
    plan_step_id: str | None = None
    max_attempts: int = 1
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        step_id = required_text(self.id, "execution recipe step id")
        kind = required_text(self.kind, "execution recipe step kind")
        dependencies = tuple(
            str(value or "").strip() for value in self.depends_on
        )
        if any(not value for value in dependencies):
            raise ValueError("execution recipe dependencies must be non-empty")
        if len(dependencies) != len(set(dependencies)):
            raise ValueError("execution recipe dependencies must be unique")
        if step_id in dependencies:
            raise ValueError("execution recipe step cannot depend on itself")
        object.__setattr__(self, "id", step_id)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "depends_on", dependencies)
        object.__setattr__(self, "input_ref", optional_text(self.input_ref))
        object.__setattr__(self, "executor", optional_text(self.executor))
        object.__setattr__(
            self,
            "plan_step_id",
            optional_text(self.plan_step_id),
        )
        object.__setattr__(
            self,
            "max_attempts",
            positive_int(
                self.max_attempts,
                "execution recipe step max_attempts",
            ),
        )
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))


@dataclass(frozen=True, slots=True)
class ExecutionRecipe:
    """Validated host-owned DAG for deterministic execution after admission."""

    kind: str
    steps: tuple[ExecutionRecipeStep, ...]
    max_parallelism: int = 1
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        kind = required_text(self.kind, "execution recipe kind")
        steps = tuple(self.steps)
        if not steps:
            raise ValueError("execution recipe requires at least one step")
        if not all(isinstance(step, ExecutionRecipeStep) for step in steps):
            raise TypeError("execution recipe steps must be ExecutionRecipeStep values")
        ids = tuple(step.id for step in steps)
        if len(ids) != len(set(ids)):
            raise ValueError("execution recipe step ids must be unique")
        known: set[str] = set()
        known_ids = set(ids)
        for step in steps:
            unknown = set(step.depends_on) - known_ids
            if unknown:
                raise ValueError(
                    "execution recipe names unknown dependencies: "
                    + ", ".join(sorted(unknown))
                )
            if not set(step.depends_on).issubset(known):
                raise ValueError(
                    "execution recipe steps must be topologically ordered"
                )
            known.add(step.id)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "steps", steps)
        object.__setattr__(self, "max_parallelism", positive_int(
            self.max_parallelism, "execution recipe max_parallelism"
        ))
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))

    def to_metadata(self) -> dict[str, Any]:
        return {
            **self.metadata,
            "kind": self.kind,
            "maxParallelism": self.max_parallelism,
            "steps": [
                {
                    **step.metadata,
                    "id": step.id,
                    "kind": step.kind,
                    "dependsOn": list(step.depends_on),
                    **({"inputRef": step.input_ref} if step.input_ref else {}),
                    **({"executor": step.executor} if step.executor else {}),
                    **(
                        {"plannerStepId": step.plan_step_id}
                        if step.plan_step_id
                        else {}
                    ),
                    "maxAttempts": step.max_attempts,
                }
                for step in self.steps
            ],
        }


__all__ = ["ExecutionRecipe", "ExecutionRecipeStep", "RunBinding"]
