"""Host-configured limits for recursive Child Agents."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from purra.errors import ContractViolationError
from purra.normalization import positive_int


@dataclass(frozen=True, slots=True)
class AgentTreePolicy:
    """Bound Child Agent creation without granting additional authority."""

    max_children_per_call: int = 3
    max_parallel_runs: int = 3
    max_agent_name_chars: int = 64
    max_title_chars: int = 120
    max_instruction_chars: int = 4_000
    max_objective_chars: int = 4_000
    max_depth: int = 3
    max_agents_per_root: int = 16
    allow_recursive_agents: bool = False

    def __post_init__(self) -> None:
        for name in (
            "max_children_per_call",
            "max_parallel_runs",
            "max_agent_name_chars",
            "max_title_chars",
            "max_instruction_chars",
            "max_objective_chars",
            "max_depth",
            "max_agents_per_root",
        ):
            object.__setattr__(self, name, positive_int(getattr(self, name), name))
        if self.max_parallel_runs > self.max_children_per_call:
            raise ValueError(
                "max_parallel_runs cannot exceed max_children_per_call"
            )
        if not isinstance(self.allow_recursive_agents, bool):
            raise TypeError("allow_recursive_agents must be boolean")

    def snapshot_mapping(self) -> dict[str, object]:
        return {
            "enabled": True,
            "maxChildrenPerCall": self.max_children_per_call,
            "maxParallelRuns": self.max_parallel_runs,
            "maxAgentNameChars": self.max_agent_name_chars,
            "maxTitleChars": self.max_title_chars,
            "maxInstructionChars": self.max_instruction_chars,
            "maxObjectiveChars": self.max_objective_chars,
            "maxDepth": self.max_depth,
            "maxAgentsPerRoot": self.max_agents_per_root,
            "allowsRecursiveAgents": self.allow_recursive_agents,
        }

    def validate_children(
        self,
        value: object,
    ) -> tuple[dict[str, Any], ...]:
        if (
            not isinstance(value, Sequence)
            or isinstance(value, (str, bytes, bytearray))
            or not 1 <= len(value) <= self.max_children_per_call
        ):
            raise ContractViolationError(
                "children must contain between one and "
                f"{self.max_children_per_call} Child Agents",
                code="invalid_child_agent_batch",
            )
        children: list[dict[str, Any]] = []
        names: set[str] = set()
        for raw in value:
            if not isinstance(raw, Mapping):
                raise ContractViolationError(
                    "Child Agent definition must be an object",
                    code="invalid_child_agent",
                )
            name = _bounded_text(
                raw.get("name"),
                "name",
                self.max_agent_name_chars,
            )
            if name in names:
                raise ContractViolationError(
                    "Child Agent names must be unique within one call",
                    code="duplicate_child_agent",
                )
            names.add(name)
            input_payload = raw.get("input")
            if input_payload is not None and not isinstance(input_payload, Mapping):
                raise ContractViolationError(
                    "Child Agent input must be an object",
                    code="invalid_child_agent",
                )
            required = raw.get("required", True)
            if not isinstance(required, bool):
                raise ContractViolationError(
                    "Child Agent required must be boolean",
                    code="invalid_child_agent",
                )
            priority = raw.get("priority", 0)
            if not isinstance(priority, int) or isinstance(priority, bool):
                raise ContractViolationError(
                    "Child Agent priority must be an integer",
                    code="invalid_child_agent",
                )
            children.append({
                "name": name,
                "title": _bounded_text(
                    raw.get("title"),
                    "title",
                    self.max_title_chars,
                ),
                "instruction": _bounded_text(
                    raw.get("instruction"),
                    "instruction",
                    self.max_instruction_chars,
                ),
                "objective": _bounded_text(
                    raw.get("objective"),
                    "objective",
                    self.max_objective_chars,
                ),
                "input": dict(input_payload or {}),
                "required": required,
                "priority": priority,
            })
        return tuple(children)


def _bounded_text(value: object, label: str, maximum: int) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > maximum:
        raise ContractViolationError(
            f"Child Agent {label} must contain between 1 and {maximum} characters",
            code="invalid_child_agent",
        )
    return normalized


__all__ = ["AgentTreePolicy"]
