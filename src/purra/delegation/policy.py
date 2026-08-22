"""Host-configured limits for model-defined delegation."""

from __future__ import annotations

from dataclasses import dataclass

from purra.contracts import DelegationContextMode, ToolExecutionMode
from purra.normalization import positive_int


@dataclass(frozen=True, slots=True)
class DelegationPolicy:
    """Bounds model-defined Agents without granting them new authority."""

    max_agents_per_call: int = 3
    max_parallel: int = 3
    max_agent_name_chars: int = 64
    max_title_chars: int = 120
    max_instruction_chars: int = 4_000
    max_objective_chars: int = 4_000

    def __post_init__(self) -> None:
        for name in (
            "max_agents_per_call",
            "max_parallel",
            "max_agent_name_chars",
            "max_title_chars",
            "max_instruction_chars",
            "max_objective_chars",
        ):
            object.__setattr__(self, name, positive_int(getattr(self, name), name))
        if self.max_parallel > self.max_agents_per_call:
            raise ValueError("max_parallel cannot exceed max_agents_per_call")

    @property
    def context_mode(self) -> DelegationContextMode:
        return DelegationContextMode.ISOLATED

    @property
    def tool_mode(self) -> ToolExecutionMode:
        return ToolExecutionMode.READ

    @property
    def allows_recursive_delegation(self) -> bool:
        return False

    def snapshot_mapping(self) -> dict[str, object]:
        return {
            "enabled": True,
            "maxAgentsPerCall": self.max_agents_per_call,
            "maxParallel": self.max_parallel,
            "maxAgentNameChars": self.max_agent_name_chars,
            "maxTitleChars": self.max_title_chars,
            "maxInstructionChars": self.max_instruction_chars,
            "maxObjectiveChars": self.max_objective_chars,
            "contextMode": self.context_mode.value,
            "toolMode": self.tool_mode.value,
            "allowsRecursiveDelegation": self.allows_recursive_delegation,
        }


__all__ = ["DelegationPolicy"]
