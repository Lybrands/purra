"""Typed budget snapshots shared by every context compaction phase."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from purra.normalization import non_negative_int, positive_int

class ContextCompactionPhase(str, Enum):
    PRE_PLANNING = "pre_planning"
    POST_PLANNING = "post_planning"
    MODEL_CALL = "model_call"


@dataclass(frozen=True, slots=True)
class ContextCompactionBudget:
    """One immutable view of the Core budget ledger for a compaction pass."""

    phase: ContextCompactionPhase
    provider_input_tokens: int
    context_tokens: int
    context_tokens_are_resolved: bool
    output_reserve_tokens: int
    planned_step_count: int = 0
    planned_tool_count: int = 0
    selected_tool_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "phase", ContextCompactionPhase(self.phase))
        for name in (
            "provider_input_tokens",
            "context_tokens",
            "output_reserve_tokens",
            "planned_step_count",
            "planned_tool_count",
            "selected_tool_count",
        ):
            normalizer = (
                positive_int
                if name in {"provider_input_tokens", "output_reserve_tokens"}
                else non_negative_int
            )
            object.__setattr__(self, name, normalizer(getattr(self, name), name))
        object.__setattr__(
            self,
            "context_tokens_are_resolved",
            bool(self.context_tokens_are_resolved),
        )


__all__ = ["ContextCompactionBudget", "ContextCompactionPhase"]
