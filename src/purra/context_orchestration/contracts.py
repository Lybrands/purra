"""Provider-neutral contracts for context compression orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from purra.context_orchestration.ledger import ContextCompactionBudget
from purra.contracts import AgentRunRequest
from purra.json_values import freeze_json_mapping
from purra.normalization import (
    non_negative_int,
    optional_positive_int,
    positive_int,
    required_text,
)


@dataclass(frozen=True, slots=True)
class ContextCompressionSettings:
    """Generic Core timing and fallback-window settings.

    These values deliberately contain no semantic-summary policy.  Core uses
    them only to decide when a reduction hook must be offered and, when no
    hook is installed, how many recent raw messages the built-in trimmer may
    retain before fitting them to the actual token budget.
    """

    trigger_ratio: float = 0.85
    default_keep_recent_messages: int = 20

    def __post_init__(self) -> None:
        trigger = float(self.trigger_ratio)
        if not 0 < trigger <= 1:
            raise ValueError(
                "trigger_ratio must be greater than zero and at most one"
            )
        object.__setattr__(self, "trigger_ratio", trigger)
        object.__setattr__(self, "default_keep_recent_messages", positive_int(
            self.default_keep_recent_messages,
            "default_keep_recent_messages",
        ))


@dataclass(frozen=True, slots=True)
class ContextCompressionRequest:
    """Immutable facts passed from Core to an application compression hook."""

    request: AgentRunRequest
    budget: ContextCompactionBudget
    message_tokens: int
    projected_input_tokens: int
    available_message_tokens: int
    pressure_ratio: float
    compression_required: bool
    trigger_reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.request, AgentRunRequest):
            raise TypeError("compression request requires an AgentRunRequest")
        if not isinstance(self.budget, ContextCompactionBudget):
            raise TypeError("compression request requires a budget snapshot")
        for name in (
            "message_tokens",
            "projected_input_tokens",
            "available_message_tokens",
        ):
            object.__setattr__(self, name, non_negative_int(
                getattr(self, name), name
            ))
        pressure = float(self.pressure_ratio)
        if pressure < 0:
            raise ValueError("pressure_ratio must be non-negative")
        object.__setattr__(self, "pressure_ratio", pressure)
        object.__setattr__(
            self,
            "compression_required",
            bool(self.compression_required),
        )
        object.__setattr__(self, "trigger_reason", required_text(
            self.trigger_reason, "trigger_reason"
        ))


@dataclass(frozen=True, slots=True)
class ConversationCompactionResult:
    """Validated output of one Core compaction decision and execution."""

    request: AgentRunRequest
    outcome: str
    compression_state_version: int | None = None
    compacted_turn_count: int = 0
    retained_raw_turn_count: int = 0
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.request, AgentRunRequest):
            raise TypeError("compaction request must be an AgentRunRequest")
        object.__setattr__(self, "outcome", required_text(
            self.outcome, "compaction outcome"
        ))
        object.__setattr__(
            self,
            "compression_state_version",
            optional_positive_int(
                self.compression_state_version,
                "compression_state_version",
            ),
        )
        for name in ("compacted_turn_count", "retained_raw_turn_count"):
            object.__setattr__(self, name, non_negative_int(
                getattr(self, name), name
            ))
        object.__setattr__(
            self,
            "diagnostics",
            freeze_json_mapping(self.diagnostics),
        )


__all__ = [
    "ContextCompressionRequest",
    "ContextCompressionSettings",
    "ConversationCompactionResult",
]
