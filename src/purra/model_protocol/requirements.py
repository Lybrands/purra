"""Task-owned capability requirements evaluated before a Run exists."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from purra.errors import UnsupportedModelFeatureError
from purra.model_protocol.capabilities import (
    FeatureSupport,
    ModelCapabilitySnapshot,
)

if TYPE_CHECKING:
    from purra.contracts.enums import ReasoningMode


class FeatureRequirement(StrEnum):
    REQUIRED = "required"
    OPTIONAL = "optional"
    DISABLED = "disabled"


_STRUCTURED_OUTPUT_LEVELS = {
    "none": 0,
    "unknown": 0,
    "json_object": 1,
    "json_schema": 2,
}


@dataclass(frozen=True, slots=True)
class TaskCapabilityRequirements:
    reasoning_mode: "ReasoningMode"
    tool_calling: FeatureRequirement = FeatureRequirement.OPTIONAL
    structured_output_level: str = "none"
    streaming_required: bool = False
    cancellation_required: bool = False

    def __post_init__(self) -> None:
        mode = str(getattr(self.reasoning_mode, "value", self.reasoning_mode) or "")
        if mode not in {"default", "enabled", "disabled"}:
            raise ValueError("task reasoning mode must be default, enabled, or disabled")
        object.__setattr__(
            self,
            "tool_calling",
            FeatureRequirement(self.tool_calling),
        )
        level = str(self.structured_output_level or "").strip().lower()
        if level not in _STRUCTURED_OUTPUT_LEVELS:
            raise ValueError("unsupported structured output requirement")
        object.__setattr__(self, "structured_output_level", level)
        object.__setattr__(self, "streaming_required", bool(self.streaming_required))
        object.__setattr__(
            self,
            "cancellation_required",
            bool(self.cancellation_required),
        )


def preflight_capabilities(
    snapshot: ModelCapabilitySnapshot,
    requirements: TaskCapabilityRequirements,
) -> None:
    """Reject an incompatible task before it can create execution state."""

    reasons: list[str] = []
    if not snapshot.actionable:
        reasons.append("profile_not_actionable")
    if not snapshot.protocol.reasoning_mode_is_supported(
        requirements.reasoning_mode
    ):
        reasons.append("reasoning_mode")
    if (
        requirements.tool_calling is FeatureRequirement.REQUIRED
        and snapshot.protocol.tool_calling is not FeatureSupport.SUPPORTED
    ):
        reasons.append("tool_calling")
    available_level = _STRUCTURED_OUTPUT_LEVELS.get(
        snapshot.protocol.json_schema_level,
        0,
    )
    required_level = _STRUCTURED_OUTPUT_LEVELS[
        requirements.structured_output_level
    ]
    if available_level < required_level:
        reasons.append("structured_output")
    if (
        requirements.streaming_required
        and snapshot.protocol.streaming is not FeatureSupport.SUPPORTED
    ):
        reasons.append("streaming")
    if (
        requirements.cancellation_required
        and snapshot.protocol.cancellation is not FeatureSupport.SUPPORTED
    ):
        reasons.append("cancellation")
    if reasons:
        raise UnsupportedModelFeatureError(
            "model capability snapshot does not satisfy task requirements: "
            + ", ".join(reasons),
            code="model_capability_incompatible",
            retryable=False,
        )


__all__ = [
    "FeatureRequirement",
    "TaskCapabilityRequirements",
    "preflight_capabilities",
]
