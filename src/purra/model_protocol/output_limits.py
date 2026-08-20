"""Resolve the exact provider output limit for one model invocation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from purra.errors import UnsupportedModelFeatureError
from purra.model_protocol.capabilities import ModelCapabilitySnapshot
from purra.normalization import positive_int


class InvocationOutputLimitSource(StrEnum):
    USER_OVERRIDE = "user_override"
    MODEL_PROFILE = "model_profile"
    WORKFLOW_POLICY = "workflow_policy"


@dataclass(frozen=True, slots=True)
class InvocationOutputLimit:
    max_tokens: int
    source: InvocationOutputLimitSource
    profile_max_tokens: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "max_tokens", positive_int(
            self.max_tokens, "invocation max tokens"
        ))
        object.__setattr__(self, "source", InvocationOutputLimitSource(self.source))
        object.__setattr__(self, "profile_max_tokens", positive_int(
            self.profile_max_tokens, "profile max output tokens"
        ))
        if self.max_tokens > self.profile_max_tokens:
            raise ValueError("invocation output limit exceeds model profile")

    def to_mapping(self) -> dict[str, object]:
        return {
            "maxTokens": self.max_tokens,
            "source": self.source.value,
            "profileMaxTokens": self.profile_max_tokens,
        }


def resolve_invocation_output_limit(
    snapshot: ModelCapabilitySnapshot,
    explicit_user_override: int | None,
) -> InvocationOutputLimit:
    profile_maximum = snapshot.max_output_tokens
    if profile_maximum is None:
        raise UnsupportedModelFeatureError(
            "model profile does not declare a verified output limit",
            code="model_output_limit_unknown",
            retryable=False,
        )
    if explicit_user_override is None:
        return InvocationOutputLimit(
            max_tokens=profile_maximum,
            source=InvocationOutputLimitSource.MODEL_PROFILE,
            profile_max_tokens=profile_maximum,
        )
    try:
        requested = positive_int(
            explicit_user_override,
            "explicit model output limit",
        )
    except (TypeError, ValueError) as error:
        raise UnsupportedModelFeatureError(
            "explicit model output limit must be a positive integer",
            code="model_output_limit_invalid",
            retryable=False,
        ) from error
    if requested > profile_maximum:
        raise UnsupportedModelFeatureError(
            "explicit model output limit exceeds the model profile maximum",
            code="model_output_limit_exceeded",
            retryable=False,
        )
    return InvocationOutputLimit(
        max_tokens=requested,
        source=InvocationOutputLimitSource.USER_OVERRIDE,
        profile_max_tokens=profile_maximum,
    )


__all__ = [
    "InvocationOutputLimit",
    "InvocationOutputLimitSource",
    "resolve_invocation_output_limit",
]
