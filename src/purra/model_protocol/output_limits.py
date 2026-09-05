"""Resolve one invocation's generation allowance and result-capacity target.

The Provider generation allowance and the amount of official response content
needed by a workflow are different quantities. Reasoning models may charge
private reasoning, public content, and tool arguments to one shared Provider
limit. A single ``max_tokens`` value therefore cannot represent both facts.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from purra.errors import UnsupportedModelFeatureError
from purra.model_protocol.capabilities import ModelCapabilitySnapshot
from purra.normalization import positive_int


class GenerationBudgetSource(StrEnum):
    USER = "user"
    MODEL_PROFILE = "model_profile"
    CONTEXT_CAPACITY = "context_capacity"


class ResultCapacitySource(StrEnum):
    USER = "user"
    WORKFLOW_POLICY = "workflow_policy"


@dataclass(frozen=True, slots=True)
class InvocationOutputBudget:
    """Independent Provider-generation and result-capacity quantities.

    The optional result capacity is a sizing and diagnostic target. It is not
    a Provider guarantee and is not a minimum amount of actual output.
    """

    max_generation_tokens: int
    generation_source: GenerationBudgetSource
    profile_max_generation_tokens: int
    requested_user_max_generation_tokens: int | None = None
    result_capacity_target_tokens: int | None = None
    result_capacity_source: ResultCapacitySource | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "max_generation_tokens", positive_int(
            self.max_generation_tokens,
            "invocation max generation tokens",
        ))
        object.__setattr__(
            self,
            "generation_source",
            GenerationBudgetSource(self.generation_source),
        )
        object.__setattr__(self, "profile_max_generation_tokens", positive_int(
            self.profile_max_generation_tokens,
            "profile max generation tokens",
        ))
        if self.max_generation_tokens > self.profile_max_generation_tokens:
            raise ValueError("invocation generation budget exceeds model profile")
        requested_user_maximum = self.requested_user_max_generation_tokens
        if requested_user_maximum is not None:
            requested_user_maximum = positive_int(
                requested_user_maximum,
                "requested user max generation tokens",
            )
            if requested_user_maximum > self.profile_max_generation_tokens:
                raise ValueError("requested user generation limit exceeds model profile")
            if self.max_generation_tokens > requested_user_maximum:
                raise ValueError("effective generation allowance exceeds the user limit")
        if self.generation_source is GenerationBudgetSource.USER:
            if requested_user_maximum != self.max_generation_tokens:
                raise ValueError(
                    "user-limited generation requires the exact requested user limit"
                )
        elif (
            self.generation_source is GenerationBudgetSource.MODEL_PROFILE
            and requested_user_maximum is not None
        ):
            raise ValueError(
                "model-profile generation cannot discard a user limit"
            )
        object.__setattr__(
            self,
            "requested_user_max_generation_tokens",
            requested_user_maximum,
        )
        target = self.result_capacity_target_tokens
        if target is not None:
            target = positive_int(target, "invocation result capacity target")
            if target > self.max_generation_tokens:
                raise ValueError(
                    "result capacity target exceeds the generation budget"
                )
            if self.result_capacity_source is None:
                raise ValueError("result capacity target requires its own source")
            object.__setattr__(
                self,
                "result_capacity_source",
                ResultCapacitySource(self.result_capacity_source),
            )
        elif self.result_capacity_source is not None:
            raise ValueError("result capacity source requires a target")
        object.__setattr__(self, "result_capacity_target_tokens", target)

    @property
    def non_result_headroom_tokens(self) -> int | None:
        if self.result_capacity_target_tokens is None:
            return None
        return self.max_generation_tokens - self.result_capacity_target_tokens

    def to_mapping(self) -> dict[str, object]:
        return {
            "maxGenerationTokens": self.max_generation_tokens,
            "generationSource": self.generation_source.value,
            "profileMaxGenerationTokens": self.profile_max_generation_tokens,
            "requestedUserMaxGenerationTokens": (
                self.requested_user_max_generation_tokens
            ),
            "resultCapacityTargetTokens": self.result_capacity_target_tokens,
            "resultCapacitySource": (
                self.result_capacity_source.value
                if self.result_capacity_source is not None
                else None
            ),
            "nonResultHeadroomTokens": self.non_result_headroom_tokens,
        }


def resolve_invocation_output_budget(
    snapshot: ModelCapabilitySnapshot,
    *,
    max_generation_tokens: int | None,
    generation_source: GenerationBudgetSource | None = None,
    result_capacity_target_tokens: int | None = None,
    result_capacity_source: ResultCapacitySource | None = None,
) -> InvocationOutputBudget:
    """Validate a deliberately two-dimensional invocation budget.

    The profile may supply the total Provider maximum. A workflow capacity
    target is optional and has separate provenance; it cannot shrink the
    Provider generation allowance.
    """

    profile_maximum = snapshot.max_generation_tokens
    if profile_maximum is None:
        raise UnsupportedModelFeatureError(
            "model profile does not declare a verified generation limit",
            code="model_generation_limit_unknown",
            retryable=False,
        )
    try:
        generated = (
            profile_maximum
            if max_generation_tokens is None
            else positive_int(
                max_generation_tokens,
                "maximum generated tokens",
            )
        )
    except (TypeError, ValueError) as error:
        raise UnsupportedModelFeatureError(
            "invocation output budget values must be positive integers",
            code="model_output_budget_invalid",
            retryable=False,
        ) from error
    if generated > profile_maximum:
        raise UnsupportedModelFeatureError(
            "generation budget exceeds the model profile maximum",
            code="model_generation_limit_exceeded",
            retryable=False,
        )
    selected_source = (
        GenerationBudgetSource.MODEL_PROFILE
        if max_generation_tokens is None
        else GenerationBudgetSource(generation_source or GenerationBudgetSource.USER)
    )
    if max_generation_tokens is None and generation_source not in {
        None,
        GenerationBudgetSource.MODEL_PROFILE,
    }:
        raise UnsupportedModelFeatureError(
            "generation source conflicts with the model-profile allowance",
            code="model_output_budget_invalid",
            retryable=False,
        )
    if max_generation_tokens is not None and generation_source not in {
        None,
        GenerationBudgetSource.USER,
    }:
        raise UnsupportedModelFeatureError(
            "explicit generation allowance must be attributed to the user",
            code="model_output_budget_invalid",
            retryable=False,
        )
    try:
        return InvocationOutputBudget(
            max_generation_tokens=generated,
            generation_source=selected_source,
            profile_max_generation_tokens=profile_maximum,
            requested_user_max_generation_tokens=(
                generated if max_generation_tokens is not None else None
            ),
            result_capacity_target_tokens=result_capacity_target_tokens,
            result_capacity_source=result_capacity_source,
        )
    except (TypeError, ValueError) as error:
        raise UnsupportedModelFeatureError(
            str(error),
            code="model_output_budget_invalid",
            retryable=False,
        ) from error


def constrain_output_budget_to_context(
    budget: InvocationOutputBudget,
    *,
    max_generation_tokens: int,
) -> InvocationOutputBudget:
    """Apply a physical context ceiling without reusing workflow capacity."""

    if not isinstance(budget, InvocationOutputBudget):
        raise TypeError("context constraint requires an InvocationOutputBudget")
    context_maximum = positive_int(
        max_generation_tokens,
        "context maximum generation tokens",
    )
    if budget.max_generation_tokens <= context_maximum:
        return budget
    target = budget.result_capacity_target_tokens
    if target is not None and target > context_maximum:
        raise UnsupportedModelFeatureError(
            "result capacity target does not fit the selected context window",
            code="model_result_capacity_incompatible",
            retryable=False,
        )
    return InvocationOutputBudget(
        max_generation_tokens=context_maximum,
        generation_source=GenerationBudgetSource.CONTEXT_CAPACITY,
        profile_max_generation_tokens=budget.profile_max_generation_tokens,
        requested_user_max_generation_tokens=(
            budget.requested_user_max_generation_tokens
        ),
        result_capacity_target_tokens=target,
        result_capacity_source=budget.result_capacity_source,
    )


def require_output_budget_matches_request(
    budget: InvocationOutputBudget,
    snapshot: ModelCapabilitySnapshot,
    requested_user_max_generation_tokens: int | None,
) -> None:
    """Reject a budget assembled from different model/user constraints."""

    if not isinstance(budget, InvocationOutputBudget):
        raise TypeError("request output budget must be InvocationOutputBudget")
    if budget.profile_max_generation_tokens != snapshot.max_generation_tokens:
        raise ValueError("output budget model profile does not match the request")
    if (
        budget.requested_user_max_generation_tokens
        != requested_user_max_generation_tokens
    ):
        raise ValueError("output budget user limit does not match the request")
    unconstrained = (
        requested_user_max_generation_tokens
        if requested_user_max_generation_tokens is not None
        else snapshot.max_generation_tokens
    )
    if unconstrained is None:
        raise ValueError("request model profile has no generation limit")
    if budget.generation_source is GenerationBudgetSource.CONTEXT_CAPACITY:
        if budget.max_generation_tokens >= unconstrained:
            raise ValueError("context-limited budget must reduce the request allowance")
    elif budget.max_generation_tokens != unconstrained:
        raise ValueError("output budget effective allowance does not match the request")


__all__ = [
    "constrain_output_budget_to_context",
    "GenerationBudgetSource",
    "InvocationOutputBudget",
    "ResultCapacitySource",
    "require_output_budget_matches_request",
    "resolve_invocation_output_budget",
]
