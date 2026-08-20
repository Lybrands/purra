"""Stable recovery decisions shared by runtimes and diagnostic adapters.

These contracts intentionally carry only control metadata. Model text, tool
arguments, and tool results must never enter a recovery decision trace.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from purra.normalization import non_negative_int, required_text


class RecoveryAction(StrEnum):
    RETRY_MODEL = "retry_model"
    FALLBACK_PROVIDER_MODE = "fallback_provider_mode"
    REPLAN = "replan"


class RecoveryCause(StrEnum):
    PROVIDER_REQUIRED_TOOL_CHOICE_UNSUPPORTED = (
        "provider_required_tool_choice_unsupported"
    )
    PROVIDER_STREAM_INTERRUPTED = "provider_stream_interrupted"
    MALFORMED_TOOL_CALL_BATCH = "malformed_tool_call_batch"
    MISSING_REQUIRED_TOOL_CALL = "missing_required_tool_call"
    MISSING_REQUIRED_TOOL_CALL_REPLAN = "missing_required_tool_call_replan"
    UNSTRUCTURED_TOOL_PROTOCOL = "unstructured_tool_protocol"
    EMPTY_MODEL_RESPONSE = "empty_model_response"
    RESPONSE_CONSTRAINT_DETERMINISTIC = "response_constraint_deterministic"
    RESPONSE_CONSTRAINT_SEMANTIC = "response_constraint_semantic"
    FUTURE_TOOL_STEP = "future_tool_step"
    UNAUTHORIZED_TOOL = "unauthorized_tool"
    UNAUTHORIZED_TOOL_REPLAN = "unauthorized_tool_replan"
    TOOL_INPUT_INVALID = "tool_input_invalid"
    TOOL_EXECUTION_FAILED_REPLAN = "tool_execution_failed_replan"


class RecoveryEffectState(StrEnum):
    """Whether the failed operation could already have changed host state."""

    NOT_STARTED = "not_started"
    COMMITTED = "committed"
    UNKNOWN = "unknown"


class FailureCategory(StrEnum):
    """Provider- and product-neutral cause family for durable settlement."""

    CANCELED = "canceled"
    TRANSIENT_PROVIDER = "transient_provider"
    PERMANENT_EXTERNAL = "permanent_external"
    PROTOCOL_INCOMPATIBLE = "protocol_incompatible"
    MODEL_OUTPUT_INVALID = "model_output_invalid"
    TOOL_INPUT_INVALID = "tool_input_invalid"
    TOOL_EXECUTION = "tool_execution"
    BUSINESS_INVARIANT = "business_invariant"


class FailureDisposition(StrEnum):
    """The durable owner action selected after one execution attempt fails."""

    RETRY_ATTEMPT = "retry_attempt"
    RESUME_CHECKPOINT = "resume_checkpoint"
    SPLIT_PART = "split_part"
    PAUSE_RECOVERABLE = "pause_recoverable"
    FAIL_PERMANENT = "fail_permanent"
    CANCEL = "cancel"


class FailureScope(StrEnum):
    """How broadly a failed Part invalidates further task execution."""

    LOCAL = "local"
    SYSTEMIC = "systemic"


@dataclass(frozen=True, slots=True)
class FailureSignal:
    category: FailureCategory
    code: str
    retryable: bool
    scope: FailureScope = FailureScope.LOCAL
    effect_state: RecoveryEffectState = RecoveryEffectState.NOT_STARTED
    checkpoint_available: bool = False
    part_splittable: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "category", FailureCategory(self.category))
        object.__setattr__(self, "code", required_text(
            self.code,
            "failure signal code",
        ))
        object.__setattr__(self, "retryable", bool(self.retryable))
        object.__setattr__(self, "scope", FailureScope(self.scope))
        object.__setattr__(
            self,
            "effect_state",
            RecoveryEffectState(self.effect_state),
        )
        object.__setattr__(
            self,
            "checkpoint_available",
            bool(self.checkpoint_available),
        )
        object.__setattr__(self, "part_splittable", bool(self.part_splittable))


@dataclass(frozen=True, slots=True)
class FailureDecision:
    category: FailureCategory
    code: str
    disposition: FailureDisposition
    attempts_remaining: int
    effect_state: RecoveryEffectState
    checkpoint_available: bool
    part_splittable: bool = False
    scope: FailureScope = FailureScope.LOCAL

    def __post_init__(self) -> None:
        object.__setattr__(self, "category", FailureCategory(self.category))
        object.__setattr__(self, "code", required_text(
            self.code,
            "failure decision code",
        ))
        object.__setattr__(
            self,
            "disposition",
            FailureDisposition(self.disposition),
        )
        object.__setattr__(
            self,
            "attempts_remaining",
            non_negative_int(
                self.attempts_remaining,
                "failure decision attempts_remaining",
            ),
        )
        object.__setattr__(
            self,
            "effect_state",
            RecoveryEffectState(self.effect_state),
        )
        object.__setattr__(
            self,
            "checkpoint_available",
            bool(self.checkpoint_available),
        )
        object.__setattr__(self, "part_splittable", bool(self.part_splittable))
        object.__setattr__(self, "scope", FailureScope(self.scope))


class RecoveryReason(StrEnum):
    """Stable reasons for allowing or denying a recovery action."""

    ALLOWED = "allowed"
    ATTEMPT_BUDGET_EXHAUSTED = "attempt_budget_exhausted"
    CAUSE_NOT_RETRYABLE = "cause_not_retryable"
    POLICY_DISABLED = "policy_disabled"
    REQUEST_CANCELED = "request_canceled"
    ROUND_BUDGET_EXHAUSTED = "round_budget_exhausted"
    SIDE_EFFECT_COMMITTED = "side_effect_committed"
    SIDE_EFFECT_STATE_UNKNOWN = "side_effect_state_unknown"
    VISIBLE_OUTPUT_ALREADY_EMITTED = "visible_output_already_emitted"


@dataclass(frozen=True, slots=True)
class RecoveryRequest:
    cause: RecoveryCause
    action: RecoveryAction
    scope: str = "run"
    remaining_model_rounds: int = 0
    minimum_remaining_rounds: int = 1
    retryable: bool = True
    cancellation_requested: bool = False
    visible_output_emitted: bool = False
    effect_state: RecoveryEffectState = RecoveryEffectState.NOT_STARTED
    may_repeat_side_effect: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "cause", RecoveryCause(self.cause))
        object.__setattr__(self, "action", RecoveryAction(self.action))
        object.__setattr__(self, "scope", required_text(
            self.scope, "recovery scope"
        ))
        object.__setattr__(
            self,
            "remaining_model_rounds",
            max(0, int(self.remaining_model_rounds)),
        )
        object.__setattr__(self, "minimum_remaining_rounds", non_negative_int(
            self.minimum_remaining_rounds, "minimum remaining rounds"
        ))
        object.__setattr__(self, "retryable", bool(self.retryable))
        object.__setattr__(
            self,
            "cancellation_requested",
            bool(self.cancellation_requested),
        )
        object.__setattr__(
            self,
            "visible_output_emitted",
            bool(self.visible_output_emitted),
        )
        object.__setattr__(
            self,
            "effect_state",
            RecoveryEffectState(self.effect_state),
        )
        object.__setattr__(
            self,
            "may_repeat_side_effect",
            bool(self.may_repeat_side_effect),
        )


@dataclass(frozen=True, slots=True)
class RecoveryDecision:
    cause: RecoveryCause
    action: RecoveryAction
    scope: str
    allowed: bool
    reason_code: RecoveryReason
    attempt: int
    max_attempts: int
    remaining_model_rounds: int
    minimum_remaining_rounds: int
    effect_state: RecoveryEffectState
    may_repeat_side_effect: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_code",
            RecoveryReason(self.reason_code),
        )

    def to_trace_details(self) -> dict[str, object]:
        """Return a content-free payload safe for durable Run traces."""

        return {
            "cause": self.cause.value,
            "action": self.action.value,
            "scope": self.scope,
            "allowed": self.allowed,
            "reasonCode": self.reason_code.value,
            "attempt": self.attempt,
            "maxAttempts": self.max_attempts,
            "remainingModelRounds": self.remaining_model_rounds,
            "minimumRemainingRounds": self.minimum_remaining_rounds,
            "effectState": self.effect_state.value,
            "mayRepeatSideEffect": self.may_repeat_side_effect,
        }
