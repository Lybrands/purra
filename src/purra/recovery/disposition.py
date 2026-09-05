"""Pure durable failure disposition independent from providers and products."""

from __future__ import annotations

from purra.recovery.contracts import (
    FailureCategory,
    FailureDecision,
    FailureDisposition,
    FailureSignal,
    RecoveryEffectState,
)


_PERMANENT_CATEGORIES = frozenset({
    FailureCategory.PERMANENT_EXTERNAL,
    FailureCategory.BUSINESS_INVARIANT,
})


def decide_failure(
    signal: FailureSignal,
    *,
    attempts_remaining: int,
) -> FailureDecision:
    """Select the durable owner action without changing execution state."""

    remaining = max(0, int(attempts_remaining))
    if signal.category is FailureCategory.CANCELED:
        disposition = FailureDisposition.CANCEL
    elif signal.category in _PERMANENT_CATEGORIES:
        disposition = FailureDisposition.FAIL_PERMANENT
    elif signal.effect_state is RecoveryEffectState.UNKNOWN:
        disposition = FailureDisposition.FAIL_PERMANENT
    elif signal.effect_state is RecoveryEffectState.COMMITTED:
        disposition = (
            FailureDisposition.RESUME_CHECKPOINT
            if signal.checkpoint_available
            else FailureDisposition.FAIL_PERMANENT
        )
    elif signal.checkpoint_available:
        disposition = FailureDisposition.RESUME_CHECKPOINT
    elif signal.part_splittable:
        disposition = FailureDisposition.SPLIT_PART
    elif signal.retryable and remaining:
        disposition = (
            FailureDisposition.RESUME_CHECKPOINT
            if signal.checkpoint_available
            else FailureDisposition.RETRY_ATTEMPT
        )
    else:
        disposition = FailureDisposition.FAIL_PERMANENT
    return FailureDecision(
        category=signal.category,
        code=signal.code,
        disposition=disposition,
        attempts_remaining=remaining,
        effect_state=signal.effect_state,
        checkpoint_available=signal.checkpoint_available,
        part_splittable=signal.part_splittable,
        scope=signal.scope,
    )


__all__ = ["decide_failure"]
