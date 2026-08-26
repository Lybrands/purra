"""One run's recovery decision ledger."""

from __future__ import annotations

from collections import Counter

from purra.recovery.contracts import (
    RecoveryCause,
    RecoveryDecision,
    RecoveryEffectState,
    RecoveryReason,
    RecoveryRequest,
)
from purra.recovery.policy import RecoveryPolicy


class RecoveryLedger:
    """Atomically decide and consume bounded recovery attempts in memory.

    Allowed and denied decisions are emitted by the runtime through the normal
    durable trace channel. Only a committed model-ready checkpoint may carry
    the consumed-attempt snapshot into a replacement process.
    """

    def __init__(self, policy: RecoveryPolicy = RecoveryPolicy()) -> None:
        self._policy = policy
        self._attempts: Counter[tuple[object, str]] = Counter()

    def attempts(self, request_or_cause, *, scope: str = "run") -> int:
        cause = getattr(request_or_cause, "cause", request_or_cause)
        return int(self._attempts[(cause, str(scope))])

    def snapshot(self) -> tuple[tuple[str, str, int], ...]:
        return tuple(sorted(
            (
                str(getattr(cause, "value", cause)),
                scope,
                int(attempts),
            )
            for (cause, scope), attempts in self._attempts.items()
        ))

    def restore(self, snapshot: tuple[tuple[str, str, int], ...]) -> None:
        if self._attempts:
            raise RuntimeError("recovery ledger has already been used")
        restored: Counter[tuple[object, str]] = Counter()
        for cause, scope, attempts in snapshot:
            key = (RecoveryCause(cause), str(scope))
            count = int(attempts)
            if count < 0:
                raise ValueError("recovery attempt count must be non-negative")
            restored[key] = count
        self._attempts = restored

    def decide(self, request: RecoveryRequest) -> RecoveryDecision:
        max_attempts = self._policy.max_attempts(request.cause)
        key = (request.cause, request.scope)
        used = int(self._attempts[key])
        reason = self._denial_reason(request, used, max_attempts)
        allowed = reason is None
        attempt = used + 1 if allowed else used
        if allowed:
            self._attempts[key] = attempt
        return RecoveryDecision(
            cause=request.cause,
            action=request.action,
            scope=request.scope,
            allowed=allowed,
            reason_code=(RecoveryReason.ALLOWED if allowed else reason),
            attempt=attempt,
            max_attempts=max_attempts,
            remaining_model_rounds=request.remaining_model_rounds,
            minimum_remaining_rounds=request.minimum_remaining_rounds,
            effect_state=request.effect_state,
            may_repeat_side_effect=request.may_repeat_side_effect,
        )

    @staticmethod
    def _denial_reason(
        request: RecoveryRequest,
        used: int,
        max_attempts: int,
    ) -> RecoveryReason | None:
        if request.cancellation_requested:
            return RecoveryReason.REQUEST_CANCELED
        if not request.retryable:
            return RecoveryReason.CAUSE_NOT_RETRYABLE
        if request.visible_output_emitted:
            return RecoveryReason.VISIBLE_OUTPUT_ALREADY_EMITTED
        if request.may_repeat_side_effect:
            if request.effect_state is RecoveryEffectState.COMMITTED:
                return RecoveryReason.SIDE_EFFECT_COMMITTED
            if request.effect_state is RecoveryEffectState.UNKNOWN:
                return RecoveryReason.SIDE_EFFECT_STATE_UNKNOWN
        if (
            request.remaining_model_rounds
            < request.minimum_remaining_rounds
        ):
            return RecoveryReason.ROUND_BUDGET_EXHAUSTED
        if max_attempts <= 0:
            return RecoveryReason.POLICY_DISABLED
        if used >= max_attempts:
            return RecoveryReason.ATTEMPT_BUDGET_EXHAUSTED
        return None
