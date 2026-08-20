"""Content-free reporting over durable controlled-recovery decisions."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from purra.observability._values import (
    as_mapping as _mapping,
    permissive_non_negative_integer as _non_negative_integer,
)
from purra.observability.diagnostics import build_canonical_run_observation
from purra.recovery import (
    RecoveryAction,
    RecoveryCause,
    RecoveryEffectState,
    RecoveryReason,
)


_CAUSES = frozenset(item.value for item in RecoveryCause)
_ACTIONS = frozenset(item.value for item in RecoveryAction)
_EFFECT_STATES = frozenset(item.value for item in RecoveryEffectState)
_REASON_CODES = frozenset(item.value for item in RecoveryReason)
_SAFETY_REASONS = frozenset({
    "side_effect_committed",
    "side_effect_state_unknown",
    "visible_output_already_emitted",
})


def evaluate_agent_run_recovery(
    events: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Summarize recovery decisions without copying prompts or payloads."""

    observation = build_canonical_run_observation(events)
    decisions = tuple(
        decision
        for trace in observation.by_stage.get("recovery_decision", ())
        if (decision := _decision(trace)) is not None
    )
    allowed = tuple(item for item in decisions if item["allowed"])
    denied = tuple(item for item in decisions if not item["allowed"])
    causes = Counter(str(item["cause"]) for item in decisions)
    actions = Counter(str(item["action"]) for item in allowed)
    denied_reasons = Counter(str(item["reasonCode"]) for item in denied)
    safety_protected = sum(
        count
        for reason, count in denied_reasons.items()
        if reason in _SAFETY_REASONS
    )
    return {
        "summary": {
            "decisionCount": len(decisions),
            "allowedCount": len(allowed),
            "deniedCount": len(denied),
            "safetyProtectedCount": safety_protected,
            "causes": dict(sorted(causes.items())),
            "allowedActions": dict(sorted(actions.items())),
            "deniedReasons": dict(sorted(denied_reasons.items())),
        },
        "decisions": list(decisions),
    }


def _decision(trace: Mapping[str, Any]) -> dict[str, Any] | None:
    details = _mapping(trace.get("details"))
    cause = str(details.get("cause") or "")
    action = str(details.get("action") or "")
    reason = str(details.get("reasonCode") or "")
    effect_state = str(details.get("effectState") or "")
    if (
        cause not in _CAUSES
        or action not in _ACTIONS
        or reason not in _REASON_CODES
        or effect_state not in _EFFECT_STATES
    ):
        return None
    allowed = bool(details.get("allowed"))
    if allowed != (reason == "allowed"):
        return None
    return {
        "round": _non_negative_integer(details.get("round")),
        "cause": cause,
        "action": action,
        "allowed": allowed,
        "reasonCode": reason,
        "attempt": _non_negative_integer(details.get("attempt")),
        "maxAttempts": _non_negative_integer(details.get("maxAttempts")),
        "remainingModelRounds": _non_negative_integer(
            details.get("remainingModelRounds")
        ),
        "effectState": effect_state,
        "mayRepeatSideEffect": bool(details.get("mayRepeatSideEffect")),
    }
