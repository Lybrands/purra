from __future__ import annotations

import json
from pathlib import Path

from purra.recovery import (
    RecoveryAction,
    RecoveryCause,
    RecoveryEffectState,
    RecoveryLedger,
    RecoveryPolicy,
    RecoveryRequest,
)


_FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "recovery_protocol.json").read_text()
)


def test_shared_recovery_decisions_match_python_authority() -> None:
    for row in _FIXTURE["cases"]:
        policy = RecoveryPolicy({
            RecoveryCause(cause): attempts
            for cause, attempts in row["policy"].items()
        })
        ledger = RecoveryLedger(policy)
        request = _request(row["request"])
        for _ in range(row.get("preconsume", 0)):
            assert ledger.decide(request).allowed
        decision = ledger.decide(request)
        assert {
            "allowed": decision.allowed,
            "reasonCode": decision.reason_code.value,
            "attempt": decision.attempt,
            "maxAttempts": decision.max_attempts,
        } == row["expected"], row["caseId"]


def _request(value: dict[str, object]) -> RecoveryRequest:
    return RecoveryRequest(
        cause=RecoveryCause(str(value["cause"])),
        action=RecoveryAction(str(value["action"])),
        scope=str(value.get("scope", "run")),
        remaining_model_rounds=int(value.get("remainingModelRounds", 0)),
        minimum_remaining_rounds=int(value.get("minimumRemainingRounds", 1)),
        retryable=bool(value.get("retryable", True)),
        cancellation_requested=bool(value.get("cancellationRequested", False)),
        visible_output_emitted=bool(value.get("visibleOutputEmitted", False)),
        effect_state=RecoveryEffectState(str(value.get("effectState", "not_started"))),
        may_repeat_side_effect=bool(value.get("mayRepeatSideEffect", False)),
    )
