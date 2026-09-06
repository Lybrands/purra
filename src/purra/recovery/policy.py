"""Configurable recovery budgets owned by PurrA's composition boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from purra.recovery.contracts import RecoveryCause


_STANDARD_LIMITS: Mapping[RecoveryCause, int] = MappingProxyType({
    RecoveryCause.PROVIDER_REQUIRED_TOOL_CHOICE_UNSUPPORTED: 1,
    RecoveryCause.PROVIDER_STREAM_INTERRUPTED: 1,
    RecoveryCause.MALFORMED_TOOL_CALL_BATCH: 1,
    RecoveryCause.MISSING_REQUIRED_TOOL_CALL: 1,
    RecoveryCause.MISSING_REQUIRED_TOOL_CALL_REPLAN: 1,
    RecoveryCause.UNSTRUCTURED_TOOL_PROTOCOL: 1,
    RecoveryCause.EMPTY_MODEL_RESPONSE: 2,
    RecoveryCause.STRUCTURED_OUTPUT_INVALID: 0,
    RecoveryCause.RESPONSE_CONSTRAINT_DETERMINISTIC: 1,
    RecoveryCause.RESPONSE_CONSTRAINT_SEMANTIC: 1,
    RecoveryCause.FUTURE_TOOL_STEP: 1,
    RecoveryCause.UNAUTHORIZED_TOOL: 1,
    RecoveryCause.UNAUTHORIZED_TOOL_REPLAN: 1,
    RecoveryCause.TOOL_INPUT_INVALID: 1,
    RecoveryCause.TOOL_EXECUTION_FAILED_REPLAN: 1,
})


@dataclass(frozen=True, slots=True)
class RecoveryPolicy:
    """Immutable per-profile policy injected into the Core runtime.

    A missing rule is fail-closed (zero attempts). Domains may replace the
    policy at composition time, but the runtime remains the sole decision and
    accounting authority.
    """

    attempt_limits: Mapping[RecoveryCause, int] = field(
        default_factory=lambda: _STANDARD_LIMITS
    )

    def __post_init__(self) -> None:
        normalized = {
            RecoveryCause(cause): int(attempts)
            for cause, attempts in self.attempt_limits.items()
        }
        if any(attempts < 0 for attempts in normalized.values()):
            raise ValueError("recovery max attempts must be non-negative")
        object.__setattr__(
            self,
            "attempt_limits",
            MappingProxyType(normalized),
        )

    def max_attempts(self, cause: RecoveryCause) -> int:
        return int(self.attempt_limits.get(RecoveryCause(cause), 0))

    def with_overrides(
        self,
        overrides: Mapping[RecoveryCause, int],
    ) -> "RecoveryPolicy":
        limits = dict(self.attempt_limits)
        for cause, attempts in overrides.items():
            limits[RecoveryCause(cause)] = int(attempts)
        return RecoveryPolicy(limits)
