"""Domain-neutral failures raised by PurrA boundaries."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

class AgentCoreError(Exception):
    """Base class for errors with host-controlled public handling."""


class CodedAgentCoreError(AgentCoreError):
    """Base failure carrying a stable code and immutable diagnostic details."""

    default_code = "purra_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = str(code or self.default_code)
        self.details = MappingProxyType(dict(details or {}))


class ContractViolationError(CodedAgentCoreError):
    """A registered capability violates a Core contract."""

    default_code = "contract_violation"


class RunCancellationConflictError(ContractViolationError):
    """A stale execution tried to out-race a durable Run cancellation fence."""


class OutputPersistenceError(CodedAgentCoreError):
    """A canonical output event could not become durable."""

    default_code = "output_persistence_failed"


class RunCommitProjectionError(CodedAgentCoreError):
    """A host-bound effect rejected the terminal Run transaction."""

    default_code = "completion_projection_failed"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        details: Mapping[str, Any] | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message, code=code, details=details)
        self.retryable = bool(retryable)


class ResponseJudgeContractError(ContractViolationError):
    """A semantic judge response violates its declared verdict contract."""


class ContextOverflowError(AgentCoreError):
    """The complete request cannot fit inside the configured budget."""

    def __init__(
        self,
        message: str = "context exceeds the configured budget",
        *,
        reason_code: str = "context_overflow",
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = str(reason_code or "context_overflow")
        self.details = MappingProxyType(dict(details or {}))


class InvalidPlannerOutputError(AgentCoreError):
    """A planner response could not be normalized into a safe typed plan."""

    def __init__(
        self,
        message: str = "invalid planner output",
        *,
        code: str = "invalid_plan",
    ) -> None:
        super().__init__(message)
        self.code = str(code or "invalid_plan")


class RepairablePlannerOutputError(InvalidPlannerOutputError):
    """A structurally valid plan may be corrected by one model re-plan."""


class ModelGatewayError(AgentCoreError):
    """A model adapter failed after provider-specific handling."""

    def __init__(
        self,
        message: str = "model gateway failed",
        *,
        code: str = "model_gateway_error",
        retryable: bool = False,
    ):
        super().__init__(message)
        self.code = str(code or "model_gateway_error")
        self.retryable = bool(retryable)


class UnsupportedModelFeatureError(ModelGatewayError):
    """The selected provider/model rejected a requested capability."""

    def __init__(
        self,
        message: str = "unsupported model feature",
        *,
        code: str = "unsupported_model_feature",
        retryable: bool = True,
    ):
        super().__init__(
            message,
            code=code,
            retryable=retryable,
        )


class ToolExecutionError(AgentCoreError):
    """A tool call failed inside the host-controlled execution boundary."""
