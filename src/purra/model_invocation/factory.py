"""Single construction path for :class:`AgentModelInvocationManager`.

Every call site previously reassembled the same parameter knowledge (timeout
derived from runtime limits, tool-argument cap, budget repository, evidence
validator). Centralizing it here keeps the manager's ambient configuration in
one place.
"""

from __future__ import annotations

from purra.contracts import RuntimeLimits
from purra.ports.evidence import ModelInputEvidenceValidator
from purra.ports.run_lifecycle import RunRepository

from purra.model_invocation.manager import (
    AgentModelInvocationManager,
    ModelInvocationOutputObserver,
)
from purra.operations import AgentOperationController


def create_model_invocation_manager(
    model_gateway,
    *,
    output_observer: ModelInvocationOutputObserver | None = None,
    operation_controller: AgentOperationController | None = None,
    runtime_limits: RuntimeLimits | None = None,
    max_tool_argument_chars: int | None = None,
    budget_repository: RunRepository | None = None,
    evidence_validator: ModelInputEvidenceValidator | None = None,
) -> AgentModelInvocationManager:
    """Build a manager; the invocation timeout always follows ``runtime_limits``.

    ``max_tool_argument_chars`` falls back to the manager's own default when
    not supplied, matching the historical call sites that omitted it.
    """
    kwargs = {}
    if runtime_limits is not None:
        kwargs["invocation_timeout_ms"] = runtime_limits.provider_invocation_timeout_ms
        kwargs["runtime_limits"] = runtime_limits
    if max_tool_argument_chars is not None:
        kwargs["max_tool_argument_chars"] = max_tool_argument_chars
    return AgentModelInvocationManager(
        model_gateway,
        output_observer=output_observer,
        operation_controller=operation_controller,
        budget_repository=budget_repository,
        evidence_validator=evidence_validator,
        **kwargs,
    )
