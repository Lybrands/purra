"""Run-level context and output budget computation.

Extracted from the orchestrator; consumed by the runtime phase assembly and
durable planning paths.
"""

from __future__ import annotations

from typing import Sequence

from purra.contracts import AgentRunRequest, ContextBudget, ContextBudgetClaim, RuntimeLimits
from purra.context_budget import (
    allocate_context_budget,
    max_generation_tokens_for_context,
)
from purra.engine.options import AgentCoreRunOptions
from purra.errors import UnsupportedModelFeatureError
from purra.model_protocol import (
    ResultCapacitySource,
    constrain_output_budget_to_context,
    resolve_invocation_output_budget,
)

def _selected_context_window_tokens(
    request: AgentRunRequest,
    options: AgentCoreRunOptions,
) -> int:
    """Resolve context capacity from explicit selection or the model snapshot."""

    profile_window = request.model.capability_snapshot.context_window_tokens
    selected = (
        request.context_window
        or options.default_context_window_tokens
        or profile_window
    )
    if selected > profile_window:
        raise UnsupportedModelFeatureError(
            "selected context window exceeds the model capability snapshot",
            code="model_context_capacity_exceeded",
            retryable=False,
        )
    return selected


def _resolve_run_output_budget(
    request: AgentRunRequest,
    options: AgentCoreRunOptions,
):
    target = options.result_capacity_target_tokens
    return resolve_invocation_output_budget(
        request.model.capability_snapshot,
        max_generation_tokens=request.model.max_generation_tokens,
        result_capacity_target_tokens=target,
        result_capacity_source=(
            ResultCapacitySource.WORKFLOW_POLICY if target is not None else None
        ),
    )


def _context_result_reserve_tokens(
    output_budget,
    *,
    physical_generation_capacity: int,
) -> int:
    """Size Provider input without treating its generation cap as a reserve.

    A declared result-capacity target is used for context planning only.  When
    a workflow has no target, Core keeps a conservative 8K planning reserve;
    the exact Provider maximum is still derived from actual input at the
    invocation boundary.
    """

    physical = int(physical_generation_capacity)
    target = output_budget.result_capacity_target_tokens
    desired = 8_192 if target is None else target
    return min(desired, output_budget.max_generation_tokens, physical)


def _execution_context_budget(
    request: AgentRunRequest,
    options: AgentCoreRunOptions,
    output_budget,
    schemas,
    claims: Sequence[ContextBudgetClaim],
) -> ContextBudget:
    window_tokens = _selected_context_window_tokens(request, options)
    physical = max_generation_tokens_for_context(
        window_tokens=window_tokens,
        tools=schemas,
        safety_reserve_tokens=options.safety_reserve_tokens,
        runtime_reserve_tokens=options.runtime_reserve_tokens,
        minimum_message_tokens=options.minimum_message_tokens,
    )
    constrain_output_budget_to_context(output_budget, max_generation_tokens=physical)
    output_reserve = _context_result_reserve_tokens(
        output_budget, physical_generation_capacity=physical,
    )
    return allocate_context_budget(
        window_tokens=window_tokens,
        output_reserve_tokens=output_reserve,
        tools=schemas,
        claims=claims,
        safety_reserve_tokens=options.safety_reserve_tokens,
        runtime_reserve_tokens=options.runtime_reserve_tokens,
        minimum_message_tokens=options.minimum_message_tokens,
    )
