"""Provider-neutral message estimation and complete-turn trimming."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    ContextBudget,
    ContextBudgetClaim,
    MessageOrigin,
    MessageRole,
    TaskContextRequest,
    ToolSchema,
)
from purra.errors import ContextOverflowError
from purra.json_values import thaw_json_mapping, thaw_json_value


def _validate_context_claims(
    values: Iterable[ContextBudgetClaim],
    *,
    source: str,
) -> tuple[ContextBudgetClaim, ...]:
    claims = tuple(values)
    if any(not isinstance(claim, ContextBudgetClaim) for claim in claims):
        raise TypeError(f"{source} returned an invalid claim")
    names = tuple(claim.name for claim in claims)
    if len(names) != len(set(names)):
        raise ValueError(f"{source} contains duplicate names")
    return claims


async def resolve_context_budget_claims(
    provider: object,
    request: AgentRunRequest,
    fallback_claims: Sequence[ContextBudgetClaim] = (),
    signal: Any = None,
) -> tuple[ContextBudgetClaim, ...]:
    """Resolve dynamic demand through an optional provider capability.

    Core owns this resolution contract so application entry points and the
    engine use the same demand source. Providers without the optional method
    use the host's configured static claims.
    """

    resolver = getattr(provider, "describe_context_demands", None)
    raw_claims = (
        await resolver(request, signal)
        if callable(resolver)
        else tuple(fallback_claims)
    )
    return _validate_context_claims(raw_claims, source="context demand provider")


async def resolve_task_context_budget_claims(
    provider: object,
    request: AgentRunRequest,
    task: TaskContextRequest,
    signal: Any = None,
) -> tuple[ContextBudgetClaim, ...]:
    """Resolve extra demand that exists only after semantic planning.

    These claims supplement, rather than replace, the provider's ordinary
    context claims. This prevents optional recovery or retrieval sources from
    reserving a large partition for unrelated requests.
    """

    resolver = getattr(provider, "describe_task_context_demands", None)
    if not callable(resolver):
        return ()
    return _validate_context_claims(
        await resolver(request, task, signal),
        source="task context demand provider",
    )


def _estimate_units(value: str, *, ascii_divisor: int) -> int:
    ascii_count = sum(1 for char in value if ord(char) < 128)
    non_ascii_count = len(value) - ascii_count
    return non_ascii_count + math.ceil(ascii_count / max(1, ascii_divisor))


def estimate_json_tokens(value: Any) -> int:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return _estimate_units(encoded, ascii_divisor=2)


def estimate_text_tokens(value: Any) -> int:
    """Conservatively estimate ordinary mixed-language prose.

    Domain context providers may use this generic helper when fitting plain
    text into an allocation. Structured messages and schemas must continue to
    use the denser JSON estimator above.
    """

    return _estimate_units(str(value or ""), ascii_divisor=4)


def _budget_message_mapping(message: AgentMessage) -> dict[str, Any]:
    """Return a conservative, provider-neutral representation for budgeting.

    ``AgentMessage.to_mapping`` intentionally exposes compact Core field names.
    A model adapter must expand tool calls into a nested wire protocol, though,
    so estimating the compact form can undercount later tool-continuation
    rounds.  The descriptive keys below account for that structural overhead
    without making Core depend on any one provider's request schema.
    """

    value = thaw_json_mapping(message.attributes)
    value.update({
        "role": message.role.value,
        "content": thaw_json_value(message.content),
    })
    if message.reasoning is not None:
        value["structured_reasoning_content"] = message.reasoning
    if message.tool_calls:
        value["structured_tool_calls"] = [
            {
                "tool_call_identifier": call.id,
                "tool_protocol_type": "function",
                "function_descriptor": {
                    "tool_function_name": call.name,
                    "serialized_arguments_json": call.arguments_json,
                },
            }
            for call in message.tool_calls
        ]
    if message.tool_call_id is not None:
        value["tool_call_identifier"] = message.tool_call_id
    return value


def estimate_tool_schema_tokens(tools: Iterable[ToolSchema]) -> int:
    rows = [
        {
            "tool_protocol_type": "function",
            "function_descriptor": {
                "tool_function_name": schema.name,
                "tool_description": schema.description,
                "tool_parameters_schema": thaw_json_mapping(schema.parameters),
            },
        }
        for schema in tools
    ]
    return 0 if not rows else estimate_json_tokens(rows) + 8 * len(rows)


def context_budget_contract_error(
    request: AgentRunRequest,
    budget: ContextBudget | None,
    configured_tools: Sequence[ToolSchema],
) -> str | None:
    """Validate that a host budget still describes the runtime invocation."""

    if budget is None:
        return None
    if (
        request.context_window is None
        or int(request.context_window) != budget.window_tokens
    ):
        return "context_budget_window_mismatch"
    if estimate_tool_schema_tokens(configured_tools) != budget.tool_schema_tokens:
        return "context_budget_tool_schema_mismatch"
    if budget.output_reserve_tokens <= 0:
        return "context_budget_output_reserve_invalid"
    return None


def allocate_context_budget(
    *,
    window_tokens: int,
    output_reserve_tokens: int = 8_192,
    tools: Sequence[ToolSchema] = (),
    claims: Sequence[ContextBudgetClaim] = (),
    safety_reserve_tokens: int | None = None,
    runtime_reserve_tokens: int | None = None,
    minimum_message_tokens: int | None = None,
) -> ContextBudget:
    """Allocate one deterministic provider-neutral context account.

    Claim names are opaque to Core.  A domain adapter may request any named
    partitions without adding business concepts to the budget contract.
    """

    window = int(window_tokens)
    output = int(output_reserve_tokens)
    if window <= 0:
        raise ValueError("context window must be positive")
    if output <= 0:
        raise ValueError("output reserve must be positive")

    schemas = tuple(tools)
    schema_tokens = estimate_tool_schema_tokens(schemas)
    safety = (
        int(safety_reserve_tokens)
        if safety_reserve_tokens is not None
        else min(64_000, max(4_096, math.ceil(window * 0.05)))
    )
    runtime = (
        int(runtime_reserve_tokens)
        if runtime_reserve_tokens is not None
        else (
            min(64_000, max(4_096, window // 10))
            if schemas
            else min(16_000, max(2_048, window // 25))
        )
    )
    minimum = (
        int(minimum_message_tokens)
        if minimum_message_tokens is not None
        else min(8_192, max(1_024, window // 100))
    )
    if min(safety, runtime, minimum) < 0:
        raise ValueError("context reserves must be non-negative")

    provider_input = window - output - safety - runtime - schema_tokens
    if provider_input < minimum:
        raise ContextOverflowError(
            "fixed model, tool and safety reserves leave no message budget",
            reason_code="fixed_reserves_exceed_window",
            details={
                "windowTokens": window,
                "outputReserveTokens": output,
                "safetyReserveTokens": safety,
                "runtimeReserveTokens": runtime,
                "toolSchemaTokens": schema_tokens,
                "providerInputTokens": provider_input,
                "minimumMessageTokens": minimum,
            },
        )

    normalized_claims = _validate_context_claims(
        claims,
        source="context budget",
    )

    allocations = _allocate_claims(
        normalized_claims,
        max(0, provider_input - minimum),
    )
    return ContextBudget(
        window_tokens=window,
        output_reserve_tokens=output,
        safety_reserve_tokens=safety,
        runtime_reserve_tokens=runtime,
        tool_schema_tokens=schema_tokens,
        provider_input_tokens=provider_input,
        minimum_message_tokens=minimum,
        context_allocations=allocations,
    )


def _allocate_claims(
    claims: Sequence[ContextBudgetClaim],
    available_tokens: int,
) -> dict[str, int]:
    if not claims:
        return {}
    available = max(0, int(available_tokens))
    minimum_total = sum(claim.minimum_tokens for claim in claims)
    if minimum_total > available:
        raise ContextOverflowError(
            "minimum context demand exceeds the provider input pool",
            reason_code="minimum_context_demand_exceeds_pool",
            details={
                "minimumContextDemandTokens": minimum_total,
                "availableContextPoolTokens": available,
                "overflowTokens": minimum_total - available,
            },
        )

    allocations = {
        claim.name: claim.minimum_tokens
        for claim in claims
    }
    remaining = available - minimum_total
    priorities = sorted({claim.priority for claim in claims}, reverse=True)
    for priority in priorities:
        group = [claim for claim in claims if claim.priority == priority]
        needs = [
            max(0, claim.desired_tokens - allocations[claim.name])
            for claim in group
        ]
        needed_total = sum(needs)
        if needed_total <= 0:
            continue
        if needed_total <= remaining:
            for claim, need in zip(group, needs):
                allocations[claim.name] += need
            remaining -= needed_total
            continue
        shares = _proportional_shares(needs, remaining)
        for claim, share in zip(group, shares):
            allocations[claim.name] += share
        remaining = 0
        break
    return allocations


def _proportional_shares(weights: Sequence[int], available: int) -> list[int]:
    total = sum(max(0, int(weight)) for weight in weights)
    pool = max(0, int(available))
    if total <= 0 or pool <= 0:
        return [0 for _ in weights]
    quotients_and_remainders = [
        divmod(max(0, int(weight)) * pool, total)
        for weight in weights
    ]
    shares = [quotient for quotient, _ in quotients_and_remainders]
    remaining = pool - sum(shares)
    order = sorted(
        range(len(weights)),
        key=lambda index: (-quotients_and_remainders[index][1], index),
    )
    for index in order[:remaining]:
        shares[index] += 1
    return shares


def estimate_agent_messages_tokens(messages: Iterable[AgentMessage]) -> int:
    return 2 + sum(
        estimate_json_tokens(_budget_message_mapping(message)) + 4
        for message in messages
    )


@dataclass(frozen=True, slots=True)
class TrimmedAgentMessages:
    messages: tuple[AgentMessage, ...]
    dropped_count: int
    token_estimate: int
    overflow_tokens: int


def trim_agent_messages_by_turn(
    messages: Sequence[AgentMessage],
    token_budget: int,
    *,
    max_recent_messages: int | None = None,
) -> TrimmedAgentMessages:
    """Keep trusted host context and newest complete conversation turns.

    ``max_recent_messages`` is a structural fallback window, not a semantic
    importance rule.  Whole turns may exceed that count when the newest turn
    contains a complete tool exchange; Core never slices such a turn merely
    to satisfy the message-count preference.
    """

    rows = list(messages)
    protected: list[tuple[int, AgentMessage]] = []
    turns: list[list[tuple[int, AgentMessage]]] = []
    current: list[tuple[int, AgentMessage]] = []

    for index, message in enumerate(rows):
        if (
            message.role in {MessageRole.SYSTEM, MessageRole.DEVELOPER}
            or message.origin is MessageOrigin.HOST_CONTEXT
        ):
            if current:
                turns.append(current)
                current = []
            protected.append((index, message))
            continue
        if message.role is MessageRole.USER and current:
            turns.append(current)
            current = []
        current.append((index, message))
    if current:
        turns.append(current)

    selected_indices = {index for index, _ in protected}
    selected_message_count = 0
    used = 2 + sum(
        estimate_json_tokens(_budget_message_mapping(message)) + 4
        for _, message in protected
    )
    for reverse_index, turn in enumerate(reversed(turns)):
        turn_cost = sum(
            estimate_json_tokens(_budget_message_mapping(message)) + 4
            for _, message in turn
        )
        required = reverse_index == 0
        count_fits = bool(
            max_recent_messages is None
            or selected_message_count + len(turn)
            <= max(1, int(max_recent_messages))
        )
        if required or (
            count_fits
            and used + turn_cost <= max(0, int(token_budget))
        ):
            selected_indices.update(index for index, _ in turn)
            used += turn_cost
            selected_message_count += len(turn)
            continue
        break

    selected = tuple(
        message
        for index, message in enumerate(rows)
        if index in selected_indices
    )
    actual = estimate_agent_messages_tokens(selected)
    return TrimmedAgentMessages(
        messages=selected,
        dropped_count=max(0, len(rows) - len(selected)),
        token_estimate=actual,
        overflow_tokens=max(0, actual - max(0, int(token_budget))),
    )


__all__ = [
    "TrimmedAgentMessages",
    "allocate_context_budget",
    "context_budget_contract_error",
    "estimate_agent_messages_tokens",
    "estimate_json_tokens",
    "estimate_text_tokens",
    "estimate_tool_schema_tokens",
    "resolve_context_budget_claims",
    "resolve_task_context_budget_claims",
    "trim_agent_messages_by_turn",
]
