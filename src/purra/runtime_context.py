"""Stage-specific projection of canonical Run messages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from purra.context_budget import estimate_agent_messages_tokens
from purra.contracts import (
    AgentMessage,
    MessageRole,
    ToolContextContract,
    ToolResultProjection,
)
from purra.evidence import RunEvidenceStore


@dataclass(frozen=True, slots=True)
class RuntimeContextProjection:
    messages: tuple[AgentMessage, ...]
    dropped_context_blocks: tuple[str, ...] = ()
    compacted_tool_results: tuple[str, ...] = ()
    saved_tokens: int = 0
    mode: str = "full"
    target_tokens: int | None = None


def project_intermediate_tool_context(
    messages: Sequence[AgentMessage],
    *,
    visible_tool_names: frozenset[str],
    contracts: Mapping[str, ToolContextContract],
    evidence_store: RunEvidenceStore | None = None,
    enabled: bool,
    initial_round: bool,
    token_budget: int | None = None,
    final_soft_pressure_ratio: float = 0.85,
    final_target_ratio: float = 0.75,
) -> RuntimeContextProjection:
    """Project canonical evidence for the current execution stage.

    Intermediate rounds remove only contract-proven optional context. Final
    rounds retain complete evidence unless the context is under pressure, and
    then replace only results whose host contract explicitly permits a final
    receipt. The canonical messages and complete evidence store are untouched.
    """

    rows = tuple(messages)
    if not enabled or initial_round:
        return RuntimeContextProjection(rows)
    if not visible_tool_names:
        return _project_final_context(
            rows,
            contracts=contracts,
            evidence_store=evidence_store,
            token_budget=token_budget,
            soft_pressure_ratio=final_soft_pressure_ratio,
            target_ratio=final_target_ratio,
        )
    current_contracts = tuple(
        contracts[name]
        for name in visible_tool_names
        if name in contracts
    )
    if len(current_contracts) != len(visible_tool_names):
        return RuntimeContextProjection(rows)
    optional_blocks = {
        block
        for contract in contracts.values()
        for block in contract.required_context_blocks
    }
    required_blocks = {
        block
        for contract in current_contracts
        for block in contract.required_context_blocks
    }
    removable = optional_blocks - required_blocks
    selected_context = tuple(
        message
        for message in rows
        if str(message.attributes.get("context_name") or "") not in removable
    )
    required_result_tools = {
        dependency
        for contract in current_contracts
        for dependency in contract.prerequisite_tools
    }
    selected: list[AgentMessage] = []
    compacted: list[str] = []
    receipts = {
        receipt.tool_call_id: receipt
        for receipt in evidence_store.tool_result_receipts()
    } if evidence_store is not None else {}
    for message in selected_context:
        if (
            evidence_store is None
            or message.role is not MessageRole.TOOL
            or not message.tool_call_id
        ):
            selected.append(message)
            continue
        receipt = receipts.get(message.tool_call_id)
        producer_contract = (
            contracts.get(receipt.tool_name)
            if receipt is not None
            else None
        )
        replacement = (
            evidence_store.receipt_message(message)
            if (
                receipt is not None
                and receipt.tool_name not in required_result_tools
                and producer_contract is not None
                and producer_contract.result_projection
                is ToolResultProjection.RECEIPT
            )
            else None
        )
        if (
            replacement is not None
            and estimate_agent_messages_tokens((replacement,))
            < estimate_agent_messages_tokens((message,))
        ):
            selected.append(replacement)
            compacted.append(receipt.evidence_id)
        else:
            selected.append(message)
    selected_rows = tuple(selected)
    if selected_rows == rows:
        return RuntimeContextProjection(rows)
    return RuntimeContextProjection(
        messages=selected_rows,
        dropped_context_blocks=tuple(sorted(removable)),
        compacted_tool_results=tuple(compacted),
        saved_tokens=max(
            0,
            estimate_agent_messages_tokens(rows)
            - estimate_agent_messages_tokens(selected_rows),
        ),
        mode="stage_contract",
    )


def _project_final_context(
    rows: tuple[AgentMessage, ...],
    *,
    contracts: Mapping[str, ToolContextContract],
    evidence_store: RunEvidenceStore | None,
    token_budget: int | None,
    soft_pressure_ratio: float,
    target_ratio: float,
) -> RuntimeContextProjection:
    if evidence_store is None or token_budget is None:
        return RuntimeContextProjection(rows)
    budget = max(1, int(token_budget))
    soft_ratio = min(1.0, max(0.0, float(soft_pressure_ratio)))
    target_ratio = min(soft_ratio, max(0.0, float(target_ratio)))
    original_tokens = estimate_agent_messages_tokens(rows)
    if original_tokens <= round(budget * soft_ratio):
        return RuntimeContextProjection(rows)

    target_tokens = max(1, round(budget * target_ratio))
    selected: list[AgentMessage] = []
    compacted: list[str] = []
    projected_tokens = original_tokens
    receipts = {
        receipt.tool_call_id: receipt
        for receipt in evidence_store.tool_result_receipts()
    }
    for message in rows:
        receipt = (
            receipts.get(message.tool_call_id)
            if message.role is MessageRole.TOOL and message.tool_call_id
            else None
        )
        producer_contract = (
            contracts.get(receipt.tool_name)
            if receipt is not None
            else None
        )
        replacement = (
            evidence_store.receipt_message(message)
            if (
                projected_tokens > target_tokens
                and receipt is not None
                and producer_contract is not None
                and producer_contract.final_projection
                is ToolResultProjection.RECEIPT
            )
            else None
        )
        if replacement is None:
            selected.append(message)
            continue
        original_message_tokens = estimate_agent_messages_tokens((message,))
        replacement_tokens = estimate_agent_messages_tokens((replacement,))
        if replacement_tokens >= original_message_tokens:
            selected.append(message)
            continue
        selected.append(replacement)
        compacted.append(receipt.evidence_id)
        projected_tokens -= original_message_tokens - replacement_tokens

    selected_rows = tuple(selected)
    if selected_rows == rows:
        return RuntimeContextProjection(
            rows,
            mode="final_full_required",
            target_tokens=target_tokens,
        )
    return RuntimeContextProjection(
        messages=selected_rows,
        compacted_tool_results=tuple(compacted),
        saved_tokens=max(0, original_tokens - projected_tokens),
        mode="final_budget_pressure",
        target_tokens=target_tokens,
    )


__all__ = ["RuntimeContextProjection", "project_intermediate_tool_context"]
