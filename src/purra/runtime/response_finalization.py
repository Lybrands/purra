"""Buffered-response validation and deterministic final-response fallbacks."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping

from purra.contracts import AgentMessage, MessageRole, TraceRecord
from purra.output.contracts import ResponseTransactionMode
from purra.recovery import (
    RecoveryAction,
    RecoveryCause,
    RecoveryDecision,
    RecoveryLedger,
    RecoveryReason,
    RecoveryRequest,
)
from purra.recovery.guidance import (
    EMPTY_RESPONSE_RETRY_GUIDANCE,
    FAILED_TOOL_OUTPUT_RETRY_GUIDANCE,
    FINAL_PUBLIC_PRESENTATION_GUIDANCE,
    TEXTUAL_TOOL_CALL_RETRY_GUIDANCE,
)


_DECLINED_FINAL_RESPONSE_ZH = "您已拒绝审批；操作未执行，相关数据仍保留。"
_DECLINED_FINAL_RESPONSE_EN = (
    "You rejected the approval. The operation was not executed, and the "
    "related data remains unchanged."
)
_FAILED_TOOL_FINAL_RESPONSE_ZH = (
    "工具步骤未能完成，本轮没有生成可应用的正式结果。已完成的前序结果仍会保留，"
    "请重试；系统没有把未执行的工具文本或参数 JSON 当作成功结果。"
)
_FAILED_TOOL_FINAL_RESPONSE_EN = (
    "The tool step did not complete, so this run produced no applicable formal "
    "result. Earlier completed work remains available; please retry. Plain-text "
    "tool markup or argument JSON was not treated as a successful result."
)
_CJK_CHARACTER = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_TEXTUAL_TOOL_PROTOCOL_MARKER = re.compile(
    r"<\s*/?\s*(?:"
    r"tool(?:\s*[_-]?\s*(?:c(?:a(?:l(?:l)?)?)?)?)?(?=\s|>|/|$)"
    r"|function\s*="
    r"|parameter\s*="
    r")",
    re.IGNORECASE,
)
_TOP_LEVEL_NUMBERED_ITEM = re.compile(
    r"^(?P<number>[1-9][0-9]*)[.\u3001\uff0e)]\s+\S",
    re.MULTILINE,
)


class ResponseFinalizationDisposition(StrEnum):
    RETRY_MODEL = "retry_model"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class ResponseFinalizationResolution:
    disposition: ResponseFinalizationDisposition
    messages: tuple[AgentMessage, ...] = ()
    traces: tuple[TraceRecord, ...] = ()
    error_code: str | None = None
    response_repair_pending: bool = False


def is_textual_tool_call(content: str) -> bool:
    """Recognize inert text that imitates a structured tool protocol."""

    return bool(_TEXTUAL_TOOL_PROTOCOL_MARKER.search(str(content or "")))


def is_unstructured_tool_output(content: str) -> bool:
    """Reject inert protocol markup and JSON-shaped tool arguments."""

    normalized = str(content or "").strip()
    if is_textual_tool_call(normalized):
        return True
    if normalized.startswith("```") and normalized.endswith("```"):
        lines = normalized.splitlines()
        if len(lines) >= 3:
            normalized = "\n".join(lines[1:-1]).strip()
    try:
        value = json.loads(normalized)
    except ValueError:
        return False
    return isinstance(value, dict) and len(value) >= 2


def top_level_numbered_items(content: str) -> tuple[int, ...]:
    return tuple(
        int(match.group("number"))
        for match in _TOP_LEVEL_NUMBERED_ITEM.finditer(str(content or ""))
    )


def exact_item_count_repair_guidance(expected_count: int) -> str:
    return (
        f"Rewrite the complete answer with exactly {expected_count} top-level "
        "items. Each item must start at column zero with consecutive Arabic "
        f"markers 1. through {expected_count}. Use bullets, not numbered "
        "sublists, for details inside an item. Do not add another top-level "
        "table, list, appendix, or optional item."
    )


def response_constraint_repair_guidance(requirements: Sequence[str]) -> str:
    joined = "\n".join(
        f"- {str(requirement).strip()}"
        for requirement in requirements
        if str(requirement).strip()
    )
    return (
        "The preceding final response was withheld because it violated one "
        "or more host-owned response constraints. Rewrite the complete answer "
        "to satisfy every requirement below:\n"
        f"{joined}\n"
        "Preserve the user's requested language and all other host "
        "instructions. Do not make a tool call. Return the answer only."
    )


def resolve_response_recovery(
    *,
    content: str,
    reasoning: str,
    declined_response_pending: bool,
    failed_tool_recovery_error_code: str | None,
    recovery_ledger: RecoveryLedger,
    remaining_model_rounds: int,
    round_number: int,
    cancellation_requested: bool,
    visible_output_emitted: bool,
) -> ResponseFinalizationResolution | None:
    """Resolve invalid buffered model output before it can become public."""

    if declined_response_pending and is_textual_tool_call(content):
        decision = recovery_ledger.decide(RecoveryRequest(
            cause=RecoveryCause.UNSTRUCTURED_TOOL_PROTOCOL,
            action=RecoveryAction.RETRY_MODEL,
            remaining_model_rounds=remaining_model_rounds,
            cancellation_requested=cancellation_requested,
            visible_output_emitted=visible_output_emitted,
        ))
        return ResponseFinalizationResolution(
            disposition=_disposition(decision),
            messages=(
                (AgentMessage(
                    role=MessageRole.DEVELOPER,
                    content=TEXTUAL_TOOL_CALL_RETRY_GUIDANCE,
                ),)
                if decision.allowed
                else ()
            ),
            traces=(
                _recovery_trace(decision, round_number=round_number),
                TraceRecord(
                    stage="model_output",
                    outcome=(
                        "textual_tool_call_retry"
                        if decision.allowed
                        else "textual_tool_call_rejected"
                    ),
                    details={
                        "round": round_number,
                        "retryUsed": (
                            decision.attempt > 1
                            or decision.reason_code
                            is RecoveryReason.ATTEMPT_BUDGET_EXHAUSTED
                        ),
                    },
                ),
            ),
            error_code=(
                None
                if decision.allowed
                else "unstructured_tool_call_after_rejection"
            ),
        )

    if (
        failed_tool_recovery_error_code is not None
        and is_unstructured_tool_output(content)
    ):
        decision = recovery_ledger.decide(RecoveryRequest(
            cause=RecoveryCause.UNSTRUCTURED_TOOL_PROTOCOL,
            action=RecoveryAction.RETRY_MODEL,
            remaining_model_rounds=remaining_model_rounds,
            cancellation_requested=cancellation_requested,
            visible_output_emitted=visible_output_emitted,
        ))
        return ResponseFinalizationResolution(
            disposition=_disposition(decision),
            messages=(
                (AgentMessage(
                    role=MessageRole.DEVELOPER,
                    content=FAILED_TOOL_OUTPUT_RETRY_GUIDANCE,
                ),)
                if decision.allowed
                else ()
            ),
            traces=(
                _recovery_trace(decision, round_number=round_number),
                TraceRecord(
                    stage="model_output",
                    outcome=(
                        "unstructured_tool_output_retry"
                        if decision.allowed
                        else "unstructured_tool_output_replaced"
                    ),
                    details={
                        "round": round_number,
                        "retryUsed": (
                            decision.attempt > 1
                            or decision.reason_code
                            is RecoveryReason.ATTEMPT_BUDGET_EXHAUSTED
                        ),
                        "primaryErrorCode": failed_tool_recovery_error_code,
                    },
                ),
            ),
            error_code=(
                None
                if decision.allowed
                else failed_tool_recovery_error_code or "tool_execution_failed"
            ),
        )

    if declined_response_pending or content.strip():
        return None

    retry_count = recovery_ledger.attempts(RecoveryCause.EMPTY_MODEL_RESPONSE)
    decision = recovery_ledger.decide(RecoveryRequest(
        cause=RecoveryCause.EMPTY_MODEL_RESPONSE,
        action=RecoveryAction.RETRY_MODEL,
        remaining_model_rounds=remaining_model_rounds,
        cancellation_requested=cancellation_requested,
        visible_output_emitted=visible_output_emitted,
    ))
    return ResponseFinalizationResolution(
        disposition=_disposition(decision),
        messages=(
            (
                AgentMessage(
                    role=MessageRole.ASSISTANT,
                    content="",
                    reasoning=reasoning or None,
                ),
                AgentMessage(
                    role=MessageRole.DEVELOPER,
                    content=EMPTY_RESPONSE_RETRY_GUIDANCE,
                ),
            )
            if decision.allowed
            else ()
        ),
        traces=(
            _recovery_trace(decision, round_number=round_number),
            TraceRecord(
                stage="model_output",
                outcome=(
                    "empty_response_retry"
                    if decision.allowed
                    else "empty_response_rejected"
                ),
                details={
                    "round": round_number,
                    "retryCount": retry_count,
                    "retryScheduled": decision.allowed,
                    "reasoningCharacters": len(reasoning or ""),
                },
            ),
        ),
        error_code=None if decision.allowed else "empty_model_response",
    )


def resolve_response_constraint_recovery(
    *,
    content: str,
    reasoning: str,
    violation_codes: Sequence[str],
    repair_guidance: Sequence[str],
    validation_details: Sequence[Mapping[str, object]],
    exact_item_count: int | None,
    observed_items: Sequence[int],
    recovery_ledger: RecoveryLedger,
    remaining_model_rounds: int,
    round_number: int,
    cancellation_requested: bool,
    visible_output_emitted: bool,
) -> ResponseFinalizationResolution | None:
    """Choose the bounded repair action after host validation completes."""

    if not violation_codes:
        return None
    repair_phase = (
        "semantic"
        if validation_details
        and all(item.get("source") == "judge" for item in validation_details)
        else "deterministic"
    )
    repair_cause = (
        RecoveryCause.RESPONSE_CONSTRAINT_SEMANTIC
        if repair_phase == "semantic"
        else RecoveryCause.RESPONSE_CONSTRAINT_DETERMINISTIC
    )
    repair_phases_used = tuple(
        phase
        for phase, cause in (
            ("deterministic", RecoveryCause.RESPONSE_CONSTRAINT_DETERMINISTIC),
            ("semantic", RecoveryCause.RESPONSE_CONSTRAINT_SEMANTIC),
        )
        if recovery_ledger.attempts(cause) > 0
    )
    phase_retry_used = recovery_ledger.attempts(repair_cause) > 0
    decision = recovery_ledger.decide(RecoveryRequest(
        cause=repair_cause,
        action=RecoveryAction.RETRY_MODEL,
        remaining_model_rounds=remaining_model_rounds,
        retryable=len(repair_phases_used) < 2,
        cancellation_requested=cancellation_requested,
        visible_output_emitted=visible_output_emitted,
    ))
    details: dict[str, object] = {
        "round": round_number,
        "violationCodes": list(violation_codes),
        "retryUsed": phase_retry_used,
        "repairPhase": repair_phase,
        "repairAttempts": len(repair_phases_used),
        "repairPhasesUsed": sorted(repair_phases_used),
    }
    if exact_item_count is not None:
        details.update({
            "expectedItemCount": exact_item_count,
            "observedItems": list(observed_items),
        })
    if validation_details:
        details["validatorResults"] = [dict(item) for item in validation_details]
    return ResponseFinalizationResolution(
        disposition=_disposition(decision),
        messages=(
            (
                AgentMessage(
                    role=MessageRole.ASSISTANT,
                    content=content,
                    reasoning=reasoning or None,
                ),
                AgentMessage(
                    role=MessageRole.DEVELOPER,
                    content=response_constraint_repair_guidance(repair_guidance),
                ),
            )
            if decision.allowed
            else ()
        ),
        traces=(
            _recovery_trace(decision, round_number=round_number),
            TraceRecord(
                stage="model_output",
                outcome=(
                    "response_constraint_retry"
                    if decision.allowed
                    else "response_constraint_rejected"
                ),
                details=details,
            ),
        ),
        error_code=None if decision.allowed else "response_constraint_violation",
        response_repair_pending=decision.allowed,
    )


def public_presentation_messages(
    *,
    content: str,
    reasoning: str,
    invocation_had_tools: bool,
    buffered_model_content: bool,
    transaction_mode: ResponseTransactionMode,
    already_pending: bool,
) -> tuple[AgentMessage, ...]:
    """Return the tool-free round that turns private working output public."""

    if (
        not (invocation_had_tools or buffered_model_content)
        or transaction_mode is not ResponseTransactionMode.DIRECT_LIVE
        or already_pending
    ):
        return ()
    return (
        AgentMessage(
            role=MessageRole.ASSISTANT,
            content=content,
            reasoning=reasoning or None,
        ),
        AgentMessage(
            role=MessageRole.DEVELOPER,
            content=FINAL_PUBLIC_PRESENTATION_GUIDANCE,
        ),
    )


def declined_final_response(messages: Sequence[AgentMessage]) -> str:
    return _localized_response(
        messages,
        zh=_DECLINED_FINAL_RESPONSE_ZH,
        en=_DECLINED_FINAL_RESPONSE_EN,
    )


def failed_tool_final_response(messages: Sequence[AgentMessage]) -> str:
    return _localized_response(
        messages,
        zh=_FAILED_TOOL_FINAL_RESPONSE_ZH,
        en=_FAILED_TOOL_FINAL_RESPONSE_EN,
    )


def _disposition(
    decision: RecoveryDecision,
) -> ResponseFinalizationDisposition:
    return (
        ResponseFinalizationDisposition.RETRY_MODEL
        if decision.allowed
        else ResponseFinalizationDisposition.REJECT
    )


def _recovery_trace(
    decision: RecoveryDecision,
    *,
    round_number: int,
) -> TraceRecord:
    return TraceRecord(
        stage="recovery_decision",
        outcome="allowed" if decision.allowed else "denied",
        details={"round": round_number, **decision.to_trace_details()},
    )


def _localized_response(
    messages: Sequence[AgentMessage],
    *,
    zh: str,
    en: str,
) -> str:
    user_text = next(
        (
            str(message.content or "")
            for message in reversed(messages)
            if message.role is MessageRole.USER
        ),
        "",
    )
    return zh if _CJK_CHARACTER.search(user_text) else en


__all__ = [
    "ResponseFinalizationDisposition",
    "ResponseFinalizationResolution",
    "declined_final_response",
    "exact_item_count_repair_guidance",
    "failed_tool_final_response",
    "is_textual_tool_call",
    "is_unstructured_tool_output",
    "public_presentation_messages",
    "resolve_response_constraint_recovery",
    "resolve_response_recovery",
    "response_constraint_repair_guidance",
    "top_level_numbered_items",
]
