"""Zero-side-effect authorization decisions for one model tool-call batch."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Sequence

from purra.contracts import (
    AgentMessage,
    MessageOrigin,
    MessageRole,
    ToolCall,
    ToolCallResult,
    TraceRecord,
)
from purra.recovery import (
    RecoveryAction,
    RecoveryCause,
    RecoveryDecision,
    RecoveryLedger,
    RecoveryReason,
    RecoveryRequest,
)
from purra.recovery.guidance import (
    MALFORMED_TOOL_CALL_RETRY_GUIDANCE,
    MISSING_REQUIRED_TOOL_CALL_REPLAN_GUIDANCE,
    MISSING_REQUIRED_TOOL_CALL_RETRY_GUIDANCE,
    UNAUTHORIZED_TOOL_REPLAN_GUIDANCE,
)
from purra.runtime.tool_round import continuation_messages


class ToolAuthorizationDisposition(StrEnum):
    EXECUTE = "execute"
    RETRY_MODEL = "retry_model"
    REPLAN = "replan"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class ToolAuthorizationResolution:
    disposition: ToolAuthorizationDisposition
    requested_names: frozenset[str]
    messages: tuple[AgentMessage, ...] = ()
    traces: tuple[TraceRecord, ...] = ()
    error_code: str | None = None


def resolve_tool_protocol(
    calls: Sequence[ToolCall],
    *,
    tool_call_count: int,
    malformed_call_error: str | None,
    tool_finish: bool,
    require_tool: bool,
    planning_available: bool,
    declined_response_pending: bool,
    response_repair_pending: bool,
    public_presentation_pending: bool,
    recovery_ledger: RecoveryLedger,
    remaining_model_rounds: int,
    round_number: int,
    cancellation_requested: bool,
    visible_output_emitted: bool,
) -> ToolAuthorizationResolution | None:
    """Fail closed before authorization when the model protocol is invalid."""

    requested_names = frozenset(call.name for call in calls)
    if malformed_call_error is not None:
        decision = recovery_ledger.decide(RecoveryRequest(
            cause=RecoveryCause.MALFORMED_TOOL_CALL_BATCH,
            action=RecoveryAction.RETRY_MODEL,
            remaining_model_rounds=remaining_model_rounds,
            cancellation_requested=cancellation_requested,
            visible_output_emitted=visible_output_emitted,
        ))
        traces = (
            _recovery_trace(
                decision,
                round_number=round_number,
                details={"protocolReason": malformed_call_error},
            ),
            TraceRecord(
                stage="tool_authorization",
                outcome=(
                    "malformed_batch_retry"
                    if decision.allowed
                    else "malformed_batch"
                ),
                details={
                    "round": round_number,
                    "callCount": tool_call_count,
                    "reason": malformed_call_error,
                    "retryScheduled": decision.allowed,
                },
            ),
        )
        return ToolAuthorizationResolution(
            disposition=(
                ToolAuthorizationDisposition.RETRY_MODEL
                if decision.allowed
                else ToolAuthorizationDisposition.REJECT
            ),
            requested_names=requested_names,
            messages=(
                (AgentMessage(
                    role=MessageRole.DEVELOPER,
                    content=MALFORMED_TOOL_CALL_RETRY_GUIDANCE,
                ),)
                if decision.allowed
                else ()
            ),
            traces=traces,
            error_code=(
                None if decision.allowed else "malformed_tool_call_batch"
            ),
        )

    if require_tool and not calls:
        return _resolve_missing_required_call(
            requested_names=requested_names,
            planning_available=planning_available,
            recovery_ledger=recovery_ledger,
            remaining_model_rounds=remaining_model_rounds,
            round_number=round_number,
            cancellation_requested=cancellation_requested,
            visible_output_emitted=visible_output_emitted,
        )

    if calls and not tool_finish:
        return ToolAuthorizationResolution(
            disposition=ToolAuthorizationDisposition.REJECT,
            requested_names=requested_names,
            traces=(TraceRecord(
                stage="tool_authorization",
                outcome="malformed_batch",
                details={
                    "round": round_number,
                    "callCount": len(calls),
                    "reason": "tool_calls_without_supported_finish_reason",
                },
            ),),
            error_code="malformed_tool_call_batch",
        )

    forbidden: tuple[str, str] | None = None
    if calls and declined_response_pending:
        forbidden = (
            "rejected_after_approval_decline",
            "tool_call_after_approval_rejection",
        )
    elif calls and response_repair_pending:
        forbidden = (
            "rejected_during_response_repair",
            "tool_call_during_response_repair",
        )
    elif calls and public_presentation_pending:
        forbidden = (
            "rejected_during_public_presentation",
            "tool_call_during_public_presentation",
        )
    if forbidden is None:
        return None
    trace_outcome, error_code = forbidden
    return ToolAuthorizationResolution(
        disposition=ToolAuthorizationDisposition.REJECT,
        requested_names=requested_names,
        traces=(TraceRecord(
            stage="tool_authorization",
            outcome=trace_outcome,
            details={
                "round": round_number,
                "requestedTools": sorted(requested_names),
                "callCount": len(calls),
            },
        ),),
        error_code=error_code,
    )


def _resolve_missing_required_call(
    *,
    requested_names: frozenset[str],
    planning_available: bool,
    recovery_ledger: RecoveryLedger,
    remaining_model_rounds: int,
    round_number: int,
    cancellation_requested: bool,
    visible_output_emitted: bool,
) -> ToolAuthorizationResolution:
    retry_decision = recovery_ledger.decide(RecoveryRequest(
        cause=RecoveryCause.MISSING_REQUIRED_TOOL_CALL,
        action=RecoveryAction.RETRY_MODEL,
        remaining_model_rounds=remaining_model_rounds,
        minimum_remaining_rounds=2,
        cancellation_requested=cancellation_requested,
        visible_output_emitted=visible_output_emitted,
    ))
    traces = [_recovery_trace(retry_decision, round_number=round_number)]
    replan_decision: RecoveryDecision | None = None
    if not retry_decision.allowed and planning_available:
        replan_decision = recovery_ledger.decide(RecoveryRequest(
            cause=RecoveryCause.MISSING_REQUIRED_TOOL_CALL_REPLAN,
            action=RecoveryAction.REPLAN,
            remaining_model_rounds=remaining_model_rounds,
            cancellation_requested=cancellation_requested,
            visible_output_emitted=visible_output_emitted,
        ))
        traces.append(_recovery_trace(
            replan_decision,
            round_number=round_number,
        ))
    can_replan = bool(replan_decision and replan_decision.allowed)
    traces.append(TraceRecord(
        stage="tool_round",
        outcome=(
            "missing_required_call_retry"
            if retry_decision.allowed
            else (
                "missing_required_call_replan"
                if can_replan
                else "missing_required_call"
            )
        ),
        details={
            "round": round_number,
            "retryUsed": (
                retry_decision.attempt > 1
                or (not retry_decision.allowed and retry_decision.attempt > 0)
            ),
            "retryAttempt": (
                retry_decision.attempt if retry_decision.allowed else 0
            ),
            "replanUsed": bool(
                replan_decision
                and (
                    replan_decision.attempt > 1
                    or (
                        not replan_decision.allowed
                        and replan_decision.attempt > 0
                    )
                )
            ),
            "replanScheduled": can_replan,
            "roundsRemaining": remaining_model_rounds,
            "batchExecuted": False,
        },
    ))
    if retry_decision.allowed:
        return ToolAuthorizationResolution(
            disposition=ToolAuthorizationDisposition.RETRY_MODEL,
            requested_names=requested_names,
            messages=(AgentMessage(
                role=MessageRole.DEVELOPER,
                content=MISSING_REQUIRED_TOOL_CALL_RETRY_GUIDANCE,
            ),),
            traces=tuple(traces),
        )
    if can_replan:
        return ToolAuthorizationResolution(
            disposition=ToolAuthorizationDisposition.REPLAN,
            requested_names=requested_names,
            messages=(AgentMessage(
                role=MessageRole.DEVELOPER,
                content=MISSING_REQUIRED_TOOL_CALL_REPLAN_GUIDANCE,
            ),),
            traces=tuple(traces),
            error_code="missing_required_tool_call",
        )
    return ToolAuthorizationResolution(
        disposition=ToolAuthorizationDisposition.REJECT,
        requested_names=requested_names,
        traces=tuple(traces),
        error_code="missing_required_tool_call",
    )


def resolve_tool_authorization(
    calls: Sequence[ToolCall],
    *,
    allowed_names: frozenset[str],
    future_names: frozenset[str],
    require_tool: bool,
    planning_available: bool,
    content: str,
    reasoning: str,
    recovery_ledger: RecoveryLedger,
    remaining_model_rounds: int,
    round_number: int,
    cancellation_requested: bool,
    visible_output_emitted: bool,
) -> ToolAuthorizationResolution:
    """Classify a complete batch before any tool handler is allowed to run."""

    requested_names = frozenset(call.name for call in calls)
    if requested_names.issubset(allowed_names):
        return ToolAuthorizationResolution(
            disposition=ToolAuthorizationDisposition.EXECUTE,
            requested_names=requested_names,
        )

    includes_future = bool(requested_names - allowed_names) and bool(
        requested_names & future_names
    )
    if (
        includes_future
        and requested_names.issubset(allowed_names | future_names)
    ):
        return _resolve_future_batch(
            calls,
            requested_names=requested_names,
            allowed_names=allowed_names,
            future_names=future_names,
            content=content,
            reasoning=reasoning,
            recovery_ledger=recovery_ledger,
            remaining_model_rounds=remaining_model_rounds,
            round_number=round_number,
            cancellation_requested=cancellation_requested,
            visible_output_emitted=visible_output_emitted,
        )

    return _resolve_unauthorized_batch(
        calls,
        requested_names=requested_names,
        allowed_names=allowed_names,
        future_names=future_names,
        require_tool=require_tool,
        planning_available=planning_available,
        recovery_ledger=recovery_ledger,
        remaining_model_rounds=remaining_model_rounds,
        round_number=round_number,
        cancellation_requested=cancellation_requested,
        visible_output_emitted=visible_output_emitted,
    )


def _resolve_future_batch(
    calls: Sequence[ToolCall],
    *,
    requested_names: frozenset[str],
    allowed_names: frozenset[str],
    future_names: frozenset[str],
    content: str,
    reasoning: str,
    recovery_ledger: RecoveryLedger,
    remaining_model_rounds: int,
    round_number: int,
    cancellation_requested: bool,
    visible_output_emitted: bool,
) -> ToolAuthorizationResolution:
    decision = recovery_ledger.decide(RecoveryRequest(
        cause=RecoveryCause.FUTURE_TOOL_STEP,
        action=RecoveryAction.RETRY_MODEL,
        scope=_recovery_scope(allowed_names),
        remaining_model_rounds=remaining_model_rounds,
        minimum_remaining_rounds=2,
        retryable=bool(allowed_names),
        cancellation_requested=cancellation_requested,
        visible_output_emitted=visible_output_emitted,
    ))
    traces = [_recovery_trace(decision, round_number=round_number)]
    if decision.allowed:
        traces.append(TraceRecord(
            stage="tool_authorization",
            outcome="future_step_retry",
            details={
                "round": round_number,
                "requestedTools": sorted(requested_names),
                "currentTools": sorted(allowed_names),
                "futureTools": sorted(future_names),
                "callCount": len(calls),
                "batchExecuted": False,
                "executed": False,
            },
        ))
        return ToolAuthorizationResolution(
            disposition=ToolAuthorizationDisposition.RETRY_MODEL,
            requested_names=requested_names,
            messages=_future_step_retry_messages(
                calls,
                current_allowed=allowed_names,
                content=content,
                reasoning=reasoning,
            ),
            traces=tuple(traces),
        )

    traces.append(TraceRecord(
        stage="tool_authorization",
        outcome="future_step_rejected",
        details={
            "round": round_number,
            "requestedTools": sorted(requested_names),
            "currentTools": sorted(allowed_names),
            "futureTools": sorted(future_names),
            "retryAlreadyUsed": decision.attempt > 0,
            "roundsAvailable": decision.remaining_model_rounds >= 2,
        },
    ))
    return ToolAuthorizationResolution(
        disposition=ToolAuthorizationDisposition.REJECT,
        requested_names=requested_names,
        traces=tuple(traces),
        error_code="tool_step_out_of_order",
    )


def _resolve_unauthorized_batch(
    calls: Sequence[ToolCall],
    *,
    requested_names: frozenset[str],
    allowed_names: frozenset[str],
    future_names: frozenset[str],
    require_tool: bool,
    planning_available: bool,
    recovery_ledger: RecoveryLedger,
    remaining_model_rounds: int,
    round_number: int,
    cancellation_requested: bool,
    visible_output_emitted: bool,
) -> ToolAuthorizationResolution:
    request = RecoveryRequest(
        cause=RecoveryCause.UNAUTHORIZED_TOOL,
        action=RecoveryAction.RETRY_MODEL,
        scope=_recovery_scope(allowed_names),
        remaining_model_rounds=remaining_model_rounds,
        minimum_remaining_rounds=2,
        retryable=require_tool and bool(allowed_names),
        cancellation_requested=cancellation_requested,
        visible_output_emitted=visible_output_emitted,
    )
    decision = recovery_ledger.decide(request)
    traces = [_recovery_trace(decision, round_number=round_number)]
    details = {
        "round": round_number,
        "requestedTools": sorted(requested_names),
        "currentTools": sorted(allowed_names),
        "futureTools": sorted(future_names),
    }
    if decision.allowed:
        traces.append(TraceRecord(
            stage="tool_authorization",
            outcome="unauthorized_tool_retry",
            details={
                **details,
                "callCount": len(calls),
                "batchExecuted": False,
                "executed": False,
            },
        ))
        return ToolAuthorizationResolution(
            disposition=ToolAuthorizationDisposition.RETRY_MODEL,
            requested_names=requested_names,
            messages=_unauthorized_tool_retry_messages(allowed_names),
            traces=tuple(traces),
        )

    if (
        planning_available
        and decision.reason_code is RecoveryReason.ATTEMPT_BUDGET_EXHAUSTED
    ):
        replan_decision = recovery_ledger.decide(RecoveryRequest(
            cause=RecoveryCause.UNAUTHORIZED_TOOL_REPLAN,
            action=RecoveryAction.REPLAN,
            scope=_recovery_scope(allowed_names),
            remaining_model_rounds=remaining_model_rounds,
            minimum_remaining_rounds=2,
            retryable=require_tool and bool(allowed_names),
            cancellation_requested=cancellation_requested,
            visible_output_emitted=visible_output_emitted,
        ))
        traces.append(_recovery_trace(
            replan_decision,
            round_number=round_number,
        ))
        if replan_decision.allowed:
            traces.append(TraceRecord(
                stage="tool_authorization",
                outcome="unauthorized_tool_replan",
                details={
                    **details,
                    "batchExecuted": False,
                    "executed": False,
                },
            ))
            return ToolAuthorizationResolution(
                disposition=ToolAuthorizationDisposition.REPLAN,
                requested_names=requested_names,
                messages=(AgentMessage(
                    role=MessageRole.DEVELOPER,
                    content=UNAUTHORIZED_TOOL_REPLAN_GUIDANCE,
                ),),
                traces=tuple(traces),
                error_code="tool_not_authorized",
            )

    traces.append(TraceRecord(
        stage="tool_authorization",
        outcome="rejected",
        details={
            **details,
            "retryAlreadyUsed": decision.attempt > 0,
        },
    ))
    return ToolAuthorizationResolution(
        disposition=ToolAuthorizationDisposition.REJECT,
        requested_names=requested_names,
        traces=tuple(traces),
        error_code="tool_not_authorized",
    )


def _recovery_trace(
    decision: RecoveryDecision,
    *,
    round_number: int,
    details: dict[str, object] | None = None,
) -> TraceRecord:
    trace_details = {"round": round_number, **decision.to_trace_details()}
    if details:
        trace_details.update(details)
    return TraceRecord(
        stage="recovery_decision",
        outcome="allowed" if decision.allowed else "denied",
        details=trace_details,
    )


def _recovery_scope(allowed_names: frozenset[str]) -> str:
    return "tool-authorization:" + ",".join(sorted(allowed_names))


def _future_step_retry_messages(
    calls: Sequence[ToolCall],
    *,
    current_allowed: frozenset[str],
    content: str,
    reasoning: str,
) -> tuple[AgentMessage, ...]:
    error_code = "tool_step_out_of_order"
    error_content = json.dumps(
        {
            "success": False,
            "errorCode": error_code,
            "batchExecuted": False,
            "retryable": True,
            "currentAllowed": sorted(current_allowed),
            "error": (
                "The whole batch was not executed because it included a tool "
                "from a future plan step."
            ),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    messages = continuation_messages(
        calls,
        tuple(
            ToolCallResult(
                tool_call_id=call.id,
                tool_name=call.name,
                content=error_content,
                error=error_code,
            )
            for call in calls
        ),
        content=content,
        reasoning=reasoning,
    )
    messages.append(AgentMessage(
        role=MessageRole.DEVELOPER,
        content=(
            "The preceding tool-call batch had zero execution. In the next "
            "round, the tool schemas supplied with the invocation are the "
            "complete authorized set. Do not call a tool whose schema is "
            "absent."
        ),
    ))
    return tuple(messages)


def _unauthorized_tool_retry_messages(
    current_allowed: frozenset[str],
) -> tuple[AgentMessage, ...]:
    authorized = ", ".join(sorted(current_allowed))
    selection = (
        f"this host-authorized tool: {authorized}"
        if len(current_allowed) == 1
        else f"one of these host-authorized tools: {authorized}"
    )
    return (AgentMessage(
        role=MessageRole.DEVELOPER,
        content=(
            "The preceding tool-call batch was rejected with zero execution "
            "and has been removed from the retry context. Retry the current "
            f"plan step by calling {selection}. Do not call or imitate "
            "any other tool, including tools remembered from prior rounds or "
            "conversations."
        ),
    ),)
