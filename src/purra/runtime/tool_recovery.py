"""Recovery decisions after an authoritative tool batch result exists."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Collection

from purra.contracts import (
    AgentMessage,
    MessageRole,
    ToolBatchOutcome,
    ToolBatchResult,
    ToolPlanningDisposition,
    TraceRecord,
)
from purra.recovery import (
    RecoveryAction,
    RecoveryCause,
    RecoveryDecision,
    RecoveryEffectState,
    RecoveryLedger,
    RecoveryRequest,
)
from purra.recovery.guidance import TOOL_INPUT_RETRY_GUIDANCE


_RECOVERABLE_TOOL_INPUT_ERROR_CODES = frozenset({
    "duplicate_tool_call_id",
    "invalid_tool_arguments_json",
    "invalid_tool_arguments_schema",
    "invalid_tool_arguments_shape",
    "invalid_tool_arguments_type",
    "invalid_tool_arguments_value",
    "invalid_tool_call_id",
    "invalid_tool_name",
    "too_many_tool_calls",
    "tool_arguments_too_large",
    "tool_input_invalid",
})


class ToolRecoveryDisposition(StrEnum):
    CONTINUE = "continue"
    RETRY_MODEL = "retry_model"
    REPLAN = "replan"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class ToolRecoveryResolution:
    disposition: ToolRecoveryDisposition
    next_input_recovery_epoch: int
    messages: tuple[AgentMessage, ...] = ()
    traces: tuple[TraceRecord, ...] = ()
    error_code: str | None = None


def resolve_tool_recovery(
    batch_result: ToolBatchResult,
    *,
    requested_names: Collection[str],
    planning_available: bool,
    recovery_ledger: RecoveryLedger,
    input_recovery_epoch: int,
    remaining_model_rounds: int,
    round_number: int,
    cancellation_requested: bool,
) -> ToolRecoveryResolution:
    """Select a bounded recovery without repeating uncertain side effects."""

    outcome = batch_result.outcome
    next_epoch = input_recovery_epoch + int(
        outcome is not ToolBatchOutcome.FAILED
    )
    traces: list[TraceRecord] = []

    if (
        outcome is ToolBatchOutcome.FAILED
        and _is_recoverable_tool_input_error(batch_result.error)
    ):
        input_decision = recovery_ledger.decide(RecoveryRequest(
            cause=RecoveryCause.TOOL_INPUT_INVALID,
            action=RecoveryAction.RETRY_MODEL,
            scope=f"tool-input-sequence:{input_recovery_epoch}",
            remaining_model_rounds=remaining_model_rounds,
            cancellation_requested=cancellation_requested,
            effect_state=RecoveryEffectState(batch_result.effect_state.value),
            may_repeat_side_effect=True,
        ))
        traces.append(_recovery_trace(
            input_decision,
            round_number=round_number,
            details={"sourceErrorCode": batch_result.error},
        ))
        if input_decision.allowed:
            traces.append(TraceRecord(
                stage="tool_recovery",
                outcome="input_retry_scheduled",
                details={
                    "round": round_number,
                    "errorCode": batch_result.error,
                    "retryAttempt": input_decision.attempt,
                    "requestedTools": sorted(requested_names),
                    "effectState": batch_result.effect_state.value,
                },
            ))
            return ToolRecoveryResolution(
                disposition=ToolRecoveryDisposition.RETRY_MODEL,
                next_input_recovery_epoch=next_epoch,
                messages=(AgentMessage(
                    role=MessageRole.DEVELOPER,
                    content=TOOL_INPUT_RETRY_GUIDANCE,
                ),),
                traces=tuple(traces),
            )

    if outcome is ToolBatchOutcome.FAILED:
        error_code = batch_result.error or "tool_execution_failed"
        if not planning_available:
            return ToolRecoveryResolution(
                disposition=ToolRecoveryDisposition.REJECT,
                next_input_recovery_epoch=next_epoch,
                traces=tuple(traces),
                error_code=error_code,
            )
        replan_decision = recovery_ledger.decide(RecoveryRequest(
            cause=RecoveryCause.TOOL_EXECUTION_FAILED_REPLAN,
            action=RecoveryAction.REPLAN,
            scope=f"tool-round:{round_number}",
            remaining_model_rounds=remaining_model_rounds,
            cancellation_requested=cancellation_requested,
            effect_state=RecoveryEffectState(batch_result.effect_state.value),
            may_repeat_side_effect=True,
        ))
        traces.append(_recovery_trace(
            replan_decision,
            round_number=round_number,
            details={"sourceErrorCode": batch_result.error},
        ))
        return ToolRecoveryResolution(
            disposition=(
                ToolRecoveryDisposition.REPLAN
                if replan_decision.allowed
                else ToolRecoveryDisposition.REJECT
            ),
            next_input_recovery_epoch=next_epoch,
            traces=tuple(traces),
            error_code=error_code,
        )

    if (
        planning_available
        and outcome in {
            ToolBatchOutcome.PROGRESSED,
            ToolBatchOutcome.COMPLETED,
        }
        and batch_result.replan_requested
    ):
        requested_by = sorted({
            result.tool_name
            for result in batch_result.results
            if result.planning_disposition is ToolPlanningDisposition.REPLAN
        })
        return ToolRecoveryResolution(
            disposition=ToolRecoveryDisposition.REPLAN,
            next_input_recovery_epoch=next_epoch,
            traces=(TraceRecord(
                stage="planning",
                outcome="replan_requested",
                details={
                    "round": round_number,
                    "outcome": outcome.value,
                    "requestedByTools": requested_by,
                },
            ),),
        )

    return ToolRecoveryResolution(
        disposition=ToolRecoveryDisposition.CONTINUE,
        next_input_recovery_epoch=next_epoch,
        traces=tuple(traces),
    )


def _is_recoverable_tool_input_error(error_code: str | None) -> bool:
    return str(error_code or "").strip() in _RECOVERABLE_TOOL_INPUT_ERROR_CODES


def _recovery_trace(
    decision: RecoveryDecision,
    *,
    round_number: int,
    details: dict[str, object] | None = None,
) -> TraceRecord:
    trace_details = {
        "round": round_number,
        **decision.to_trace_details(),
    }
    if details:
        trace_details.update(details)
    return TraceRecord(
        stage="recovery_decision",
        outcome="allowed" if decision.allowed else "denied",
        details=trace_details,
    )
