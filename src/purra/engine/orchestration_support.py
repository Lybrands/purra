"""Shared settlement and phase helpers for the orchestration modules."""

from __future__ import annotations

from dataclasses import (
    dataclass,
)
from purra.cancellation import (
    ExecutionDeadlineExceeded,
    OperationCanceled,
)
from purra.contracts import (
    AgentRunRequest,
    AgentRunResult,
    AgentRuntimeResult,
    ContextBudget,
    ExecutionState,
    RunId,
    RuntimeOutcome,
    TraceRecord,
)
from purra.engine.canonical_sink import BufferedEventSink as _BufferedEventSink
from purra.engine.durable_execution import (
    DurableExecutionCompletion,
)
from purra.engine.options import (
    AgentCoreRunOptions,
)
from purra.errors import (
    AgentCoreError,
    ContextOverflowError,
    ContractViolationError,
)
from purra.events import (
    AgentEvent,
)
from purra.output.contracts import (
    ResponseTransactionMode,
)
from purra.run_controller import (
    AgentRunController,
)
from purra.task_admission import (
    LongTaskExecutionStatus,
)
from purra.timing import duration_ms as _duration_ms
from typing import (
    Any,
    AsyncIterator,
    Mapping,
)


def _run_result(
    controller: AgentRunController,
    *,
    model: str | None = None,
) -> AgentRunResult:
    snapshot = controller.snapshot
    if snapshot is None or not snapshot.terminal:
        raise RuntimeError("agent run has no terminal snapshot")
    return AgentRunResult(
        run_id=snapshot.run_id,
        status=snapshot.status,
        final_response=snapshot.final_response,
        error=snapshot.error,
        model=model,
    )


def _runtime_result_from_durable(
    completion: DurableExecutionCompletion,
    *,
    run_id: RunId | None,
    model: str,
) -> AgentRuntimeResult:
    result = completion.result
    if result.status is not LongTaskExecutionStatus.COMPLETED:
        raise ContractViolationError(
            "deferred durable completion must be successful"
        )
    return AgentRuntimeResult(
        run_id=run_id,
        outcome=RuntimeOutcome.COMPLETED,
        final_response=result.final_response,
        model=model,
        round_count=0,
    )


def _uses_validated_result(options: AgentCoreRunOptions) -> bool:
    return (
        options.resolved_response_transaction_policy.mode
        is ResponseTransactionMode.VALIDATED_RESULT
    )


async def _collect_durable_updates(
    updates: AsyncIterator[AgentEvent | DurableExecutionCompletion],
    *,
    run_id: RunId | None,
    model: str,
) -> tuple[AgentRuntimeResult | None, tuple[AgentEvent, ...]]:
    result = None
    events: list[AgentEvent] = []
    async for update in updates:
        if isinstance(update, DurableExecutionCompletion):
            result = _runtime_result_from_durable(
                update,
                run_id=run_id,
                model=model,
            )
        else:
            events.append(update)
    return result, tuple(events)


async def _record_safe_exception(
    controller: AgentRunController,
    *,
    stage: str,
    outcome: str,
    error: Exception,
    started: float | None = None,
    safe_details: Mapping[str, Any] | None = None,
) -> None:
    overflow_details = (
        {
            "reasonCode": error.reason_code,
            **error.details,
        }
        if isinstance(error, ContextOverflowError)
        else {}
    )
    await controller.record_trace(TraceRecord(
        stage=stage,
        outcome=outcome,
        details={
            "errorType": type(error).__name__,
            **overflow_details,
            **(safe_details or {}),
        },
        duration_ms=(_duration_ms(started) if started is not None else None),
    ))


async def _settle_execution_exception(
    controller: AgentRunController,
    sink: _BufferedEventSink,
    error: Exception,
) -> tuple[AgentEvent | AgentRunResult, ...] | None:
    snapshot = controller.snapshot
    if snapshot is None:
        return None
    if not snapshot.terminal:
        if isinstance(error, OperationCanceled):
            await controller.cancel("request_canceled")
        else:
            if not isinstance(error, ExecutionDeadlineExceeded):
                await _record_safe_exception(
                    controller,
                    stage="execution",
                    outcome="failed",
                    error=error,
                )
            await controller.fail(_execution_error_code(error))
    return (*sink.drain(), _run_result(controller))


async def record_stage_failure(
    controller: AgentRunController,
    *,
    error: Exception,
    stage: str,
    outcome: str,
    code: str,
    started: float | None = None,
) -> None:
    """Record a stage-scoped trace and fail the run with the given code."""
    await _record_safe_exception(
        controller,
        stage=stage,
        outcome=outcome,
        error=error,
        started=started,
    )
    await controller.fail(code)


def _execution_error_code(error: Exception) -> str:
    if isinstance(error, AgentCoreError):
        code = str(getattr(error, "code", "") or "").strip()
        if code:
            return code
    return "agent_execution_failed"


@dataclass(frozen=True, slots=True)
class _PreparedRuntimePhase:
    request: AgentRunRequest
    prepared_request: AgentRunRequest
    budget: ContextBudget
    state: ExecutionState
    events: tuple[AgentEvent, ...]
