"""Provider-neutral Agent model/tool loop orchestration.

This stage-3 runtime starts after the host has built and initially
budgeted context.  It owns model rounds, typed tool continuation, authorization,
round trimming, cancellation and the model-round limit. Host transports,
domain concepts and concrete tool handlers stay behind ports.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from time import perf_counter
from typing import Any, AsyncIterator, Mapping, Sequence
from uuid import uuid4

from purra.cancellation import (
    OperationCanceled,
    await_with_cancellation,
    is_canceled as _is_canceled,
    stop_reason,
)
from purra.context_budget import (
    context_budget_contract_error as _context_budget_contract_error,
    estimate_agent_messages_tokens,
    estimate_tool_schema_tokens,
    trim_agent_messages_by_turn,
)
from purra.context_orchestration.ledger import (
    ContextCompactionBudget,
    ContextCompactionPhase,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    AgentRuntimeResult,
    ContextBudget,
    ExecutionState,
    MessageOrigin,
    MessageRole,
    ModelFinishReason,
    ModelInvocation,
    ModelTokenUsage,
    ReasoningMode,
    ResponseConstraints,
    RuntimeLimits,
    RuntimeOutcome,
    RunId,
    ToolBatchOutcome,
    ToolBatchRequest,
    ToolBatchResult,
    ToolCall,
    ToolCallResult,
    ToolChoiceMode,
    ToolContextContract,
    ToolSchema,
    TraceRecord,
)
from purra.errors import (
    ContractViolationError,
    ModelGatewayError,
)
from purra.events import AgentEvent, CoreEventType
from purra.evidence import RunEvidenceStore
from purra.host_planned_tool_gateway import (
    HOST_PLANNED_EXECUTION_ROUTE,
)
from purra.model_invocation import (
    AgentModelInvocationManager,
    ModelInvocationContext,
)
from purra.model_invocation.manager import ModelInvocationOutputObserver
from purra.model_protocol import InvocationOutputLimit, classify_model_termination
from purra.output.contracts import ResponseTransactionMode
from purra.operations import (
    AgentOperationController,
    OperationScope,
)
from purra.output.response_validation import ResponseValidationCoordinator
from purra.recovery.guidance import (
    DECLINED_TOOL_GUIDANCE as _DECLINED_TOOL_GUIDANCE,
)
from purra.runtime_context import project_intermediate_tool_context
from purra.runtime.model_round import (
    ModelRoundAccumulator as _ModelRoundAccumulator,
    PendingProviderAttempt as _PendingProviderAttempt,
    ProviderFailureDisposition,
    ProviderFailurePhase,
    build_agent_model_call as _agent_model_call,
    model_request_fingerprint as _model_request_fingerprint,
    provider_retry_round_capacity,
    resolve_provider_failure,
    root_error_type as _root_error_type,
    truncation_trace_details,
)
from purra.runtime.response_finalization import (
    ResponseFinalizationDisposition,
    exact_item_count_repair_guidance as _exact_item_count_repair_guidance,
    public_presentation_messages,
    resolve_response_constraint_recovery,
    resolve_response_recovery,
    top_level_numbered_items as _top_level_numbered_items,
)
from purra.runtime.tool_authorization import (
    ToolAuthorizationDisposition,
    resolve_tool_authorization,
    resolve_tool_protocol,
)
from purra.runtime.tool_recovery import (
    ToolRecoveryDisposition,
    resolve_tool_recovery,
)
from purra.runtime.tool_round import (
    close_async_iterator as _close_async_iterator,
    continuation_messages as _continuation_messages,
    extend_progress_round_budget as _extend_progress_round_budget,
    results_match_calls as _results_match_calls,
    stream_tool_batch as _stream_tool_batch,
    tool_call_payload as _tool_call_payload,
    tool_result_payload as _tool_result_payload,
    tool_round_trace_details as _tool_round_trace_details,
)
from purra.timing import duration_ms as _duration_ms
from purra.recovery import (
    RecoveryLedger,
    RecoveryPolicy,
)
from purra.ports import (
    CancellationSignal,
    ConversationCompactor,
    EventSink,
    ModelGateway,
    ResponseJudge,
    ResponseValidator,
    RuntimeObserver,
    RuntimePlanningHook,
    ToolExecutionGateway,
)


RuntimeUpdate = AgentEvent | AgentRuntimeResult


@dataclass(slots=True)
class _RuntimeLoopState:
    messages: list[AgentMessage]
    invocation_context: ModelInvocationContext
    execution_state: ExecutionState
    evidence_store: RunEvidenceStore
    context_contracts: dict[str, ToolContextContract]
    configured_tools: tuple[ToolSchema, ...]
    tool_display_names: dict[str, Mapping[str, str]]
    validators: tuple[ResponseValidator, ...]
    judges: tuple[ResponseJudge, ...]
    transaction_mode: ResponseTransactionMode
    used_model: str
    logical_required_tool_call_enabled: bool
    provider_required_tool_choice_enabled: bool
    recovery_ledger: RecoveryLedger
    round_limit: int
    absolute_round_limit: int
    declined_response_pending: bool = False
    response_repair_pending: bool = False
    public_presentation_pending: bool = False
    pending_provider_attempt: _PendingProviderAttempt | None = None
    logical_round_number: int = 0
    dynamic_replan_pending: bool = False
    last_tool_outcome: ToolBatchOutcome = ToolBatchOutcome.COMPLETED
    pending_recovery_error_code: str | None = None
    failed_tool_recovery_error_code: str | None = None
    progress_rounds: int = 0
    tool_input_recovery_epoch: int = 0
    round_index: int = 0
    round_number: int = 0
    retry_round: bool = False
    terminal_result: AgentRuntimeResult | None = None
    provider_attempt: _PendingProviderAttempt | None = None
    stream: Any = None
    accumulator: _ModelRoundAccumulator | None = None
    direct_content_released: bool = False
    allowed_names: frozenset[str] = frozenset()
    future_names: frozenset[str] = frozenset()
    require_tool: bool = False
    buffer_model_content: bool = False
    calls: tuple[ToolCall, ...] = ()
    requested_names: frozenset[str] = frozenset()
    malformed_call_error: str | None = None
    tool_finish: bool = False
    request_fingerprint: str = ""
    emitted_delta_count: int = 0


class AgentRuntime:
    def __init__(
        self,
        *,
        model_gateway: ModelGateway,
        tool_execution_gateway: ToolExecutionGateway | None = None,
        observer: RuntimeObserver | None = None,
        context_compressor: ConversationCompactor | None = None,
        limits: RuntimeLimits = RuntimeLimits(),
        recovery_policy: RecoveryPolicy = RecoveryPolicy(),
        operation_controller: AgentOperationController | None = None,
        output_observer: ModelInvocationOutputObserver | None = None,
        model_manager: AgentModelInvocationManager | None = None,
    ):
        self._model_manager = model_manager or AgentModelInvocationManager(
            model_gateway,
            output_observer=output_observer,
            operation_controller=operation_controller,
            invocation_timeout_ms=limits.provider_invocation_timeout_ms,
            runtime_limits=limits,
        )
        self._tool_execution_gateway = tool_execution_gateway
        self._observer = observer
        self._context_compressor = context_compressor
        self._limits = limits
        self._recovery_policy = recovery_policy
        self._response_validation = ResponseValidationCoordinator(
            operation_controller
        )

    async def run(
        self,
        request: AgentRunRequest,
        *,
        tools: Sequence[ToolSchema] = (),
        response_constraints: ResponseConstraints = ResponseConstraints(),
        response_validators: Sequence[ResponseValidator] = (),
        response_judges: Sequence[ResponseJudge] = (),
        response_transaction_mode: ResponseTransactionMode | None = None,
        execution_state: ExecutionState | None = None,
        run_id: RunId | None = None,
        turn_id: str | None = None,
        context_budget: ContextBudget | None = None,
        output_limit: InvocationOutputLimit | None = None,
        round_input_tokens: int | None = None,
        scope_tools_to_observer: bool = True,
        force_tool_choice: bool = False,
        reasoning_mode: ReasoningMode = ReasoningMode.DEFAULT,
        require_tool_call: bool | None = None,
        tools_executable: bool = True,
        planning_hook: RuntimePlanningHook | None = None,
        tool_context_contracts: Mapping[str, ToolContextContract] | None = None,
        tool_argument_limits: Mapping[str, int] | None = None,
        stage_context_projection_enabled: bool = False,
        signal: CancellationSignal | None = None,
    ) -> AsyncIterator[RuntimeUpdate]:
        if not request.model.protocol_capabilities.reasoning_mode_is_supported(
            reasoning_mode
        ):
            yield _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                request.model.model,
                0,
                error_code="unsupported_reasoning_selection",
            )
            return
        loop = await self._build_loop_state(
            request,
            tools=tools,
            response_constraints=response_constraints,
            response_validators=response_validators,
            response_judges=response_judges,
            response_transaction_mode=response_transaction_mode,
            execution_state=execution_state,
            run_id=run_id,
            turn_id=turn_id,
            force_tool_choice=force_tool_choice,
            require_tool_call=require_tool_call,
            tool_context_contracts=tool_context_contracts,
            tool_argument_limits=tool_argument_limits,
        )
        budget_error = _context_budget_contract_error(
            request,
            context_budget,
            loop.configured_tools,
        )
        if budget_error is not None:
            await self._trace(
                "context_budget",
                "contract_mismatch",
                details={"errorCode": budget_error},
            )
            yield _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                0,
                error_code=budget_error,
            )
            return

        for round_index in range(loop.absolute_round_limit):
            loop.round_index = round_index
            loop.round_number = round_index + 1
            loop.retry_round = False
            loop.terminal_result = None
            loop.provider_attempt = None
            loop.stream = None
            loop.accumulator = None
            loop.calls = ()
            loop.requested_names = frozenset()
            if round_index >= loop.round_limit:
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    loop.used_model,
                    round_index,
                    error_code="max_model_rounds",
                )
                return
            if _is_canceled(signal):
                reason = stop_reason(signal)
                yield _runtime_result(
                    run_id,
                    (
                        RuntimeOutcome.FAILED
                        if reason.endswith("_deadline_exceeded")
                        else RuntimeOutcome.CANCELED
                    ),
                    loop.used_model,
                    round_index,
                    error_code=reason,
                )
                return

            await self._prepare_provider_attempt(
                loop,
                request,
                context_budget=context_budget,
                round_input_tokens=round_input_tokens,
                output_limit=output_limit,
                scope_tools_to_observer=scope_tools_to_observer,
                reasoning_mode=reasoning_mode,
                planning_hook=planning_hook,
                stage_context_projection_enabled=(
                    stage_context_projection_enabled
                ),
                signal=signal,
                run_id=run_id,
            )
            if loop.terminal_result is not None:
                yield loop.terminal_result
                return

            async for event in self._execute_provider_stream(
                loop,
                run_id=run_id,
                output_limit=output_limit,
                signal=signal,
            ):
                yield event
            if loop.terminal_result is not None:
                yield loop.terminal_result
                return
            if loop.retry_round:
                continue

            async for event in self._classify_model_output(
                loop,
                response_constraints=response_constraints,
                output_limit=output_limit,
                planning_hook=planning_hook,
                signal=signal,
                run_id=run_id,
            ):
                yield event
            if loop.terminal_result is not None:
                yield loop.terminal_result
                return
            if loop.retry_round:
                continue

            async for event in self._authorize_tool_batch(
                loop,
                planning_hook=planning_hook,
                scope_tools_to_observer=scope_tools_to_observer,
                tools_executable=tools_executable,
                signal=signal,
                run_id=run_id,
            ):
                yield event
            if loop.terminal_result is not None:
                yield loop.terminal_result
                return
            if loop.retry_round:
                continue

            async for event in self._execute_tool_batch(
                loop,
                planning_hook=planning_hook,
                scope_tools_to_observer=scope_tools_to_observer,
                signal=signal,
                run_id=run_id,
            ):
                yield event
            if loop.terminal_result is not None:
                yield loop.terminal_result
                return

        yield _runtime_result(
            run_id,
            RuntimeOutcome.FAILED,
            loop.used_model,
            loop.round_limit,
            error_code="max_model_rounds",
        )


    async def _authorize_tool_batch(
        self,
        loop: _RuntimeLoopState,
        *,
        planning_hook: RuntimePlanningHook | None,
        scope_tools_to_observer: bool,
        tools_executable: bool,
        signal: CancellationSignal | None,
        run_id: RunId | None,
    ) -> AsyncIterator[AgentEvent]:
        accumulator = loop.accumulator
        if accumulator is None or loop.stream is None:
            raise RuntimeError("tool batch has no settled model response")
        authorization = resolve_tool_authorization(
            loop.calls,
            allowed_names=loop.allowed_names,
            future_names=loop.future_names,
            require_tool=loop.require_tool,
            planning_available=planning_hook is not None,
            content=accumulator.content,
            reasoning=accumulator.reasoning,
            recovery_ledger=loop.recovery_ledger,
            remaining_model_rounds=max(
                0,
                loop.round_limit - loop.round_number,
            ),
            round_number=loop.round_number,
            cancellation_requested=_is_canceled(signal),
            visible_output_emitted=loop.direct_content_released,
        )
        for trace in authorization.traces:
            await self._trace(
                trace.stage,
                trace.outcome,
                details=dict(trace.details),
            )
        loop.requested_names = authorization.requested_names
        if authorization.disposition is ToolAuthorizationDisposition.RETRY_MODEL:
            loop.messages.extend(authorization.messages)
            loop.retry_round = True
            return
        if authorization.disposition is ToolAuthorizationDisposition.REPLAN:
            loop.last_tool_outcome = ToolBatchOutcome.FAILED
            loop.pending_recovery_error_code = authorization.error_code
            loop.failed_tool_recovery_error_code = authorization.error_code
            loop.dynamic_replan_pending = True
            loop.messages.extend(authorization.messages)
            loop.retry_round = True
            return
        if authorization.disposition is ToolAuthorizationDisposition.REJECT:
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                loop.round_number,
                error_code=authorization.error_code,
            )
            return
        if loop.round_index >= loop.round_limit - 1:
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                loop.round_number,
                error_code="max_model_rounds",
            )
            return
        await self._model_manager.publish_model_stream_commentary(
            loop.stream.receipt.output_stream_id
        )
        if scope_tools_to_observer and self._observer is not None:
            await self._observer.on_tool_calls_started(
                tuple(sorted(loop.requested_names))
            )
        yield AgentEvent(
            type=CoreEventType.TOOL_CALLS_STARTED,
            run_id=run_id,
            payload={
                "calls": [
                    _tool_call_payload(
                        call,
                        display_names=loop.tool_display_names.get(call.name),
                    )
                    for call in loop.calls
                ],
                "in_progress": True,
                "model": loop.used_model,
                "required": loop.require_tool,
            },
        )
        if not tools_executable or self._tool_execution_gateway is None:
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.COMPLETED,
                loop.used_model,
                loop.round_number,
                final_response=accumulator.content,
            )

    async def _execute_tool_batch(
        self,
        loop: _RuntimeLoopState,
        *,
        planning_hook: RuntimePlanningHook | None,
        scope_tools_to_observer: bool,
        signal: CancellationSignal | None,
        run_id: RunId | None,
    ) -> AsyncIterator[AgentEvent]:
        accumulator = loop.accumulator
        if accumulator is None or loop.stream is None:
            raise RuntimeError("tool execution has no settled model response")
        tool_started = perf_counter()
        try:
            batch_result: ToolBatchResult | None = None
            batch_stream = _stream_tool_batch(
                self._tool_execution_gateway,
                ToolBatchRequest(
                    run_id=run_id,
                    invocation_id=loop.stream.receipt.invocation_id,
                    calls=loop.calls,
                    allowed_tool_names=loop.allowed_names,
                    state=loop.execution_state,
                ),
                signal,
            )
            try:
                async for update in batch_stream:
                    if isinstance(update, AgentEvent):
                        yield update
                    else:
                        batch_result = update
            finally:
                await _close_async_iterator(batch_stream)
            if batch_result is None:
                raise RuntimeError("tool gateway returned no result")
        except OperationCanceled:
            await self._trace(
                "tool_round",
                "canceled",
                details={"round": loop.round_number},
                duration_ms=_duration_ms(tool_started),
            )
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.CANCELED,
                loop.used_model,
                loop.round_number,
                error_code="request_canceled",
            )
            return
        except Exception as error:
            await self._trace(
                "tool_round",
                "exception",
                details={
                    "round": loop.round_number,
                    "errorType": type(error).__name__,
                },
                duration_ms=_duration_ms(tool_started),
            )
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                loop.round_number,
                error_code="tool_execution_error",
            )
            return

        receipts = (
            loop.evidence_store.record_batch(loop.calls, batch_result)
            if _results_match_calls(loop.calls, batch_result.results)
            else ()
        )
        yield AgentEvent(
            type=CoreEventType.TOOL_RESULTS,
            run_id=run_id,
            payload={
                "results": [
                    _tool_result_payload(result)
                    for result in batch_result.results
                ],
                "toolResultReceipts": [
                    receipt.to_mapping() for receipt in receipts
                ],
            },
        )
        outcome = batch_result.outcome
        await self._trace(
            "tool_round",
            outcome.value,
            details=_tool_round_trace_details(
                round_number=loop.round_number,
                requested_names=loop.requested_names,
                allowed_names=loop.allowed_names,
                calls=loop.calls,
                batch_result=batch_result,
                evidence_record_count=len(
                    loop.evidence_store.tool_result_receipts()
                ),
                evidence_tokens=loop.evidence_store.token_estimate,
            ),
            duration_ms=_duration_ms(tool_started),
        )
        (
            loop.progress_rounds,
            loop.round_limit,
            progress_extension,
        ) = _extend_progress_round_budget(
            outcome,
            progress_rounds=loop.progress_rounds,
            round_limit=loop.round_limit,
            max_progress_rounds=self._limits.max_progress_rounds,
            round_number=loop.round_number,
        )
        if progress_extension is not None:
            await self._trace(
                "runtime_round_budget",
                "progress_extended",
                details=progress_extension,
            )
        if outcome is ToolBatchOutcome.CANCELED:
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.CANCELED,
                loop.used_model,
                loop.round_number,
                error_code=batch_result.error or "tool_execution_canceled",
            )
            return
        if outcome is ToolBatchOutcome.REJECTED:
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                loop.round_number,
                error_code=batch_result.error or "tool_execution_failed",
            )
            return
        if not _results_match_calls(loop.calls, batch_result.results):
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                loop.round_number,
                error_code="invalid_tool_results",
            )
            return
        if scope_tools_to_observer and self._observer is not None:
            if outcome is not ToolBatchOutcome.FAILED:
                await self._observer.on_tool_round_completed(outcome)
        elif not scope_tools_to_observer:
            loop.logical_required_tool_call_enabled = False
        yield AgentEvent(
            type=CoreEventType.TOOL_ROUND_COMPLETED,
            run_id=run_id,
            payload={"outcome": outcome.value},
        )
        loop.messages.extend(_continuation_messages(
            loop.calls,
            batch_result.results,
            content="" if loop.require_tool else accumulator.content,
            reasoning=accumulator.reasoning,
        ))
        recovery = resolve_tool_recovery(
            batch_result,
            requested_names=loop.requested_names,
            planning_available=planning_hook is not None,
            recovery_ledger=loop.recovery_ledger,
            input_recovery_epoch=loop.tool_input_recovery_epoch,
            remaining_model_rounds=max(
                0,
                loop.round_limit - loop.round_number,
            ),
            round_number=loop.round_number,
            cancellation_requested=_is_canceled(signal),
        )
        loop.tool_input_recovery_epoch = recovery.next_input_recovery_epoch
        for trace in recovery.traces:
            await self._trace(
                trace.stage,
                trace.outcome,
                details=dict(trace.details),
            )
        if recovery.disposition is ToolRecoveryDisposition.RETRY_MODEL:
            loop.messages.extend(recovery.messages)
            return
        if recovery.disposition is ToolRecoveryDisposition.REJECT:
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                loop.round_number,
                error_code=recovery.error_code,
            )
            return
        if recovery.disposition is ToolRecoveryDisposition.REPLAN:
            loop.last_tool_outcome = outcome
            code = (
                recovery.error_code
                if outcome is ToolBatchOutcome.FAILED
                else None
            )
            loop.pending_recovery_error_code = code
            loop.failed_tool_recovery_error_code = code
            loop.dynamic_replan_pending = True
        if outcome is ToolBatchOutcome.DECLINED:
            loop.declined_response_pending = True
            loop.messages.append(AgentMessage(
                role=MessageRole.DEVELOPER,
                content=_DECLINED_TOOL_GUIDANCE,
            ))

    async def _classify_model_output(
        self,
        loop: _RuntimeLoopState,
        *,
        response_constraints: ResponseConstraints,
        output_limit: InvocationOutputLimit | None,
        planning_hook: RuntimePlanningHook | None,
        signal: CancellationSignal | None,
        run_id: RunId | None,
    ) -> AsyncIterator[AgentEvent]:
        accumulator = loop.accumulator
        attempt = loop.provider_attempt
        if accumulator is None or attempt is None:
            raise RuntimeError("provider stream was not settled")
        finish_reason = accumulator.finish_reason
        if finish_reason is None:
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                loop.round_number,
                error_code="upstream_stream_interrupted",
            )
            return
        termination = classify_model_termination(
            finish_reason,
            tool_call_count=accumulator.tool_call_count,
        )
        if termination.incomplete:
            error_code = termination.error_code or "model_output_truncated"
            await self._trace(
                "model_output",
                "truncated",
                details=truncation_trace_details(
                    accumulator=accumulator,
                    round_number=loop.round_number,
                    attempt=attempt.attempt,
                    request_fingerprint=loop.request_fingerprint,
                    finish_reason=finish_reason,
                    error_code=error_code,
                    emitted_delta_count=loop.emitted_delta_count,
                    output_limit=output_limit,
                ),
            )
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                loop.round_number,
                error_code=error_code,
            )
            return
        loop.tool_finish = termination.authorizes_tool_calls
        protocol = resolve_tool_protocol(
            loop.calls,
            tool_call_count=accumulator.tool_call_count,
            malformed_call_error=loop.malformed_call_error,
            tool_finish=loop.tool_finish,
            require_tool=loop.require_tool,
            planning_available=planning_hook is not None,
            declined_response_pending=loop.declined_response_pending,
            response_repair_pending=loop.response_repair_pending,
            public_presentation_pending=loop.public_presentation_pending,
            recovery_ledger=loop.recovery_ledger,
            remaining_model_rounds=max(
                0,
                loop.round_limit - loop.round_number,
            ),
            round_number=loop.round_number,
            cancellation_requested=_is_canceled(signal),
            visible_output_emitted=loop.direct_content_released,
        )
        if protocol is not None:
            for trace in protocol.traces:
                await self._trace(
                    trace.stage,
                    trace.outcome,
                    details=dict(trace.details),
                )
            if protocol.disposition is ToolAuthorizationDisposition.RETRY_MODEL:
                loop.messages.extend(protocol.messages)
                loop.retry_round = True
                return
            if protocol.disposition is ToolAuthorizationDisposition.REPLAN:
                loop.last_tool_outcome = ToolBatchOutcome.FAILED
                loop.pending_recovery_error_code = protocol.error_code
                loop.failed_tool_recovery_error_code = protocol.error_code
                loop.dynamic_replan_pending = True
                loop.messages.extend(protocol.messages)
                loop.retry_round = True
                return
            if protocol.disposition is ToolAuthorizationDisposition.REJECT:
                loop.terminal_result = _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    loop.used_model,
                    loop.round_number,
                    error_code=protocol.error_code,
                )
                return
        if not loop.tool_finish or not loop.calls:
            async for event in self._finalize_model_response(
                loop,
                response_constraints=response_constraints,
                signal=signal,
                run_id=run_id,
            ):
                yield event

    async def _finalize_model_response(
        self,
        loop: _RuntimeLoopState,
        *,
        response_constraints: ResponseConstraints,
        signal: CancellationSignal | None,
        run_id: RunId | None,
    ) -> AsyncIterator[AgentEvent]:
        accumulator = loop.accumulator
        attempt = loop.provider_attempt
        if accumulator is None or attempt is None:
            raise RuntimeError("provider response was not prepared")
        recovery = resolve_response_recovery(
            content=accumulator.content,
            reasoning=accumulator.reasoning,
            declined_response_pending=loop.declined_response_pending,
            failed_tool_recovery_error_code=loop.failed_tool_recovery_error_code,
            recovery_ledger=loop.recovery_ledger,
            remaining_model_rounds=max(
                0,
                loop.round_limit - loop.round_number,
            ),
            round_number=loop.round_number,
            cancellation_requested=_is_canceled(signal),
            visible_output_emitted=loop.direct_content_released,
        )
        if recovery is not None:
            for trace in recovery.traces:
                await self._trace(
                    trace.stage,
                    trace.outcome,
                    details=dict(trace.details),
                )
            if recovery.disposition is ResponseFinalizationDisposition.RETRY_MODEL:
                loop.messages.extend(recovery.messages)
                loop.retry_round = True
            else:
                loop.terminal_result = _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    loop.used_model,
                    loop.round_number,
                    error_code=recovery.error_code,
                )
            return

        if not loop.declined_response_pending:
            exact_item_count = response_constraints.exact_top_level_item_count
            observed_items = _top_level_numbered_items(accumulator.content)
            violation_codes: list[str] = []
            repair_guidance: list[str] = []
            validation_details: list[dict[str, Any]] = []
            if exact_item_count is not None:
                expected_items = tuple(range(1, exact_item_count + 1))
                if observed_items != expected_items:
                    violation_codes.append("core.exact_top_level_item_count")
                    repair_guidance.append(
                        _exact_item_count_repair_guidance(exact_item_count)
                    )
            registered = await self._response_validation.validate_registered(
                content=accumulator.content,
                messages=loop.messages,
                validators=loop.validators,
                run_id=loop.invocation_context.run_id,
                round_number=loop.round_number,
            )
            for trace in registered.traces:
                await self._trace(
                    trace.stage,
                    trace.outcome,
                    details=dict(trace.details),
                    duration_ms=trace.duration_ms,
                )
            if registered.error_code is not None:
                loop.terminal_result = _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    loop.used_model,
                    loop.round_number,
                    error_code=registered.error_code,
                )
                return
            violation_codes.extend(registered.violation_codes)
            repair_guidance.extend(registered.repair_guidance)
            validation_details.extend(registered.validation_details)

            if not violation_codes:
                for index, judge in enumerate(loop.judges):
                    judge_attempt = await self._response_validation.begin_judge(
                        run_id=loop.invocation_context.run_id,
                        index=index,
                    )
                    yield AgentEvent(
                        type=CoreEventType.MODEL_CALL_RECORDED,
                        run_id=run_id,
                        payload={
                            "phase": "response_judge",
                            "count": 1,
                            "toolNames": [],
                            "toolChoice": "none",
                            "round": loop.round_number,
                            "judgeIndex": index,
                        },
                    )
                    semantic = await self._response_validation.judge(
                        judge_attempt,
                        judge,
                        content=accumulator.content,
                        messages=loop.messages,
                        signal=signal,
                        round_number=loop.round_number,
                    )
                    for trace in semantic.traces:
                        await self._trace(
                            trace.stage,
                            trace.outcome,
                            details=dict(trace.details),
                            duration_ms=trace.duration_ms,
                        )
                    if semantic.error_code is not None:
                        loop.terminal_result = _runtime_result(
                            run_id,
                            (
                                RuntimeOutcome.CANCELED
                                if semantic.canceled
                                else RuntimeOutcome.FAILED
                            ),
                            loop.used_model,
                            loop.round_number,
                            error_code=semantic.error_code,
                        )
                        return
                    violation_codes.extend(semantic.violation_codes)
                    repair_guidance.extend(semantic.repair_guidance)
                    validation_details.extend(semantic.validation_details)

            constraint_recovery = resolve_response_constraint_recovery(
                content=accumulator.content,
                reasoning=accumulator.reasoning,
                violation_codes=violation_codes,
                repair_guidance=repair_guidance,
                validation_details=validation_details,
                exact_item_count=exact_item_count,
                observed_items=observed_items,
                recovery_ledger=loop.recovery_ledger,
                remaining_model_rounds=max(
                    0,
                    loop.round_limit - loop.round_number,
                ),
                round_number=loop.round_number,
                cancellation_requested=_is_canceled(signal),
                visible_output_emitted=loop.direct_content_released,
            )
            if constraint_recovery is not None:
                for trace in constraint_recovery.traces:
                    await self._trace(
                        trace.stage,
                        trace.outcome,
                        details=dict(trace.details),
                    )
                if (
                    constraint_recovery.disposition
                    is ResponseFinalizationDisposition.RETRY_MODEL
                ):
                    loop.response_repair_pending = (
                        constraint_recovery.response_repair_pending
                    )
                    loop.messages.extend(constraint_recovery.messages)
                    loop.retry_round = True
                else:
                    loop.terminal_result = _runtime_result(
                        run_id,
                        RuntimeOutcome.FAILED,
                        loop.used_model,
                        loop.round_number,
                        error_code=constraint_recovery.error_code,
                    )
                return

        final_response = accumulator.content
        presentation_messages = public_presentation_messages(
            content=final_response,
            reasoning=accumulator.reasoning,
            invocation_had_tools=bool(attempt.invocation.tools),
            buffered_model_content=loop.buffer_model_content,
            transaction_mode=loop.transaction_mode,
            already_pending=loop.public_presentation_pending,
        )
        if presentation_messages:
            loop.public_presentation_pending = True
            loop.round_limit += 1
            loop.messages.extend(presentation_messages)
            loop.retry_round = True
            return
        if loop.buffer_model_content:
            if self._observer is not None:
                await self._observer.on_model_delta()
        elif not loop.direct_content_released and final_response:
            if self._observer is not None:
                await self._observer.on_model_delta()
        loop.terminal_result = _runtime_result(
            run_id,
            RuntimeOutcome.COMPLETED,
            loop.used_model,
            loop.round_number,
            final_response=final_response,
        )

    async def _execute_provider_stream(
        self,
        loop: _RuntimeLoopState,
        *,
        run_id: RunId | None,
        output_limit: InvocationOutputLimit | None,
        signal: CancellationSignal | None,
    ) -> AsyncIterator[AgentEvent]:
        attempt = loop.provider_attempt
        if attempt is None:
            raise RuntimeError("provider attempt was not prepared")
        invocation = attempt.invocation
        model_started = perf_counter()
        accumulator = _ModelRoundAccumulator()
        received_chunks = 0
        emitted_deltas = 0
        direct_content_released = False
        request_fingerprint = _model_request_fingerprint(
            attempt.messages,
            invocation,
        )
        try:
            stream = await self._model_manager.stream(
                attempt.messages,
                _agent_model_call(
                    invocation,
                    require_tool=attempt.require_tool,
                    requires_full_text_validation=bool(
                        attempt.buffer_model_content
                        or loop.validators
                        or loop.judges
                    ),
                ),
                replace(
                    loop.invocation_context,
                    attempt_source_key=(
                        f"runtime:{loop.invocation_context.run_id}:"
                        f"round:{loop.round_number}:request:{request_fingerprint}"
                    ),
                ),
                signal,
            )
            parameters = stream.receipt.call_parameters[0]
            host_planned_dispatch = (
                parameters.get("executionRoute")
                == HOST_PLANNED_EXECUTION_ROUTE
            )
            yield AgentEvent(
                type=(
                    CoreEventType.HOST_PLANNED_TOOL_DISPATCHED
                    if host_planned_dispatch
                    else CoreEventType.MODEL_CALL_RECORDED
                ),
                run_id=run_id,
                payload={
                    "phase": "generation",
                    "count": 0 if host_planned_dispatch else 1,
                    "toolNames": [
                        schema.name for schema in invocation.tools
                    ],
                    "toolChoice": invocation.tool_choice.value,
                    "round": loop.round_number,
                    "logicalRound": attempt.logical_round,
                    "attempt": attempt.attempt,
                    "requestFingerprint": request_fingerprint,
                    "parameters": parameters,
                },
            )
        except Exception as error:
            failure = resolve_provider_failure(
                error,
                phase=ProviderFailurePhase.OPENING,
                attempt=attempt,
                recovery_ledger=loop.recovery_ledger,
                remaining_model_rounds=max(
                    0,
                    loop.round_limit - loop.round_number,
                ),
                round_number=loop.round_number,
                cancellation_requested=_is_canceled(signal),
                received_chunk_count=0,
                emitted_delta_count=0,
                duration_ms=_duration_ms(model_started),
            )
            await self._record_provider_failure(loop, failure)
            if failure.next_attempt is not None:
                loop.round_limit += 1
                loop.pending_provider_attempt = failure.next_attempt
                loop.retry_round = True
            else:
                loop.terminal_result = _runtime_result(
                    run_id,
                    (
                        RuntimeOutcome.CANCELED
                        if failure.disposition
                        is ProviderFailureDisposition.CANCEL
                        else RuntimeOutcome.FAILED
                    ),
                    loop.used_model,
                    loop.round_number,
                    error_code=failure.error_code,
                )
            return

        loop.used_model = stream.receipt.model or loop.used_model
        stream_error: Exception | None = None
        chunks = stream.chunks
        try:
            while True:
                try:
                    chunk = await await_with_cancellation(anext(chunks), signal)
                except StopAsyncIteration:
                    break
                received_chunks += 1
                accumulator.add(chunk)
                if chunk.reasoning_delta:
                    emitted_deltas += 1
                if (
                    chunk.content_delta
                    and not attempt.require_tool
                    and not attempt.buffer_model_content
                    and not invocation.tools
                ):
                    direct_content_released = True
                    if self._observer is not None:
                        await self._observer.on_model_delta()
                    emitted_deltas += 1
                if chunk.finish_reason is not None:
                    break
        except Exception as error:
            stream_error = error
        finally:
            await _close_async_iterator(chunks)
        if stream_error is None and accumulator.finish_reason is None:
            stream_error = (
                OperationCanceled("agent run was canceled")
                if _is_canceled(signal)
                else ModelGatewayError(
                    "model stream ended without a finish reason",
                    code="upstream_stream_interrupted",
                    retryable=True,
                )
            )
        if stream_error is not None:
            deadline_code = str(getattr(stream_error, "code", "") or "")
            if deadline_code.endswith("_deadline_exceeded"):
                loop.terminal_result = _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    loop.used_model,
                    loop.round_number,
                    error_code=deadline_code,
                )
                return
            failure = resolve_provider_failure(
                stream_error,
                phase=ProviderFailurePhase.STREAMING,
                attempt=attempt,
                recovery_ledger=loop.recovery_ledger,
                remaining_model_rounds=max(
                    0,
                    loop.round_limit - loop.round_number,
                ),
                round_number=loop.round_number,
                cancellation_requested=_is_canceled(signal),
                received_chunk_count=received_chunks,
                emitted_delta_count=emitted_deltas,
                visible_output_emitted=direct_content_released,
                provider_finish_observed=accumulator.finish_reason is not None,
                duration_ms=_duration_ms(model_started),
            )
            await self._record_provider_failure(loop, failure)
            if failure.next_attempt is not None:
                loop.round_limit += 1
                loop.pending_provider_attempt = failure.next_attempt
                loop.retry_round = True
            else:
                loop.terminal_result = _runtime_result(
                    run_id,
                    (
                        RuntimeOutcome.CANCELED
                        if failure.disposition
                        is ProviderFailureDisposition.CANCEL
                        else RuntimeOutcome.FAILED
                    ),
                    loop.used_model,
                    loop.round_number,
                    error_code=failure.error_code,
                )
            return

        calls, malformed_call_error = accumulator.tool_calls()
        finish_reason = accumulator.finish_reason
        local_input_estimate = (
            estimate_agent_messages_tokens(attempt.messages)
            + estimate_tool_schema_tokens(invocation.tools)
        )
        if accumulator.usage is not None:
            usage = accumulator.usage
            await self._trace(
                "model_usage",
                "provider_reported",
                details={
                    "round": loop.round_number,
                    "attempt": attempt.attempt,
                    "logicalRound": attempt.logical_round,
                    "actualInputTokens": usage.input_tokens,
                    "actualOutputTokens": usage.output_tokens,
                    "actualTotalTokens": usage.total_tokens,
                    "cachedInputTokens": usage.cached_input_tokens,
                    "reasoningOutputTokens": usage.reasoning_output_tokens,
                    "requestedOutputTokens": invocation.max_output_tokens,
                    "finishReason": (
                        finish_reason.value if finish_reason is not None else None
                    ),
                    "outputLimit": (
                        invocation.output_limit.to_mapping()
                        if invocation.output_limit is not None
                        else None
                    ),
                    "localInputEstimate": local_input_estimate,
                },
            )
            if attempt.logical_round == 1:
                yield AgentEvent(
                    type=CoreEventType.CONTEXT_USAGE_RECORDED,
                    run_id=run_id,
                    payload={
                        "actualInputTokens": usage.input_tokens,
                        "actualOutputTokens": usage.output_tokens,
                        "actualTotalTokens": usage.total_tokens,
                        "cachedInputTokens": usage.cached_input_tokens,
                        "reasoningOutputTokens": usage.reasoning_output_tokens,
                        "actualUsageRound": attempt.logical_round,
                        "inputTokenEstimateAtUsage": local_input_estimate,
                        "usageSource": "provider",
                        "requestedOutputTokens": invocation.max_output_tokens,
                        "finishReason": (
                            finish_reason.value
                            if finish_reason is not None
                            else None
                        ),
                        "outputLimit": (
                            invocation.output_limit.to_mapping()
                            if invocation.output_limit is not None
                            else None
                        ),
                    },
                )
        await self._trace(
            "tool_dispatch" if host_planned_dispatch else "model_round",
            (
                "host_planned_call"
                if host_planned_dispatch
                else finish_reason.value
                if finish_reason is not None
                else "stream_end"
            ),
            details={
                "round": loop.round_number,
                "attempt": attempt.attempt,
                "logicalRound": attempt.logical_round,
                "toolCallCount": accumulator.tool_call_count,
                "receivedChunkCount": received_chunks,
                "emittedDeltaCount": emitted_deltas,
                "retryScheduled": False,
                "providerAttemptTerminal": True,
                "batchExecuted": False,
            },
            duration_ms=_duration_ms(model_started),
        )
        loop.stream = stream
        loop.accumulator = accumulator
        loop.direct_content_released = direct_content_released
        loop.allowed_names = attempt.allowed_names
        loop.future_names = attempt.future_names
        loop.require_tool = attempt.require_tool
        loop.buffer_model_content = attempt.buffer_model_content
        loop.calls = calls
        loop.malformed_call_error = malformed_call_error
        loop.request_fingerprint = request_fingerprint
        loop.emitted_delta_count = emitted_deltas

    async def _record_provider_failure(
        self,
        loop: _RuntimeLoopState,
        failure,
    ) -> None:
        for trace in failure.traces:
            await self._trace(
                trace.stage,
                trace.outcome,
                details=dict(trace.details),
                duration_ms=trace.duration_ms,
            )
        if failure.disable_required_tool_choice:
            loop.provider_required_tool_choice_enabled = False

    async def _prepare_provider_attempt(
        self,
        loop: _RuntimeLoopState,
        request: AgentRunRequest,
        *,
        context_budget: ContextBudget | None,
        round_input_tokens: int | None,
        output_limit: InvocationOutputLimit | None,
        scope_tools_to_observer: bool,
        reasoning_mode: ReasoningMode,
        planning_hook: RuntimePlanningHook | None,
        stage_context_projection_enabled: bool,
        signal: CancellationSignal | None,
        run_id: RunId | None,
    ) -> None:
        if loop.pending_provider_attempt is not None:
            loop.provider_attempt = loop.pending_provider_attempt
            loop.pending_provider_attempt = None
            return
        if loop.dynamic_replan_pending and planning_hook is not None:
            started = perf_counter()
            try:
                guidance = await planning_hook.replan_after_tool(
                    loop.evidence_store.project_messages_for_planning(
                        loop.messages
                    ),
                    round_number=loop.round_number,
                    remaining_model_rounds=max(
                        0,
                        loop.round_limit - loop.round_index,
                    ),
                    outcome=loop.last_tool_outcome,
                    signal=signal,
                )
                if guidance is not None:
                    loop.messages.append(guidance)
            except OperationCanceled:
                await self._trace(
                    "planning",
                    "canceled",
                    details={"dynamic": True, "round": loop.round_number},
                    duration_ms=_duration_ms(started),
                )
                loop.terminal_result = _runtime_result(
                    run_id,
                    RuntimeOutcome.CANCELED,
                    loop.used_model,
                    loop.round_index,
                    error_code="request_canceled",
                )
                return
            except Exception as error:
                primary_error_code = loop.pending_recovery_error_code
                await self._trace(
                    "planning",
                    "failed",
                    details={
                        "dynamic": True,
                        "round": loop.round_number,
                        "errorType": _root_error_type(error),
                        "primaryErrorCode": primary_error_code,
                        "recoveryErrorCode": "dynamic_planning_failed",
                    },
                    duration_ms=_duration_ms(started),
                )
                loop.terminal_result = _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    loop.used_model,
                    loop.round_index,
                    error_code=primary_error_code or "dynamic_planning_failed",
                )
                return
            loop.dynamic_replan_pending = False
            loop.pending_recovery_error_code = None
        await self._prepare_projected_context(
            loop,
            request,
            context_budget=context_budget,
            round_input_tokens=round_input_tokens,
            output_limit=output_limit,
            scope_tools_to_observer=scope_tools_to_observer,
            reasoning_mode=reasoning_mode,
            stage_context_projection_enabled=stage_context_projection_enabled,
            signal=signal,
            run_id=run_id,
        )

    async def _prepare_projected_context(
        self,
        loop: _RuntimeLoopState,
        request: AgentRunRequest,
        *,
        context_budget: ContextBudget | None,
        round_input_tokens: int | None,
        output_limit: InvocationOutputLimit | None,
        scope_tools_to_observer: bool,
        reasoning_mode: ReasoningMode,
        stage_context_projection_enabled: bool,
        signal: CancellationSignal | None,
        run_id: RunId | None,
    ) -> None:
        initial_round = loop.logical_round_number == 0
        token_budget = (
            (
                context_budget.provider_input_tokens
                if initial_round
                else context_budget.round_input_tokens
            )
            if context_budget is not None
            else (None if initial_round else round_input_tokens)
        )
        allowed_names = self._allowed_names(
            loop.configured_tools,
            scope_tools_to_observer=scope_tools_to_observer,
        )
        future_names = self._future_allowed_names(
            loop.configured_tools,
            scope_tools_to_observer=scope_tools_to_observer,
        )
        visible_tools = (
            ()
            if (
                loop.declined_response_pending
                or loop.response_repair_pending
                or loop.public_presentation_pending
            )
            else tuple(
                schema
                for schema in loop.configured_tools
                if schema.name in allowed_names
            )
        )
        require_tool = bool(
            loop.logical_required_tool_call_enabled and visible_tools
        )
        force_required = bool(
            loop.provider_required_tool_choice_enabled and visible_tools
        )
        buffer_content = bool(
            require_tool
            or (
                not loop.public_presentation_pending
                and (
                    loop.declined_response_pending
                    or loop.failed_tool_recovery_error_code is not None
                    or loop.transaction_mode
                    is ResponseTransactionMode.VALIDATED_RESULT
                )
            )
        )
        projection = project_intermediate_tool_context(
            loop.messages,
            visible_tool_names=frozenset(
                schema.name for schema in visible_tools
            ),
            contracts=loop.context_contracts,
            evidence_store=loop.evidence_store,
            enabled=stage_context_projection_enabled,
            initial_round=initial_round,
            token_budget=token_budget,
        )
        canonical_tokens = estimate_agent_messages_tokens(loop.messages)
        projected_tokens = estimate_agent_messages_tokens(projection.messages)
        compression_outcome: str | None = None
        compression_strategy: str | None = None
        if token_budget is not None and self._context_compressor is not None:
            compressed = await await_with_cancellation(
                self._context_compressor.prepare(
                    replace(
                        request,
                        messages=projection.messages,
                        metadata={
                            **request.metadata,
                            "contextCompressionScope": "runtime",
                            "runtimeLogicalRound": loop.logical_round_number + 1,
                        },
                    ),
                    signal,
                    budget=ContextCompactionBudget(
                        phase=ContextCompactionPhase.MODEL_CALL,
                        provider_input_tokens=token_budget,
                        context_tokens=0,
                        context_tokens_are_resolved=True,
                        output_reserve_tokens=(
                            context_budget.output_reserve_tokens
                            if context_budget is not None
                            else 1
                        ),
                    ),
                    operation_scope=OperationScope(
                        run_id=loop.invocation_context.run_id,
                    ),
                ),
                signal,
            )
            round_messages = compressed.request.messages
            sent_tokens = estimate_agent_messages_tokens(round_messages)
            dropped_messages = max(
                0,
                len(projection.messages) - len(round_messages),
            )
            overflow_tokens = max(0, sent_tokens - token_budget)
            compression_outcome = compressed.outcome
            compression_strategy = str(
                compressed.diagnostics.get("strategy") or ""
            )
        elif token_budget is not None:
            trimmed = trim_agent_messages_by_turn(
                projection.messages,
                token_budget,
            )
            round_messages = trimmed.messages
            sent_tokens = trimmed.token_estimate
            dropped_messages = trimmed.dropped_count
            overflow_tokens = trimmed.overflow_tokens
        else:
            round_messages = projection.messages
            sent_tokens = projected_tokens
            dropped_messages = 0
            overflow_tokens = 0
        if projection.dropped_context_blocks or projection.compacted_tool_results:
            await self._trace(
                "context_projection",
                projection.mode,
                details={
                    "round": loop.round_number,
                    "toolNames": sorted(schema.name for schema in visible_tools),
                    "droppedContextBlocks": list(
                        projection.dropped_context_blocks
                    ),
                    "compactedToolResults": list(
                        projection.compacted_tool_results
                    ),
                    "savedTokens": projection.saved_tokens,
                    "targetTokens": projection.target_tokens,
                    "canonicalTokens": canonical_tokens,
                    "projectedTokens": projected_tokens,
                },
            )
        await self._trace(
            "runtime_context_budget",
            (
                "overflow"
                if overflow_tokens
                else "rebalanced"
                if projection.saved_tokens > 0 or dropped_messages > 0
                else "within_budget"
            ),
            details={
                "round": loop.round_number,
                "logicalRound": loop.logical_round_number + 1,
                "initialRound": initial_round,
                "tokenBudget": token_budget,
                "canonicalTokens": canonical_tokens,
                "projectedTokens": projected_tokens,
                "sentTokens": sent_tokens,
                "pressureRatio": (
                    round(sent_tokens / token_budget, 4) if token_budget else None
                ),
                "projectionMode": projection.mode,
                "projectionSavedTokens": projection.saved_tokens,
                "compressionOutcome": compression_outcome,
                "compressionStrategy": compression_strategy,
                "droppedMessages": dropped_messages,
                "overflowTokens": overflow_tokens,
                "completeEvidenceTokens": loop.evidence_store.token_estimate,
            },
        )
        if overflow_tokens > 0:
            await self._trace(
                "context_budget",
                "overflow_initial" if initial_round else "overflow_after_tool",
                details={
                    "round": loop.round_number,
                    "tokenBudget": token_budget,
                    "canonicalTokens": canonical_tokens,
                    "projectedTokens": projected_tokens,
                    "sentTokens": sent_tokens,
                    "projectionMode": projection.mode,
                    "projectionSavedTokens": projection.saved_tokens,
                    "overflowTokens": overflow_tokens,
                },
            )
            loop.terminal_result = _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                loop.used_model,
                loop.round_index,
                error_code=(
                    "context_overflow_initial"
                    if initial_round
                    else "context_overflow_after_tool"
                ),
            )
            return
        loop.logical_round_number += 1
        loop.provider_attempt = _PendingProviderAttempt(
            messages=round_messages,
            invocation=ModelInvocation(
                request=request.model,
                tools=visible_tools,
                tool_choice=(
                    ToolChoiceMode.REQUIRED
                    if force_required
                    else ToolChoiceMode.AUTO
                    if visible_tools
                    else ToolChoiceMode.NONE
                ),
                output_limit=output_limit,
                reasoning_mode=reasoning_mode,
            ),
            allowed_names=allowed_names,
            future_names=future_names,
            require_tool=require_tool,
            buffer_model_content=buffer_content,
            logical_round=loop.logical_round_number,
            attempt=1,
        )

    async def _build_loop_state(
        self,
        request: AgentRunRequest,
        *,
        tools: Sequence[ToolSchema],
        response_constraints: ResponseConstraints,
        response_validators: Sequence[ResponseValidator],
        response_judges: Sequence[ResponseJudge],
        response_transaction_mode: ResponseTransactionMode | None,
        execution_state: ExecutionState | None,
        run_id: RunId | None,
        turn_id: str | None,
        force_tool_choice: bool,
        require_tool_call: bool | None,
        tool_context_contracts: Mapping[str, ToolContextContract] | None,
        tool_argument_limits: Mapping[str, int] | None,
    ) -> _RuntimeLoopState:
        messages = list(request.messages)
        validators = tuple(response_validators)
        judges = tuple(response_judges)
        transaction_mode = (
            ResponseTransactionMode(response_transaction_mode)
            if response_transaction_mode is not None
            else (
                ResponseTransactionMode.VALIDATED_RESULT
                if (
                    response_constraints.exact_top_level_item_count is not None
                    or validators
                    or judges
                )
                else ResponseTransactionMode.DIRECT_LIVE
            )
        )
        if (
            transaction_mode is ResponseTransactionMode.DIRECT_LIVE
            and (
                response_constraints.exact_top_level_item_count is not None
                or validators
                or judges
            )
        ):
            raise ContractViolationError(
                "direct-live response cannot require full-text validation"
            )
        evidence_store = RunEvidenceStore()
        context_receipts = evidence_store.record_context_messages(messages)
        if context_receipts:
            await self._trace(
                "context_evidence",
                "recorded",
                details={"receiptCount": len(context_receipts)},
            )
        configured_tools = tuple(tools) if request.tools_enabled else ()
        return _RuntimeLoopState(
            messages=messages,
            invocation_context=ModelInvocationContext(
                run_id=str(run_id or f"runtime-{uuid4().hex}"),
                turn_id=turn_id,
                tool_argument_limits=tool_argument_limits or {},
            ),
            execution_state=execution_state or ExecutionState(),
            evidence_store=evidence_store,
            context_contracts=dict(tool_context_contracts or {}),
            configured_tools=configured_tools,
            tool_display_names={
                schema.name: schema.display_names
                for schema in configured_tools
                if schema.display_names
            },
            validators=validators,
            judges=judges,
            transaction_mode=transaction_mode,
            used_model=request.model.model,
            logical_required_tool_call_enabled=bool(
                force_tool_choice or require_tool_call
            ),
            provider_required_tool_choice_enabled=bool(force_tool_choice),
            recovery_ledger=RecoveryLedger(self._recovery_policy),
            round_limit=self._limits.max_model_rounds,
            absolute_round_limit=(
                self._limits.max_model_rounds
                + self._limits.max_progress_rounds
                + provider_retry_round_capacity(self._recovery_policy)
                + 1
            ),
        )

    def _allowed_names(
        self,
        tools: Sequence[ToolSchema],
        *,
        scope_tools_to_observer: bool,
    ) -> frozenset[str]:
        all_names = frozenset(schema.name for schema in tools)
        if not scope_tools_to_observer:
            return all_names
        if self._observer is None:
            return frozenset()
        return transition.allowed_tool_names & all_names if (transition := self._observer.current_execution_transition()) is not None else frozenset()

    def _future_allowed_names(
        self,
        tools: Sequence[ToolSchema],
        *,
        scope_tools_to_observer: bool,
    ) -> frozenset[str]:
        if not scope_tools_to_observer or self._observer is None:
            return frozenset()
        all_names = frozenset(schema.name for schema in tools)
        return transition.future_tool_names & all_names if (transition := self._observer.current_execution_transition()) is not None else frozenset()

    async def _trace(
        self,
        stage: str,
        outcome: str,
        *,
        details: dict | None = None,
        duration_ms: int | None = None,
    ) -> None:
        if self._observer is None:
            return
        await self._observer.record_trace(TraceRecord(
            stage=stage,
            outcome=outcome,
            details=details or {},
            duration_ms=duration_ms,
        ))


def _runtime_result(
    run_id: RunId | None,
    outcome: RuntimeOutcome,
    model: str,
    round_count: int,
    *,
    final_response: str = "",
    error_code: str | None = None,
) -> AgentRuntimeResult:
    return AgentRuntimeResult(
        run_id=run_id,
        outcome=outcome,
        final_response=final_response,
        model=model,
        round_count=round_count,
        error_code=error_code,
    )
