"""Provider-neutral Agent model/tool loop orchestration.

This stage-3 runtime starts after the host has built and initially
budgeted context.  It owns model rounds, typed tool continuation, authorization,
round trimming, cancellation and the model-round limit. Host transports,
domain concepts and concrete tool handlers stay behind ports.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from time import perf_counter
from typing import Any, AsyncIterator, Mapping, Sequence
from uuid import uuid4

from purra.cancellation import (
    OperationCanceled,
    await_with_cancellation,
    is_canceled as _is_canceled,
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
        stage_context_projection_enabled: bool = False,
        signal: CancellationSignal | None = None,
    ) -> AsyncIterator[RuntimeUpdate]:
        messages = list(request.messages)
        invocation_context = ModelInvocationContext(
            run_id=str(run_id or f"runtime-{uuid4().hex}"),
            turn_id=turn_id,
        )
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
        state = execution_state or ExecutionState()
        evidence_store = RunEvidenceStore()
        context_receipts = evidence_store.record_context_messages(messages)
        if context_receipts:
            await self._trace(
                "context_evidence",
                "recorded",
                details={
                    "receiptCount": len(context_receipts),
                },
            )
        context_contracts = dict(tool_context_contracts or {})
        configured_tools = tuple(tools) if request.tools_enabled else ()
        tool_display_names = {
            schema.name: schema.display_names
            for schema in configured_tools
            if schema.display_names
        }
        used_model = request.model.model
        # A provider-level REQUIRED hint can never weaken the host guard.
        # ``require_tool_call=True`` also keeps that guard when the provider
        # capability cache already requires AUTO transport.
        logical_required_tool_call_enabled = bool(
            force_tool_choice or require_tool_call
        )
        provider_required_tool_choice_enabled = bool(force_tool_choice)
        recovery_ledger = RecoveryLedger(self._recovery_policy)
        declined_response_pending = False
        response_repair_pending = False
        public_presentation_pending = False
        pending_provider_attempt: _PendingProviderAttempt | None = None
        logical_round_number = 0
        dynamic_replan_pending = False
        last_tool_outcome = ToolBatchOutcome.COMPLETED
        pending_recovery_error_code: str | None = None
        failed_tool_recovery_error_code: str | None = None
        budget_contract_error = _context_budget_contract_error(
            request,
            context_budget,
            configured_tools,
        )
        if budget_contract_error is not None:
            await self._trace(
                "context_budget",
                "contract_mismatch",
                details={"errorCode": budget_contract_error},
            )
            yield _runtime_result(
                run_id,
                RuntimeOutcome.FAILED,
                used_model,
                0,
                error_code=budget_contract_error,
            )
            return

        maximum_output_tokens = (
            output_limit.max_tokens
            if output_limit is not None
            else None
        )
        round_limit = self._limits.max_model_rounds
        progress_rounds = 0
        # Tool-input repair is bounded per contiguous invalid-input sequence,
        # not once for the entire Run. A successful tool round starts a new
        # sequence so a later, independent schema mistake can be corrected
        # without granting repeated retries to the same invalid call.
        tool_input_recovery_epoch = 0

        def remaining_model_rounds(completed_rounds: int) -> int:
            return max(0, round_limit - int(completed_rounds))

        absolute_round_limit = self._limits.max_model_rounds + (
            self._limits.max_progress_rounds
            + provider_retry_round_capacity(self._recovery_policy)
            + 1
        )

        for round_index in range(absolute_round_limit):
            round_number = round_index + 1
            if round_index >= round_limit:
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    used_model,
                    round_index,
                    error_code="max_model_rounds",
                )
                return
            if _is_canceled(signal):
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.CANCELED,
                    used_model,
                    round_index,
                    error_code="request_canceled",
                )
                return

            if pending_provider_attempt is not None:
                provider_attempt = pending_provider_attempt
                pending_provider_attempt = None
            else:
                if dynamic_replan_pending and planning_hook is not None:
                    replanning_started = perf_counter()
                    try:
                        revised_guidance = await planning_hook.replan_after_tool(
                            evidence_store.project_messages_for_planning(messages),
                            round_number=round_number,
                            remaining_model_rounds=remaining_model_rounds(
                                round_index
                            ),
                            outcome=last_tool_outcome,
                            signal=signal,
                        )
                        if revised_guidance is not None:
                            messages.append(revised_guidance)
                    except OperationCanceled:
                        await self._trace(
                            "planning",
                            "canceled",
                            details={
                                "dynamic": True,
                                "round": round_number,
                            },
                            duration_ms=_duration_ms(replanning_started),
                        )
                        yield _runtime_result(
                            run_id,
                            RuntimeOutcome.CANCELED,
                            used_model,
                            round_index,
                            error_code="request_canceled",
                        )
                        return
                    except Exception as error:
                        primary_error_code = pending_recovery_error_code
                        await self._trace(
                            "planning",
                            "failed",
                            details={
                                "dynamic": True,
                                "round": round_number,
                                "errorType": _root_error_type(error),
                                "primaryErrorCode": primary_error_code,
                                "recoveryErrorCode": "dynamic_planning_failed",
                            },
                            duration_ms=_duration_ms(replanning_started),
                        )
                        yield _runtime_result(
                            run_id,
                            RuntimeOutcome.FAILED,
                            used_model,
                            round_index,
                            error_code=(
                                primary_error_code
                                or "dynamic_planning_failed"
                            ),
                        )
                        return
                    dynamic_replan_pending = False
                    pending_recovery_error_code = None
                initial_logical_round = logical_round_number == 0
                active_token_budget = (
                    (
                        context_budget.provider_input_tokens
                        if initial_logical_round
                        else context_budget.round_input_tokens
                    )
                    if context_budget is not None
                    else (
                        None if initial_logical_round else round_input_tokens
                    )
                )

                allowed_names = self._allowed_names(
                    configured_tools,
                    scope_tools_to_observer=scope_tools_to_observer,
                )
                future_names = self._future_allowed_names(
                    configured_tools,
                    scope_tools_to_observer=scope_tools_to_observer,
                )
                visible_tools = (
                    ()
                    if (
                        declined_response_pending
                        or response_repair_pending
                        or public_presentation_pending
                    )
                    else tuple(
                        schema
                        for schema in configured_tools
                        if schema.name in allowed_names
                    )
                )
                require_tool = bool(
                    logical_required_tool_call_enabled and visible_tools
                )
                force_required_tool_choice = bool(
                    provider_required_tool_choice_enabled and visible_tools
                )
                buffer_model_content = bool(
                    require_tool
                    or (
                        not public_presentation_pending
                        and (
                            declined_response_pending
                            or failed_tool_recovery_error_code is not None
                            or transaction_mode
                            is ResponseTransactionMode.VALIDATED_RESULT
                        )
                    )
                )
                projection = project_intermediate_tool_context(
                    messages,
                    visible_tool_names=frozenset(
                        schema.name for schema in visible_tools
                    ),
                    contracts=context_contracts,
                    evidence_store=evidence_store,
                    enabled=stage_context_projection_enabled,
                    initial_round=initial_logical_round,
                    token_budget=active_token_budget,
                )
                canonical_tokens = estimate_agent_messages_tokens(messages)
                projected_tokens = estimate_agent_messages_tokens(
                    projection.messages
                )
                compression_outcome: str | None = None
                compression_strategy: str | None = None
                if (
                    active_token_budget is not None
                    and self._context_compressor is not None
                ):
                    runtime_request = replace(
                        request,
                        messages=projection.messages,
                        metadata={
                            **request.metadata,
                            "contextCompressionScope": "runtime",
                            "runtimeLogicalRound": logical_round_number + 1,
                        },
                    )
                    compressed = await await_with_cancellation(
                        self._context_compressor.prepare(
                            runtime_request,
                            signal,
                            budget=ContextCompactionBudget(
                                phase=ContextCompactionPhase.MODEL_CALL,
                                provider_input_tokens=active_token_budget,
                                context_tokens=0,
                                context_tokens_are_resolved=True,
                                output_reserve_tokens=(
                                    context_budget.output_reserve_tokens
                                    if context_budget is not None
                                    else 1
                                ),
                            ),
                            operation_scope=OperationScope(
                                run_id=invocation_context.run_id,
                            ),
                        ),
                        signal,
                    )
                    round_context_messages = compressed.request.messages
                    sent_tokens = estimate_agent_messages_tokens(
                        round_context_messages
                    )
                    dropped_messages = max(
                        0,
                        len(projection.messages)
                        - len(round_context_messages),
                    )
                    overflow_tokens = max(
                        0,
                        sent_tokens - active_token_budget,
                    )
                    compression_outcome = compressed.outcome
                    compression_strategy = str(
                        compressed.diagnostics.get("strategy") or ""
                    )
                elif active_token_budget is not None:
                    trimmed = trim_agent_messages_by_turn(
                        projection.messages,
                        active_token_budget,
                    )
                    round_context_messages = trimmed.messages
                    sent_tokens = trimmed.token_estimate
                    dropped_messages = trimmed.dropped_count
                    overflow_tokens = trimmed.overflow_tokens
                else:
                    round_context_messages = projection.messages
                    sent_tokens = projected_tokens
                    dropped_messages = 0
                    overflow_tokens = 0
                if (
                    projection.dropped_context_blocks
                    or projection.compacted_tool_results
                ):
                    await self._trace(
                        "context_projection",
                        projection.mode,
                        details={
                            "round": round_number,
                            "toolNames": sorted(
                                schema.name for schema in visible_tools
                            ),
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
                runtime_budget_outcome = (
                    "overflow"
                    if overflow_tokens
                    else "rebalanced"
                    if (
                        projection.saved_tokens > 0
                        or dropped_messages > 0
                    )
                    else "within_budget"
                )
                await self._trace(
                    "runtime_context_budget",
                    runtime_budget_outcome,
                    details={
                        "round": round_number,
                        "logicalRound": logical_round_number + 1,
                        "initialRound": initial_logical_round,
                        "tokenBudget": active_token_budget,
                        "canonicalTokens": canonical_tokens,
                        "projectedTokens": projected_tokens,
                        "sentTokens": sent_tokens,
                        "pressureRatio": (
                            round(
                                sent_tokens / active_token_budget,
                                4,
                            )
                            if active_token_budget
                            else None
                        ),
                        "projectionMode": projection.mode,
                        "projectionSavedTokens": projection.saved_tokens,
                        "compressionOutcome": compression_outcome,
                        "compressionStrategy": compression_strategy,
                        "droppedMessages": dropped_messages,
                        "overflowTokens": overflow_tokens,
                        "completeEvidenceTokens": evidence_store.token_estimate,
                    },
                )
                if overflow_tokens > 0:
                    overflow_outcome = (
                        "overflow_initial"
                        if initial_logical_round
                        else "overflow_after_tool"
                    )
                    await self._trace(
                        "context_budget",
                        overflow_outcome,
                        details={
                            "round": round_number,
                            "tokenBudget": active_token_budget,
                            "canonicalTokens": canonical_tokens,
                            "projectedTokens": projected_tokens,
                            "sentTokens": sent_tokens,
                            "projectionMode": projection.mode,
                            "projectionSavedTokens": (
                                projection.saved_tokens
                            ),
                            "overflowTokens": overflow_tokens,
                        },
                    )
                    yield _runtime_result(
                        run_id,
                        RuntimeOutcome.FAILED,
                        used_model,
                        round_index,
                        error_code=(
                            "context_overflow_initial"
                            if initial_logical_round
                            else "context_overflow_after_tool"
                        ),
                    )
                    return
                logical_round_number += 1
                provider_attempt = _PendingProviderAttempt(
                    messages=round_context_messages,
                    invocation=ModelInvocation(
                        request=request.model,
                        tools=visible_tools,
                        tool_choice=(
                            ToolChoiceMode.REQUIRED
                            if force_required_tool_choice
                            else (
                                ToolChoiceMode.AUTO
                                if visible_tools
                                else ToolChoiceMode.NONE
                            )
                        ),
                        output_limit=output_limit,
                        reasoning_mode=reasoning_mode,
                    ),
                    allowed_names=allowed_names,
                    future_names=future_names,
                    require_tool=require_tool,
                    buffer_model_content=buffer_model_content,
                    logical_round=logical_round_number,
                    attempt=1,
                )

            round_messages = provider_attempt.messages
            invocation = provider_attempt.invocation
            allowed_names = provider_attempt.allowed_names
            future_names = provider_attempt.future_names
            require_tool = provider_attempt.require_tool
            buffer_model_content = provider_attempt.buffer_model_content
            model_started = perf_counter()
            accumulator = _ModelRoundAccumulator()
            received_chunk_count = 0
            emitted_delta_count = 0
            direct_content_released = False
            request_fingerprint = _model_request_fingerprint(
                round_messages,
                invocation,
            )
            try:
                stream = await self._model_manager.stream(
                    round_messages,
                    _agent_model_call(
                        invocation,
                        require_tool=require_tool,
                        requires_full_text_validation=bool(
                            buffer_model_content or validators or judges
                        ),
                    ),
                    invocation_context,
                    signal,
                )
                invocation_parameters = stream.receipt.call_parameters[0]
                host_planned_dispatch = (
                    invocation_parameters.get("executionRoute")
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
                        "round": round_number,
                        "logicalRound": provider_attempt.logical_round,
                        "attempt": provider_attempt.attempt,
                        "requestFingerprint": request_fingerprint,
                        "parameters": invocation_parameters,
                    },
                )
            except Exception as error:
                provider_failure = resolve_provider_failure(
                    error,
                    phase=ProviderFailurePhase.OPENING,
                    attempt=provider_attempt,
                    recovery_ledger=recovery_ledger,
                    remaining_model_rounds=remaining_model_rounds(round_number),
                    round_number=round_number,
                    cancellation_requested=_is_canceled(signal),
                    received_chunk_count=0,
                    emitted_delta_count=0,
                    duration_ms=_duration_ms(model_started),
                )
                for trace in provider_failure.traces:
                    await self._trace(
                        trace.stage,
                        trace.outcome,
                        details=dict(trace.details),
                        duration_ms=trace.duration_ms,
                    )
                if provider_failure.disable_required_tool_choice:
                    provider_required_tool_choice_enabled = False
                if provider_failure.next_attempt is not None:
                    round_limit += 1
                    pending_provider_attempt = provider_failure.next_attempt
                    continue
                yield _runtime_result(
                    run_id,
                    (
                        RuntimeOutcome.CANCELED
                        if provider_failure.disposition
                        is ProviderFailureDisposition.CANCEL
                        else RuntimeOutcome.FAILED
                    ),
                    used_model,
                    round_number,
                    error_code=provider_failure.error_code,
                )
                return

            used_model = stream.receipt.model or used_model
            stream_error: Exception | None = None
            chunks = stream.chunks
            try:
                while True:
                    try:
                        chunk = await await_with_cancellation(anext(chunks), signal)
                    except StopAsyncIteration:
                        break
                    received_chunk_count += 1
                    accumulator.add(chunk)
                    if chunk.reasoning_delta:
                        emitted_delta_count += 1
                    if (
                        chunk.content_delta
                        and not require_tool
                        and not buffer_model_content
                        and not invocation.tools
                    ):
                        direct_content_released = True
                        if self._observer is not None:
                            await self._observer.on_model_delta()
                        emitted_delta_count += 1
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
                provider_failure = resolve_provider_failure(
                    stream_error,
                    phase=ProviderFailurePhase.STREAMING,
                    attempt=provider_attempt,
                    recovery_ledger=recovery_ledger,
                    remaining_model_rounds=remaining_model_rounds(round_number),
                    round_number=round_number,
                    cancellation_requested=_is_canceled(signal),
                    received_chunk_count=received_chunk_count,
                    emitted_delta_count=emitted_delta_count,
                    visible_output_emitted=direct_content_released,
                    provider_finish_observed=(
                        accumulator.finish_reason is not None
                    ),
                    duration_ms=_duration_ms(model_started),
                )
                for trace in provider_failure.traces:
                    await self._trace(
                        trace.stage,
                        trace.outcome,
                        details=dict(trace.details),
                        duration_ms=trace.duration_ms,
                    )
                if provider_failure.next_attempt is not None:
                    round_limit += 1
                    pending_provider_attempt = provider_failure.next_attempt
                    continue
                yield _runtime_result(
                    run_id,
                    (
                        RuntimeOutcome.CANCELED
                        if provider_failure.disposition
                        is ProviderFailureDisposition.CANCEL
                        else RuntimeOutcome.FAILED
                    ),
                    used_model,
                    round_number,
                    error_code=provider_failure.error_code,
                )
                return

            calls, malformed_call_error = accumulator.tool_calls()
            finish_reason = accumulator.finish_reason
            local_input_estimate = (
                estimate_agent_messages_tokens(round_messages)
                + estimate_tool_schema_tokens(invocation.tools)
            )
            if accumulator.usage is not None:
                usage = accumulator.usage
                await self._trace(
                    "model_usage",
                    "provider_reported",
                    details={
                        "round": round_number,
                        "attempt": provider_attempt.attempt,
                        "logicalRound": provider_attempt.logical_round,
                        "actualInputTokens": usage.input_tokens,
                        "actualOutputTokens": usage.output_tokens,
                        "actualTotalTokens": usage.total_tokens,
                        "cachedInputTokens": usage.cached_input_tokens,
                        "reasoningOutputTokens": (
                            usage.reasoning_output_tokens
                        ),
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
                        "localInputEstimate": local_input_estimate,
                    },
                )
                # Later tool rounds can contain transient EvidenceStore
                # projections that are not retained by the conversation.
                # Only the first logical request is a valid UI anchor.
                if provider_attempt.logical_round == 1:
                    yield AgentEvent(
                        type=CoreEventType.CONTEXT_USAGE_RECORDED,
                        run_id=run_id,
                        payload={
                            "actualInputTokens": usage.input_tokens,
                            "actualOutputTokens": usage.output_tokens,
                            "actualTotalTokens": usage.total_tokens,
                            "cachedInputTokens": (
                                usage.cached_input_tokens
                            ),
                            "reasoningOutputTokens": (
                                usage.reasoning_output_tokens
                            ),
                            "actualUsageRound": (
                                provider_attempt.logical_round
                            ),
                            "inputTokenEstimateAtUsage": (
                                local_input_estimate
                            ),
                            "usageSource": "provider",
                            "requestedOutputTokens": (
                                invocation.max_output_tokens
                            ),
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
                    else (
                        finish_reason.value
                        if finish_reason is not None
                        else "stream_end"
                    )
                ),
                details={
                    "round": round_number,
                    "attempt": provider_attempt.attempt,
                    "logicalRound": provider_attempt.logical_round,
                    "toolCallCount": accumulator.tool_call_count,
                    "receivedChunkCount": received_chunk_count,
                    "emittedDeltaCount": emitted_delta_count,
                    "retryScheduled": False,
                    "providerAttemptTerminal": True,
                    "batchExecuted": False,
                },
                duration_ms=_duration_ms(model_started),
            )

            # A provider-declared output limit is never a commit boundary.  It
            # may arrive after ids, names, and syntactically valid-looking
            # argument fragments, but the provider has explicitly declared the
            # generation incomplete.  Classify this before malformed-call or
            # tool-input handling so truncation remains the primary cause and
            # no partial assistant/tool continuation can pollute the next round.
            if finish_reason is None:  # Defensive; stream handling rejects this.
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    used_model,
                    round_number,
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
                        round_number=round_number,
                        attempt=provider_attempt.attempt,
                        request_fingerprint=request_fingerprint,
                        finish_reason=finish_reason,
                        error_code=error_code,
                        emitted_delta_count=emitted_delta_count,
                        output_limit=output_limit,
                    ),
                )
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    used_model,
                    round_number,
                    error_code=error_code,
                )
                return

            tool_finish = termination.authorizes_tool_calls
            protocol = resolve_tool_protocol(
                calls,
                tool_call_count=accumulator.tool_call_count,
                malformed_call_error=malformed_call_error,
                tool_finish=tool_finish,
                require_tool=require_tool,
                planning_available=planning_hook is not None,
                declined_response_pending=declined_response_pending,
                response_repair_pending=response_repair_pending,
                public_presentation_pending=public_presentation_pending,
                recovery_ledger=recovery_ledger,
                remaining_model_rounds=remaining_model_rounds(round_number),
                round_number=round_number,
                cancellation_requested=_is_canceled(signal),
                visible_output_emitted=direct_content_released,
            )
            if protocol is not None:
                for trace in protocol.traces:
                    await self._trace(
                        trace.stage,
                        trace.outcome,
                        details=dict(trace.details),
                    )
                if (
                    protocol.disposition
                    is ToolAuthorizationDisposition.RETRY_MODEL
                ):
                    messages.extend(protocol.messages)
                    continue
                if protocol.disposition is ToolAuthorizationDisposition.REPLAN:
                    last_tool_outcome = ToolBatchOutcome.FAILED
                    pending_recovery_error_code = protocol.error_code
                    failed_tool_recovery_error_code = protocol.error_code
                    dynamic_replan_pending = True
                    messages.extend(protocol.messages)
                    continue
                if protocol.disposition is ToolAuthorizationDisposition.REJECT:
                    yield _runtime_result(
                        run_id,
                        RuntimeOutcome.FAILED,
                        used_model,
                        round_number,
                        error_code=protocol.error_code,
                    )
                    return

            if not tool_finish or not calls:
                response_recovery = resolve_response_recovery(
                    content=accumulator.content,
                    reasoning=accumulator.reasoning,
                    declined_response_pending=declined_response_pending,
                    failed_tool_recovery_error_code=(
                        failed_tool_recovery_error_code
                    ),
                    recovery_ledger=recovery_ledger,
                    remaining_model_rounds=remaining_model_rounds(round_number),
                    round_number=round_number,
                    cancellation_requested=_is_canceled(signal),
                    visible_output_emitted=direct_content_released,
                )
                if response_recovery is not None:
                    for trace in response_recovery.traces:
                        await self._trace(
                            trace.stage,
                            trace.outcome,
                            details=dict(trace.details),
                        )
                    if (
                        response_recovery.disposition
                        is ResponseFinalizationDisposition.RETRY_MODEL
                    ):
                        messages.extend(response_recovery.messages)
                        continue
                    yield _runtime_result(
                        run_id,
                        RuntimeOutcome.FAILED,
                        used_model,
                        round_number,
                        error_code=response_recovery.error_code,
                    )
                    return

                if not declined_response_pending:
                    exact_item_count = (
                        response_constraints.exact_top_level_item_count
                    )
                    observed_items = _top_level_numbered_items(
                        accumulator.content
                    )
                    violation_codes: list[str] = []
                    repair_guidance: list[str] = []
                    validation_details: list[dict[str, Any]] = []
                    if exact_item_count is not None:
                        expected_items = tuple(range(1, exact_item_count + 1))
                        if observed_items != expected_items:
                            violation_codes.append(
                                "core.exact_top_level_item_count"
                            )
                            repair_guidance.append(
                                _exact_item_count_repair_guidance(exact_item_count)
                            )

                    registered_validation = (
                        await self._response_validation.validate_registered(
                            content=accumulator.content,
                            messages=messages,
                            validators=validators,
                            run_id=invocation_context.run_id,
                            round_number=round_number,
                        )
                    )
                    for trace in registered_validation.traces:
                        await self._trace(
                            trace.stage,
                            trace.outcome,
                            details=dict(trace.details),
                            duration_ms=trace.duration_ms,
                        )
                    if registered_validation.error_code is not None:
                        yield _runtime_result(
                            run_id,
                            RuntimeOutcome.FAILED,
                            used_model,
                            round_number,
                            error_code=registered_validation.error_code,
                        )
                        return
                    violation_codes.extend(
                        registered_validation.violation_codes
                    )
                    repair_guidance.extend(
                        registered_validation.repair_guidance
                    )
                    validation_details.extend(
                        registered_validation.validation_details
                    )

                    # Semantic judges are potentially expensive and receive a
                    # structurally valid candidate only. Their domain logic
                    # stays behind the injected async port; Core owns
                    # cancellation and fail-closed behavior. Deterministic and
                    # semantic violations each receive at most one tool-free
                    # repair, with two repairs total across the response.
                    if not violation_codes:
                        for judge_index, judge in enumerate(judges):
                            judge_attempt = (
                                await self._response_validation.begin_judge(
                                    run_id=invocation_context.run_id,
                                    index=judge_index,
                                )
                            )
                            yield AgentEvent(
                                type=CoreEventType.MODEL_CALL_RECORDED,
                                run_id=run_id,
                                payload={
                                    "phase": "response_judge",
                                    "count": 1,
                                    "toolNames": [],
                                    "toolChoice": "none",
                                    "round": round_number,
                                    "judgeIndex": judge_index,
                                },
                            )
                            semantic_validation = (
                                await self._response_validation.judge(
                                    judge_attempt,
                                    judge,
                                    content=accumulator.content,
                                    messages=messages,
                                    signal=signal,
                                    round_number=round_number,
                                )
                            )
                            for trace in semantic_validation.traces:
                                await self._trace(
                                    trace.stage,
                                    trace.outcome,
                                    details=dict(trace.details),
                                    duration_ms=trace.duration_ms,
                                )
                            if semantic_validation.error_code is not None:
                                yield _runtime_result(
                                    run_id,
                                    (
                                        RuntimeOutcome.CANCELED
                                        if semantic_validation.canceled
                                        else RuntimeOutcome.FAILED
                                    ),
                                    used_model,
                                    round_number,
                                    error_code=semantic_validation.error_code,
                                )
                                return
                            violation_codes.extend(
                                semantic_validation.violation_codes
                            )
                            repair_guidance.extend(
                                semantic_validation.repair_guidance
                            )
                            validation_details.extend(
                                semantic_validation.validation_details
                            )

                    constraint_recovery = resolve_response_constraint_recovery(
                        content=accumulator.content,
                        reasoning=accumulator.reasoning,
                        violation_codes=violation_codes,
                        repair_guidance=repair_guidance,
                        validation_details=validation_details,
                        exact_item_count=exact_item_count,
                        observed_items=observed_items,
                        recovery_ledger=recovery_ledger,
                        remaining_model_rounds=remaining_model_rounds(
                            round_number
                        ),
                        round_number=round_number,
                        cancellation_requested=_is_canceled(signal),
                        visible_output_emitted=direct_content_released,
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
                            response_repair_pending = (
                                constraint_recovery.response_repair_pending
                            )
                            messages.extend(constraint_recovery.messages)
                            continue
                        yield _runtime_result(
                            run_id,
                            RuntimeOutcome.FAILED,
                            used_model,
                            round_number,
                            error_code=constraint_recovery.error_code,
                        )
                        return

                final_response = accumulator.content
                presentation_messages = public_presentation_messages(
                    content=final_response,
                    reasoning=accumulator.reasoning,
                    invocation_had_tools=bool(invocation.tools),
                    buffered_model_content=buffer_model_content,
                    transaction_mode=transaction_mode,
                    already_pending=public_presentation_pending,
                )
                if presentation_messages:
                    public_presentation_pending = True
                    round_limit += 1
                    messages.extend(presentation_messages)
                    continue
                if buffer_model_content:
                    if self._observer is not None:
                        await self._observer.on_model_delta()
                elif not direct_content_released and final_response:
                    if self._observer is not None:
                        await self._observer.on_model_delta()
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.COMPLETED,
                    used_model,
                    round_number,
                    final_response=final_response,
                )
                return

            authorization = resolve_tool_authorization(
                calls,
                allowed_names=allowed_names,
                future_names=future_names,
                require_tool=require_tool,
                planning_available=planning_hook is not None,
                content=accumulator.content,
                reasoning=accumulator.reasoning,
                recovery_ledger=recovery_ledger,
                remaining_model_rounds=remaining_model_rounds(round_number),
                round_number=round_number,
                cancellation_requested=_is_canceled(signal),
                visible_output_emitted=direct_content_released,
            )
            for trace in authorization.traces:
                await self._trace(
                    trace.stage,
                    trace.outcome,
                    details=dict(trace.details),
                )
            requested_names = authorization.requested_names
            if (
                authorization.disposition
                is ToolAuthorizationDisposition.RETRY_MODEL
            ):
                messages.extend(authorization.messages)
                continue
            if authorization.disposition is ToolAuthorizationDisposition.REPLAN:
                last_tool_outcome = ToolBatchOutcome.FAILED
                pending_recovery_error_code = authorization.error_code
                failed_tool_recovery_error_code = authorization.error_code
                dynamic_replan_pending = True
                messages.extend(authorization.messages)
                continue
            if authorization.disposition is ToolAuthorizationDisposition.REJECT:
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    used_model,
                    round_number,
                    error_code=authorization.error_code,
                )
                return

            if round_index >= round_limit - 1:
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    used_model,
                    round_number,
                    error_code="max_model_rounds",
                )
                return

            await self._model_manager.publish_model_stream_commentary(stream.receipt.output_stream_id)
            if scope_tools_to_observer and self._observer is not None:
                await self._observer.on_tool_calls_started(tuple(sorted(requested_names)))
            yield AgentEvent(
                type=CoreEventType.TOOL_CALLS_STARTED,
                run_id=run_id,
                payload={
                    "calls": [
                        _tool_call_payload(
                            call,
                            display_names=tool_display_names.get(call.name),
                        )
                        for call in calls
                    ],
                    "in_progress": True,
                    "model": used_model,
                    "required": require_tool,
                },
            )

            if not tools_executable or self._tool_execution_gateway is None:
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.COMPLETED,
                    used_model,
                    round_number,
                    final_response=accumulator.content,
                )
                return

            tool_started = perf_counter()
            try:
                batch_result: ToolBatchResult | None = None
                batch_stream = _stream_tool_batch(
                    self._tool_execution_gateway,
                    ToolBatchRequest(
                        run_id=run_id,
                        invocation_id=stream.receipt.invocation_id,
                        calls=calls,
                        allowed_tool_names=allowed_names,
                        state=state,
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
                    details={"round": round_number},
                    duration_ms=_duration_ms(tool_started),
                )
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.CANCELED,
                    used_model,
                    round_number,
                    error_code="request_canceled",
                )
                return
            except Exception as error:
                await self._trace(
                    "tool_round",
                    "exception",
                    details={
                        "round": round_number,
                        "errorType": type(error).__name__,
                    },
                    duration_ms=_duration_ms(tool_started),
                )
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    used_model,
                    round_number,
                    error_code="tool_execution_error",
                )
                return

            receipts = (
                evidence_store.record_batch(calls, batch_result)
                if _results_match_calls(calls, batch_result.results)
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
                        receipt.to_mapping()
                        for receipt in receipts
                    ],
                },
            )
            outcome = batch_result.outcome
            await self._trace(
                "tool_round",
                outcome.value,
                details=_tool_round_trace_details(
                    round_number=round_number,
                    requested_names=requested_names,
                    allowed_names=allowed_names,
                    calls=calls,
                    batch_result=batch_result,
                    evidence_record_count=len(evidence_store.tool_result_receipts()),
                    evidence_tokens=evidence_store.token_estimate,
                ),
                duration_ms=_duration_ms(tool_started),
            )
            progress_rounds, round_limit, progress_extension = (
                _extend_progress_round_budget(
                    outcome,
                    progress_rounds=progress_rounds,
                    round_limit=round_limit,
                    max_progress_rounds=self._limits.max_progress_rounds,
                    round_number=round_number,
                )
            )
            if progress_extension is not None:
                await self._trace(
                    "runtime_round_budget",
                    "progress_extended",
                    details=progress_extension,
                )
            if outcome is ToolBatchOutcome.CANCELED:
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.CANCELED,
                    used_model,
                    round_number,
                    error_code=batch_result.error or "tool_execution_canceled",
                )
                return
            if outcome is ToolBatchOutcome.REJECTED:
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    used_model,
                    round_number,
                    error_code=batch_result.error or "tool_execution_failed",
                )
                return

            if not _results_match_calls(calls, batch_result.results):
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    used_model,
                    round_number,
                    error_code="invalid_tool_results",
                )
                return
            if scope_tools_to_observer and self._observer is not None:
                if outcome is not ToolBatchOutcome.FAILED:
                    await self._observer.on_tool_round_completed(outcome)
            elif not scope_tools_to_observer:
                # Caller-owned REQUIRED applies to the initial selection. A
                # successful unscoped tool round must still leave room for the
                # model to consume its result and answer. Planned runs keep
                # the logical requirement active and advance via the observer.
                logical_required_tool_call_enabled = False
            yield AgentEvent(
                type=CoreEventType.TOOL_ROUND_COMPLETED,
                run_id=run_id,
                payload={"outcome": outcome.value},
            )
            messages.extend(_continuation_messages(
                calls,
                batch_result.results,
                content="" if require_tool else accumulator.content,
                reasoning=accumulator.reasoning,
            ))
            tool_recovery = resolve_tool_recovery(
                batch_result,
                requested_names=requested_names,
                planning_available=planning_hook is not None,
                recovery_ledger=recovery_ledger,
                input_recovery_epoch=tool_input_recovery_epoch,
                remaining_model_rounds=remaining_model_rounds(round_number),
                round_number=round_number,
                cancellation_requested=_is_canceled(signal),
            )
            tool_input_recovery_epoch = (
                tool_recovery.next_input_recovery_epoch
            )
            for trace in tool_recovery.traces:
                await self._trace(
                    trace.stage,
                    trace.outcome,
                    details=dict(trace.details),
                )
            if tool_recovery.disposition is ToolRecoveryDisposition.RETRY_MODEL:
                messages.extend(tool_recovery.messages)
                continue
            if tool_recovery.disposition is ToolRecoveryDisposition.REJECT:
                yield _runtime_result(
                    run_id,
                    RuntimeOutcome.FAILED,
                    used_model,
                    round_number,
                    error_code=tool_recovery.error_code,
                )
                return
            if (
                tool_recovery.disposition
                is ToolRecoveryDisposition.REPLAN
            ):
                last_tool_outcome = outcome
                pending_recovery_error_code = (
                    tool_recovery.error_code
                    if outcome is ToolBatchOutcome.FAILED
                    else None
                )
                failed_tool_recovery_error_code = (
                    tool_recovery.error_code
                    if outcome is ToolBatchOutcome.FAILED
                    else None
                )
                dynamic_replan_pending = True
            if outcome is ToolBatchOutcome.DECLINED:
                declined_response_pending = True
                messages.append(AgentMessage(
                    role=MessageRole.DEVELOPER,
                    content=_DECLINED_TOOL_GUIDANCE,
                ))

        yield _runtime_result(
            run_id,
            RuntimeOutcome.FAILED,
            used_model,
            round_limit,
            error_code="max_model_rounds",
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
