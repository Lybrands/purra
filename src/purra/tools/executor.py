"""The single policy-enforcing execution path for Core tool handlers."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any

from purra.cancellation import (
    OperationCanceled,
    await_with_cancellation,
    is_canceled as _is_canceled,
)
from purra.contracts import (
    ApprovalRequest,
    ApprovalStatus,
    DomainEffect,
    ToolBatchOutcome,
    ToolBatchRequest,
    ToolBatchResult,
    ToolCall,
    ToolCallResult,
    ToolExecutionLimits,
    ToolExecutionMode,
    ToolEffectState,
    ToolHandlerResult,
    ToolPlanningDisposition,
    ToolStepDisposition,
)
from purra.errors import ContractViolationError
from purra.events import AgentEvent, CoreEventType
from purra.json_values import thaw_json_mapping
from purra.operations import (
    AgentOperationController,
    OperationDisplay,
    OperationKind,
    OperationScope,
)
from purra.ports import (
    ApprovalGateway,
    CancellationSignal,
    CONTROLLER_OWNED_RUN_EVENT_TYPES,
    EventSink,
    ToolCatalog,
    ToolRegistration,
    ToolIdempotencyGateway,
)
from purra.tools.contract import validate_tool_contract
from purra.tools.policy import (
    aggregate_outcomes,
    approval_error_code,
    approval_outcome,
)
from purra.tools.security import (
    ParsedToolCall,
    normalize_error_code,
    normalize_tool_arguments_to_schema,
    preflight_tool_calls,
    safe_error_content,
    sanitize_error_message,
    sanitize_tool_result,
    summarize_tool_arguments,
    validate_tool_arguments_schema,
)


class CoreToolExecutor:
    """Execute injected handlers without allowing domains to bypass Core gates."""

    def __init__(
        self,
        catalog: ToolCatalog,
        approval_gateway: ApprovalGateway | None = None,
        limits: ToolExecutionLimits = ToolExecutionLimits(),
        idempotency_gateway: ToolIdempotencyGateway | None = None,
        operation_controller: AgentOperationController | None = None,
    ) -> None:
        registrations = validate_tool_contract(catalog.registrations())
        self._registrations = MappingProxyType({
            registration.schema.name: registration
            for registration in registrations
        })
        self._approval_gateway = approval_gateway
        self._limits = limits
        self._idempotency_gateway = idempotency_gateway
        self._operations = operation_controller

    async def execute_batch(
        self,
        request: ToolBatchRequest,
        event_sink: EventSink,
        signal: CancellationSignal | None = None,
    ) -> ToolBatchResult:
        if _is_canceled(signal):
            return ToolBatchResult(
                results=(),
                outcome=ToolBatchOutcome.CANCELED,
                error="tool_execution_canceled",
            )

        parsed_calls, failure = preflight_tool_calls(
            request.calls,
            self._limits,
            argument_limits={
                name: registration.max_argument_chars
                for name, registration in self._registrations.items()
                if registration.max_argument_chars is not None
            },
        )
        if failure is not None:
            return _whole_batch_failure(
                request.calls,
                outcome=ToolBatchOutcome.FAILED,
                code=failure.code,
                message=failure.message,
                diagnostics=failure.diagnostics,
            )

        requested_names = frozenset(item.call.name for item in parsed_calls)
        if not requested_names.issubset(self._registrations):
            return _whole_batch_failure(
                request.calls,
                outcome=ToolBatchOutcome.REJECTED,
                code="unknown_tool",
                message="The requested tool is not registered.",
            )
        unknown_scope_names = request.allowed_tool_names - self._registrations.keys()
        if unknown_scope_names:
            return _whole_batch_failure(
                request.calls,
                outcome=ToolBatchOutcome.REJECTED,
                code="invalid_tool_scope",
                message="The execution scope contains an unregistered tool.",
            )
        if not requested_names.issubset(request.allowed_tool_names):
            return _whole_batch_failure(
                request.calls,
                outcome=ToolBatchOutcome.REJECTED,
                code="tool_not_authorized",
                message="The requested tool is outside the current execution scope.",
            )
        if (
            len(parsed_calls) > 1
            and any(
                self._registrations[item.call.name].policy.mode
                is not ToolExecutionMode.READ
                for item in parsed_calls
            )
            and not _is_recoverable_artifact_batch(
                parsed_calls,
                self._registrations,
            )
        ):
            return _whole_batch_failure(
                request.calls,
                outcome=ToolBatchOutcome.REJECTED,
                code="multi_call_batch_requires_read_only_tools",
                message=(
                    "A batch with multiple tool calls is allowed only when every "
                    "tool is read-only, or when every call targets the same "
                    "host-durable recoverable artifact batch tool."
                ),
            )

        normalized_calls: list[ParsedToolCall] = []
        for parsed in parsed_calls:
            normalized, normalization_failure = normalize_tool_arguments_to_schema(
                parsed,
                self._registrations[parsed.call.name].schema.parameters,
            )
            if normalization_failure is not None:
                return _whole_batch_failure(
                    request.calls,
                    outcome=ToolBatchOutcome.FAILED,
                    code=normalization_failure.code,
                    message=normalization_failure.message,
                    diagnostics=normalization_failure.diagnostics,
                )
            if normalized is None:
                raise ContractViolationError(
                    "tool argument normalization returned no result"
                )
            normalized_calls.append(normalized)
        parsed_calls = tuple(normalized_calls)

        for parsed in parsed_calls:
            schema_failure = validate_tool_arguments_schema(
                parsed,
                self._registrations[parsed.call.name].schema.parameters,
            )
            if schema_failure is not None:
                return _whole_batch_failure(
                    request.calls,
                    outcome=ToolBatchOutcome.FAILED,
                    code=schema_failure.code,
                    message=schema_failure.message,
                    diagnostics=schema_failure.diagnostics,
                )

        results: list[ToolCallResult] = []
        outcomes: list[ToolBatchOutcome] = []
        cache_hits: list[bool] = []

        for index, parsed in enumerate(parsed_calls):
            registration = self._registrations[parsed.call.name]
            if _is_canceled(signal):
                return await self._canceled_result(
                    request,
                    event_sink,
                    parsed,
                    index,
                    results,
                    cache_hits,
                )
            operation_id = await self._start_tool_operation(
                request,
                parsed,
                registration,
            )

            scope_failure = await self._validate_scope(
                registration,
                request,
                parsed,
                signal,
            )
            if scope_failure is not None:
                code, message, canceled = scope_failure
                outcome = (
                    ToolBatchOutcome.CANCELED
                    if canceled
                    else (
                        ToolBatchOutcome.REJECTED
                        if code == "tool_scope_violation"
                        else ToolBatchOutcome.FAILED
                    )
                )
                await self._finish_tool_operation(
                    operation_id,
                    outcome,
                    code,
                )
                return await _failed_call_batch(
                    request,
                    event_sink,
                    parsed,
                    index,
                    results,
                    cache_hits,
                    remaining=parsed_calls[index + 1:],
                    outcome=outcome,
                    code=code,
                    message=message,
                    approval_status=(
                        ApprovalStatus.CANCELED if canceled else None
                    ),
                    effect_state=ToolEffectState.NOT_STARTED,
                )

            # Defense in depth: authorization never comes from mutable state.
            if parsed.call.name not in request.allowed_tool_names:
                await self._finish_tool_operation(
                    operation_id,
                    ToolBatchOutcome.REJECTED,
                    "tool_not_authorized",
                )
                return await _failed_call_batch(
                    request,
                    event_sink,
                    parsed,
                    index,
                    results,
                    cache_hits,
                    remaining=parsed_calls[index + 1:],
                    outcome=ToolBatchOutcome.REJECTED,
                    code="tool_not_authorized",
                    message=(
                        "The requested tool is outside the current execution "
                        "scope."
                    ),
                    effect_state=ToolEffectState.NOT_STARTED,
                )

            policy = registration.policy
            failure_effect_state = (
                ToolEffectState.NOT_STARTED
                if policy.mode is ToolExecutionMode.READ
                else ToolEffectState.UNKNOWN
            )
            predicted_cache_hit = _probe_cache(registration, request, parsed)
            cache_hits.append(predicted_cache_hit)

            approval_status: ApprovalStatus | None = None
            if policy.requires_user_approval:
                approval_status = await self._request_approval(
                    registration,
                    request,
                    parsed,
                    event_sink,
                    signal,
                )
                approval_batch_outcome = approval_outcome(approval_status)
                if approval_status is not ApprovalStatus.APPROVED:
                    code = approval_error_code(approval_status) or "approval_unavailable"
                    result = _failure_result(
                        parsed.call,
                        code,
                        _approval_message(approval_status),
                        approval_status=approval_status,
                    )
                    results.append(result)
                    outcomes.append(approval_batch_outcome)
                    await _emit_completed(
                        event_sink,
                        request,
                        index,
                        result,
                        approval_batch_outcome,
                        normalized_argument_paths=parsed.normalized_argument_paths,
                    )
                    await self._finish_tool_operation(
                        operation_id,
                        approval_batch_outcome,
                        code,
                    )
                    if approval_batch_outcome in {
                        ToolBatchOutcome.CANCELED,
                        ToolBatchOutcome.FAILED,
                    }:
                        await _append_unexecuted_results(
                            request,
                            event_sink,
                            parsed_calls[index + 1:],
                            start_index=index + 1,
                            results=results,
                            cache_hits=cache_hits,
                        )
                        return _batch_result(
                            results,
                            approval_batch_outcome,
                            cache_hits,
                            error=code,
                            effect_state=ToolEffectState.NOT_STARTED,
                        )
                    continue

            try:
                async def execute_handler() -> ToolHandlerResult:
                    # Core keeps model-generated arguments recursively immutable
                    # while applying scope, approval, and idempotency policy. Domain
                    # handlers are an adapter boundary, however, and expect ordinary
                    # Python JSON containers (dict/list). Give each invocation a
                    # detached mutable copy without weakening Core's trusted snapshot.
                    arguments = thaw_json_mapping(parsed.arguments)
                    if registration.call_handler is not None:
                        return await registration.call_handler(
                            request.state,
                            arguments,
                            parsed.call,
                            signal,
                        )
                    return await registration.handler(
                        request.state,
                        arguments,
                        signal,
                    )

                operation = execute_handler
                if (
                    policy.mode is not ToolExecutionMode.READ
                    and self._idempotency_gateway is not None
                    and request.run_id is not None
                    and not registration.host_managed_durability
                ):
                    operation = lambda: self._idempotency_gateway.execute_once(
                        request.run_id,
                        parsed.call,
                        execute_handler,
                    )
                handler_result = await await_with_cancellation(
                    operation(),
                    signal,
                    completion_wins_after_cancel=(
                        registration.cancellation_linearizable
                    ),
                )
            except OperationCanceled:
                await self._finish_tool_operation(
                    operation_id,
                    ToolBatchOutcome.CANCELED,
                    "tool_execution_canceled",
                )
                return await self._canceled_result(
                    request,
                    event_sink,
                    parsed,
                    index,
                    results,
                    cache_hits,
                )
            except asyncio.CancelledError:
                await self._finish_tool_operation(
                    operation_id,
                    ToolBatchOutcome.CANCELED,
                    "tool_execution_canceled",
                )
                raise
            except Exception as error:
                await self._finish_tool_operation(
                    operation_id,
                    ToolBatchOutcome.FAILED,
                    "tool_execution_failed",
                )
                return await _failed_call_batch(
                    request,
                    event_sink,
                    parsed,
                    index,
                    results,
                    cache_hits,
                    remaining=parsed_calls[index + 1:],
                    outcome=ToolBatchOutcome.FAILED,
                    code="tool_execution_failed",
                    message="Tool execution failed.",
                    approval_status=approval_status,
                    exception_type=type(error).__name__,
                    effect_state=failure_effect_state,
                )

            if not isinstance(handler_result, ToolHandlerResult):
                await self._finish_tool_operation(
                    operation_id,
                    ToolBatchOutcome.FAILED,
                    "invalid_tool_result",
                )
                return await _failed_call_batch(
                    request,
                    event_sink,
                    parsed,
                    index,
                    results,
                    cache_hits,
                    remaining=parsed_calls[index + 1:],
                    outcome=ToolBatchOutcome.FAILED,
                    code="invalid_tool_result",
                    message="Tool handler returned an invalid result.",
                    approval_status=approval_status,
                    effect_state=failure_effect_state,
                )

            handler_error = (
                normalize_error_code(handler_result.error_code)
                if handler_result.error_code
                else None
            )
            oversized = len(handler_result.content) > self._limits.max_result_chars
            if oversized:
                handler_error = "tool_result_too_large"
            content = sanitize_tool_result(
                handler_result.content,
                max_chars=self._limits.max_result_chars,
            )
            effects = () if handler_error else handler_result.effects
            result = ToolCallResult(
                tool_call_id=parsed.call.id,
                tool_name=parsed.call.name,
                content=content,
                from_cache=handler_result.from_cache,
                approval_status=approval_status,
                error=handler_error,
                effects=effects,
                step_disposition=handler_result.step_disposition,
                planning_disposition=(
                    handler_result.planning_disposition
                    if not handler_error
                    else ToolPlanningDisposition.KEEP_PLAN
                ),
            )
            results.append(result)

            if handler_error:
                await _emit_completed(
                    event_sink,
                    request,
                    index,
                    result,
                    ToolBatchOutcome.FAILED,
                    normalized_argument_paths=parsed.normalized_argument_paths,
                )
                await _append_unexecuted_results(
                    request,
                    event_sink,
                    parsed_calls[index + 1:],
                    start_index=index + 1,
                    results=results,
                    cache_hits=cache_hits,
                )
                await self._finish_tool_operation(
                    operation_id,
                    ToolBatchOutcome.FAILED,
                    handler_error,
                )
                return _batch_result(
                    results,
                    ToolBatchOutcome.FAILED,
                    cache_hits,
                    error=handler_error,
                    effect_state=(
                        ToolEffectState.NOT_STARTED
                        if policy.mode is ToolExecutionMode.READ
                        else ToolEffectState.COMMITTED
                        if outcomes
                        else handler_result.effect_state
                    ),
                )

            await _emit_effects(event_sink, request, effects)
            call_outcome = (
                ToolBatchOutcome.PROGRESSED
                if handler_result.step_disposition
                is ToolStepDisposition.CONTINUE
                else ToolBatchOutcome.COMPLETED
            )
            outcomes.append(call_outcome)
            await _emit_completed(
                event_sink,
                request,
                index,
                result,
                call_outcome,
                normalized_argument_paths=parsed.normalized_argument_paths,
            )
            await self._finish_tool_operation(
                operation_id,
                call_outcome,
                None,
            )

        return _batch_result(
            results,
            aggregate_outcomes(outcomes),
            cache_hits,
        )

    async def _start_tool_operation(
        self,
        request: ToolBatchRequest,
        parsed: ParsedToolCall,
        registration: ToolRegistration,
    ) -> str | None:
        if self._operations is None or request.run_id is None:
            return None
        label_params = {
            "toolCallId": parsed.call.id,
            "toolName": parsed.call.name,
        }
        retry_of = request.retry_of_tool_call_ids.get(parsed.call.id)
        if retry_of is not None:
            label_params["retryOfToolCallId"] = retry_of
        if registration.operation_display_params is not None:
            projected = registration.operation_display_params(
                request.state,
                parsed.arguments,
                parsed.call,
            )
            if not isinstance(projected, Mapping):
                raise TypeError("tool operation display params must be a mapping")
            for key, value in projected.items():
                normalized_key = str(key or "").strip()
                if normalized_key and normalized_key not in label_params:
                    label_params[normalized_key] = value
        receipt = await self._operations.start(
            OperationKind.TOOL,
            OperationScope(
                run_id=request.run_id,
                invocation_id=request.invocation_id,
                display=OperationDisplay(
                    label_key="agent.operation.tool",
                    label_params=label_params,
                ),
            ),
        )
        return receipt.operation_id

    async def _finish_tool_operation(
        self,
        operation_id: str | None,
        outcome: ToolBatchOutcome,
        error_code: str | None,
    ) -> None:
        if self._operations is None or operation_id is None:
            return
        if outcome in {
            ToolBatchOutcome.COMPLETED,
            ToolBatchOutcome.PROGRESSED,
        }:
            await self._operations.succeed(operation_id)
            return
        code = normalize_error_code(error_code or outcome.value)
        if outcome is ToolBatchOutcome.CANCELED:
            await self._operations.cancel(operation_id, code)
            return
        await self._operations.fail(operation_id, code)

    async def _validate_scope(
        self,
        registration: ToolRegistration,
        request: ToolBatchRequest,
        parsed: ParsedToolCall,
        signal: CancellationSignal | None,
    ) -> tuple[str, str, bool] | None:
        if registration.scope_validator is None:
            return None
        try:
            message = await await_with_cancellation(
                registration.scope_validator(request.state, parsed.arguments, signal),
                signal,
            )
        except OperationCanceled:
            return "tool_execution_canceled", "Tool execution was canceled.", True
        except asyncio.CancelledError:
            raise
        except Exception:
            return (
                "tool_scope_validation_failed",
                "Tool scope validation failed.",
                False,
            )
        if message:
            return "tool_scope_violation", sanitize_error_message(message), False
        return None

    async def _request_approval(
        self,
        registration: ToolRegistration,
        request: ToolBatchRequest,
        parsed: ParsedToolCall,
        event_sink: EventSink,
        signal: CancellationSignal | None,
    ) -> ApprovalStatus:
        if request.run_id is None or self._approval_gateway is None:
            return ApprovalStatus.UNAVAILABLE
        approval = ApprovalRequest(
            tool_call=parsed.call,
            title=registration.policy.title,
            risk_level=registration.policy.risk_level,
            summary=summarize_tool_arguments(
                parsed.arguments,
                max_chars=self._limits.approval_summary_chars,
            ),
            timeout_seconds=self._limits.approval_timeout_seconds,
        )
        try:
            result = await self._approval_gateway.request(
                request.run_id,
                approval,
                event_sink,
                signal,
            )
            return result.status
        except OperationCanceled:
            return ApprovalStatus.CANCELED
        except asyncio.CancelledError:
            raise
        except Exception:
            return ApprovalStatus.UNAVAILABLE

    async def _canceled_result(
        self,
        request: ToolBatchRequest,
        event_sink: EventSink,
        parsed: ParsedToolCall,
        index: int,
        results: list[ToolCallResult],
        cache_hits: list[bool],
    ) -> ToolBatchResult:
        return await _failed_call_batch(
            request,
            event_sink,
            parsed,
            index,
            results,
            cache_hits,
            outcome=ToolBatchOutcome.CANCELED,
            code="tool_execution_canceled",
            message="Tool execution was canceled.",
            approval_status=ApprovalStatus.CANCELED,
        )


def _probe_cache(
    registration: ToolRegistration,
    request: ToolBatchRequest,
    parsed: ParsedToolCall,
) -> bool:
    if registration.cache_probe is None:
        return False
    try:
        return bool(registration.cache_probe.will_hit(request.state, parsed.arguments))
    except Exception:
        return False


def _is_recoverable_artifact_batch(
    calls: Sequence[ParsedToolCall],
    registrations: Mapping[str, ToolRegistration],
) -> bool:
    names = {item.call.name for item in calls}
    if len(names) != 1:
        return False
    registration = registrations[next(iter(names))]
    return bool(
        registration.policy.mode is ToolExecutionMode.PROPOSE
        and registration.data_contract.payload_mode.value == "batch"
        and registration.cancellation_linearizable
        and registration.host_managed_durability
    )


async def _emit_effects(
    event_sink: EventSink,
    request: ToolBatchRequest,
    effects: tuple[DomainEffect, ...],
) -> None:
    reserved = next(
        (
            effect.type
            for effect in effects
            if effect.type in CONTROLLER_OWNED_RUN_EVENT_TYPES
        ),
        None,
    )
    if reserved is not None:
        raise ContractViolationError(
            "domain effects cannot use controller-owned event type "
            f"{reserved!r}"
        )
    for effect in effects:
        await event_sink.emit(AgentEvent(
            type=effect.type,
            run_id=request.run_id,
            payload=effect.payload,
        ))


async def _emit_completed(
    event_sink: EventSink,
    request: ToolBatchRequest,
    index: int,
    result: ToolCallResult,
    outcome: ToolBatchOutcome,
    *,
    exception_type: str | None = None,
    normalized_argument_paths: tuple[str, ...] = (),
) -> None:
    payload: dict[str, Any] = {
        "index": index,
        "toolCallId": result.tool_call_id,
        "toolName": result.tool_name,
        "fromCache": result.from_cache,
        "outcome": outcome.value,
    }
    if result.error:
        payload["errorCode"] = result.error
    if result.approval_status is not None:
        payload["approvalStatus"] = result.approval_status.value
    if exception_type:
        payload["exceptionType"] = exception_type
    if normalized_argument_paths:
        payload["normalizedArgumentPaths"] = list(normalized_argument_paths)
    await event_sink.emit(AgentEvent(
        type=CoreEventType.TOOL_CALL_COMPLETED,
        run_id=request.run_id,
        payload=payload,
    ))


def _whole_batch_failure(
    calls: tuple[ToolCall, ...],
    *,
    outcome: ToolBatchOutcome,
    code: str,
    message: str,
    diagnostics: Mapping[str, Any] | None = None,
) -> ToolBatchResult:
    normalized = normalize_error_code(code)
    return ToolBatchResult(
        results=tuple(
            _failure_result(
                call,
                normalized,
                message,
                diagnostics=diagnostics,
            )
            for call in calls
        ),
        outcome=outcome,
        error=normalized,
        cache_hits=tuple(False for _ in calls),
        effect_state=ToolEffectState.NOT_STARTED,
    )


async def _failed_call_batch(
    request: ToolBatchRequest,
    event_sink: EventSink,
    parsed: ParsedToolCall,
    index: int,
    results: list[ToolCallResult],
    cache_hits: list[bool],
    *,
    remaining: Sequence[ParsedToolCall] = (),
    outcome: ToolBatchOutcome,
    code: str,
    message: str,
    approval_status: ApprovalStatus | None = None,
    exception_type: str | None = None,
    effect_state: ToolEffectState = ToolEffectState.UNKNOWN,
) -> ToolBatchResult:
    result = _failure_result(
        parsed.call,
        code,
        message,
        approval_status=approval_status,
    )
    results.append(result)
    if len(cache_hits) < len(results):
        cache_hits.append(False)
    await _emit_completed(
        event_sink,
        request,
        index,
        result,
        outcome,
        exception_type=exception_type,
        normalized_argument_paths=parsed.normalized_argument_paths,
    )
    await _append_unexecuted_results(
        request,
        event_sink,
        remaining,
        start_index=index + 1,
        results=results,
        cache_hits=cache_hits,
    )
    return _batch_result(
        results,
        outcome,
        cache_hits,
        error=result.error,
        effect_state=effect_state,
    )


async def _append_unexecuted_results(
    request: ToolBatchRequest,
    event_sink: EventSink,
    remaining: Sequence[ParsedToolCall],
    *,
    start_index: int,
    results: list[ToolCallResult],
    cache_hits: list[bool],
) -> None:
    """Close every provider tool call after a sequential batch aborts.

    Provider protocols require one tool result for every announced call.  A
    handler failure stops later calls from executing, but omitting their
    receipts makes the otherwise-correctable failure look like a corrupt
    gateway batch and prevents Runtime recovery.
    """

    for offset, parsed in enumerate(remaining, start=start_index):
        result = _failure_result(
            parsed.call,
            "tool_batch_aborted",
            "Tool was not executed because an earlier call in the batch failed.",
        )
        results.append(result)
        cache_hits.append(False)
        await _emit_completed(
            event_sink,
            request,
            offset,
            result,
            ToolBatchOutcome.FAILED,
            normalized_argument_paths=parsed.normalized_argument_paths,
        )


def _batch_result(
    results: Sequence[ToolCallResult],
    outcome: ToolBatchOutcome,
    cache_hits: Sequence[bool],
    *,
    error: str | None = None,
    effect_state: ToolEffectState = ToolEffectState.UNKNOWN,
) -> ToolBatchResult:
    return ToolBatchResult(
        results=tuple(results),
        outcome=outcome,
        error=error,
        cache_hits=tuple(cache_hits),
        effect_state=effect_state,
    )


def _failure_result(
    call: ToolCall,
    code: str,
    message: str,
    *,
    approval_status: ApprovalStatus | None = None,
    diagnostics: Mapping[str, Any] | None = None,
) -> ToolCallResult:
    normalized = normalize_error_code(code)
    return ToolCallResult(
        tool_call_id=call.id,
        tool_name=call.name,
        content=safe_error_content(
            normalized,
            message,
            diagnostics=diagnostics,
        ),
        approval_status=approval_status,
        error=normalized,
    )


def _approval_message(status: ApprovalStatus) -> str:
    if status is ApprovalStatus.REJECTED:
        return "The operation was not approved."
    if status is ApprovalStatus.CANCELED:
        return "The approval request was canceled."
    if status is ApprovalStatus.TIMED_OUT:
        return "The approval request timed out."
    return "Approval is unavailable."
