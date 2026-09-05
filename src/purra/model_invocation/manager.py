"""Single Provider invocation boundary owned by PurrA."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from hashlib import sha256
import json
import time
from typing import Protocol
from uuid import uuid4

from purra.cancellation import (
    ExecutionStopSignal,
    OperationCanceled,
    await_with_cancellation,
    raise_if_stopped,
)
from purra.contracts import (
    AgentMessage,
    ModelCompletion,
    ModelFinishReason,
    ModelInvocation,
    ModelStreamActivity,
    ModelStreamActivityKind,
    ModelStreamActivitySupport,
    ModelStreamItem,
    ModelStreamChunk,
    ToolCallDelta,
    RuntimeLimits,
)
from purra.errors import ContractViolationError, InvalidPlannerOutputError, ModelGatewayError
from purra.context_budget import max_generation_tokens_for_actual_input
from purra.evidence import context_evidence_receipts
from purra.model_invocation.evidence import (
    current_model_input_evidence,
    merge_context_evidence,
)
from purra.json_values import thaw_json_mapping
from purra.model_call_parameters import describe_model_call
from purra.model_invocation.contracts import (
    AgentModelCall,
    ManagedInvocationCompletion,
    ManagedInvocationStream,
    ModelInvocationContext,
    ModelInvocationReceipt,
)
from purra.model_protocol import (
    classify_model_termination,
    constrain_output_budget_to_context,
    resolve_invocation_output_budget,
)
from purra.operations import (
    AgentOperationController,
    OperationDisplay,
    OperationKind,
    OperationScope,
)
from purra.output.contracts import (
    AgentOutputIntent,
    OutputCommitMode,
    OutputStreamSpec,
)
from purra.ports import (
    CancellationSignal,
    ModelGateway,
    ModelInputEvidenceValidator,
)
from purra.ports import RunRepository
from purra.stream_ownership import OwnedAsyncIterator, close_async_resource
from purra.planning_stream import PLANNING_STREAM_SCHEMA, PlanningStreamParser
from purra.model_protocol import (
    FeatureSupport,
    ReasoningUsageDetail,
    ThinkingTokenAccounting,
)


class ModelInvocationOutputObserver(Protocol):
    async def open_model_stream(
        self,
        receipt: ModelInvocationReceipt,
        spec: OutputStreamSpec,
    ) -> object: ...

    async def accept_provider_chunk(
        self,
        output_stream_id: str,
        chunk: ModelStreamChunk,
    ) -> object: ...

    async def finish_model_stream(
        self,
        output_stream_id: str,
        finish_reason: ModelFinishReason,
    ) -> object: ...

    async def abort_model_stream(
        self,
        output_stream_id: str,
        error_code: str,
    ) -> object: ...

    async def publish_model_stream_commentary(
        self,
        output_stream_id: str,
    ) -> object: ...

    async def publish_model_stream_final(
        self,
        output_stream_id: str,
    ) -> object: ...


class _NullOutputObserver:
    async def open_model_stream(self, receipt, spec):
        del receipt, spec

    async def accept_provider_chunk(self, output_stream_id, chunk):
        del output_stream_id, chunk

    async def finish_model_stream(self, output_stream_id, finish_reason):
        del output_stream_id, finish_reason

    async def abort_model_stream(self, output_stream_id, error_code):
        del output_stream_id, error_code

    async def publish_model_stream_commentary(self, output_stream_id):
        del output_stream_id

    async def publish_model_stream_final(self, output_stream_id):
        del output_stream_id


class AgentModelInvocationManager:
    """Authorize, identify, execute, observe, and classify Provider calls."""

    def __init__(
        self,
        gateway: ModelGateway,
        *,
        output_observer: ModelInvocationOutputObserver | None = None,
        operation_controller: AgentOperationController | None = None,
        invocation_timeout_ms: int | None = 300_000,
        runtime_limits: RuntimeLimits = RuntimeLimits(
            max_run_generation_tokens=None,
        ),
        max_tool_argument_chars: int = 1_000_000,
        budget_repository: RunRepository | None = None,
        evidence_validator: ModelInputEvidenceValidator | None = None,
    ) -> None:
        if not isinstance(gateway, ModelGateway):
            raise TypeError("model invocation manager requires a ModelGateway")
        self._gateway = gateway
        self._output = output_observer or _NullOutputObserver()
        self._operations = operation_controller
        if invocation_timeout_ms is not None and int(invocation_timeout_ms) <= 0:
            raise ValueError("model invocation timeout must be positive")
        self._invocation_timeout_ms = (
            None if invocation_timeout_ms is None else int(invocation_timeout_ms)
        )
        if not isinstance(runtime_limits, RuntimeLimits):
            raise TypeError("model invocation runtime_limits must be RuntimeLimits")
        self._limits = runtime_limits
        if int(max_tool_argument_chars) <= 0:
            raise ValueError("model stream tool argument limit must be positive")
        self._max_tool_argument_chars = int(max_tool_argument_chars)
        self._budget_repository = budget_repository
        if evidence_validator is not None and not isinstance(
            evidence_validator, ModelInputEvidenceValidator
        ):
            raise TypeError(
                "evidence_validator must implement ModelInputEvidenceValidator"
            )
        self._evidence_validator = evidence_validator

    async def stream(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelCall,
        context: ModelInvocationContext,
        signal: CancellationSignal | None = None,
        *,
        on_attempt: Callable[[Mapping[str, object]], Awaitable[None]] | None = None,
        _on_chunk=None,
        _diagnostics: dict | None = None,
    ) -> ManagedInvocationStream:
        self._validate_call(call, context, public_stream_allowed=True)
        call = self._fit_call_to_context(messages, call, context)
        invocation = _invocation(call)
        receipt, spec = self._receipt_and_spec(messages, call, context, invocation)
        if on_attempt is not None:
            await on_attempt(receipt.call_parameters[0])
        operation_id = await self._start_operation(receipt)
        invocation_signal = self._invocation_signal(context, signal)
        stream_opened = False
        attempt_reserved = False
        stream = None
        liveness = None
        try:
            await self._validate_evidence(receipt, invocation_signal)
            await self._reserve_attempt(receipt)
            attempt_reserved = True
            if context.planning_scope is not None:
                from purra.planning_context import current_planning_context
                phase = current_planning_context()
                if phase is not None and phase.scope == context.planning_scope:
                    phase.model_attempts += 1
            await self._output.open_model_stream(receipt, spec)
            stream_opened = True
            gateway_started = time.monotonic()
            if _diagnostics is not None:
                _diagnostics["gatewayStartedAtMs"] = int(time.time() * 1000)
            async def open_gateway_stream():
                nonlocal stream
                stream = await self._gateway.stream(messages, invocation, invocation_signal)
                return stream

            stream = await await_with_cancellation(
                open_gateway_stream(),
                invocation_signal,
            )
            raise_if_stopped(invocation_signal)
            _require_applied_generation_budget(
                invocation,
                stream.applied_generation_limit,
            )
            meter = self._stream_meter(context)
            liveness = _StreamLiveness(
                invocation_signal,
                support=stream.activity_support,
                activity_timeout_ms=self._limits.provider_activity_idle_timeout_ms,
                progress_timeout_ms=self._limits.provider_progress_idle_timeout_ms,
                started=gateway_started,
            )

            liveness.accept_transport_diagnostics(stream.transport_diagnostics)
        except BaseException as error:
            selected_error = error
            try:
                if stream is not None:
                    await _close_async_iterator(stream.chunks)
                if attempt_reserved:
                    try:
                        await self._settle_attempt(receipt, None)
                    except BaseException as budget_error:
                        selected_error = budget_error
                if stream_opened:
                    await self._output.abort_model_stream(
                        receipt.output_stream_id,
                        _error_code(selected_error),
                    )
            finally:
                if liveness is not None:
                    liveness.close()
                invocation_signal.close()
                await self._fail_operation(operation_id, selected_error)
                if stream_opened and _diagnostics is not None:
                    record = getattr(self._output, "record_model_diagnostics", None)
                    if record is not None:
                        try:
                            await record(receipt, _diagnostics)
                        except BaseException:
                            pass  # Preserve the original open/transport failure.
            if selected_error is not error:
                raise selected_error from error
            raise

        def close_invocation() -> None:
            liveness.close()
            invocation_signal.close()

        async def close_unstarted() -> None:
            close_invocation()
            try:
                await self._settle_attempt(receipt, None)
            finally:
                try:
                    await self._output.abort_model_stream(receipt.output_stream_id, "invocation_consumer_closed")
                finally:
                    await self._cancel_operation(operation_id, "invocation_consumer_closed")

        return ManagedInvocationStream(
            chunks=OwnedAsyncIterator(
                self._observe_chunks(
                    stream.chunks,
                    receipt,
                    invocation_signal,
                    operation_id,
                    meter,
                    liveness,
                    invocation,
                    call.request.protocol_capabilities.public_progress
                    is FeatureSupport.SUPPORTED,
                    receipt,
                    close_invocation,
                    _on_chunk,
                    _diagnostics,
                ),
                stream.chunks,
                terminal_predicate=lambda chunk: chunk.finish_reason is not None,
                on_unstarted_close=close_unstarted,
            ),
            receipt=receipt,
        )

    async def plan(
        self, messages, call: AgentModelCall, context: ModelInvocationContext,
        signal: CancellationSignal | None = None, *, validate_plan=None,
        on_attempt=None,
    ) -> ManagedInvocationCompletion:
        """One managed planning attempt, with no hidden empty-response retries.

        Only complete typed progress records are projected. The supplied complete
        plan validator runs before the invocation commits; Core still compiles and
        admits the semantic result before ending the planning operation.
        """
        if call.request.protocol_capabilities.streaming is not FeatureSupport.SUPPORTED:
            raise ModelGatewayError("Planner requires a streaming Gateway", code="model_stream_unavailable")
        parser = PlanningStreamParser()
        started = time.monotonic()
        diagnostics = {"gatewayStartedAtMs": None, "firstActivityMs": None, "firstProgressMs": None,
                       "firstSemanticChunkMs": None, "invocationDurationMs": None,
                       "attemptStartedAtMs": int(time.time() * 1000), "firstPublicProgressMs": None, "planReceivedMs": None,
                       "rejectedPublicProgressRecords": 0,
                       "validationMs": None, "httpRequestSentAtMs": None,
                       "httpFirstByteAtMs": None, "sdkHttpAttempts": None}
        plan = None
        rejected_output = ""

        async def accept(receipt, chunk, invocation_signal):
            nonlocal plan, rejected_output
            if len(rejected_output) < 65_536:
                rejected_output += chunk.content_delta[:65_536 - len(rejected_output)]
            if chunk.tool_call_deltas:
                raise ModelGatewayError("Planner cannot call tools", code="unexpected_model_tool_calls")
            records = parser.feed(chunk.content_delta)
            diagnostics["rejectedPublicProgressRecords"] = parser.rejected_progress_records
            if parser.plan_received and diagnostics["planReceivedMs"] is None:
                diagnostics["planReceivedMs"] = round((time.monotonic() - started) * 1000)
            for record in records:
                raise_if_stopped(invocation_signal)
                if context.planning_scope is not None:
                    publish = getattr(self._output, "accept_planning_progress", None)
                    if publish is not None:
                        event = await publish(receipt.output_stream_id, record, invocation_signal)
                        if event is not None and diagnostics["firstPublicProgressMs"] is None:
                            diagnostics["firstPublicProgressMs"] = round((time.monotonic() - started) * 1000)
            if chunk.finish_reason is not None:
                termination = classify_model_termination(chunk.finish_reason, tool_call_count=0)
                if termination.incomplete:
                    raise ModelGatewayError("Incomplete planning stream", code=termination.error_code or "model_output_truncated")
                plan = parser.finish()
                diagnostics["rejectedPublicProgressRecords"] = parser.rejected_progress_records
                if diagnostics["planReceivedMs"] is None:
                    diagnostics["planReceivedMs"] = round(
                        (time.monotonic() - started) * 1000
                    )
                validation_started = time.monotonic()
                try:
                    if validate_plan is not None:
                        validate_plan(plan)
                finally:
                    diagnostics["validationMs"] = round((time.monotonic() - validation_started) * 1000)

        stream = await self.stream(messages, replace(call, output_protocol=PLANNING_STREAM_SCHEMA),
                                   context, signal, on_attempt=on_attempt,
                                   _on_chunk=accept, _diagnostics=diagnostics)
        from purra.contracts import MessageRole
        usage = None
        reason = None
        try:
            async for chunk in stream.chunks:
                usage = chunk.usage or usage
                reason = chunk.finish_reason or reason
        except InvalidPlannerOutputError as error:
            error.rejected_output = rejected_output or None
            raise
        finally:
            await stream.chunks.aclose()
        return ManagedInvocationCompletion(
            completion=ModelCompletion(
                message=AgentMessage(role=MessageRole.ASSISTANT, content=json.dumps(thaw_json_mapping(plan))),
                model=call.request.model, usage=usage, finish_reason=reason,
                applied_generation_limit=(
                    stream.receipt.output_budget.max_generation_tokens
                ),
            ), receipt=stream.receipt,
        )

    async def publish_model_stream_commentary(
        self,
        output_stream_id: str,
    ) -> object:
        return await self._output.publish_model_stream_commentary(
            str(output_stream_id)
        )

    async def publish_model_stream_final(
        self,
        output_stream_id: str,
    ) -> object:
        return await self._output.publish_model_stream_final(
            str(output_stream_id)
        )

    async def complete(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelCall,
        context: ModelInvocationContext,
        signal: CancellationSignal | None = None,
        *,
        on_attempt: Callable[[Mapping[str, object]], Awaitable[None]] | None = None,
    ) -> ManagedInvocationCompletion:
        self._validate_call(call, context, public_stream_allowed=False)
        call = self._fit_call_to_context(messages, call, context)
        invocation = _invocation(call)
        receipt, spec = self._receipt_and_spec(messages, call, context, invocation)
        if on_attempt is not None:
            await on_attempt(receipt.call_parameters[0])
        operation_id = await self._start_operation(receipt)
        invocation_signal = self._invocation_signal(context, signal)
        stream_opened = False
        output_settled = False
        operation_settled = False
        attempt_reserved = False
        attempt_settled = False
        meter = self._stream_meter(context)
        usage = None
        try:
            await self._validate_evidence(receipt, invocation_signal)
            await self._reserve_attempt(receipt)
            attempt_reserved = True
            await self._output.open_model_stream(receipt, spec)
            stream_opened = True
            completion = await await_with_cancellation(
                self._gateway.complete(messages, invocation, invocation_signal),
                invocation_signal,
            )
            usage = completion.usage
            raise_if_stopped(invocation_signal)
            _require_applied_generation_budget(
                invocation,
                completion.applied_generation_limit,
            )
            _require_reported_usage_within_generation_budget(
                invocation,
                usage,
                require_usage=self._requires_reported_usage(invocation),
                require_reasoning_usage=(
                    self._limits.max_reasoning_tokens is not None
                ),
            )
            reason = completion.finish_reason
            if reason is None:
                raise ModelGatewayError(
                    "model completion ended without a finish reason",
                    code="upstream_stream_interrupted",
                )
            termination = classify_model_termination(
                reason,
                tool_call_count=len(completion.message.tool_calls),
            )
            if termination.incomplete:
                raise ModelGatewayError(
                    "model output is incomplete",
                    code=termination.error_code or "model_output_truncated",
                    retryable=False,
                )
            if completion.message.tool_calls and not call.tools:
                raise ModelGatewayError(
                    "managed no-tool model call returned tool calls",
                    code="unexpected_model_tool_calls",
                    retryable=False,
                )
            attempt_settled = True
            await self._settle_attempt(receipt, completion.usage)
            completion_chunk = _completion_chunk(completion)
            if completion_chunk is not None:
                meter.accept(completion_chunk)
                await self._output.accept_provider_chunk(
                    receipt.output_stream_id,
                    completion_chunk,
                )
            await self._output.finish_model_stream(
                receipt.output_stream_id,
                reason,
            )
            output_settled = True
            await self._succeed_operation(operation_id)
            operation_settled = True
            return ManagedInvocationCompletion(
                completion=completion,
                receipt=receipt,
            )
        except BaseException as error:
            selected_error = error
            try:
                if attempt_reserved and not attempt_settled:
                    attempt_settled = True
                    try:
                        await self._settle_attempt(receipt, usage)
                    except BaseException as budget_error:
                        selected_error = budget_error
                if stream_opened and not output_settled:
                    await self._output.abort_model_stream(
                        receipt.output_stream_id,
                        _error_code(selected_error),
                    )
            finally:
                if not operation_settled:
                    await self._fail_operation(operation_id, selected_error)
            if selected_error is not error:
                raise selected_error from error
            raise
        finally:
            invocation_signal.close()

    @staticmethod
    def _validate_call(
        call: AgentModelCall,
        context: ModelInvocationContext,
        *,
        public_stream_allowed: bool,
    ) -> None:
        if not isinstance(call, AgentModelCall):
            raise TypeError("model invocation manager requires an AgentModelCall")
        if not isinstance(context, ModelInvocationContext):
            raise TypeError("model invocation requires a ModelInvocationContext")
        if call.reasoning_mode is not context.requested_reasoning_mode:
            raise ContractViolationError(
                "model call reasoning mode differs from the Run request",
                code="model_reasoning_mode_conflict",
            )
        if call.requires_full_text_validation and call.commit_mode is OutputCommitMode.LIVE:
            raise ContractViolationError(
                "full-text validation cannot use live output"
            )
        if (
            not public_stream_allowed
            and call.output_intent
            in {
                AgentOutputIntent.EXECUTION_PUBLIC,
                AgentOutputIntent.FINAL_PUBLIC,
            }
        ):
            raise ContractViolationError(
                "public output requires the streaming Provider boundary"
            )

    def _fit_call_to_context(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelCall,
        context: ModelInvocationContext,
    ) -> AgentModelCall:
        """Apply the physical remainder of this exact Provider request.

        Context planning reserves only a workflow result-capacity target.  The
        actual Provider generation ceiling is resolved here, after messages
        and tool schemas are final, so a large profile allowance cannot erase
        the input budget and later rounds cannot silently overrun the window.
        """

        snapshot = call.request.capability_snapshot
        window = context.context_window_tokens or snapshot.context_window_tokens
        if window > snapshot.context_window_tokens:
            raise ContractViolationError(
                "model invocation context window exceeds the model profile",
                code="model_context_capacity_incompatible",
                details={
                    "selectedContextWindowTokens": window,
                    "profileContextWindowTokens": snapshot.context_window_tokens,
                },
            )
        physical_maximum = max_generation_tokens_for_actual_input(
            window_tokens=window,
            messages=messages,
            tools=call.tools,
            safety_reserve_tokens=context.safety_reserve_tokens,
            runtime_reserve_tokens=context.runtime_reserve_tokens,
        )
        base_budget = resolve_invocation_output_budget(
            snapshot,
            max_generation_tokens=call.request.max_generation_tokens,
            result_capacity_target_tokens=(
                call.output_budget.result_capacity_target_tokens
            ),
            result_capacity_source=call.output_budget.result_capacity_source,
        )
        budget = constrain_output_budget_to_context(
            base_budget,
            max_generation_tokens=physical_maximum,
        )
        return call if budget == call.output_budget else replace(
            call,
            output_budget=budget,
        )

    def _requires_reported_usage(self, invocation: ModelInvocation) -> bool:
        return (
            invocation.request.capability_snapshot.output.reasoning_usage_detail
            is ReasoningUsageDetail.REQUIRED
            or any(
                value is not None
                for value in (
                    self._limits.max_input_tokens,
                    self._limits.max_run_generation_tokens,
                    self._limits.max_reasoning_tokens,
                )
            )
        )

    def _receipt_and_spec(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelCall,
        context: ModelInvocationContext,
        invocation: ModelInvocation,
    ) -> tuple[ModelInvocationReceipt, OutputStreamSpec]:
        if not isinstance(context, ModelInvocationContext):
            raise TypeError("model invocation requires a ModelInvocationContext")
        invocation_id = f"invocation-{uuid4().hex}"
        output_stream_id = f"output-{uuid4().hex}"
        parameters = describe_model_call(self._gateway, messages, invocation)
        receipt = ModelInvocationReceipt(
            invocation_id=invocation_id,
            output_stream_id=output_stream_id,
            run_id=context.run_id,
            turn_id=context.turn_id,
            model=call.request.model,
            output_intent=call.output_intent,
            commit_mode=call.commit_mode,
            output_budget=call.output_budget,
            input_fingerprint=_fingerprint({
                "messages": [message.to_mapping() for message in messages],
                "modelRequest": {
                    "provider": call.request.provider,
                    "model": call.request.model,
                    "profileId": call.request.profile_id,
                    "capabilitySnapshotDigest": (
                        call.request.capability_snapshot.digest()
                    ),
                    "requestedUserMaxGenerationTokens": (
                        call.request.max_generation_tokens
                    ),
                    "options": thaw_json_mapping(call.request.options),
                },
            }),
            tool_schema_fingerprint=_fingerprint([
                {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": thaw_json_mapping(tool.parameters),
                }
                for tool in invocation.tools
            ]),
            budget_key=context.attempt_source_key,
            output_protocol=call.output_protocol,
            planning_scope=context.planning_scope,
            planning_attempt=context.planning_attempt,
            context_evidence=merge_context_evidence(
                context_evidence_receipts(messages),
                current_model_input_evidence(),
            ),
            call_parameters=(parameters,),
        )
        return receipt, OutputStreamSpec(
            output_stream_id=output_stream_id,
            run_id=context.run_id,
            turn_id=context.turn_id,
            invocation_id=invocation_id,
            intent=call.output_intent,
            commit_mode=call.commit_mode,
            output_protocol=call.output_protocol,
            planning_scope=context.planning_scope,
            planning_attempt=context.planning_attempt,
        )

    async def _validate_evidence(
        self,
        receipt: ModelInvocationReceipt,
        signal: CancellationSignal | None,
    ) -> None:
        if not receipt.context_evidence:
            return
        validator = self._evidence_validator
        if validator is None:
            return
        raise_if_stopped(signal)
        await validator.validate_evidence(
            receipt.context_evidence,
            signal=signal,
        )
        raise_if_stopped(signal)

    def _invocation_signal(
        self,
        context: ModelInvocationContext,
        parent: CancellationSignal | None,
    ) -> ExecutionStopSignal:
        timeout_deadline = (
            None
            if self._invocation_timeout_ms is None
            else int(time.time() * 1000) + self._invocation_timeout_ms
        )
        deadlines = tuple(
            candidate
            for candidate in (
                (
                    context.deadline_at_ms,
                    context.deadline_code,
                ),
                (timeout_deadline, "model_invocation_deadline_exceeded"),
            )
            if candidate[0] is not None
        )
        deadline, code = min(deadlines, key=lambda item: item[0]) if deadlines else (
            None,
            "model_invocation_deadline_exceeded",
        )
        return ExecutionStopSignal(
            parent,
            deadline_at_ms=deadline,
            deadline_code=code,
        )

    def _stream_meter(self, context: ModelInvocationContext) -> "_StreamMeter":
        return _StreamMeter(
            max_chunks=self._limits.max_stream_chunks,
            max_content_chars=self._limits.max_stream_content_chars,
            max_reasoning_chars=self._limits.max_stream_reasoning_chars,
            max_tool_argument_chars=self._max_tool_argument_chars,
            tool_argument_limits=context.tool_argument_limits,
        )

    async def _reserve_attempt(self, receipt: ModelInvocationReceipt) -> None:
        if self._budget_repository is not None:
            await self._budget_repository.reserve_model_attempt(
                receipt.run_id,
                receipt.budget_key or receipt.invocation_id,
            )

    async def _settle_attempt(
        self,
        receipt: ModelInvocationReceipt,
        usage,
    ) -> None:
        if self._budget_repository is not None:
            await self._budget_repository.settle_model_attempt(
                receipt.run_id,
                receipt.budget_key or receipt.invocation_id,
                usage,
            )

    async def _observe_chunks(
        self,
        chunks: AsyncIterator[ModelStreamItem],
        receipt: ModelInvocationReceipt,
        signal: CancellationSignal | None,
        operation_id: str | None,
        meter: "_StreamMeter",
        liveness: "_StreamLiveness",
        invocation: ModelInvocation,
        public_progress_allowed: bool,
        budget_receipt: ModelInvocationReceipt,
        close_signal: Callable[[], None],
        on_chunk=None,
        diagnostics: dict | None = None,
    ) -> AsyncIterator[ModelStreamChunk]:
        finish_reason: ModelFinishReason | None = None
        tool_indices: set[int] = set()
        output_settled = False
        operation_settled = False
        attempt_settled = False
        usage = None
        try:
            while True:
                try:
                    item = await await_with_cancellation(anext(chunks), signal)
                except StopAsyncIteration:
                    break
                raise_if_stopped(signal)
                if isinstance(item, ModelStreamActivity):
                    liveness.accept_activity(item)
                    continue
                if not isinstance(item, ModelStreamChunk):
                    raise ContractViolationError(
                        "model stream yielded an unsupported item",
                        code="model_stream_item_invalid",
                    )
                chunk = item
                if chunk.usage is not None:
                    usage = chunk.usage
                    _require_reported_usage_within_generation_budget(
                        invocation,
                        usage,
                        require_reasoning_usage=(
                            self._limits.max_reasoning_tokens is not None
                        ),
                    )
                if chunk.progress_delta and not public_progress_allowed:
                    raise ContractViolationError(
                        "model gateway emitted undeclared public progress",
                        code="model_gateway_contract_violation",
                    )
                meaningful = liveness.accept_chunk(chunk)
                meter.accept(chunk)
                tool_indices.update(delta.index for delta in chunk.tool_call_deltas)
                await await_with_cancellation(self._output.accept_provider_chunk(
                    receipt.output_stream_id, chunk,
                ), signal)
                if on_chunk is not None:
                    await await_with_cancellation(on_chunk(receipt, chunk, signal), signal)
                raise_if_stopped(signal)
                if chunk.finish_reason is not None:
                    liveness.close()
                    _require_reported_usage_within_generation_budget(
                        invocation,
                        usage,
                        require_usage=self._requires_reported_usage(invocation),
                        require_reasoning_usage=(
                            self._limits.max_reasoning_tokens is not None
                        ),
                    )
                    attempt_settled = True
                    await self._settle_attempt(budget_receipt, usage)
                    finish_reason = chunk.finish_reason
                    termination = classify_model_termination(
                        finish_reason,
                        tool_call_count=len(tool_indices),
                    )
                    if termination.incomplete:
                        await self._output.abort_model_stream(
                            receipt.output_stream_id,
                            termination.error_code or "model_output_truncated",
                        )
                        output_settled = True
                        await self._fail_operation_code(
                            operation_id,
                            termination.error_code or "model_output_truncated",
                        )
                        operation_settled = True
                        raise ModelGatewayError(
                            "model output is incomplete",
                            code=(
                                termination.error_code
                                or "model_output_truncated"
                            ),
                            retryable=False,
                        )
                    else:
                        await self._output.finish_model_stream(
                            receipt.output_stream_id,
                            finish_reason,
                        )
                        output_settled = True
                        await self._succeed_operation(operation_id)
                    operation_settled = True
                    yield chunk
                    break
                yield chunk
                if meaningful:
                    liveness.resume()
            if finish_reason is None:
                raise ModelGatewayError(
                    "model stream ended without a finish reason",
                    code="upstream_stream_interrupted",
                    retryable=True,
                )
        except BaseException as error:
            selected_error = error
            try:
                if not attempt_settled:
                    attempt_settled = True
                    try:
                        await self._settle_attempt(budget_receipt, usage)
                    except BaseException as budget_error:
                        selected_error = budget_error
                if not output_settled:
                    try:
                        await self._output.abort_model_stream(
                            receipt.output_stream_id,
                            _error_code(selected_error),
                        )
                    finally:
                        output_settled = True
            finally:
                if not operation_settled:
                    await self._fail_operation(operation_id, selected_error)
                    operation_settled = True
            if selected_error is not error:
                raise selected_error from error
            raise
        finally:
            liveness.close()
            consumer_error: BaseException | None = None
            try:
                if not attempt_settled:
                    attempt_settled = True
                    try:
                        await self._settle_attempt(budget_receipt, usage)
                    except BaseException as error:
                        consumer_error = error
                await _close_async_iterator(chunks)
                try:
                    if not output_settled:
                        try:
                            await self._output.abort_model_stream(
                                receipt.output_stream_id,
                                (
                                    _error_code(consumer_error)
                                    if consumer_error is not None
                                    else "invocation_consumer_closed"
                                ),
                            )
                        finally:
                            output_settled = True
                finally:
                    if not operation_settled:
                        if consumer_error is None:
                            await self._cancel_operation(
                                operation_id,
                                "invocation_consumer_closed",
                            )
                        else:
                            await self._fail_operation(
                                operation_id,
                                consumer_error,
                            )
            finally:
                close_signal()
                if diagnostics is not None:
                    diagnostics.update(liveness.diagnostics())
                    record = getattr(self._output, "record_model_diagnostics", None)
                    if record is not None:
                        await record(receipt, diagnostics)
            if consumer_error is not None:
                raise consumer_error

    async def _start_operation(
        self,
        receipt: ModelInvocationReceipt,
    ) -> str | None:
        if self._operations is None:
            return None
        operation = await self._operations.start(
            OperationKind.MODEL,
            OperationScope(
                run_id=receipt.run_id,
                invocation_id=receipt.invocation_id,
                parent_operation_id=(receipt.planning_scope.operation_id if receipt.planning_scope else None),
                display=OperationDisplay(
                    label_key="agent.operation.model",
                    label_params={"model": receipt.model},
                ),
            ),
        )
        return operation.operation_id

    async def _succeed_operation(self, operation_id: str | None) -> None:
        if self._operations is not None and operation_id is not None:
            await self._operations.succeed(operation_id)

    async def _fail_operation(
        self,
        operation_id: str | None,
        error: BaseException,
    ) -> None:
        if isinstance(error, (OperationCanceled, asyncio.CancelledError)):
            await self._cancel_operation(operation_id, "request_canceled")
        else:
            await self._fail_operation_code(operation_id, _error_code(error))

    async def _fail_operation_code(
        self,
        operation_id: str | None,
        error_code: str,
    ) -> None:
        if self._operations is not None and operation_id is not None:
            await self._operations.fail(operation_id, error_code)

    async def _cancel_operation(
        self,
        operation_id: str | None,
        error_code: str,
    ) -> None:
        if self._operations is not None and operation_id is not None:
            await self._operations.cancel(operation_id, error_code)


def _invocation(call: AgentModelCall) -> ModelInvocation:
    if not call.request.protocol_capabilities.reasoning_mode_is_supported(
        call.reasoning_mode
    ):
        from purra.errors import UnsupportedModelFeatureError

        raise UnsupportedModelFeatureError(
            "selected reasoning mode is incompatible with model capabilities"
        )
    return ModelInvocation(
        request=call.request,
        tools=call.tools,
        tool_choice=call.tool_choice,
        output_budget=call.output_budget,
        reasoning_mode=call.reasoning_mode,
    )


def _require_applied_generation_budget(
    invocation: ModelInvocation,
    applied_generation_limit: int | None,
) -> None:
    expected = invocation.output_budget.max_generation_tokens
    if applied_generation_limit != expected:
        raise ContractViolationError(
            "Model gateway did not apply the requested invocation output limit",
            code="model_gateway_contract_violation",
            details={
                "expectedGenerationLimit": expected,
                "appliedGenerationLimit": applied_generation_limit,
            },
        )
def _require_reported_usage_within_generation_budget(
    invocation: ModelInvocation,
    usage,
    *,
    require_usage: bool = False,
    require_reasoning_usage: bool = False,
) -> None:
    if usage is None:
        if require_usage:
            raise ContractViolationError(
                "Model gateway omitted required Provider usage",
                code="model_gateway_contract_violation",
                details={"usage": None},
            )
        return
    expected = invocation.output_budget.max_generation_tokens
    if usage.generation_tokens > expected:
        raise ContractViolationError(
            "Model gateway reported generation usage above the invocation budget",
            code="model_gateway_contract_violation",
            details={
                "expectedGenerationLimit": expected,
                "reportedGenerationTokens": usage.generation_tokens,
            },
        )
    capabilities = invocation.request.capability_snapshot.output
    if (
        (
            capabilities.reasoning_usage_detail is ReasoningUsageDetail.REQUIRED
            or require_reasoning_usage
        )
        and usage.reasoning_tokens is None
    ):
        raise ContractViolationError(
            "Model gateway omitted required reasoning-token usage",
            code="model_gateway_contract_violation",
            details={"reasoningTokens": None},
        )
    if (
        capabilities.thinking_token_accounting
        is ThinkingTokenAccounting.INCLUDED
        and usage.reasoning_tokens is not None
        and usage.reasoning_tokens > usage.generation_tokens
    ):
        raise ContractViolationError(
            "Model gateway reported included reasoning usage above total generation usage",
            code="model_gateway_contract_violation",
            details={
                "reportedGenerationTokens": usage.generation_tokens,
                "reportedReasoningTokens": usage.reasoning_tokens,
                "thinkingTokenAccounting": "included",
            },
        )


def _fingerprint(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _completion_chunk(completion: ModelCompletion) -> ModelStreamChunk | None:
    content = completion.message.content
    content_delta = content if isinstance(content, str) else ""
    chunk = ModelStreamChunk(
        content_delta=content_delta,
        reasoning_delta=completion.message.reasoning or "",
        tool_call_deltas=tuple(
            ToolCallDelta(
                index=index,
                id=call.id,
                type="function",
                name=call.name,
                arguments_fragment=call.arguments_json,
            )
            for index, call in enumerate(completion.message.tool_calls)
        ),
        usage=completion.usage,
    )
    if not (
        chunk.content_delta
        or chunk.reasoning_delta
        or chunk.progress_delta
        or chunk.tool_call_deltas
        or chunk.usage is not None
    ):
        return None
    return chunk


def _error_code(error: BaseException) -> str:
    if isinstance(error, (OperationCanceled, asyncio.CancelledError)):
        return "request_canceled"
    code = str(getattr(error, "code", "") or "").strip()
    return code or "model_invocation_failed"


async def _close_async_iterator(iterator: object) -> None:
    await close_async_resource(iterator)


class _StreamLiveness:
    def __init__(
        self,
        signal: ExecutionStopSignal,
        *,
        support: ModelStreamActivitySupport,
        activity_timeout_ms: int | None,
        progress_timeout_ms: int | None,
        started: float | None = None,
    ) -> None:
        self._signal = signal
        self._support = ModelStreamActivitySupport(support)
        self._activity_timeout_ms = activity_timeout_ms
        self._progress_timeout_ms = progress_timeout_ms
        self._started = time.monotonic() if started is None else started
        self._first_activity_ms: int | None = None
        self._first_progress_ms: int | None = None
        self._first_semantic_ms: int | None = None
        self._last_activity_ms: int | None = None
        self._last_progress_ms: int | None = None
        self._max_activity_gap_ms: int | None = None
        self._max_progress_gap_ms: int | None = None
        self._activity_timer: asyncio.TimerHandle | None = None
        self._progress_timer: asyncio.TimerHandle | None = None
        self._closed = False
        self._transport_diagnostics = None
        self.resume()

    def accept_activity(self, item: ModelStreamActivity) -> None:
        if self._support is ModelStreamActivitySupport.SEMANTIC_ONLY:
            raise ContractViolationError(
                "semantic-only model stream yielded an activity item",
                code="model_stream_activity_unsupported",
            )
        if (
            item.kind is ModelStreamActivityKind.WORKING
            and self._support is not ModelStreamActivitySupport.WORKING
        ):
            raise ContractViolationError(
                "model stream yielded undeclared working activity",
                code="model_stream_activity_unsupported",
            )
        self.accept_transport_diagnostics(item.transport_diagnostics)
        now = self._elapsed_ms()
        self._record_activity(now)
        self._arm_activity()
        if item.kind is ModelStreamActivityKind.WORKING:
            self._record_progress(now)
            self._arm_progress()

    def accept_chunk(self, chunk: ModelStreamChunk) -> bool:
        if not _is_meaningful_chunk(chunk):
            return False
        now = self._elapsed_ms()
        if (
            chunk.content_delta
            or chunk.reasoning_delta
            or chunk.progress_delta
            or chunk.tool_call_deltas
        ) and self._first_semantic_ms is None:
            self._first_semantic_ms = now
        self._record_activity(now)
        self._record_progress(now)
        self._cancel_idle_timers()
        return True

    def resume(self) -> None:
        if self._closed or self._support is ModelStreamActivitySupport.SEMANTIC_ONLY:
            return
        self._arm_activity()
        self._arm_progress()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._cancel_idle_timers()

    def accept_transport_diagnostics(self, evidence) -> None:
        if evidence is None:
            return
        from purra.model_protocol import ModelTransportDiagnostics
        if not isinstance(evidence, ModelTransportDiagnostics):
            raise ContractViolationError("invalid transport diagnostics", code="model_gateway_contract_violation")
        self._transport_diagnostics = evidence

    def diagnostics(self) -> dict[str, object]:
        return {**(self._transport_diagnostics.to_mapping() if self._transport_diagnostics else {}),
                "activitySupport": self._support.value,
                "firstActivityMs": self._first_activity_ms,
                "firstProgressMs": self._first_progress_ms,
                "firstSemanticChunkMs": self._first_semantic_ms,
                "invocationDurationMs": self._elapsed_ms()}

    def _record_activity(self, now: int) -> None:
        if self._first_activity_ms is None:
            self._first_activity_ms = now
        if self._last_activity_ms is not None:
            gap = max(0, now - self._last_activity_ms)
            self._max_activity_gap_ms = max(
                gap,
                self._max_activity_gap_ms or 0,
            )
        self._last_activity_ms = now

    def _record_progress(self, now: int) -> None:
        if self._first_progress_ms is None:
            self._first_progress_ms = now
        if self._last_progress_ms is not None:
            gap = max(0, now - self._last_progress_ms)
            self._max_progress_gap_ms = max(
                gap,
                self._max_progress_gap_ms or 0,
            )
        self._last_progress_ms = now

    def _arm_activity(self) -> None:
        if self._activity_timer is not None:
            self._activity_timer.cancel()
        self._activity_timer = self._timer(
            self._activity_timeout_ms,
            "model_activity_deadline_exceeded",
            "activity_idle",
        )

    def _arm_progress(self) -> None:
        if self._progress_timer is not None:
            self._progress_timer.cancel()
        self._progress_timer = self._timer(
            self._progress_timeout_ms,
            "model_progress_deadline_exceeded",
            "progress_idle",
        )

    def _timer(
        self,
        timeout_ms: int | None,
        code: str,
        boundary: str,
    ) -> asyncio.TimerHandle | None:
        if self._closed or timeout_ms is None:
            return None
        return asyncio.get_running_loop().call_later(
            timeout_ms / 1000,
            self._expire,
            code,
            boundary,
        )

    def _expire(self, code: str, boundary: str) -> None:
        if self._closed:
            return
        details = {
            "activitySupport": self._support.value,
            "phase": "stream",
            "elapsedMs": self._elapsed_ms(),
            "firstActivityMs": self._first_activity_ms,
            "firstProgressMs": self._first_progress_ms,
            "lastActivityMs": self._last_activity_ms,
            "lastProgressMs": self._last_progress_ms,
            "maxActivityGapMs": self._max_activity_gap_ms,
            "maxProgressGapMs": self._max_progress_gap_ms,
            "selectedBoundary": boundary,
        }
        self.close()
        self._signal.set(code, details)

    def _cancel_idle_timers(self) -> None:
        for timer in (self._activity_timer, self._progress_timer):
            if timer is not None:
                timer.cancel()
        self._activity_timer = None
        self._progress_timer = None

    def _elapsed_ms(self) -> int:
        return max(0, round((time.monotonic() - self._started) * 1000))


def _is_meaningful_chunk(chunk: ModelStreamChunk) -> bool:
    return bool(
        chunk.content_delta
        or chunk.reasoning_delta
        or chunk.progress_delta
        or chunk.usage is not None
        or chunk.finish_reason is not None
        or any(
            delta.id is not None
            or delta.type is not None
            or delta.name is not None
            or bool(delta.arguments_fragment)
            for delta in chunk.tool_call_deltas
        )
    )


@dataclass(slots=True)
class _ToolStreamSize:
    name: str = ""
    argument_chars: int = 0


class _StreamMeter:
    def __init__(
        self,
        *,
        max_chunks: int,
        max_content_chars: int,
        max_reasoning_chars: int,
        max_tool_argument_chars: int,
        tool_argument_limits: Mapping[str, int],
    ) -> None:
        self._max_chunks = max_chunks
        self._max_content = max_content_chars
        self._max_reasoning = max_reasoning_chars
        self._max_tool = max_tool_argument_chars
        self._tool_limits = dict(tool_argument_limits)
        self._chunks = 0
        self._content = 0
        self._reasoning = 0
        self._tools: dict[int, _ToolStreamSize] = {}

    def accept(self, chunk: ModelStreamChunk) -> None:
        self._chunks += 1
        self._content += len(chunk.content_delta)
        self._reasoning += len(chunk.reasoning_delta)
        self._require_within(self._chunks, self._max_chunks, "chunk_count")
        self._require_within(self._content, self._max_content, "content_chars")
        self._require_within(
            self._reasoning,
            self._max_reasoning,
            "reasoning_chars",
        )
        for delta in chunk.tool_call_deltas:
            current = self._tools.setdefault(delta.index, _ToolStreamSize())
            if delta.name is not None:
                current.name = str(delta.name).strip()
            current.argument_chars += len(str(delta.arguments_fragment or ""))
            limit = self._tool_limits.get(current.name, self._max_tool)
            self._require_within(current.argument_chars, limit, "tool_argument_chars")

    @staticmethod
    def _require_within(value: int, limit: int, kind: str) -> None:
        if value > limit:
            raise ModelGatewayError(
                f"model stream exceeded the {kind} limit ({limit})",
                code="model_stream_limit_exceeded",
                retryable=False,
            )


__all__ = ["AgentModelInvocationManager", "ModelInvocationOutputObserver"]
