"""Single Provider invocation boundary owned by PurrA."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from hashlib import sha256
import json
from typing import Protocol
from uuid import uuid4

from purra.cancellation import await_with_cancellation
from purra.contracts import (
    AgentMessage,
    ModelCompletion,
    ModelFinishReason,
    ModelInvocation,
    ModelStreamChunk,
    ToolCallDelta,
)
from purra.errors import ContractViolationError, ModelGatewayError
from purra.evidence import context_evidence_receipts
from purra.json_values import thaw_json_mapping
from purra.model_call_parameters import describe_model_call
from purra.model_invocation.contracts import (
    AgentModelCall,
    ManagedInvocationCompletion,
    ManagedInvocationStream,
    ModelInvocationContext,
    ModelInvocationReceipt,
)
from purra.model_protocol import classify_model_termination
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
from purra.ports import CancellationSignal, ModelGateway
from purra.stream_ownership import OwnedAsyncIterator


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


class AgentModelInvocationManager:
    """Authorize, identify, execute, observe, and classify Provider calls."""

    def __init__(
        self,
        gateway: ModelGateway,
        *,
        output_observer: ModelInvocationOutputObserver | None = None,
        operation_controller: AgentOperationController | None = None,
    ) -> None:
        if not isinstance(gateway, ModelGateway):
            raise TypeError("model invocation manager requires a ModelGateway")
        self._gateway = gateway
        self._output = output_observer or _NullOutputObserver()
        self._operations = operation_controller

    async def stream(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelCall,
        context: ModelInvocationContext,
        signal: CancellationSignal | None = None,
        *,
        on_attempt: Callable[[Mapping[str, object]], Awaitable[None]] | None = None,
    ) -> ManagedInvocationStream:
        self._validate_call(call, public_stream_allowed=True)
        invocation = _invocation(call)
        receipt, spec = self._receipt_and_spec(messages, call, context, invocation)
        if on_attempt is not None:
            await on_attempt(receipt.call_parameters[0])
        operation_id = await self._start_operation(receipt)
        stream_opened = False
        try:
            await self._output.open_model_stream(receipt, spec)
            stream_opened = True
            stream = await await_with_cancellation(
                self._gateway.stream(messages, invocation, signal),
                signal,
            )
        except BaseException as error:
            try:
                if stream_opened:
                    await self._output.abort_model_stream(
                        receipt.output_stream_id,
                        _error_code(error),
                    )
            finally:
                await self._fail_operation(operation_id, error)
            raise
        return ManagedInvocationStream(
            chunks=OwnedAsyncIterator(
                self._observe_chunks(
                    stream.chunks,
                    receipt,
                    signal,
                    operation_id,
                ),
                stream.chunks,
                terminal_predicate=lambda chunk: chunk.finish_reason is not None,
            ),
            receipt=receipt,
        )

    async def publish_model_stream_commentary(
        self,
        output_stream_id: str,
    ) -> object:
        return await self._output.publish_model_stream_commentary(
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
        self._validate_call(call, public_stream_allowed=False)
        invocation = _invocation(call)
        receipt, spec = self._receipt_and_spec(messages, call, context, invocation)
        if on_attempt is not None:
            await on_attempt(receipt.call_parameters[0])
        operation_id = await self._start_operation(receipt)
        stream_opened = False
        output_settled = False
        operation_settled = False
        try:
            await self._output.open_model_stream(receipt, spec)
            stream_opened = True
            completion = await await_with_cancellation(
                self._gateway.complete(messages, invocation, signal),
                signal,
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
            completion_chunk = _completion_chunk(completion)
            if completion_chunk is not None:
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
            try:
                if stream_opened and not output_settled:
                    await self._output.abort_model_stream(
                        receipt.output_stream_id,
                        _error_code(error),
                    )
            finally:
                if not operation_settled:
                    await self._fail_operation(operation_id, error)
            raise

    @staticmethod
    def _validate_call(
        call: AgentModelCall,
        *,
        public_stream_allowed: bool,
    ) -> None:
        if not isinstance(call, AgentModelCall):
            raise TypeError("model invocation manager requires an AgentModelCall")
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
            output_limit=call.output_limit,
            input_fingerprint=_fingerprint([
                message.to_mapping() for message in messages
            ]),
            tool_schema_fingerprint=_fingerprint([
                {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": thaw_json_mapping(tool.parameters),
                }
                for tool in invocation.tools
            ]),
            context_evidence=context_evidence_receipts(messages),
            call_parameters=(parameters,),
        )
        return receipt, OutputStreamSpec(
            output_stream_id=output_stream_id,
            run_id=context.run_id,
            turn_id=context.turn_id,
            invocation_id=invocation_id,
            intent=call.output_intent,
            commit_mode=call.commit_mode,
        )

    async def _observe_chunks(
        self,
        chunks: AsyncIterator[ModelStreamChunk],
        receipt: ModelInvocationReceipt,
        signal: CancellationSignal | None,
        operation_id: str | None,
    ) -> AsyncIterator[ModelStreamChunk]:
        finish_reason: ModelFinishReason | None = None
        tool_indices: set[int] = set()
        output_settled = False
        operation_settled = False
        try:
            while True:
                try:
                    chunk = await await_with_cancellation(anext(chunks), signal)
                except StopAsyncIteration:
                    break
                tool_indices.update(delta.index for delta in chunk.tool_call_deltas)
                await self._output.accept_provider_chunk(
                    receipt.output_stream_id,
                    chunk,
                )
                if chunk.finish_reason is not None:
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
            if finish_reason is None:
                raise ModelGatewayError(
                    "model stream ended without a finish reason",
                    code="upstream_stream_interrupted",
                    retryable=True,
                )
        except BaseException as error:
            try:
                if not output_settled:
                    await self._output.abort_model_stream(
                        receipt.output_stream_id,
                        _error_code(error),
                    )
                    output_settled = True
            finally:
                if not operation_settled:
                    await self._fail_operation(operation_id, error)
                    operation_settled = True
            raise
        finally:
            await _close_async_iterator(chunks)
            try:
                if not output_settled:
                    await self._output.abort_model_stream(
                        receipt.output_stream_id,
                        "invocation_consumer_closed",
                    )
            finally:
                if not operation_settled:
                    await self._cancel_operation(
                        operation_id,
                        "invocation_consumer_closed",
                    )

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
        output_limit=call.output_limit,
        reasoning_mode=call.reasoning_mode,
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
        or chunk.tool_call_deltas
        or chunk.usage is not None
    ):
        return None
    return chunk


def _error_code(error: BaseException) -> str:
    code = str(getattr(error, "code", "") or "").strip()
    return code or "model_invocation_failed"


async def _close_async_iterator(iterator: object) -> None:
    close = getattr(iterator, "aclose", None)
    if callable(close):
        await close()


__all__ = ["AgentModelInvocationManager", "ModelInvocationOutputObserver"]
