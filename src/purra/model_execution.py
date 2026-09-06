"""Managed provider calls for host hooks and bounded framework operations.

Applications supply a versioned model snapshot and optional user override.
PurrA resolves the exact provider output limit, creates ``ModelInvocation``,
and classifies the terminal provider reason.
"""

from __future__ import annotations

import asyncio

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from uuid import uuid4

from purra.cancellation import await_with_cancellation, is_canceled
from purra.contracts import (
    AgentMessage,
    ModelCompletion,
    ModelFinishReason,
    ModelRequest,
    ModelStreamChunk,
    ModelTokenUsage,
    MessageRole,
    ReasoningMode,
    ResponseValidationResult,
)
from purra.errors import ModelGatewayError, ResponseJudgeContractError
from purra.model_invocation import (
    AgentModelCall,
    AgentModelInvocationManager,
    ModelInvocationContext,
)
from purra.model_protocol import (
    InvocationOutputBudget,
    ResultCapacitySource,
    classify_model_termination,
    resolve_invocation_output_budget,
)
from purra.structured import StructuredOutputContract, StructuredOutputError
from purra.structured_task import (
    StructuredInvocationRef, StructuredModelTaskReceipt, StructuredModelTaskResult, total_usage,
)
from purra.output import AgentOutputIntent, OutputCommitMode
from purra.ports import CancellationSignal, ResponseJudgePolicy
from purra.recovery import (
    EMPTY_RESPONSE_RETRY_GUIDANCE,
    RecoveryAction,
    RecoveryCause,
    RecoveryLedger,
    RecoveryPolicy,
    RecoveryRequest,
)


@dataclass(frozen=True, slots=True)
class AgentModelTask:
    """Extension-declared intent for one PurrA-owned private model task."""

    request: ModelRequest
    result_capacity_target_tokens: int | None = None
    reasoning_mode: ReasoningMode | None = None
    output_budget: InvocationOutputBudget = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.request, ModelRequest):
            raise TypeError("managed model call requires a ModelRequest")
        budget = resolve_invocation_output_budget(
            self.request.capability_snapshot,
            max_generation_tokens=self.request.max_generation_tokens,
            result_capacity_target_tokens=self.result_capacity_target_tokens,
            result_capacity_source=(
                ResultCapacitySource.WORKFLOW_POLICY
                if self.result_capacity_target_tokens is not None
                else None
            ),
        )
        object.__setattr__(self, "output_budget", budget)
        if self.reasoning_mode is not None:
            object.__setattr__(
                self,
                "reasoning_mode",
                ReasoningMode(self.reasoning_mode),
            )


@dataclass(frozen=True, slots=True)
class AgentModelTaskCompletion:
    completion: ModelCompletion
    output_budget: InvocationOutputBudget
    call_parameters: tuple[Mapping[str, object], ...]


@dataclass(frozen=True, slots=True)
class AgentModelTaskStream:
    chunks: AsyncIterator[ModelStreamChunk]
    model: str
    output_budget: InvocationOutputBudget
    call_parameters: tuple[Mapping[str, object], ...]


@dataclass(frozen=True, slots=True)
class AgentModelTextResult:
    content: str
    reasoning: str
    finish_reason: ModelFinishReason
    usage: ModelTokenUsage | None
    attempts: int


AgentModelChunkObserver = Callable[[ModelStreamChunk], Awaitable[None]]


class AgentModelTaskRunner:
    """Run-bound PurrA capability injected into extension factories."""

    def __init__(
        self,
        manager: AgentModelInvocationManager,
        context: ModelInvocationContext,
        request: ModelRequest,
        *,
        signal: CancellationSignal | None = None,
    ) -> None:
        if not isinstance(manager, AgentModelInvocationManager):
            raise TypeError("model task runner requires PurrA's invocation manager")
        if not isinstance(context, ModelInvocationContext):
            raise TypeError("model task runner requires a Run context")
        if not isinstance(request, ModelRequest):
            raise TypeError("model task runner requires the Root ModelRequest")
        self._manager = manager
        self._context = context
        self._request = request
        self._signal = signal

    @property
    def run_id(self) -> str:
        """Return the immutable Core-owned Run identity for host adapters."""

        return self._context.run_id

    async def complete(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelTask,
        signal: CancellationSignal | None = None,
        *,
        on_attempt: (
            Callable[[Mapping[str, object]], Awaitable[None]] | None
        ) = None,
    ) -> AgentModelTaskCompletion:
        self._require_root_request(call.request)
        managed = await self._manager.complete(
            messages,
            _agent_call(call, self._context.requested_reasoning_mode),
            self._context,
            signal,
            on_attempt=on_attempt,
        )
        return AgentModelTaskCompletion(
            completion=managed.completion,
            output_budget=managed.receipt.output_budget,
            call_parameters=managed.receipt.call_parameters,
        )

    async def complete_structured(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelTask,
        *,
        output: StructuredOutputContract,
        signal: CancellationSignal | None = None,
        repair_attempts: int = 0,
    ) -> StructuredModelTaskResult:
        signal = _TaskStopSignal(self._signal, signal)
        self._require_root_request(call.request)
        if not isinstance(output, StructuredOutputContract):
            raise StructuredOutputError("An output contract is required", code="structured_output_schema_invalid")
        if type(repair_attempts) is not int or not 0 <= repair_attempts <= 2**53 - 1:
            raise ValueError("repair_attempts must be a non-negative safe integer")
        ledger = RecoveryLedger(RecoveryPolicy({RecoveryCause.STRUCTURED_OUTPUT_INVALID: repair_attempts}))
        task_id = f"structured-task-{uuid4().hex}"
        original_messages = tuple(messages)
        agent_call = replace(_agent_call(call, self._context.requested_reasoning_mode), output_contract=output)
        refs: tuple[StructuredInvocationRef, ...] = ()
        while True:
            try:
                attempt_call = replace(agent_call, structured_task={"taskId": task_id, "attempt": len(refs) + 1,
                    "previousInvocationId": refs[-1].invocation_id if refs else None})
                attempt_messages = original_messages if not refs else (*original_messages, AgentMessage(
                    role=MessageRole.DEVELOPER, content="Produce a new complete JSON object satisfying the schema; the previous response failed format validation."))
                managed = await self._manager.complete(attempt_messages, attempt_call, self._context, signal)
            except BaseException as error:
                ref = getattr(error, "structured_invocation_ref", None)
                if isinstance(ref, StructuredInvocationRef):
                    refs = (*refs, ref)
                error.invocation_refs = refs
                if not (isinstance(error, StructuredOutputError)
                        and error.code in {"structured_output_invalid_json", "structured_output_schema_mismatch"}
                        and ref is not None and ref.settled and ref.usage is not None):
                    raise
                decision = ledger.decide(RecoveryRequest(
                    cause=RecoveryCause.STRUCTURED_OUTPUT_INVALID,
                    action=RecoveryAction.RETRY_MODEL,
                    remaining_model_rounds=repair_attempts - len(refs) + 1,
                    cancellation_requested=is_canceled(signal),
                ))
                if not decision.allowed:
                    raise
                continue
            refs = (*refs, StructuredInvocationRef(
                managed.receipt.invocation_id, managed.receipt.output_budget,
                managed.completion.usage, True, True,
            ))
            persistence, root_budget = self._manager.structured_binding
            assert managed.structured_value is not None
            return StructuredModelTaskResult(managed.structured_value, StructuredModelTaskReceipt(
                self.run_id, managed.receipt.output_contract, refs, total_usage(refs),
                persistence, root_budget,
            ))

    async def stream(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelTask,
        signal: CancellationSignal | None = None,
        *,
        on_attempt: (
            Callable[[Mapping[str, object]], Awaitable[None]] | None
        ) = None,
    ) -> AgentModelTaskStream:
        self._require_root_request(call.request)
        managed = await self._manager.stream(
            messages,
            _agent_call(call, self._context.requested_reasoning_mode),
            self._context,
            signal,
            on_attempt=on_attempt,
        )
        return AgentModelTaskStream(
            chunks=_validated_chunks(managed.chunks, signal),
            model=managed.receipt.model,
            output_budget=managed.receipt.output_budget,
            call_parameters=managed.receipt.call_parameters,
        )

    def _require_root_request(self, request: ModelRequest) -> None:
        if request != self._request:
            raise ModelGatewayError(
                "managed model task differs from the Root model request",
                code="model_request_identity_conflict",
                retryable=False,
            )

    async def stream_text(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelTask,
        signal: CancellationSignal | None = None,
        *,
        on_attempt: (
            Callable[[Mapping[str, object]], Awaitable[None]] | None
        ) = None,
        on_chunk: AgentModelChunkObserver | None = None,
        recovery_policy: RecoveryPolicy = RecoveryPolicy(),
    ) -> AgentModelTextResult:
        """Collect one no-tool response with bounded empty-output recovery."""

        original_messages = tuple(messages)
        active_messages = original_messages
        recovery_ledger = RecoveryLedger(recovery_policy)
        attempt = 0
        while True:
            attempt += 1
            managed = await self.stream(
                active_messages,
                call,
                signal,
                on_attempt=on_attempt,
            )
            content_parts: list[str] = []
            reasoning_parts: list[str] = []
            finish_reason: ModelFinishReason | None = None
            usage: ModelTokenUsage | None = None
            try:
                async for chunk in managed.chunks:
                    if on_chunk is not None:
                        await on_chunk(chunk)
                    content_parts.append(chunk.content_delta)
                    reasoning_parts.append(chunk.reasoning_delta)
                    if chunk.finish_reason is not None:
                        finish_reason = chunk.finish_reason
                    if chunk.usage is not None:
                        usage = chunk.usage
            finally:
                await _close_async_iterator(managed.chunks)

            if finish_reason is None:
                raise ModelGatewayError(
                    "model stream ended without a finish reason",
                    code="upstream_stream_interrupted",
                )
            content = "".join(content_parts)
            reasoning = "".join(reasoning_parts)
            if content.strip():
                return AgentModelTextResult(
                    content=content,
                    reasoning=reasoning,
                    finish_reason=finish_reason,
                    usage=usage,
                    attempts=attempt,
                )

            remaining_retries = (
                recovery_policy.max_attempts(RecoveryCause.EMPTY_MODEL_RESPONSE)
                - recovery_ledger.attempts(RecoveryCause.EMPTY_MODEL_RESPONSE)
            )
            decision = recovery_ledger.decide(RecoveryRequest(
                cause=RecoveryCause.EMPTY_MODEL_RESPONSE,
                action=RecoveryAction.RETRY_MODEL,
                remaining_model_rounds=remaining_retries,
                cancellation_requested=is_canceled(signal),
            ))
            if not decision.allowed:
                raise ModelGatewayError(
                    "model returned no official response content",
                    code="empty_model_response",
                    retryable=False,
                )
            active_messages = (*original_messages, AgentMessage(
                role=MessageRole.ASSISTANT,
                content="",
                reasoning=reasoning or None,
            ), AgentMessage(
                role=MessageRole.DEVELOPER,
                content=EMPTY_RESPONSE_RETRY_GUIDANCE,
            ))


@dataclass(frozen=True, slots=True)
class AgentModelResponseJudge:
    """PurrA-owned model invocation around a domain-only judge policy."""

    model_tasks: AgentModelTaskRunner
    model_request: ModelRequest
    policy: ResponseJudgePolicy

    def __post_init__(self) -> None:
        if not isinstance(self.model_tasks, AgentModelTaskRunner):
            raise TypeError("model response judge requires PurrA Run model tasks")
        if not isinstance(self.model_request, ModelRequest):
            raise TypeError("model response judge requires a ModelRequest")
        if not isinstance(self.policy, ResponseJudgePolicy):
            raise TypeError("model response judge requires a ResponseJudgePolicy")

    async def judge(
        self,
        *,
        content: str,
        messages: Sequence[AgentMessage],
        signal: CancellationSignal | None = None,
    ) -> ResponseValidationResult:
        judge_messages = self.policy.build_messages(
            content=content,
            messages=messages,
        )
        completion = (
            await self.model_tasks.complete(
                judge_messages,
                AgentModelTask(
                    request=self.model_request,
                ),
                signal,
            )
        ).completion
        if (
            completion.message.role is not MessageRole.ASSISTANT
            or completion.message.tool_calls
            or not isinstance(completion.message.content, str)
        ):
            raise ResponseJudgeContractError(
                "semantic judge returned an unsupported message"
            )
        return self.policy.evaluate(
            judgment_content=completion.message.content,
            candidate_content=content,
        )


def _agent_call(
    call: AgentModelTask,
    inherited_reasoning_mode: ReasoningMode,
) -> AgentModelCall:
    return AgentModelCall(
        request=call.request,
        output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
        commit_mode=OutputCommitMode.PRIVATE,
        requires_full_text_validation=True,
        reasoning_mode=call.reasoning_mode or inherited_reasoning_mode,
        output_budget=call.output_budget,
    )


async def _validated_chunks(
    chunks: AsyncIterator[ModelStreamChunk],
    signal: CancellationSignal | None,
) -> AsyncIterator[ModelStreamChunk]:
    finish_reason: ModelFinishReason | None = None
    tool_indices: set[int] = set()
    try:
        while True:
            try:
                chunk = await await_with_cancellation(anext(chunks), signal)
            except StopAsyncIteration:
                break
            tool_indices.update(delta.index for delta in chunk.tool_call_deltas)
            if chunk.finish_reason is not None:
                finish_reason = chunk.finish_reason
            yield chunk
            if finish_reason is not None:
                break
    finally:
        await _close_async_iterator(chunks)
    if finish_reason is None:
        raise ModelGatewayError(
            "model stream ended without a finish reason",
            code="upstream_stream_interrupted",
        )
    termination = classify_model_termination(
        finish_reason,
        tool_call_count=len(tool_indices),
    )
    if termination.incomplete:
        raise ModelGatewayError(
            "model output is incomplete",
            code=termination.error_code or "model_output_truncated",
            retryable=False,
        )
    if tool_indices:
        raise ModelGatewayError(
            "managed no-tool model call returned tool calls",
            code="unexpected_model_tool_calls",
            retryable=False,
        )


async def _close_async_iterator(iterator: object) -> None:
    close = getattr(iterator, "aclose", None)
    if callable(close):
        await close()


__all__ = [
    "AgentModelTask",
    "AgentModelTaskCompletion",
    "AgentModelTaskRunner",
    "AgentModelTaskStream",
    "AgentModelTextResult",
    "AgentModelResponseJudge",
]


class _TaskStopSignal:
    """Keep the bound Run cancellation even when a host omits its call signal."""

    def __init__(self, *signals):
        self._signals = tuple(s for s in signals if s is not None)

    def is_set(self):
        return any(s.is_set() for s in self._signals)

    @property
    def reason_code(self):
        return next((getattr(s, "reason_code", None) or "request_canceled" for s in self._signals if s.is_set()), None)

    @property
    def reason_details(self):
        return next((getattr(s, "reason_details", {}) for s in self._signals if s.is_set()), {})

    async def wait(self):
        if self.is_set():
            return True
        waits = [asyncio.create_task(s.wait()) for s in self._signals]
        if not waits:
            waits = [asyncio.create_task(asyncio.Event().wait())]
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
            return True
        finally:
            for task in waits:
                task.cancel()
            await asyncio.gather(*waits, return_exceptions=True)
