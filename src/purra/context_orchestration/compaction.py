"""Generic Core timing, hook invocation, and compression-result validation."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any, Awaitable, Callable, Mapping, Sequence

from purra.context_budget import (
    allocate_context_budget,
    estimate_agent_messages_tokens,
    trim_agent_messages_by_turn,
)
from purra.context_orchestration.contracts import (
    ContextCompressionRequest,
    ContextCompressionSettings,
    ConversationCompactionResult,
)
from purra.context_orchestration.ledger import (
    ContextCompactionBudget,
    ContextCompactionPhase,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    MessageOrigin,
    MessageRole,
)
from purra.errors import ContextOverflowError, ContractViolationError
from purra.cancellation import OperationCanceled
from purra.operations import (
    AgentOperationController,
    OperationDisplay,
    OperationKind,
    OperationScope,
)
from purra.ports import CancellationSignal, ContextCompressionHook


class ContextCompressionCoordinator:
    """Own compression timing while delegating all semantic reduction.

    When no application hook is installed, Core uses a bounded recent-message
    trimmer.  The default never generates a summary, mutates canonical
    conversation storage, or infers semantic importance.
    """

    def __init__(
        self,
        hook: ContextCompressionHook | None = None,
        settings: ContextCompressionSettings = ContextCompressionSettings(),
        operation_controller: AgentOperationController | None = None,
    ) -> None:
        if hook is not None and not isinstance(hook, ContextCompressionHook):
            raise TypeError("context compression hook has an invalid contract")
        if not isinstance(settings, ContextCompressionSettings):
            raise TypeError("context compression settings are required")
        self._hook = hook
        self._settings = settings
        self._operations = operation_controller

    @property
    def hook(self) -> ContextCompressionHook | None:
        return self._hook

    @property
    def settings(self) -> ContextCompressionSettings:
        return self._settings

    async def prepare(
        self,
        request: AgentRunRequest,
        signal: CancellationSignal | None = None,
        *,
        on_compaction_started: (
            Callable[[Mapping[str, Any]], Awaitable[None]] | None
        ) = None,
        budget: ContextCompactionBudget | None = None,
        operation_scope: OperationScope | None = None,
    ) -> ConversationCompactionResult:
        snapshot = budget or _default_budget(request)
        message_tokens = estimate_agent_messages_tokens(request.messages)
        context_tokens = min(
            snapshot.provider_input_tokens,
            max(0, snapshot.context_tokens),
        )
        available_message_tokens = max(
            0,
            snapshot.provider_input_tokens - context_tokens,
        )
        projected_input_tokens = message_tokens + context_tokens
        pressure_ratio = (
            projected_input_tokens / snapshot.provider_input_tokens
        )
        over_message_budget = message_tokens > available_message_tokens
        threshold_reached = pressure_ratio >= self._settings.trigger_ratio
        compression_required = over_message_budget or threshold_reached
        trigger_reason = (
            "message_budget_exceeded"
            if over_message_budget
            else "pressure_threshold"
            if threshold_reached
            else "below_threshold"
        )
        compression = ContextCompressionRequest(
            request=request,
            budget=snapshot,
            message_tokens=message_tokens,
            projected_input_tokens=projected_input_tokens,
            available_message_tokens=available_message_tokens,
            pressure_ratio=pressure_ratio,
            compression_required=compression_required,
            trigger_reason=trigger_reason,
        )

        operation_id = (
            await self._start_compaction_operation(
                operation_scope,
                snapshot.phase,
            )
            if compression_required
            else None
        )
        try:
            result = await self._prepare_result(
                request,
                compression,
                signal,
                on_compaction_started=on_compaction_started,
                snapshot=snapshot,
                message_tokens=message_tokens,
                context_tokens=context_tokens,
                available_message_tokens=available_message_tokens,
                projected_input_tokens=projected_input_tokens,
                pressure_ratio=pressure_ratio,
                compression_required=compression_required,
                trigger_reason=trigger_reason,
            )
        except (OperationCanceled, asyncio.CancelledError):
            await self._cancel_compaction_operation(operation_id)
            raise
        except Exception as error:
            await self._fail_compaction_operation(operation_id, error)
            raise
        await self._succeed_compaction_operation(operation_id)
        return result

    async def _prepare_result(
        self,
        request: AgentRunRequest,
        compression: ContextCompressionRequest,
        signal: CancellationSignal | None,
        *,
        on_compaction_started: (
            Callable[[Mapping[str, Any]], Awaitable[None]] | None
        ),
        snapshot: ContextCompactionBudget,
        message_tokens: int,
        context_tokens: int,
        available_message_tokens: int,
        projected_input_tokens: int,
        pressure_ratio: float,
        compression_required: bool,
        trigger_reason: str,
    ) -> ConversationCompactionResult:

        if compression_required and on_compaction_started is not None:
            await on_compaction_started({
                "strategy": (
                    "application_hook"
                    if self._hook is not None
                    else "recent_messages"
                ),
                "triggerReason": trigger_reason,
                "pressureRatio": round(pressure_ratio, 4),
                "triggerRatio": self._settings.trigger_ratio,
                "availableMessageTokens": available_message_tokens,
                "messageTokens": message_tokens,
            })

        if self._hook is not None:
            result = await self._hook.compress(compression, signal)
        elif compression_required:
            result = _default_trim(compression, self._settings)
        else:
            result = ConversationCompactionResult(
                request=request,
                outcome="below_threshold",
                retained_raw_turn_count=_conversation_turn_count(
                    request.messages
                ),
            )

        _validate_result_contract(request, result)
        candidate_tokens = estimate_agent_messages_tokens(
            result.request.messages
        )
        overflow_tokens = max(
            0,
            candidate_tokens - available_message_tokens,
        )
        if overflow_tokens:
            raise ContextOverflowError(
                "context compression result exceeds the message budget",
                reason_code="context_compression_result_exceeds_budget",
                details={
                    "phase": snapshot.phase.value,
                    "strategy": (
                        "application_hook"
                        if self._hook is not None
                        else "recent_messages"
                    ),
                    "availableMessageTokens": available_message_tokens,
                    "compressedMessageTokens": candidate_tokens,
                    "overflowTokens": overflow_tokens,
                    "compressionOutcome": result.outcome,
                },
            )

        diagnostics = {
            **result.diagnostics,
            "compactionPhase": snapshot.phase.value,
            "strategy": (
                "application_hook"
                if self._hook is not None
                else "recent_messages"
            ),
            "triggerReason": trigger_reason,
            "compressionRequired": compression_required,
            "triggerRatio": self._settings.trigger_ratio,
            "pressureRatio": round(pressure_ratio, 4),
            "providerInputTokens": snapshot.provider_input_tokens,
            "contextReserveTokens": context_tokens,
            "availableMessageTokens": available_message_tokens,
            "messageTokensBefore": message_tokens,
            "messageTokensAfter": candidate_tokens,
            "projectedInputTokens": projected_input_tokens,
            "projectedInputTokensAfter": candidate_tokens + context_tokens,
            "droppedMessageCount": max(
                0,
                len(request.messages) - len(result.request.messages),
            ),
        }
        return ConversationCompactionResult(
            request=result.request,
            outcome=result.outcome,
            compression_state_version=result.compression_state_version,
            compacted_turn_count=result.compacted_turn_count,
            retained_raw_turn_count=result.retained_raw_turn_count,
            diagnostics=diagnostics,
        )

    async def _start_compaction_operation(
        self,
        scope: OperationScope | None,
        phase: ContextCompactionPhase,
    ) -> str | None:
        if self._operations is None:
            return None
        if not isinstance(scope, OperationScope):
            raise ContractViolationError(
                "context compaction operation requires a run scope"
            )
        receipt = await self._operations.start(
            OperationKind.CONTEXT_COMPACTION,
            OperationScope(
                run_id=scope.run_id,
                invocation_id=scope.invocation_id,
                display=OperationDisplay(
                    label_key="agent.operation.context_compaction",
                    label_params={"phase": phase.value},
                    resource_ref=scope.display.resource_ref,
                ),
            ),
        )
        return receipt.operation_id

    async def _succeed_compaction_operation(
        self,
        operation_id: str | None,
    ) -> None:
        if self._operations is not None and operation_id is not None:
            await self._operations.succeed(operation_id)

    async def _cancel_compaction_operation(
        self,
        operation_id: str | None,
    ) -> None:
        if self._operations is not None and operation_id is not None:
            await self._operations.cancel(
                operation_id,
                "context_compaction_canceled",
            )

    async def _fail_compaction_operation(
        self,
        operation_id: str | None,
        error: Exception,
    ) -> None:
        if self._operations is None or operation_id is None:
            return
        code = str(
            getattr(error, "reason_code", "")
            or getattr(error, "code", "")
            or "context_compaction_failed"
        )
        await self._operations.fail(operation_id, code)


def _default_budget(request: AgentRunRequest) -> ContextCompactionBudget:
    generation_tokens = 8_192
    allocated = allocate_context_budget(
        window_tokens=max(1, int(request.context_window or 128_000)),
        output_reserve_tokens=generation_tokens,
    )
    return ContextCompactionBudget(
        phase=ContextCompactionPhase.PRE_PLANNING,
        provider_input_tokens=allocated.provider_input_tokens,
        context_tokens=0,
        context_tokens_are_resolved=False,
        output_reserve_tokens=generation_tokens,
    )


def _default_trim(
    compression: ContextCompressionRequest,
    settings: ContextCompressionSettings,
) -> ConversationCompactionResult:
    trimmed = trim_agent_messages_by_turn(
        compression.request.messages,
        compression.available_message_tokens,
        max_recent_messages=settings.default_keep_recent_messages,
    )
    if trimmed.overflow_tokens:
        raise ContextOverflowError(
            "protected messages exceed the default compression budget",
            reason_code="protected_messages_exceed_compression_budget",
            details={
                "availableMessageTokens": (
                    compression.available_message_tokens
                ),
                "estimatedInputTokens": trimmed.token_estimate,
                "overflowTokens": trimmed.overflow_tokens,
            },
        )
    metadata = dict(compression.request.metadata)
    metadata["conversationCompaction"] = {
        "outcome": "compacted_default_trim",
        "strategy": "recent_messages",
        "keepRecentMessages": settings.default_keep_recent_messages,
        "droppedMessageCount": trimmed.dropped_count,
    }
    return ConversationCompactionResult(
        request=replace(
            compression.request,
            messages=trimmed.messages,
            metadata=metadata,
        ),
        outcome="compacted_default_trim",
        retained_raw_turn_count=_conversation_turn_count(trimmed.messages),
        diagnostics={
            "keepRecentMessages": settings.default_keep_recent_messages,
            "droppedMessageCount": trimmed.dropped_count,
        },
    )


def _validate_result_contract(
    source: AgentRunRequest,
    result: ConversationCompactionResult,
) -> None:
    if not isinstance(result, ConversationCompactionResult):
        raise ContractViolationError(
            "context compression hook returned an invalid result"
        )
    candidate = result.request
    immutable_fields = (
        "model",
        "domain_context",
        "session_id",
        "mode",
        "context_window",
        "tools_enabled",
        "planning_mode",
    )
    if any(
        getattr(candidate, name) != getattr(source, name)
        for name in immutable_fields
    ):
        raise ContractViolationError(
            "context compression hook changed immutable request fields"
        )

    source_privileged = tuple(
        message
        for message in source.messages
        if message.role in {MessageRole.SYSTEM, MessageRole.DEVELOPER}
    )
    candidate_privileged = tuple(
        message
        for message in candidate.messages
        if message.role in {MessageRole.SYSTEM, MessageRole.DEVELOPER}
    )
    if candidate_privileged != source_privileged:
        raise ContractViolationError(
            "context compression hook changed privileged instructions"
        )

    current = _last_caller_user_message(source.messages)
    if current is not None and current not in candidate.messages:
        raise ContractViolationError(
            "context compression hook removed the current user request"
        )

    source_messages = tuple(source.messages)
    for message in candidate.messages:
        if message in source_messages:
            continue
        if message.origin not in {
            MessageOrigin.HOST_CONTEXT,
            MessageOrigin.HOST_TOOL_RESULT,
        }:
            raise ContractViolationError(
                "context compression hook introduced untrusted caller content"
            )
    _validate_tool_protocol(candidate.messages)


def _validate_tool_protocol(messages: Sequence[AgentMessage]) -> None:
    pending: set[str] = set()
    for message in messages:
        if pending and message.role is not MessageRole.TOOL:
            raise ContractViolationError(
                "tool calls must be followed by their complete tool results"
            )
        if message.role is MessageRole.TOOL:
            tool_call_id = str(message.tool_call_id or "")
            if tool_call_id not in pending:
                raise ContractViolationError(
                    "context compression produced an orphan tool result"
                )
            pending.remove(tool_call_id)
            continue
        if message.tool_calls:
            ids = {call.id for call in message.tool_calls}
            if len(ids) != len(message.tool_calls):
                raise ContractViolationError(
                    "context compression produced duplicate tool call ids"
                )
            pending.update(ids)
    if pending:
        raise ContractViolationError(
            "context compression produced tool calls without results"
        )


def _last_caller_user_message(
    messages: Sequence[AgentMessage],
) -> AgentMessage | None:
    return next(
        (
            message
            for message in reversed(messages)
            if message.role is MessageRole.USER
            and message.origin is MessageOrigin.CALLER
        ),
        None,
    )


def _conversation_turn_count(messages: Sequence[AgentMessage]) -> int:
    return sum(
        message.role is MessageRole.USER
        and message.origin is MessageOrigin.CALLER
        for message in messages
    )


__all__ = [
    "ContextCompressionCoordinator",
]
