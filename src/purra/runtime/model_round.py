"""Model-round state that is independent from Runtime orchestration."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from enum import StrEnum
from hashlib import sha256
from typing import Any, Sequence

from purra.contracts import (
    AgentMessage,
    ModelFinishReason,
    ModelInvocation,
    ModelTokenUsage,
    ToolCall,
    ToolChoiceMode,
    TraceRecord,
)
from purra.cancellation import OperationCanceled
from purra.errors import ModelGatewayError, UnsupportedModelFeatureError
from purra.json_values import thaw_json_mapping
from purra.model_invocation.contracts import AgentModelCall
from purra.output import AgentOutputIntent, OutputCommitMode
from purra.recovery import (
    RecoveryAction,
    RecoveryCause,
    RecoveryDecision,
    RecoveryLedger,
    RecoveryPolicy,
    RecoveryRequest,
)


@dataclass(frozen=True, slots=True)
class PendingProviderAttempt:
    """Frozen inputs for a physical attempt in one logical model round."""

    messages: tuple[AgentMessage, ...]
    invocation: ModelInvocation
    allowed_names: frozenset[str]
    future_names: frozenset[str]
    require_tool: bool
    buffer_model_content: bool
    logical_round: int
    attempt: int


class ProviderFailurePhase(StrEnum):
    OPENING = "opening"
    STREAMING = "streaming"


class ProviderFailureDisposition(StrEnum):
    RETRY = "retry"
    FALLBACK = "fallback"
    CANCEL = "cancel"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class ProviderFailureResolution:
    disposition: ProviderFailureDisposition
    traces: tuple[TraceRecord, ...]
    error_code: str
    next_attempt: PendingProviderAttempt | None = None
    disable_required_tool_choice: bool = False


def resolve_provider_failure(
    error: Exception,
    *,
    phase: ProviderFailurePhase,
    attempt: PendingProviderAttempt,
    recovery_ledger: RecoveryLedger,
    remaining_model_rounds: int,
    round_number: int,
    cancellation_requested: bool,
    received_chunk_count: int,
    emitted_delta_count: int,
    visible_output_emitted: bool = False,
    provider_finish_observed: bool = False,
    duration_ms: int | None = None,
) -> ProviderFailureResolution:
    """Settle one failed physical Provider attempt without leaking its text."""

    phase = ProviderFailurePhase(phase)
    base_details = {
        "round": round_number,
        "attempt": attempt.attempt,
        "logicalRound": attempt.logical_round,
        "receivedChunkCount": received_chunk_count,
        "emittedDeltaCount": emitted_delta_count,
        "providerAttemptTerminal": True,
        "batchExecuted": False,
    }
    if isinstance(error, OperationCanceled) or cancellation_requested:
        return ProviderFailureResolution(
            disposition=ProviderFailureDisposition.CANCEL,
            traces=(TraceRecord(
                stage=(
                    "model_round"
                    if phase is ProviderFailurePhase.OPENING
                    else "stream"
                ),
                outcome="canceled",
                details={**base_details, "retryScheduled": False},
                duration_ms=duration_ms,
            ),),
            error_code="request_canceled",
        )

    if isinstance(error, UnsupportedModelFeatureError):
        decision: RecoveryDecision | None = None
        required = attempt.invocation.tool_choice is ToolChoiceMode.REQUIRED
        if required:
            decision = recovery_ledger.decide(RecoveryRequest(
                cause=RecoveryCause.PROVIDER_REQUIRED_TOOL_CHOICE_UNSUPPORTED,
                action=RecoveryAction.FALLBACK_PROVIDER_MODE,
                remaining_model_rounds=remaining_model_rounds,
                cancellation_requested=cancellation_requested,
            ))
        can_fallback = bool(decision is not None and decision.allowed)
        traces = [] if decision is None else [
            _recovery_trace(decision, round_number=round_number)
        ]
        traces.append(TraceRecord(
            stage="model_round",
            outcome="unsupported_model_feature",
            details={
                **base_details,
                "retryScheduled": can_fallback,
                "errorType": root_error_type(error),
                "errorChainTypes": error_chain_types(error),
            },
            duration_ms=duration_ms,
        ))
        next_attempt = None
        if can_fallback:
            next_attempt = replace(
                attempt,
                invocation=replace(
                    attempt.invocation,
                    tool_choice=ToolChoiceMode.AUTO,
                ),
                attempt=attempt.attempt + 1,
            )
            traces.append(TraceRecord(
                stage="tool_choice",
                outcome="provider_fallback_auto",
                details={
                    "round": round_number,
                    "attempt": attempt.attempt,
                    "logicalRound": attempt.logical_round,
                    "retryScheduled": True,
                    "batchExecuted": False,
                },
            ))
        return ProviderFailureResolution(
            disposition=(
                ProviderFailureDisposition.FALLBACK
                if can_fallback
                else ProviderFailureDisposition.FAIL
            ),
            traces=tuple(traces),
            error_code=error.code,
            next_attempt=next_attempt,
            disable_required_tool_choice=required,
        )

    error_code = (
        error.code
        if isinstance(error, ModelGatewayError)
        else (
            "model_gateway_error"
            if phase is ProviderFailurePhase.OPENING
            else "model_stream_error"
        )
    )
    decision = None
    if is_retryable_stream_interruption(error):
        decision = recovery_ledger.decide(RecoveryRequest(
            cause=RecoveryCause.PROVIDER_STREAM_INTERRUPTED,
            action=RecoveryAction.RETRY_MODEL,
            remaining_model_rounds=remaining_model_rounds,
            retryable=not provider_finish_observed,
            cancellation_requested=cancellation_requested,
            visible_output_emitted=visible_output_emitted,
        ))
    retry_scheduled = bool(decision is not None and decision.allowed)
    interrupted = error_code == "upstream_stream_interrupted"
    traces = [] if decision is None else [
        _recovery_trace(decision, round_number=round_number)
    ]
    traces.append(TraceRecord(
        stage="stream" if interrupted else "model_round",
        outcome=(
            ("interrupted_retry" if retry_scheduled else "interrupted")
            if interrupted
            else (
                "exception"
                if phase is ProviderFailurePhase.OPENING
                else "stream_exception"
            )
        ),
        details={
            **base_details,
            "retryScheduled": retry_scheduled,
            "errorType": root_error_type(error),
            "errorChainTypes": error_chain_types(error),
        },
        duration_ms=duration_ms,
    ))
    return ProviderFailureResolution(
        disposition=(
            ProviderFailureDisposition.RETRY
            if retry_scheduled
            else ProviderFailureDisposition.FAIL
        ),
        traces=tuple(traces),
        error_code=error_code,
        next_attempt=(
            replace(attempt, attempt=attempt.attempt + 1)
            if retry_scheduled
            else None
        ),
    )


def provider_retry_round_capacity(policy: RecoveryPolicy) -> int:
    """Return the bounded physical-attempt allowance outside logical rounds."""

    return sum(
        policy.max_attempts(cause)
        for cause in (
            RecoveryCause.PROVIDER_REQUIRED_TOOL_CHOICE_UNSUPPORTED,
            RecoveryCause.PROVIDER_STREAM_INTERRUPTED,
        )
    )


def build_agent_model_call(
    invocation: ModelInvocation,
    *,
    require_tool: bool,
    requires_full_text_validation: bool,
) -> AgentModelCall:
    """Assign one model round to the closed public/private output contract."""

    private_round = bool(invocation.tools or require_tool)
    if private_round:
        intent = AgentOutputIntent.STRUCTURED_PRIVATE
        commit_mode = OutputCommitMode.PRIVATE
    elif requires_full_text_validation:
        intent = AgentOutputIntent.STRUCTURED_PRIVATE
        commit_mode = OutputCommitMode.GATED
    else:
        intent = AgentOutputIntent.FINAL_PUBLIC
        commit_mode = OutputCommitMode.LIVE
    return AgentModelCall(
        request=invocation.request,
        output_intent=intent,
        commit_mode=commit_mode,
        requires_full_text_validation=requires_full_text_validation,
        reasoning_mode=invocation.reasoning_mode,
        output_limit=invocation.output_limit,
        tools=invocation.tools,
        tool_choice=invocation.tool_choice,
    )


def truncation_trace_details(
    *,
    accumulator: "ModelRoundAccumulator",
    round_number: int,
    attempt: int,
    request_fingerprint: str,
    finish_reason: ModelFinishReason,
    error_code: str,
    emitted_delta_count: int,
    output_limit: Any | None,
) -> dict[str, Any]:
    return {
        "round": round_number,
        "attempt": attempt,
        "requestFingerprint": request_fingerprint,
        "finishReason": finish_reason.value,
        "errorCode": error_code,
        "retryScheduled": False,
        "batchExecuted": False,
        "toolCallCount": accumulator.tool_call_count,
        "toolNames": list(accumulator.tool_call_names),
        "toolArgumentCharacters": accumulator.tool_argument_characters,
        "contentCharacters": len(accumulator.content),
        "reasoningOnly": bool(
            not accumulator.content.strip()
            and accumulator.reasoning.strip()
        ),
        "emittedDeltaCount": emitted_delta_count,
        "outputLimit": (
            output_limit.to_mapping() if output_limit is not None else None
        ),
    }


def model_request_fingerprint(
    messages: Sequence[AgentMessage],
    invocation: ModelInvocation,
) -> str:
    payload = {
        "messages": [message.to_mapping() for message in messages],
        "provider": invocation.request.provider,
        "model": invocation.request.model,
        "profileDigest": invocation.request.capability_snapshot.digest(),
        "options": thaw_json_mapping(invocation.request.options),
        "tools": [
            {
                "name": tool.name,
                "description": tool.description,
                "parameters": thaw_json_mapping(tool.parameters),
            }
            for tool in invocation.tools
        ],
        "toolChoice": invocation.tool_choice.value,
        "reasoningMode": invocation.reasoning_mode.value,
        "outputLimit": (
            invocation.output_limit.to_mapping()
            if invocation.output_limit is not None
            else None
        ),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return f"sha256:{sha256(encoded).hexdigest()}"


def is_retryable_stream_interruption(error: Exception) -> bool:
    return bool(
        isinstance(error, ModelGatewayError)
        and error.code == "upstream_stream_interrupted"
        and error.retryable
    )


def root_error_type(error: Exception) -> str:
    cause = error.__cause__
    return type(cause if isinstance(cause, Exception) else error).__name__


def error_chain_types(error: BaseException, *, limit: int = 8) -> list[str]:
    types: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and len(types) < max(1, int(limit)):
        identity = id(current)
        if identity in seen:
            break
        seen.add(identity)
        types.append(type(current).__name__)
        cause = current.__cause__
        current = cause if cause is not None else current.__context__
    return types


def _recovery_trace(
    decision: RecoveryDecision,
    *,
    round_number: int,
) -> TraceRecord:
    return TraceRecord(
        stage="recovery_decision",
        outcome="allowed" if decision.allowed else "denied",
        details={"round": round_number, **decision.to_trace_details()},
    )


@dataclass(slots=True)
class _ToolCallParts:
    id: str = ""
    name: str = ""
    argument_fragments: list[str] = field(default_factory=list)
    argument_chars: int = 0
    _arguments_cache: str | None = None

    @property
    def arguments(self) -> str:
        if self._arguments_cache is None:
            self._arguments_cache = "".join(self.argument_fragments)
        return self._arguments_cache

    def add_arguments(self, fragment: str) -> None:
        if not fragment:
            return
        self.argument_fragments.append(fragment)
        self.argument_chars += len(fragment)
        self._arguments_cache = None


class ModelRoundAccumulator:
    """Accumulate one provider stream without making lifecycle decisions."""

    def __init__(self) -> None:
        self._content_fragments: list[str] = []
        self._reasoning_fragments: list[str] = []
        self._content_cache: str | None = None
        self._reasoning_cache: str | None = None
        self.finish_reason: ModelFinishReason | None = None
        self.usage: ModelTokenUsage | None = None
        self._calls: dict[int, _ToolCallParts] = {}
        self._malformed_reason: str | None = None

    @property
    def content(self) -> str:
        if self._content_cache is None:
            self._content_cache = "".join(self._content_fragments)
        return self._content_cache

    @property
    def reasoning(self) -> str:
        if self._reasoning_cache is None:
            self._reasoning_cache = "".join(self._reasoning_fragments)
        return self._reasoning_cache

    @property
    def tool_call_count(self) -> int:
        return len(self._calls)

    @property
    def tool_argument_characters(self) -> int:
        return sum(parts.argument_chars for parts in self._calls.values())

    @property
    def tool_call_names(self) -> tuple[str, ...]:
        return tuple(
            parts.name
            for _, parts in sorted(self._calls.items())
            if parts.name
        )

    def add(self, chunk) -> None:
        if chunk.content_delta:
            self._content_fragments.append(chunk.content_delta)
            self._content_cache = None
        if chunk.reasoning_delta:
            self._reasoning_fragments.append(chunk.reasoning_delta)
            self._reasoning_cache = None
        if chunk.finish_reason is not None:
            self.finish_reason = chunk.finish_reason
        if chunk.usage is not None:
            self.usage = chunk.usage
        for delta in chunk.tool_call_deltas:
            current = self._calls.setdefault(delta.index, _ToolCallParts())
            if delta.id is not None:
                call_id = str(delta.id).strip()
                if current.id and call_id != current.id:
                    self._malformed_reason = "conflicting_tool_call_id_for_index"
                else:
                    current.id = call_id
            if delta.name is not None:
                name = str(delta.name).strip()
                if current.name and name != current.name:
                    self._malformed_reason = "conflicting_tool_name_for_index"
                else:
                    current.name = name
            current.add_arguments(str(delta.arguments_fragment or ""))

    def tool_calls(self) -> tuple[tuple[ToolCall, ...], str | None]:
        if self._malformed_reason is not None:
            return (), self._malformed_reason
        if not self._calls:
            return (), None
        ordered = tuple(parts for _, parts in sorted(self._calls.items()))
        if any(not parts.id.strip() for parts in ordered):
            return (), "missing_tool_call_id"
        if any(not parts.name.strip() for parts in ordered):
            return (), "missing_tool_call_name"
        ids = tuple(parts.id for parts in ordered)
        if len(ids) != len(set(ids)):
            return (), "duplicate_tool_call_id"
        return (
            tuple(
                ToolCall(
                    id=parts.id,
                    name=parts.name,
                    arguments_json=parts.arguments,
                )
                for parts in ordered
            ),
            None,
        )
