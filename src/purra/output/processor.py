"""The only PurrA component allowed to create canonical outward output."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import json
from typing import Protocol

from purra.contracts import (
    ModelFinishReason,
    ModelStreamChunk,
    RunCreateParams,
)
from purra.errors import (
    ContractViolationError,
    OutputPersistenceError,
    RunCommitProjectionError,
)
from purra.json_values import thaw_json_mapping
from purra.model_invocation.contracts import ModelInvocationReceipt
from purra.operations import OperationFinished, OperationStarted
from purra.events import AgentEvent
from purra.output.contracts import (
    AgentOutputEvent,
    AgentOutputEventDraft,
    AgentOutputIntent,
    DelegationOutputEvent,
    DomainEffectOutput,
    OutputChannel,
    OutputEventKind,
    OutputSource,
    OutputStreamSpec,
    OutputVisibility,
    AGENT_PROGRESS_SCHEMA,
    MAX_AGENT_PROGRESS_CHARS,
    PROVIDER_DELTA_BATCH_SCHEMA,
    RunLifecycleOutputDraft,
    RuntimeOutputEvent,
    ToolOutputEvent,
    provider_delta_batch_digest,
)
from purra.output.ports import (
    AgentOutputPolicy,
    AgentOutputPublisher,
    AgentOutputRepository,
)
from purra.ports.run_lifecycle import RunBeginResult, RunCommit
from purra.cancellation import raise_if_stopped
from purra.planning_stream import PlanningProgress, PLANNING_STREAM_SCHEMA


class OutputRecoveryObserver(Protocol):
    async def notify_output_failure(self, run_id: str, code: str) -> None: ...


class _AllowProviderChunks:
    async def authorize_provider_chunk(self, spec, chunk):
        del spec
        return chunk


@dataclass(frozen=True, slots=True)
class OutputBatchLimits:
    max_payload_bytes: int = 16_384
    max_fragments: int = 64
    max_latency_ms: int = 25
    max_background_latency_ms: int = 250

    def __post_init__(self) -> None:
        for name in (
            "max_payload_bytes",
            "max_fragments",
            "max_latency_ms",
            "max_background_latency_ms",
        ):
            value = int(getattr(self, name))
            if value <= 0:
                raise ValueError(f"{name.replace('_', ' ')} must be positive")
            object.__setattr__(self, name, value)


@dataclass(frozen=True, slots=True)
class _PendingProviderDelta:
    source_chunk_index: int
    source_part_index: int
    kind: OutputEventKind
    channel: OutputChannel
    visibility: OutputVisibility
    payload: Mapping[str, object]
    occurred_at: datetime

    def entry(self) -> dict[str, object]:
        return {
            "sourceChunkIndex": self.source_chunk_index,
            "sourcePartIndex": self.source_part_index,
            "kind": self.kind.value,
            "payload": dict(self.payload),
        }


@dataclass(slots=True)
class _StreamBatch:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    entries: list[_PendingProviderDelta] = field(default_factory=list)
    payload_bytes: int = 0
    timer: asyncio.Task[None] | None = None
    background_error: BaseException | None = None


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _validate_agent_progress_text(text: str) -> None:
    if (
        text != text.strip()
        or not text
        or "\n" in text
        or "\r" in text
        or len(text) > MAX_AGENT_PROGRESS_CHARS
    ):
        raise ContractViolationError(
            "Provider progress must be one trimmed line within the public limit"
        )


class AgentOutputProcessor:
    """Normalize typed producers into persist-before-publish output events."""

    def __init__(
        self,
        repository: AgentOutputRepository,
        publisher: AgentOutputPublisher,
        *,
        policy: AgentOutputPolicy | None = None,
        recovery_observer: OutputRecoveryObserver | None = None,
        batch_limits: OutputBatchLimits | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._repository = repository
        self._publisher = publisher
        self._policy = policy or _AllowProviderChunks()
        self._recovery = recovery_observer
        self._batch_limits = batch_limits or OutputBatchLimits()
        if not callable(sleep):
            raise TypeError("output batch sleep must be callable")
        self._sleep = sleep
        self._streams: dict[str, OutputStreamSpec] = {}
        self._stream_batches: dict[str, _StreamBatch] = {}
        self._chunk_indices: dict[str, int] = {}
        self._run_turn_ids: dict[str, str | None] = {}

    async def open_model_stream(
        self,
        receipt: ModelInvocationReceipt,
        spec: OutputStreamSpec,
    ) -> OutputStreamSpec:
        if not isinstance(receipt, ModelInvocationReceipt):
            raise TypeError("output processor requires a ModelInvocationReceipt")
        if not isinstance(spec, OutputStreamSpec):
            raise TypeError("output processor requires an OutputStreamSpec")
        if (
            receipt.output_stream_id != spec.output_stream_id
            or receipt.invocation_id != spec.invocation_id
            or receipt.run_id != spec.run_id
            or receipt.turn_id != spec.turn_id
            or receipt.output_intent is not spec.intent
            or receipt.commit_mode is not spec.commit_mode
            or receipt.output_protocol != spec.output_protocol
            or receipt.planning_scope != spec.planning_scope
            or receipt.planning_attempt != spec.planning_attempt
        ):
            raise ContractViolationError(
                "model invocation receipt does not match output stream"
            )
        opened = await self._persist_open(spec)
        self._streams[spec.output_stream_id] = opened
        self._stream_batches.setdefault(spec.output_stream_id, _StreamBatch())
        self._chunk_indices.setdefault(spec.output_stream_id, 0)
        try:
            await self._append(AgentOutputEventDraft(
                run_id=spec.run_id,
                turn_id=spec.turn_id,
                output_stream_id=spec.output_stream_id,
                invocation_id=spec.invocation_id,
                source_event_key=f"provider:{spec.invocation_id}:stream-opened",
                source=OutputSource.PROVIDER,
                kind=OutputEventKind.STREAM_OPENED,
                channel=OutputChannel.DIAGNOSTIC,
                visibility=OutputVisibility.PRIVATE,
                payload=receipt.to_mapping(),
                occurred_at=datetime.now(timezone.utc),
            ))
        except BaseException:
            try:
                await self.abort_model_stream(
                    spec.output_stream_id,
                    "invocation_receipt_persistence_failed",
                )
            finally:
                self._streams.pop(spec.output_stream_id, None)
                self._discard_batch(spec.output_stream_id)
                self._chunk_indices.pop(spec.output_stream_id, None)
            raise
        return opened

    async def accept_planning_progress(self, output_stream_id, progress: PlanningProgress, signal=None):
        """Project a complete Provider record; the repository verifies its bytes."""
        spec = self._require_stream(output_stream_id)
        if spec.output_protocol != PLANNING_STREAM_SCHEMA or spec.planning_scope is None:
            raise ContractViolationError("planning projection requires a bound planning stream")
        batch = self._require_batch(output_stream_id)
        async with batch.lock:
            self._raise_background_error(batch)
            raise_if_stopped(signal)
            authorize = getattr(self._policy, "authorize_planning_progress", None)
            if authorize is not None:
                authorized = await authorize(spec, progress)
                if authorized is None:
                    return None
                if authorized != progress:
                    raise ContractViolationError("output policy cannot rewrite Provider planning progress")
            await self._flush_batch_locked(spec, batch)
            raise_if_stopped(signal)
            return await self._append(AgentOutputEventDraft(
                run_id=spec.run_id, turn_id=spec.turn_id,
                output_stream_id=spec.output_stream_id, invocation_id=spec.invocation_id,
                source_event_key=f"planning:{spec.invocation_id}:{progress.record_index}",
                source=OutputSource.PROVIDER, kind=OutputEventKind.PLANNING_PROGRESS,
                channel=OutputChannel.COMMENTARY, visibility=OutputVisibility.PUBLIC,
                payload={"schemaVersion": PLANNING_STREAM_SCHEMA,
                         "operationId": spec.planning_scope.operation_id,
                         "revision": spec.planning_scope.revision, "attempt": spec.planning_attempt,
                         **progress.to_mapping()}, occurred_at=datetime.now(timezone.utc),
            ))

    async def record_model_diagnostics(self, receipt, metrics):
        return await self._append(AgentOutputEventDraft(
            run_id=receipt.run_id, turn_id=receipt.turn_id, output_stream_id=None,
            invocation_id=receipt.invocation_id,
            source_event_key=f"diagnostics:{receipt.invocation_id}",
            source=OutputSource.RUNTIME, kind=OutputEventKind.MODEL_DIAGNOSTICS,
            channel=OutputChannel.DIAGNOSTIC, visibility=OutputVisibility.PRIVATE,
            payload={"planningScope": receipt.planning_scope.to_mapping() if receipt.planning_scope else None,
                     "attempt": receipt.planning_attempt, **metrics},
            occurred_at=datetime.now(timezone.utc),
        ))

    async def begin_run_lifecycle(
        self,
        params: RunCreateParams,
        started_event: AgentEvent,
    ) -> RunBeginResult:
        try:
            begun, output = await self._repository.begin_run_lifecycle(
                params,
                started_event,
            )
        except (ContractViolationError, RunCommitProjectionError):
            raise
        except Exception as error:
            raise await self._persistence_error("unbound", error) from error
        self._run_turn_ids[begun.run_id] = params.turn_id
        await self._publish_if_visible(output)
        return begun

    async def accept_provider_chunk(
        self,
        output_stream_id: str,
        chunk: ModelStreamChunk,
    ) -> tuple[AgentOutputEvent, ...]:
        spec = self._require_stream(output_stream_id)
        if not isinstance(chunk, ModelStreamChunk):
            raise TypeError("output processor requires a ModelStreamChunk")
        authorized = await self._policy.authorize_provider_chunk(spec, chunk)
        if authorized is None:
            return ()
        if not isinstance(authorized, ModelStreamChunk):
            raise ContractViolationError(
                "output policy must return a ModelStreamChunk or None"
            )
        if authorized.progress_delta != chunk.progress_delta:
            raise ContractViolationError(
                "output policy cannot add or rewrite Provider progress"
            )
        if authorized.progress_delta:
            if spec.output_protocol == PLANNING_STREAM_SCHEMA:
                raise ContractViolationError(
                    "planning streams cannot emit agent progress"
                )
            _validate_agent_progress_text(authorized.progress_delta)
        if spec.output_protocol == PLANNING_STREAM_SCHEMA and authorized != chunk:
            raise ContractViolationError("output policy cannot rewrite planning Provider bytes")
        batch = self._require_batch(output_stream_id)
        async with batch.lock:
            self._raise_background_error(batch)
            chunk_index = self._chunk_indices[output_stream_id] + 1
            self._chunk_indices[output_stream_id] = chunk_index
            occurred_at = datetime.now(timezone.utc)
            additions = self._pending_provider_deltas(
                spec,
                authorized,
                chunk_index,
                occurred_at,
            )
            batch.entries.extend(additions)
            batch.payload_bytes += sum(
                len(_canonical_json(entry.entry())) for entry in additions
            )

            events: list[AgentOutputEvent] = []
            must_flush = (
                bool(authorized.progress_delta)
                or
                authorized.usage is not None
                or authorized.finish_reason is not None
                or batch.payload_bytes >= self._batch_limits.max_payload_bytes
                or len(batch.entries) >= self._batch_limits.max_fragments
            )
            if must_flush:
                events.extend(await self._flush_batch_locked(spec, batch))
            elif additions:
                self._schedule_batch_timer(spec, batch)
            if authorized.usage is not None:
                events.append(await self._append(self._provider_draft(
                    spec,
                    chunk_index,
                    "usage",
                    kind=OutputEventKind.PROVIDER_USAGE,
                    channel=OutputChannel.DIAGNOSTIC,
                    visibility=OutputVisibility.PRIVATE,
                    payload={
                        "inputTokens": authorized.usage.input_tokens,
                        "outputTokens": authorized.usage.output_tokens,
                        "totalTokens": authorized.usage.total_tokens,
                        "cachedInputTokens": authorized.usage.cached_input_tokens,
                        "reasoningOutputTokens": (
                            authorized.usage.reasoning_output_tokens
                        ),
                    },
                    occurred_at=occurred_at,
                )))
            if authorized.progress_delta:
                events.append(await self._append(AgentOutputEventDraft(
                    run_id=spec.run_id,
                    turn_id=spec.turn_id,
                    output_stream_id=spec.output_stream_id,
                    invocation_id=spec.invocation_id,
                    source_event_key=(
                        f"agent-progress:{spec.invocation_id}:{chunk_index}"
                    ),
                    source=OutputSource.PROVIDER,
                    kind=OutputEventKind.AGENT_PROGRESS,
                    channel=OutputChannel.COMMENTARY,
                    visibility=OutputVisibility.PUBLIC,
                    payload={
                        "schemaVersion": AGENT_PROGRESS_SCHEMA,
                        "text": authorized.progress_delta,
                        "sourceChunkIndex": chunk_index,
                    },
                    occurred_at=occurred_at,
                )))
            return tuple(events)

    async def finish_model_stream(
        self,
        output_stream_id: str,
        finish_reason: ModelFinishReason,
    ) -> AgentOutputEvent:
        spec = self._require_stream(output_stream_id)
        batch = self._require_batch(output_stream_id)
        async with batch.lock:
            self._raise_background_error(batch)
            await self._flush_batch_locked(spec, batch)
        try:
            event = await self._repository.commit_stream(
                output_stream_id,
                finish_reason,
            )
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(spec.run_id, error) from error
        self._discard_batch(output_stream_id)
        await self._publish_if_visible(event)
        return event

    async def abort_model_stream(
        self,
        output_stream_id: str,
        error_code: str,
    ) -> AgentOutputEvent:
        spec = self._require_stream(output_stream_id)
        batch = self._require_batch(output_stream_id)
        flush_error: BaseException | None = None
        try:
            async with batch.lock:
                self._raise_background_error(batch)
                await self._flush_batch_locked(spec, batch)
        except BaseException as error:
            flush_error = error
        try:
            event = await self._repository.abort_stream(
                output_stream_id,
                error_code,
            )
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(spec.run_id, error) from error
        finally:
            self._discard_batch(output_stream_id)
        await self._publish_if_visible(event)
        if flush_error is not None:
            raise flush_error
        return event

    async def publish_model_stream_commentary(
        self,
        output_stream_id: str,
    ) -> tuple[AgentOutputEvent, ...]:
        spec = self._require_stream(output_stream_id)
        try:
            events = await self._repository.publish_stream_content_as_commentary(
                output_stream_id
            )
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(spec.run_id, error) from error
        for event in events:
            await self._publish_if_visible(event)
        return events

    async def publish_model_stream_final(
        self,
        output_stream_id: str,
    ) -> tuple[AgentOutputEvent, ...]:
        spec = self._require_stream(output_stream_id)
        try:
            events = await self._repository.publish_stream_content_as_final(
                output_stream_id
            )
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(spec.run_id, error) from error
        for event in events:
            await self._publish_if_visible(event)
        return events

    async def accept_operation_event(
        self,
        event: OperationStarted | OperationFinished,
    ) -> AgentOutputEvent:
        if isinstance(event, OperationStarted):
            draft = AgentOutputEventDraft(
                run_id=event.run_id,
                turn_id=self._run_turn_ids.get(event.run_id),
                output_stream_id=None,
                invocation_id=event.invocation_id,
                source_event_key=f"operation:{event.operation_id}:started",
                source=OutputSource.RUNTIME,
                kind=OutputEventKind.OPERATION_STARTED,
                channel=OutputChannel.OPERATION,
                visibility=OutputVisibility.PUBLIC,
                payload={
                    "operationId": event.operation_id,
                    "parentOperationId": event.parent_operation_id,
                    "kind": event.kind.value,
                    "startedAt": event.started_at.isoformat(),
                    "display": thaw_json_mapping(event.display),
                },
                occurred_at=event.started_at,
            )
        elif isinstance(event, OperationFinished):
            draft = AgentOutputEventDraft(
                run_id=event.run_id,
                turn_id=self._run_turn_ids.get(event.run_id),
                output_stream_id=None,
                invocation_id=event.invocation_id,
                source_event_key=f"operation:{event.operation_id}:finished",
                source=OutputSource.RUNTIME,
                kind=OutputEventKind.OPERATION_FINISHED,
                channel=OutputChannel.OPERATION,
                visibility=OutputVisibility.PUBLIC,
                payload={
                    "operationId": event.operation_id,
                    "parentOperationId": event.parent_operation_id,
                    "status": event.status.value,
                    "finishedAt": event.finished_at.isoformat(),
                    "durationMs": event.duration_ms,
                    "errorCode": event.error_code,
                    "display": thaw_json_mapping(event.display),
                },
                occurred_at=event.finished_at,
            )
        else:
            raise TypeError("output processor requires a typed operation event")
        return await self._append(draft)

    async def accept_run_lifecycle_event(
        self,
        commit: RunCommit,
        event: RunLifecycleOutputDraft,
    ) -> tuple[AgentOutputEvent, ...]:
        run_ids = {item.run_id for item in commit.events if item.run_id}
        if len(run_ids) != 1:
            raise ContractViolationError(
                "run lifecycle commit requires one bound run id"
            )
        run_id = next(iter(run_ids))
        event = replace(event, turn_id=self._run_turn_ids.get(run_id))
        related_drafts = tuple(
            AgentOutputEventDraft(
                run_id=run_id,
                turn_id=self._run_turn_ids.get(run_id),
                output_stream_id=None,
                invocation_id=None,
                source_event_key=(
                    f"run-commit:{run_id}:{index}:{runtime_event.type}"
                ),
                source=OutputSource.RUNTIME,
                kind=OutputEventKind.RUNTIME,
                channel=OutputChannel.LIFECYCLE,
                visibility=OutputVisibility.PUBLIC,
                payload={
                    "eventType": str(runtime_event.type),
                    "data": payload,
                },
                occurred_at=event.occurred_at,
            )
            for index, runtime_event in enumerate(commit.events)
            if (
                payload := _public_runtime_payload(
                    str(runtime_event.type),
                    runtime_event.payload,
                )
            ) is not None
        )
        try:
            committed = await self._repository.commit_run_lifecycle(
                run_id,
                commit,
                event,
                related_drafts,
            )
        except (ContractViolationError, RunCommitProjectionError):
            raise
        except Exception as error:
            raise await self._persistence_error(run_id, error) from error
        for output in committed:
            await self._publish_if_visible(output)
        return committed

    async def accept_tool_event(self, event: ToolOutputEvent) -> AgentOutputEvent:
        if not isinstance(event, ToolOutputEvent):
            raise TypeError("output processor requires a ToolOutputEvent")
        return await self._append(AgentOutputEventDraft(
            run_id=event.run_id,
            turn_id=self._run_turn_ids.get(event.run_id),
            output_stream_id=None,
            invocation_id=event.invocation_id,
            source_event_key=(
                f"tool:{event.operation_id}:{event.tool_call_id}:{event.status}"
            ),
            source=OutputSource.TOOL,
            kind=OutputEventKind.TOOL,
            channel=OutputChannel.OPERATION,
            visibility=OutputVisibility.PUBLIC,
            payload={
                "operationId": event.operation_id,
                "toolCallId": event.tool_call_id,
                "toolName": event.tool_name,
                "status": event.status,
            },
            occurred_at=event.occurred_at,
        ))

    async def accept_domain_effect_event(
        self,
        event: DomainEffectOutput,
    ) -> AgentOutputEvent:
        if not isinstance(event, DomainEffectOutput):
            raise TypeError("output processor requires a DomainEffectOutput")
        return await self._append(AgentOutputEventDraft(
            run_id=event.run_id,
            turn_id=self._run_turn_ids.get(event.run_id),
            output_stream_id=None,
            invocation_id=None,
            source_event_key=f"domain:{event.effect_id}",
            source=OutputSource.DOMAIN,
            kind=OutputEventKind.DOMAIN_EFFECT,
            channel=OutputChannel.DIAGNOSTIC,
            visibility=OutputVisibility.PRIVATE,
            payload={
                "type": event.effect.type,
                "payload": thaw_json_mapping(event.effect.payload),
            },
            occurred_at=event.occurred_at,
        ))

    async def accept_runtime_event(
        self,
        event: RuntimeOutputEvent,
    ) -> AgentOutputEvent | None:
        if not isinstance(event, RuntimeOutputEvent):
            raise TypeError("output processor requires a RuntimeOutputEvent")
        private = event.event_type in _PRIVATE_RUNTIME_EVENT_TYPES
        payload = (
            thaw_json_mapping(event.payload)
            if private
            else _public_runtime_payload(event.event_type, event.payload)
        )
        if payload is None:
            return None
        return await self._append(AgentOutputEventDraft(
            run_id=event.run_id,
            turn_id=(
                event.turn_id
                if event.turn_id is not None
                else self._run_turn_ids.get(event.run_id)
            ),
            output_stream_id=None,
            invocation_id=None,
            source_event_key=f"runtime:{event.event_id}",
            source=OutputSource.RUNTIME,
            kind=OutputEventKind.RUNTIME,
            channel=(
                OutputChannel.DIAGNOSTIC
                if private else OutputChannel.LIFECYCLE
            ),
            visibility=(
                OutputVisibility.PRIVATE
                if private else OutputVisibility.PUBLIC
            ),
            payload={
                "eventType": event.event_type,
                "data": payload,
            },
            occurred_at=event.occurred_at,
        ))

    async def accept_delegation_event(
        self,
        event: DelegationOutputEvent,
    ) -> AgentOutputEvent:
        if not isinstance(event, DelegationOutputEvent):
            raise TypeError("output processor requires a DelegationOutputEvent")
        return await self._append(AgentOutputEventDraft(
            run_id=event.run_id,
            turn_id=self._run_turn_ids.get(event.run_id),
            output_stream_id=None,
            invocation_id=None,
            source_event_key=f"delegation-status:{event.event_id}",
            source=OutputSource.RUNTIME,
            kind=OutputEventKind.DELEGATION,
            channel=OutputChannel.DELEGATION,
            visibility=OutputVisibility.PUBLIC,
            payload={
                "eventType": "status",
                "batchId": event.batch_id,
                "delegationId": event.delegation_id,
                "runId": event.run_id,
                "agentName": event.agent_name,
                "agentTitle": event.agent_title,
                "objective": event.objective,
                "status": event.status,
                "errorCode": event.error_code,
            },
            occurred_at=event.occurred_at,
        ))

    def _provider_draft(
        self,
        spec: OutputStreamSpec,
        chunk_index: int,
        part: str,
        *,
        kind: OutputEventKind,
        channel: OutputChannel,
        visibility: OutputVisibility,
        payload: Mapping[str, object],
        occurred_at: datetime,
    ) -> AgentOutputEventDraft:
        return AgentOutputEventDraft(
            run_id=spec.run_id,
            turn_id=spec.turn_id,
            output_stream_id=spec.output_stream_id,
            invocation_id=spec.invocation_id,
            source_event_key=(
                f"provider:{spec.invocation_id}:{chunk_index}:{part}"
            ),
            source=OutputSource.PROVIDER,
            kind=kind,
            channel=channel,
            visibility=visibility,
            payload=payload,
            occurred_at=occurred_at,
        )

    def _pending_provider_deltas(
        self,
        spec: OutputStreamSpec,
        chunk: ModelStreamChunk,
        chunk_index: int,
        occurred_at: datetime,
    ) -> tuple[_PendingProviderDelta, ...]:
        pending: list[_PendingProviderDelta] = []
        if chunk.content_delta:
            channel, visibility = _content_destination(spec)
            pending.append(_PendingProviderDelta(
                chunk_index,
                0,
                OutputEventKind.PROVIDER_CONTENT_DELTA,
                channel,
                visibility,
                {"delta": chunk.content_delta},
                occurred_at,
            ))
        if chunk.reasoning_delta:
            pending.append(_PendingProviderDelta(
                chunk_index,
                1,
                OutputEventKind.PROVIDER_REASONING_DELTA,
                OutputChannel.DIAGNOSTIC,
                OutputVisibility.DIAGNOSTIC,
                {"delta": chunk.reasoning_delta},
                occurred_at,
            ))
        if chunk.tool_call_deltas:
            pending.append(_PendingProviderDelta(
                chunk_index,
                2,
                OutputEventKind.PROVIDER_TOOL_CALL_DELTA,
                OutputChannel.DIAGNOSTIC,
                OutputVisibility.PRIVATE,
                {
                    "deltas": [
                        {
                            "index": delta.index,
                            "id": delta.id,
                            "type": delta.type,
                            "name": delta.name,
                            "argumentsFragment": delta.arguments_fragment,
                        }
                        for delta in chunk.tool_call_deltas
                    ]
                },
                occurred_at,
            ))
        if chunk.progress_delta:
            pending.append(_PendingProviderDelta(
                chunk_index,
                3,
                OutputEventKind.PROVIDER_PROGRESS_DELTA,
                OutputChannel.DIAGNOSTIC,
                OutputVisibility.PRIVATE,
                {"delta": chunk.progress_delta},
                occurred_at,
            ))
        return tuple(pending)

    def _batch_drafts(
        self,
        spec: OutputStreamSpec,
        entries: tuple[_PendingProviderDelta, ...],
    ) -> tuple[AgentOutputEventDraft, ...]:
        grouped: dict[
            tuple[OutputChannel, OutputVisibility],
            list[_PendingProviderDelta],
        ] = {}
        for entry in entries:
            grouped.setdefault((entry.channel, entry.visibility), []).append(entry)
        groups = sorted(
            grouped.items(),
            key=lambda item: (
                item[1][0].source_chunk_index,
                item[1][0].source_part_index,
            ),
        )
        drafts = []
        for (channel, visibility), values in groups:
            normalized = [entry.entry() for entry in values]
            start = min(entry.source_chunk_index for entry in values)
            end = max(entry.source_chunk_index for entry in values)
            payload = {
                "schemaVersion": PROVIDER_DELTA_BATCH_SCHEMA,
                "sourceChunkStart": start,
                "sourceChunkEnd": end,
                "entries": normalized,
                "payloadDigest": provider_delta_batch_digest(normalized),
            }
            drafts.append(AgentOutputEventDraft(
                run_id=spec.run_id,
                turn_id=spec.turn_id,
                output_stream_id=spec.output_stream_id,
                invocation_id=spec.invocation_id,
                source_event_key=(
                    f"provider-batch:{spec.invocation_id}:"
                    f"{channel.value}:{visibility.value}:{start}:{end}"
                ),
                source=OutputSource.PROVIDER,
                kind=OutputEventKind.PROVIDER_DELTA_BATCH,
                channel=channel,
                visibility=visibility,
                payload=payload,
                occurred_at=values[0].occurred_at,
            ))
        return tuple(drafts)

    async def _flush_batch_locked(
        self,
        spec: OutputStreamSpec,
        batch: _StreamBatch,
    ) -> tuple[AgentOutputEvent, ...]:
        if not batch.entries:
            self._cancel_batch_timer(batch)
            return ()
        drafts = self._batch_drafts(spec, tuple(batch.entries))
        events = await self._persist_batch(spec.run_id, drafts)
        batch.entries.clear()
        batch.payload_bytes = 0
        self._cancel_batch_timer(batch)
        for event in events:
            await self._publish_if_visible(event)
        return events

    async def _persist_batch(
        self,
        run_id: str,
        drafts: tuple[AgentOutputEventDraft, ...],
    ) -> tuple[AgentOutputEvent, ...]:
        try:
            return await self._repository.append_batch(drafts)
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(run_id, error) from error

    def _schedule_batch_timer(
        self,
        spec: OutputStreamSpec,
        batch: _StreamBatch,
    ) -> None:
        if batch.timer is None or batch.timer.done():
            batch.timer = asyncio.create_task(
                self._flush_after_latency(spec, batch)
            )

    async def _flush_after_latency(
        self,
        spec: OutputStreamSpec,
        batch: _StreamBatch,
    ) -> None:
        current = asyncio.current_task()
        try:
            latency_ms = (
                self._batch_limits.max_latency_ms
                if spec.intent in {
                    AgentOutputIntent.EXECUTION_PUBLIC,
                    AgentOutputIntent.FINAL_PUBLIC,
                }
                else self._batch_limits.max_background_latency_ms
            )
            await self._sleep(latency_ms / 1000)
            async with batch.lock:
                self._raise_background_error(batch)
                await self._flush_batch_locked(spec, batch)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            batch.background_error = error
        finally:
            if batch.timer is current:
                batch.timer = None

    @staticmethod
    def _cancel_batch_timer(batch: _StreamBatch) -> None:
        timer = batch.timer
        if timer is not None and timer is not asyncio.current_task():
            timer.cancel()
        batch.timer = None

    @staticmethod
    def _raise_background_error(batch: _StreamBatch) -> None:
        if batch.background_error is not None:
            raise batch.background_error

    def _require_batch(self, output_stream_id: str) -> _StreamBatch:
        try:
            return self._stream_batches[output_stream_id]
        except KeyError as error:
            raise ContractViolationError(
                f"output stream {output_stream_id!r} is not open"
            ) from error

    def _discard_batch(self, output_stream_id: str) -> None:
        batch = self._stream_batches.pop(output_stream_id, None)
        if batch is not None:
            self._cancel_batch_timer(batch)

    async def _append(self, draft: AgentOutputEventDraft) -> AgentOutputEvent:
        try:
            event = await self._repository.append_event(draft)
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(draft.run_id, error) from error
        await self._publish_if_visible(event)
        return event

    async def _persist_open(self, spec: OutputStreamSpec) -> OutputStreamSpec:
        try:
            return await self._repository.open_stream(spec)
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(spec.run_id, error) from error

    async def _publish_if_visible(self, event: AgentOutputEvent) -> None:
        if event.visibility is OutputVisibility.PUBLIC:
            try:
                await self._publisher.publish_committed(event)
            except ContractViolationError:
                raise
            except Exception as error:
                raise OutputPersistenceError("committed output could not be published",
                    code="output_publish_failed", details={"causeType": type(error).__name__}) from error

    async def _persistence_error(
        self,
        run_id: str,
        error: Exception,
    ) -> OutputPersistenceError:
        code = "output_persistence_failed"
        if self._recovery is not None:
            try:
                await self._recovery.notify_output_failure(run_id, code)
            except Exception:
                pass
        return OutputPersistenceError(
            "canonical output could not be persisted",
            code=code,
            details={"causeType": type(error).__name__},
        )

    def _require_stream(self, output_stream_id: str) -> OutputStreamSpec:
        stream_id = str(output_stream_id or "").strip()
        spec = self._streams.get(stream_id)
        if spec is None:
            raise ContractViolationError(
                f"output stream {stream_id!r} is not open"
            )
        return spec


def _content_destination(
    spec: OutputStreamSpec,
) -> tuple[OutputChannel, OutputVisibility]:
    if spec.intent is AgentOutputIntent.EXECUTION_PUBLIC:
        return OutputChannel.COMMENTARY, OutputVisibility.PUBLIC
    if spec.intent is AgentOutputIntent.FINAL_PUBLIC:
        return OutputChannel.FINAL, OutputVisibility.PUBLIC
    return OutputChannel.DIAGNOSTIC, OutputVisibility.PRIVATE


def _public_runtime_payload(
    event_type: str,
    payload: Mapping[str, object],
) -> dict[str, object] | None:
    if event_type not in _PUBLIC_RUNTIME_EVENT_TYPES:
        return None
    public = thaw_json_mapping(payload)
    if event_type == "run.todos_updated":
        steps = public.get("steps")
        public = {key: public[key] for key in ("title", "status") if key in public}
        public["steps"] = [
            {key: step[key] for key in ("id", "title", "status", "type") if key in step}
            for step in (steps if isinstance(steps, list) else [])
            if isinstance(step, Mapping) and not step.get("protocol_private")
        ]
    elif event_type == "run.todo_updated":
        step = public.get("step")
        if isinstance(step, Mapping) and bool(step.get("protocol_private")):
            return None
    return public


_PUBLIC_RUNTIME_EVENT_TYPES = frozenset({
    "run.todos_updated",
    "run.todo_updated",
    "approval.requested",
    "approval.resolved",
    "context.budgeted",
    "context.usage_recorded",
    "conversation.compaction.started",
    "conversation.compaction.completed",
    "task.admission_decided",
    "long_task.dispatched",
})

_PRIVATE_RUNTIME_EVENT_TYPES = frozenset({
    "agent.execution_checkpointed",
    "long_task.progress",
})


__all__ = ["AgentOutputProcessor", "OutputRecoveryObserver"]
