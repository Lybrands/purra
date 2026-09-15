"""The only PurrA component allowed to create canonical outward output."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from datetime import datetime, timezone
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
    DomainEffectOutput,
    OutputChannel,
    OutputEventKind,
    OutputSource,
    OutputStreamSpec,
    OutputVisibility,
    AGENT_PROGRESS_SCHEMA,
    MAX_AGENT_PROGRESS_CHARS,
    RunLifecycleOutputDraft,
    RuntimeOutputEvent,
    ToolOutputEvent,
)
from purra.output.ports import (
    AgentOutputPolicy,
    AgentOutputPublisher,
    AgentOutputRepository,
)
from purra.output.buffering import OutputBatchLimits, ProviderOutputBuffer
from purra.output.planning_projection import PlanningOutputProjection
from purra.ports.run_lifecycle import RunBeginResult, RunCommit
from purra.cancellation import raise_if_stopped
from purra.planning_stream import (
    PlanningProgress,
    PLANNING_STREAM_SCHEMA,
)


class OutputRecoveryObserver(Protocol):
    async def notify_output_failure(self, run_id: str, code: str) -> None: ...


class _AllowProviderChunks:
    async def authorize_provider_chunk(self, spec, chunk):
        del spec
        return chunk


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
        self._buffer = ProviderOutputBuffer(
            limits=batch_limits or OutputBatchLimits(), sleep=sleep,
            persist=self._persist_batch, publish=self._publish_if_visible,
        )
        self._planning = PlanningOutputProjection()
        self._streams: dict[str, OutputStreamSpec] = {}
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
        self._buffer.open(spec.output_stream_id)
        self._chunk_indices.setdefault(spec.output_stream_id, 0)
        self._planning.open(spec)
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
                self._discard_stream_state(spec.output_stream_id)
            raise
        return opened

    async def accept_planning_progress(self, output_stream_id, progress: PlanningProgress, signal=None):
        """Project a complete Provider record; the repository verifies its bytes."""
        spec = self._require_stream(output_stream_id)
        if spec.output_protocol != PLANNING_STREAM_SCHEMA or spec.planning_scope is None:
            raise ContractViolationError("planning projection requires a bound planning stream")
        batch = self._buffer.require(output_stream_id)
        async with batch.lock:
            self._buffer.raise_background_error(batch)
            raise_if_stopped(signal)
            draft = await self._planning.progress_draft(spec, progress, policy=self._policy)
            if draft is None:
                return None
            await self._buffer.flush_locked(spec, batch)
            raise_if_stopped(signal)
            return await self._append(draft)

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

    async def flush_model_stream(self, output_stream_id: str) -> tuple[AgentOutputEvent, ...]:
        """Persist and publish pending increments without closing the stream."""
        spec = self._require_stream(output_stream_id)
        batch = self._buffer.require(output_stream_id)
        async with batch.lock:
            self._buffer.raise_background_error(batch)
            return tuple(await self._buffer.flush_locked(spec, batch))

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
        batch = self._buffer.require(output_stream_id)
        async with batch.lock:
            self._buffer.raise_background_error(batch)
            chunk_index = self._chunk_indices[output_stream_id] + 1
            self._chunk_indices[output_stream_id] = chunk_index
            occurred_at = datetime.now(timezone.utc)
            additions = self._buffer.pending_deltas(
                spec,
                authorized,
                chunk_index,
                occurred_at,
            )
            buffer_full = self._buffer.append(batch, additions)
            planning_deltas = self._planning.feed(spec, authorized)
            events: list[AgentOutputEvent] = []
            must_flush = (
                bool(planning_deltas) or bool(authorized.progress_delta)
                or
                authorized.usage is not None
                or authorized.finish_reason is not None
                or buffer_full
            )
            if must_flush:
                events.extend(await self._buffer.flush_locked(spec, batch))
            elif additions:
                self._buffer.schedule(spec, batch)
            for draft in await self._planning.delta_drafts(
                spec, authorized, planning_deltas, chunk_index, occurred_at, policy=self._policy,
            ):
                events.append(await self._append(draft))
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
                        "generationTokens": authorized.usage.generation_tokens,
                        "totalTokens": authorized.usage.total_tokens,
                        "cachedInputTokens": authorized.usage.cached_input_tokens,
                        "reasoningTokens": (
                            authorized.usage.reasoning_tokens
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
        batch = self._buffer.require(output_stream_id)
        async with batch.lock:
            self._buffer.raise_background_error(batch)
            await self._buffer.flush_locked(spec, batch)
        try:
            event = await self._repository.commit_stream(
                output_stream_id,
                finish_reason,
            )
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(spec.run_id, error) from error
        self._discard_stream_state(output_stream_id)
        await self._publish_if_visible(event)
        return event

    async def abort_model_stream(
        self,
        output_stream_id: str,
        error_code: str,
    ) -> AgentOutputEvent:
        spec = self._require_stream(output_stream_id)
        batch = self._buffer.require(output_stream_id)
        flush_error: BaseException | None = None
        try:
            async with batch.lock:
                self._buffer.raise_background_error(batch)
                await self._buffer.flush_locked(spec, batch)
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
            self._discard_stream_state(output_stream_id)
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

    def _discard_stream_state(self, output_stream_id: str) -> None:
        self._buffer.discard(output_stream_id)
        self._planning.discard(output_stream_id)
        self._chunk_indices.pop(output_stream_id, None)

    def _require_stream(self, output_stream_id: str) -> OutputStreamSpec:
        stream_id = str(output_stream_id or "").strip()
        spec = self._streams.get(stream_id)
        if spec is None:
            raise ContractViolationError(
                f"output stream {stream_id!r} is not open"
            )
        return spec


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
    "agent.feedback.queued",
    "agent.feedback.state",
})

_PRIVATE_RUNTIME_EVENT_TYPES = frozenset({
    "parent.stage.delivery",
    "agent.execution_checkpointed",
    "long_task.progress",
})


__all__ = ["AgentOutputProcessor", "OutputRecoveryObserver"]
