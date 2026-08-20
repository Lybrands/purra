"""The only PurrA component allowed to create canonical outward output."""

from __future__ import annotations

from collections.abc import Mapping
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
    AgentOutputIntent,
    DelegationOutputEvent,
    DomainEffectOutput,
    OutputChannel,
    OutputEventKind,
    OutputSource,
    OutputStreamSpec,
    OutputVisibility,
    RunLifecycleOutputDraft,
    RuntimeOutputEvent,
    ToolOutputEvent,
)
from purra.output.ports import (
    AgentOutputPolicy,
    AgentOutputPublisher,
    AgentOutputRepository,
)
from purra.ports.run_lifecycle import RunBeginResult, RunCommit


class OutputRecoveryObserver(Protocol):
    async def notify_output_failure(self, run_id: str, code: str) -> None: ...


class _AllowProviderChunks:
    async def authorize_provider_chunk(self, spec, chunk):
        del spec
        return chunk


class AgentOutputProcessor:
    """Normalize typed producers into persist-before-publish output events."""

    def __init__(
        self,
        repository: AgentOutputRepository,
        publisher: AgentOutputPublisher,
        *,
        policy: AgentOutputPolicy | None = None,
        recovery_observer: OutputRecoveryObserver | None = None,
    ) -> None:
        self._repository = repository
        self._publisher = publisher
        self._policy = policy or _AllowProviderChunks()
        self._recovery = recovery_observer
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
        ):
            raise ContractViolationError(
                "model invocation receipt does not match output stream"
            )
        opened = await self._persist_open(spec)
        self._streams[spec.output_stream_id] = opened
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
                self._chunk_indices.pop(spec.output_stream_id, None)
            raise
        return opened

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

        chunk_index = self._chunk_indices[output_stream_id] + 1
        self._chunk_indices[output_stream_id] = chunk_index
        occurred_at = datetime.now(timezone.utc)
        drafts: list[AgentOutputEventDraft] = []
        if authorized.content_delta:
            channel, visibility = _content_destination(spec)
            drafts.append(self._provider_draft(
                spec,
                chunk_index,
                "content",
                kind=OutputEventKind.PROVIDER_CONTENT_DELTA,
                channel=channel,
                visibility=visibility,
                payload={"delta": authorized.content_delta},
                occurred_at=occurred_at,
            ))
        if authorized.reasoning_delta:
            drafts.append(self._provider_draft(
                spec,
                chunk_index,
                "reasoning",
                kind=OutputEventKind.PROVIDER_REASONING_DELTA,
                channel=OutputChannel.DIAGNOSTIC,
                visibility=OutputVisibility.DIAGNOSTIC,
                payload={"delta": authorized.reasoning_delta},
                occurred_at=occurred_at,
            ))
        if authorized.tool_call_deltas:
            drafts.append(self._provider_draft(
                spec,
                chunk_index,
                "tools",
                kind=OutputEventKind.PROVIDER_TOOL_CALL_DELTA,
                channel=OutputChannel.DIAGNOSTIC,
                visibility=OutputVisibility.PRIVATE,
                payload={
                    "deltas": [
                        {
                            "index": delta.index,
                            "id": delta.id,
                            "type": delta.type,
                            "name": delta.name,
                            "argumentsFragment": delta.arguments_fragment,
                        }
                        for delta in authorized.tool_call_deltas
                    ]
                },
                occurred_at=occurred_at,
            ))
        if authorized.usage is not None:
            drafts.append(self._provider_draft(
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
            ))

        events = []
        for draft in drafts:
            events.append(await self._append(draft))
        return tuple(events)

    async def finish_model_stream(
        self,
        output_stream_id: str,
        finish_reason: ModelFinishReason,
    ) -> AgentOutputEvent:
        spec = self._require_stream(output_stream_id)
        try:
            event = await self._repository.commit_stream(
                output_stream_id,
                finish_reason,
            )
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(spec.run_id, error) from error
        await self._publish_if_visible(event)
        return event

    async def abort_model_stream(
        self,
        output_stream_id: str,
        error_code: str,
    ) -> AgentOutputEvent:
        spec = self._require_stream(output_stream_id)
        try:
            event = await self._repository.abort_stream(
                output_stream_id,
                error_code,
            )
        except ContractViolationError:
            raise
        except Exception as error:
            raise await self._persistence_error(spec.run_id, error) from error
        await self._publish_if_visible(event)
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
            await self._publisher.publish_committed(event)

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
        if isinstance(steps, list):
            public["steps"] = [
                step for step in steps
                if not (
                    isinstance(step, Mapping)
                    and bool(step.get("protocol_private"))
                )
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

_PRIVATE_RUNTIME_EVENT_TYPES = frozenset({"long_task.progress"})


__all__ = ["AgentOutputProcessor", "OutputRecoveryObserver"]
