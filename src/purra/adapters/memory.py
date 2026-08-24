"""Process-local Agent host adapters for examples, development, and tests.

The adapter preserves the Core's atomic Run/output-journal contract, but it is
not durable across process restarts and must not be used as production storage.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from purra.contracts import (
    AgentDelegation,
    DelegationAggregation,
    DelegationContextMode,
    DelegationStatus,
    ModelFinishReason,
    ExecutionPlan,
    RunCreateParams,
    RunId,
    RunStatus,
    TaskStep,
    ToolCall,
    ToolHandlerResult,
    TraceRecord,
)
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.output import (
    AgentOutputEvent,
    AgentOutputEventDraft,
    AgentOutputIntent,
    OutputChannel,
    OutputEventKind,
    OutputSource,
    OutputStreamSpec,
    OutputVisibility,
    RunLifecycleOutputDraft,
    TERMINAL_STREAM_ABORT_CAUSE,
    TERMINAL_STREAM_ABORT_ERROR_CODE,
)
from purra.output.ports import AgentOutputPublisher, AgentOutputRepository
from purra.ports.run_lifecycle import (
    RunBeginResult,
    RunCommit,
    RunRepository,
    validate_run_commit_lifecycle,
)
from purra.ports import DelegationRepository, ToolIdempotencyGateway
from purra.adapters.durable_memory import InMemoryDurableAdapters


_VALIDATED_RESULT_SCHEMA = "purra.run-validated-result/v1"


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class _RunRecord:
    params: RunCreateParams
    status: RunStatus = RunStatus.RUNNING
    conversation_id: int | None = None
    steps: list[TaskStep] = field(default_factory=list)
    execution_plan: ExecutionPlan | None = None
    final_response: str | None = None
    validated_result: str | None = None
    error: str | None = None
    events: list[AgentEvent] = field(default_factory=list)
    traces: list[TraceRecord] = field(default_factory=list)


@dataclass(slots=True)
class _StreamRecord:
    spec: OutputStreamSpec
    status: str = "open"
    finish_reason: ModelFinishReason | None = None
    error_code: str | None = None


class _MemoryState:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.changed = asyncio.Condition()
        self.runs: dict[str, _RunRecord] = {}
        self.output_events: dict[str, list[AgentOutputEvent]] = {}
        self.events_by_source_key: dict[str, AgentOutputEvent] = {}
        self.streams: dict[str, _StreamRecord] = {}
        self.stream_by_invocation: dict[str, str] = {}
        self.sequences: dict[str, int] = {}
        self.published_sequences: dict[str, int] = {}
        self.delegations: dict[str, AgentDelegation] = {}
        self.tool_receipts: dict[
            tuple[str, str], tuple[ToolCall, ToolHandlerResult]
        ] = {}
        self.tool_inflight: dict[
            tuple[str, str], tuple[ToolCall, asyncio.Task[ToolHandlerResult]]
        ] = {}
        self.run_count = 0
        self.delegation_count = 0

    def next_run_id(self) -> str:
        self.run_count += 1
        return f"memory-run-{self.run_count}"

    def next_delegation_id(self) -> str:
        self.delegation_count += 1
        return f"memory-delegation-{self.delegation_count}"


def _require_run(state: _MemoryState, run_id: str) -> _RunRecord:
    try:
        return state.runs[run_id]
    except KeyError as error:
        raise ContractViolationError(f"run {run_id!r} does not exist") from error


def _apply_commit(record: _RunRecord, commit: RunCommit) -> None:
    if record.status is not RunStatus.RUNNING:
        raise ContractViolationError("terminal run cannot accept another commit")
    if commit.replace_plan is not None:
        record.execution_plan = commit.replace_plan
        record.steps = list(commit.replace_plan.steps)
    for update in commit.step_updates:
        for index, step in enumerate(record.steps):
            if step.id == update.step_id:
                record.steps[index] = replace(
                    step,
                    status=update.status,
                    result_summary=update.result_summary,
                    error=update.error,
                )
                break
        else:
            raise ContractViolationError(
                f"task step {update.step_id!r} does not exist"
            )
    if commit.step_updates and record.execution_plan is not None:
        record.execution_plan = replace(
            record.execution_plan,
            steps=tuple(record.steps),
        )
    if commit.terminal_status is not None:
        record.status = RunStatus(commit.terminal_status)
        record.final_response = commit.final_response
        record.validated_result = commit.validated_result
        record.error = commit.error
    record.events.extend(commit.events)


def _event_matches_draft(
    event: AgentOutputEvent,
    draft: AgentOutputEventDraft,
) -> bool:
    return (
        event.run_id == draft.run_id
        and event.turn_id == draft.turn_id
        and event.output_stream_id == draft.output_stream_id
        and event.invocation_id == draft.invocation_id
        and event.source is draft.source
        and event.kind is draft.kind
        and event.channel is draft.channel
        and event.visibility is draft.visibility
        and event.payload == draft.payload
    )


def _existing_event(
    state: _MemoryState,
    draft: AgentOutputEventDraft,
) -> AgentOutputEvent | None:
    event = state.events_by_source_key.get(draft.source_event_key)
    if event is None:
        return None
    if not _event_matches_draft(event, draft):
        raise ContractViolationError(
            f"source event key {draft.source_event_key!r} "
            "is already bound to a different event"
        )
    return event


def _append_event(
    state: _MemoryState,
    draft: AgentOutputEventDraft,
    *,
    allow_committed_stream: bool = False,
) -> AgentOutputEvent:
    _require_run(state, draft.run_id)
    existing = _existing_event(state, draft)
    if existing is not None:
        return existing
    if draft.output_stream_id is not None:
        try:
            stream = state.streams[draft.output_stream_id]
        except KeyError as error:
            raise ContractViolationError(
                f"output stream {draft.output_stream_id!r} does not exist"
            ) from error
        if stream.status != "open" and not (
            allow_committed_stream and stream.status == "committed"
        ):
            raise ContractViolationError(
                "canonical event requires an open output stream"
            )
        if (
            stream.spec.run_id != draft.run_id
            or stream.spec.turn_id != draft.turn_id
            or stream.spec.invocation_id != draft.invocation_id
        ):
            raise ContractViolationError(
                "canonical event does not match its output stream"
            )
    is_domain_effect = draft.kind is OutputEventKind.DOMAIN_EFFECT
    if is_domain_effect != (draft.source is OutputSource.DOMAIN):
        raise ContractViolationError(
            "domain effect events require domain source and kind"
        )
    sequence = state.sequences.get(draft.run_id, 0) + 1
    state.sequences[draft.run_id] = sequence
    event = AgentOutputEvent(
        event_id=f"output-event-{uuid4().hex}",
        output_stream_id=draft.output_stream_id,
        run_id=draft.run_id,
        turn_id=draft.turn_id,
        invocation_id=draft.invocation_id,
        sequence=sequence,
        source=draft.source,
        kind=draft.kind,
        channel=draft.channel,
        visibility=draft.visibility,
        payload=draft.payload,
        occurred_at=draft.occurred_at,
        emitted_at=_now(),
    )
    state.output_events.setdefault(draft.run_id, []).append(event)
    state.events_by_source_key[draft.source_event_key] = event
    return event


class _InMemoryRunRepository:
    def __init__(self, state: _MemoryState) -> None:
        self._state = state

    async def begin(
        self,
        params: RunCreateParams,
        started_event: AgentEvent,
    ) -> RunBeginResult:
        async with self._state.lock:
            run_id = self._state.next_run_id()
            event = replace(started_event, run_id=run_id)
            self._state.runs[run_id] = _RunRecord(
                params=params,
                events=[event],
            )
            return RunBeginResult(run_id=run_id, event=event)

    async def commit(
        self,
        run_id: RunId,
        commit: RunCommit,
    ) -> tuple[AgentEvent, ...]:
        validate_run_commit_lifecycle(commit)
        async with self._state.lock:
            _apply_commit(_require_run(self._state, run_id), commit)
            return commit.events

    async def bind_conversation(
        self,
        run_id: RunId,
        conversation_id: int,
    ) -> None:
        async with self._state.lock:
            _require_run(self._state, run_id).conversation_id = conversation_id

    async def append_event(self, run_id: RunId, event: AgentEvent) -> None:
        async with self._state.lock:
            _require_run(self._state, run_id).events.append(event)

    async def append_trace(self, run_id: RunId, trace: TraceRecord) -> None:
        async with self._state.lock:
            _require_run(self._state, run_id).traces.append(trace)


class _InMemoryDelegationRepository:
    def __init__(self, state: _MemoryState) -> None:
        self._state = state

    async def create(
        self,
        *,
        run_id: RunId,
        batch_id: str,
        agent_name: str,
        agent_title: str,
        agent_instruction: str,
        objective: str,
        input_payload: Mapping[str, Any] | None = None,
        context_mode: DelegationContextMode = DelegationContextMode.ISOLATED,
        required: bool = True,
        priority: int = 0,
    ) -> AgentDelegation:
        async with self._state.lock:
            _require_run(self._state, run_id)
            timestamp = _now().isoformat()
            row = AgentDelegation(
                id=self._state.next_delegation_id(),
                batch_id=batch_id,
                run_id=run_id,
                agent_name=agent_name,
                agent_title=agent_title,
                agent_instruction=agent_instruction,
                objective=objective,
                input_payload=input_payload or {},
                context_mode=context_mode,
                required=required,
                priority=priority,
                created_at=timestamp,
                updated_at=timestamp,
            )
            self._state.delegations[row.id] = row
            return row

    async def start(
        self,
        delegation_id: str,
        *,
        run_id: RunId,
        batch_id: str,
    ) -> AgentDelegation | None:
        async with self._state.lock:
            row = self._owned(delegation_id, run_id, batch_id)
            if row.status is not DelegationStatus.QUEUED:
                return None
            row = replace(
                row,
                status=DelegationStatus.RUNNING,
                updated_at=_now().isoformat(),
            )
            self._state.delegations[row.id] = row
            return row

    async def complete(
        self,
        delegation_id: str,
        *,
        run_id: RunId,
        batch_id: str,
        result_summary: str,
    ) -> bool:
        return await self._finish(
            delegation_id,
            run_id,
            batch_id,
            status=DelegationStatus.DONE,
            result_summary=result_summary,
        )

    async def fail(
        self,
        delegation_id: str,
        *,
        run_id: RunId,
        batch_id: str,
        error: str,
    ) -> bool:
        return await self._finish(
            delegation_id,
            run_id,
            batch_id,
            status=DelegationStatus.FAILED,
            error=error,
        )

    async def cancel(
        self,
        delegation_id: str,
        *,
        run_id: RunId,
        batch_id: str,
        reason: str,
    ) -> bool:
        return await self._finish(
            delegation_id,
            run_id,
            batch_id,
            status=DelegationStatus.CANCELED,
            error=reason,
        )

    async def list_for_run(self, run_id: RunId) -> tuple[AgentDelegation, ...]:
        async with self._state.lock:
            _require_run(self._state, run_id)
            return tuple(
                row
                for row in self._state.delegations.values()
                if row.run_id == run_id
            )

    async def aggregate_batch(
        self,
        run_id: RunId,
        batch_id: str,
    ) -> DelegationAggregation:
        async with self._state.lock:
            _require_run(self._state, run_id)
            rows = tuple(
                row
                for row in self._state.delegations.values()
                if row.run_id == run_id and row.batch_id == batch_id
            )
            counts = {
                status.value: sum(row.status is status for row in rows)
                for status in DelegationStatus
            }
            failures = tuple(
                row.id
                for row in rows
                if row.required
                and row.status in {
                    DelegationStatus.FAILED,
                    DelegationStatus.CANCELED,
                }
            )
            pending = counts["queued"] + counts["running"]
            return DelegationAggregation(
                state=(
                    "pending" if pending else "blocked" if failures else "ready"
                ),
                counts=counts,
                required_failures=failures,
                results=tuple(
                    {
                        "delegationId": row.id,
                        "agentName": row.agent_name,
                        "agentTitle": row.agent_title,
                        "summary": row.result_summary or "",
                    }
                    for row in rows
                    if row.status is DelegationStatus.DONE
                ),
            )

    async def cancel_batch(self, run_id: RunId, batch_id: str) -> int:
        async with self._state.lock:
            _require_run(self._state, run_id)
            canceled = 0
            for row in tuple(self._state.delegations.values()):
                if row.run_id != run_id or row.batch_id != batch_id:
                    continue
                if row.status not in {
                    DelegationStatus.QUEUED,
                    DelegationStatus.RUNNING,
                }:
                    continue
                self._state.delegations[row.id] = replace(
                    row,
                    status=DelegationStatus.CANCELED,
                    error="delegation_canceled",
                    updated_at=_now().isoformat(),
                )
                canceled += 1
            return canceled

    async def _finish(
        self,
        delegation_id: str,
        run_id: RunId,
        batch_id: str,
        *,
        status: DelegationStatus,
        result_summary: str | None = None,
        error: str | None = None,
    ) -> bool:
        async with self._state.lock:
            row = self._owned(delegation_id, run_id, batch_id)
            allowed = (
                {DelegationStatus.RUNNING}
                if status is DelegationStatus.DONE
                else {DelegationStatus.QUEUED, DelegationStatus.RUNNING}
            )
            if row.status not in allowed:
                return False
            self._state.delegations[row.id] = replace(
                row,
                status=status,
                result_summary=result_summary,
                error=error,
                updated_at=_now().isoformat(),
            )
            return True

    def _owned(
        self,
        delegation_id: str,
        run_id: RunId,
        batch_id: str,
    ) -> AgentDelegation:
        try:
            row = self._state.delegations[delegation_id]
        except KeyError as error:
            raise ContractViolationError(
                f"delegation {delegation_id!r} does not exist"
            ) from error
        if row.run_id != run_id or row.batch_id != batch_id:
            raise ContractViolationError(
                "delegation does not belong to the requested Root Run batch"
            )
        return row


class _InMemoryToolIdempotencyGateway:
    def __init__(self, state: _MemoryState) -> None:
        self._state = state

    async def execute_once(
        self,
        run_id: RunId,
        tool_call: ToolCall,
        operation: Callable[[], Awaitable[ToolHandlerResult]],
    ) -> ToolHandlerResult:
        key = (run_id, tool_call.id)
        async with self._state.lock:
            _require_run(self._state, run_id)
            receipt = self._state.tool_receipts.get(key)
            if receipt is not None:
                self._require_same_call(receipt[0], tool_call)
                return replace(receipt[1], from_cache=True)
            inflight = self._state.tool_inflight.get(key)
            if inflight is not None:
                self._require_same_call(inflight[0], tool_call)
                task = inflight[1]
            else:
                task = asyncio.create_task(
                    self._execute_and_store(key, tool_call, operation)
                )
                self._state.tool_inflight[key] = (tool_call, task)
        return await asyncio.shield(task)

    async def _execute_and_store(
        self,
        key: tuple[str, str],
        tool_call: ToolCall,
        operation: Callable[[], Awaitable[ToolHandlerResult]],
    ) -> ToolHandlerResult:
        try:
            result = await operation()
            if not isinstance(result, ToolHandlerResult):
                raise ContractViolationError(
                    "tool idempotency operation returned an invalid result"
                )
            async with self._state.lock:
                self._state.tool_receipts[key] = (tool_call, result)
            return result
        finally:
            async with self._state.lock:
                current = self._state.tool_inflight.get(key)
                if current is not None and current[1] is asyncio.current_task():
                    self._state.tool_inflight.pop(key, None)

    @staticmethod
    def _require_same_call(expected: ToolCall, actual: ToolCall) -> None:
        if expected != actual:
            raise ContractViolationError(
                "tool call id is already bound to different arguments"
            )


class _InMemoryAgentOutputRepository:
    def __init__(self, state: _MemoryState) -> None:
        self._state = state

    async def begin_run_lifecycle(
        self,
        params: RunCreateParams,
        started_event: AgentEvent,
    ) -> tuple[RunBeginResult, AgentOutputEvent]:
        async with self._state.lock:
            run_id = self._state.next_run_id()
            event = replace(started_event, run_id=run_id)
            self._state.runs[run_id] = _RunRecord(
                params=params,
                events=[event],
            )
            output = _append_event(
                self._state,
                AgentOutputEventDraft(
                    run_id=run_id,
                    turn_id=params.turn_id,
                    output_stream_id=None,
                    invocation_id=None,
                    source_event_key=f"run:{run_id}:running",
                    source=OutputSource.RUNTIME,
                    kind=OutputEventKind.RUN_LIFECYCLE,
                    channel=OutputChannel.LIFECYCLE,
                    visibility=OutputVisibility.PUBLIC,
                    payload=event.payload,
                    occurred_at=_now(),
                ),
            )
            return RunBeginResult(run_id=run_id, event=event), output

    async def open_stream(self, spec: OutputStreamSpec) -> OutputStreamSpec:
        async with self._state.lock:
            run = _require_run(self._state, spec.run_id)
            stream_id = self._state.stream_by_invocation.get(spec.invocation_id)
            existing = self._state.streams.get(spec.output_stream_id)
            if existing is None and stream_id is not None:
                existing = self._state.streams[stream_id]
            if existing is not None:
                if existing.spec != spec:
                    raise ContractViolationError(
                        "output stream id or invocation id is already bound"
                    )
                if (
                    run.status is not RunStatus.RUNNING
                    and existing.status == "open"
                ):
                    raise ContractViolationError(
                        "terminal run cannot retain an open output stream"
                    )
                return spec
            if run.status is not RunStatus.RUNNING:
                raise ContractViolationError(
                    "terminal run cannot open a new output stream"
                )
            self._state.streams[spec.output_stream_id] = _StreamRecord(spec=spec)
            self._state.stream_by_invocation[spec.invocation_id] = (
                spec.output_stream_id
            )
            return spec

    async def append_event(
        self,
        draft: AgentOutputEventDraft,
    ) -> AgentOutputEvent:
        async with self._state.lock:
            return _append_event(self._state, draft)

    async def commit_run_lifecycle(
        self,
        run_id: RunId,
        commit: RunCommit,
        draft: RunLifecycleOutputDraft,
        related_drafts: tuple[AgentOutputEventDraft, ...] = (),
    ) -> tuple[AgentOutputEvent, ...]:
        validate_run_commit_lifecycle(commit)
        expected_status = commit.terminal_status or RunStatus.RUNNING
        if draft.status != expected_status:
            raise ContractViolationError(
                "run lifecycle output status does not match RunCommit"
            )
        payload_status = str(draft.payload.get("status") or "").strip()
        if payload_status and payload_status != draft.status.value:
            raise ContractViolationError(
                "run lifecycle output payload status does not match draft"
            )
        lifecycle_draft = AgentOutputEventDraft(
            run_id=run_id,
            turn_id=draft.turn_id,
            output_stream_id=None,
            invocation_id=None,
            source_event_key=draft.source_event_key,
            source=OutputSource.RUNTIME,
            kind=OutputEventKind.RUN_LIFECYCLE,
            channel=OutputChannel.LIFECYCLE,
            visibility=OutputVisibility.PUBLIC,
            payload=draft.payload,
            occurred_at=draft.occurred_at,
        )
        validated_draft = (
            AgentOutputEventDraft(
                run_id=run_id,
                turn_id=draft.turn_id,
                output_stream_id=None,
                invocation_id=None,
                source_event_key=f"run:{run_id}:validated-result",
                source=OutputSource.RUNTIME,
                kind=OutputEventKind.RUN_VALIDATED_RESULT,
                channel=OutputChannel.DIAGNOSTIC,
                visibility=OutputVisibility.PRIVATE,
                payload={
                    "schemaVersion": _VALIDATED_RESULT_SCHEMA,
                    "content": commit.validated_result,
                },
                occurred_at=draft.occurred_at,
            )
            if commit.validated_result is not None
            else None
        )
        async with self._state.lock:
            record = _require_run(self._state, run_id)
            existing = _existing_event(self._state, lifecycle_draft)
            if existing is not None:
                if record.status is not RunStatus(expected_status):
                    raise ContractViolationError(
                        "canonical lifecycle event does not match Run state"
                    )
                validated_events = tuple(
                    event
                    for event in self._state.output_events.get(run_id, ())
                    if event.kind is OutputEventKind.RUN_VALIDATED_RESULT
                )
                if validated_draft is None and validated_events:
                    raise ContractViolationError(
                        "terminal replay changed the validated result identity"
                    )
                if validated_draft is not None and len(validated_events) != 1:
                    raise ContractViolationError(
                        "terminal replay requires exactly one validated result"
                    )
                replay_drafts = (
                    *related_drafts,
                    *((validated_draft,) if validated_draft is not None else ()),
                )
                replay = tuple(
                    _existing_event(self._state, item) for item in replay_drafts
                )
                if any(event is None for event in replay):
                    raise ContractViolationError(
                        "partial canonical lifecycle commit already exists"
                    )
                terminal_aborts = _terminal_stream_abort_events(
                    self._state,
                    run_id,
                    commit.terminal_status,
                )
                return (
                    *(event for event in replay if event is not None),
                    *terminal_aborts,
                    existing,
                )
            terminal_abort_drafts = tuple(
                _terminal_stream_abort_draft(
                    stream.spec,
                    commit.terminal_status,
                    draft.occurred_at,
                )
                for stream in sorted(
                    self._state.streams.values(),
                    key=lambda item: item.spec.output_stream_id,
                )
                if commit.terminal_status is not None
                and stream.spec.run_id == run_id
                and stream.status == "open"
            )
            atomic_drafts = (
                *related_drafts,
                *((validated_draft,) if validated_draft is not None else ()),
                *terminal_abort_drafts,
                lifecycle_draft,
            )
            if any(
                _existing_event(self._state, item) is not None
                for item in atomic_drafts[:-1]
            ):
                raise ContractViolationError(
                    "partial canonical lifecycle commit already exists"
                )
            _apply_commit(record, commit)
            events = tuple(
                _append_event(self._state, item) for item in atomic_drafts
            )
            for terminal_abort in terminal_abort_drafts:
                stream = self._require_stream(terminal_abort.output_stream_id or "")
                stream.status = "aborted"
                stream.finish_reason = None
                stream.error_code = TERMINAL_STREAM_ABORT_ERROR_CODE
            return events

    async def commit_stream(
        self,
        output_stream_id: str,
        finish_reason: ModelFinishReason,
    ) -> AgentOutputEvent:
        reason = ModelFinishReason(finish_reason)
        async with self._state.lock:
            stream = self._require_stream(output_stream_id)
            if stream.status == "committed":
                event = self._state.events_by_source_key[
                    f"stream:{output_stream_id}:committed"
                ]
                return event
            if stream.status != "open":
                raise ContractViolationError("aborted output stream cannot commit")
            event = _append_event(
                self._state,
                AgentOutputEventDraft(
                    run_id=stream.spec.run_id,
                    turn_id=stream.spec.turn_id,
                    output_stream_id=output_stream_id,
                    invocation_id=stream.spec.invocation_id,
                    source_event_key=f"stream:{output_stream_id}:committed",
                    source=OutputSource.RUNTIME,
                    kind=OutputEventKind.STREAM_COMMITTED,
                    channel=_stream_channel(stream.spec),
                    visibility=_stream_visibility(stream.spec),
                    payload={"finishReason": reason.value},
                    occurred_at=_now(),
                ),
            )
            stream.status = "committed"
            stream.finish_reason = reason
            return event

    async def abort_stream(
        self,
        output_stream_id: str,
        error_code: str,
    ) -> AgentOutputEvent:
        normalized_error = str(error_code or "").strip()
        if not normalized_error:
            raise ValueError("output stream error code is required")
        async with self._state.lock:
            stream = self._require_stream(output_stream_id)
            if stream.status == "aborted":
                return self._state.events_by_source_key[
                    f"stream:{output_stream_id}:aborted"
                ]
            if stream.status != "open":
                raise ContractViolationError("committed output stream cannot abort")
            event = _append_event(
                self._state,
                AgentOutputEventDraft(
                    run_id=stream.spec.run_id,
                    turn_id=stream.spec.turn_id,
                    output_stream_id=output_stream_id,
                    invocation_id=stream.spec.invocation_id,
                    source_event_key=f"stream:{output_stream_id}:aborted",
                    source=OutputSource.RUNTIME,
                    kind=OutputEventKind.STREAM_ABORTED,
                    channel=_stream_channel(stream.spec),
                    visibility=_stream_visibility(stream.spec),
                    payload={"errorCode": normalized_error},
                    occurred_at=_now(),
                ),
            )
            stream.status = "aborted"
            stream.error_code = normalized_error
            return event

    async def publish_stream_content_as_commentary(
        self,
        output_stream_id: str,
    ) -> tuple[AgentOutputEvent, ...]:
        async with self._state.lock:
            stream = self._require_stream(output_stream_id)
            if stream.status != "committed":
                raise ContractViolationError(
                    "only a committed model stream can publish commentary"
                )
            if stream.spec.intent is not AgentOutputIntent.STRUCTURED_PRIVATE:
                raise ContractViolationError(
                    "only private model content can be promoted to commentary"
                )
            events = self._state.output_events.get(stream.spec.run_id, ())
            scoped = [
                event
                for event in events
                if event.output_stream_id == output_stream_id
            ]
            if not any(
                event.kind is OutputEventKind.PROVIDER_TOOL_CALL_DELTA
                for event in scoped
            ):
                raise ContractViolationError(
                    "commentary publication requires a Provider tool call"
                )
            content_events = [
                event
                for event in scoped
                if event.source is OutputSource.PROVIDER
                and event.kind is OutputEventKind.PROVIDER_CONTENT_DELTA
                and event.channel is OutputChannel.DIAGNOSTIC
                and event.visibility is OutputVisibility.PRIVATE
            ]
            content = "".join(
                str(event.payload.get("delta") or "") for event in content_events
            )
            if not content.strip():
                return ()
            commentary = AgentOutputEventDraft.public_text(
                run_id=stream.spec.run_id,
                turn_id=stream.spec.turn_id,
                output_stream_id=output_stream_id,
                invocation_id=stream.spec.invocation_id,
                source_event_key=(
                    f"provider:{stream.spec.invocation_id}:commentary"
                ),
                source=OutputSource.PROVIDER,
                channel=OutputChannel.COMMENTARY,
                delta=content,
                occurred_at=content_events[0].occurred_at,
            )
            committed = AgentOutputEventDraft(
                run_id=stream.spec.run_id,
                turn_id=stream.spec.turn_id,
                output_stream_id=output_stream_id,
                invocation_id=stream.spec.invocation_id,
                source_event_key=f"stream:{output_stream_id}:commentary:committed",
                source=OutputSource.RUNTIME,
                kind=OutputEventKind.STREAM_COMMITTED,
                channel=OutputChannel.COMMENTARY,
                visibility=OutputVisibility.PUBLIC,
                payload={
                    "finishReason": (
                        stream.finish_reason.value if stream.finish_reason else ""
                    )
                },
                occurred_at=_now(),
            )
            return (
                _append_event(
                    self._state,
                    commentary,
                    allow_committed_stream=True,
                ),
                _append_event(
                    self._state,
                    committed,
                    allow_committed_stream=True,
                ),
            )

    async def list_events(
        self,
        run_id: RunId,
        *,
        after_sequence: int,
        limit: int = 200,
    ) -> tuple[AgentOutputEvent, ...]:
        if after_sequence < 0:
            raise ValueError("after sequence must be non-negative")
        if limit <= 0:
            raise ValueError("limit must be positive")
        async with self._state.lock:
            _require_run(self._state, run_id)
            return tuple(
                event
                for event in self._state.output_events.get(run_id, ())
                if event.sequence > after_sequence
            )[:limit]

    async def load_validated_result(self, run_id: RunId) -> str:
        async with self._state.lock:
            record = _require_run(self._state, run_id)
            if record.status is not RunStatus.DONE:
                raise ContractViolationError(
                    "validated result requires a completed Run"
                )
            event = self._state.events_by_source_key.get(
                f"run:{run_id}:validated-result"
            )
            if event is None:
                raise ContractViolationError(
                    "validated result requires exactly one canonical event"
                )
            content: Any = event.payload.get("content")
            if (
                event.payload.get("schemaVersion") != _VALIDATED_RESULT_SCHEMA
                or not isinstance(content, str)
            ):
                raise ContractViolationError(
                    "validated result canonical event has invalid payload"
                )
            return content

    def _require_stream(self, output_stream_id: str) -> _StreamRecord:
        try:
            return self._state.streams[output_stream_id]
        except KeyError as error:
            raise ContractViolationError(
                f"output stream {output_stream_id!r} does not exist"
            ) from error


class _InMemoryAgentOutputPublisher:
    def __init__(self, state: _MemoryState) -> None:
        self._state = state

    async def publish_committed(self, event: AgentOutputEvent) -> None:
        async with self._state.changed:
            if event not in self._state.output_events.get(event.run_id, ()):
                raise ContractViolationError(
                    "only a persisted output event can be published"
                )
            self._state.published_sequences[event.run_id] = max(
                event.sequence,
                self._state.published_sequences.get(event.run_id, 0),
            )
            self._state.changed.notify_all()

    async def wait_for_sequence(
        self,
        run_id: RunId,
        *,
        after_sequence: int,
    ) -> None:
        async with self._state.changed:
            await self._state.changed.wait_for(
                lambda: self._state.published_sequences.get(run_id, 0)
                > after_sequence
            )


def _stream_channel(spec: OutputStreamSpec) -> OutputChannel:
    if spec.intent is AgentOutputIntent.FINAL_PUBLIC:
        return OutputChannel.FINAL
    if spec.intent is AgentOutputIntent.EXECUTION_PUBLIC:
        return OutputChannel.COMMENTARY
    return OutputChannel.DIAGNOSTIC


def _stream_visibility(spec: OutputStreamSpec) -> OutputVisibility:
    if spec.intent in {
        AgentOutputIntent.FINAL_PUBLIC,
        AgentOutputIntent.EXECUTION_PUBLIC,
    }:
        return OutputVisibility.PUBLIC
    return OutputVisibility.PRIVATE


def _terminal_stream_abort_draft(
    spec: OutputStreamSpec,
    terminal_status: RunStatus | None,
    occurred_at: datetime,
) -> AgentOutputEventDraft:
    if terminal_status is None or terminal_status is RunStatus.RUNNING:
        raise ValueError("terminal stream abort requires a terminal Run status")
    return AgentOutputEventDraft(
        run_id=spec.run_id,
        turn_id=spec.turn_id,
        output_stream_id=spec.output_stream_id,
        invocation_id=spec.invocation_id,
        source_event_key=f"stream:{spec.output_stream_id}:aborted",
        source=OutputSource.RUNTIME,
        kind=OutputEventKind.STREAM_ABORTED,
        channel=_stream_channel(spec),
        visibility=_stream_visibility(spec),
        payload={
            "errorCode": TERMINAL_STREAM_ABORT_ERROR_CODE,
            "cause": TERMINAL_STREAM_ABORT_CAUSE,
            "runStatus": terminal_status.value,
        },
        occurred_at=occurred_at,
    )


def _terminal_stream_abort_events(
    state: _MemoryState,
    run_id: str,
    terminal_status: RunStatus | None,
) -> tuple[AgentOutputEvent, ...]:
    if terminal_status is None:
        return ()
    if any(
        stream.spec.run_id == run_id and stream.status == "open"
        for stream in state.streams.values()
    ):
        raise ContractViolationError(
            "terminal Run replay found an open output stream"
        )
    events = tuple(
        event
        for event in state.output_events.get(run_id, ())
        if event.kind is OutputEventKind.STREAM_ABORTED
        and event.payload.get("cause") == TERMINAL_STREAM_ABORT_CAUSE
        and event.payload.get("runStatus") == terminal_status.value
    )
    for event in events:
        stream = state.streams.get(str(event.output_stream_id or ""))
        if (
            stream is None
            or stream.status != "aborted"
            or stream.error_code != TERMINAL_STREAM_ABORT_ERROR_CODE
        ):
            raise ContractViolationError(
                "terminal stream abort event does not match stream state"
            )
    return events


class InMemoryAgentAdapters:
    """Compose process-local implementations of PurrA's host storage ports."""

    def __init__(self) -> None:
        state = _MemoryState()
        durable = InMemoryDurableAdapters()
        self.runs: RunRepository = _InMemoryRunRepository(state)
        self.outputs: AgentOutputRepository = _InMemoryAgentOutputRepository(state)
        self.publisher: AgentOutputPublisher = _InMemoryAgentOutputPublisher(state)
        self.delegations: DelegationRepository = (
            _InMemoryDelegationRepository(state)
        )
        self.idempotency: ToolIdempotencyGateway = (
            _InMemoryToolIdempotencyGateway(state)
        )
        self.artifacts = durable.artifacts
        self.artifact_claims = durable.artifact_claims
        self.artifact_maintenance = durable.artifact_maintenance
        self.long_tasks = durable.long_tasks


__all__ = ["InMemoryAgentAdapters"]
