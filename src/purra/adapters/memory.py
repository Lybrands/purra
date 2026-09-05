"""Process-local Agent host adapters for examples, development, and tests.

The adapter preserves the Core's atomic Run/output-journal contract, but it is
not durable across process restarts and must not be used as production storage.
"""

from __future__ import annotations

import asyncio
from purra.interaction import is_input_checkpoint_update
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import json
from typing import Any
from uuid import uuid4

from purra.contracts import (
    ModelFinishReason,
    ModelTokenUsage,
    ExecutionPlan,
    RunCreateParams,
    RunId,
    RunStatus,
    TaskStep,
    ToolCall,
    ToolHandlerResult,
    TraceRecord,
)
from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.json_values import thaw_json_mapping
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
from purra.ports import ToolIdempotencyGateway
from purra.normalization import required_text
from purra.adapters.durable_memory import InMemoryDurableAdapters
from purra.run_state import RunSnapshot


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
    model_attempt_ids: set[str] = field(default_factory=set)
    model_usage_by_invocation: dict[str, ModelTokenUsage | None] = field(
        default_factory=dict
    )
    provider_output_events: int = 0
    provider_output_bytes: int = 0
    execution_checkpoint: AgentExecutionCheckpoint | None = None
    checkpoint_attempt_count: int = 0


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
        self.root_output_events: dict[str, list[AgentOutputEvent]] = {}
        self.events_by_source_key: dict[str, AgentOutputEvent] = {}
        self.streams: dict[str, _StreamRecord] = {}
        self.stream_by_invocation: dict[str, str] = {}
        self.sequences: dict[str, int] = {}
        self.root_sequences: dict[str, int] = {}
        self.published_sequences: dict[str, int] = {}
        self.tool_receipts: dict[
            tuple[str, str], tuple[ToolCall, ToolHandlerResult]
        ] = {}
        self.tool_inflight: dict[
            tuple[str, str], tuple[ToolCall, asyncio.Task[ToolHandlerResult]]
        ] = {}
        self.run_count = 0
        self.run_tree_authority = None

    def next_run_id(self) -> str:
        self.run_count += 1
        return f"memory-run-{self.run_count}"

def _require_run(state: _MemoryState, run_id: str) -> _RunRecord:
    try:
        return state.runs[run_id]
    except KeyError as error:
        raise ContractViolationError(
            f"run {run_id!r} does not exist",
            code="run_not_found",
        ) from error


def _run_snapshot(run_id: RunId, record: _RunRecord) -> RunSnapshot:
    plan = record.execution_plan
    return RunSnapshot(
        run_id=run_id,
        title=plan.title if plan is not None else "To-dos",
        goal=plan.goal if plan is not None else None,
        status=record.status,
        task_spec=plan.task_spec if plan is not None else None,
        steps=tuple(record.steps),
        work_step_ids=(plan.work_step_ids if plan is not None else ()),
        final_response=record.final_response or "",
        error=record.error,
        execution_checkpoint=record.execution_checkpoint,
        agent_preset_snapshot=record.params.agent_preset_snapshot,
        deadline_at_ms=record.params.deadline_at_ms,
        requested_user_max_generation_tokens=(
            record.params.requested_user_max_generation_tokens
        ),
        result_capacity_target_tokens=(
            record.params.result_capacity_target_tokens
        ),
        selected_context_window_tokens=(
            record.params.selected_context_window_tokens
        ),
    )


def _run_scope_id(run_id: str, run: _RunRecord) -> str:
    return run.params.root_run_id or run_id


def _require_run_write(
    state: _MemoryState,
    run_id: str,
) -> _RunRecord:
    run = _require_run(state, run_id)
    authority = state.run_tree_authority
    if (
        authority is not None
        and run.params.root_run_id != run_id
        and run.params.lease_epoch is not None
    ):
        from purra.agent_tree_lease import current_agent_run_lease

        claim = current_agent_run_lease(run_id)
        authority.require_run_claim_unlocked(
            run_id,
            claim.owner_id if claim is not None else None,
            claim.epoch if claim is not None else None,
        )
    return run


def _scope_runs(state: _MemoryState, run_id: str, run: _RunRecord):
    root_run_id = _run_scope_id(run_id, run)
    return root_run_id, tuple(
        candidate
        for candidate_id, candidate in state.runs.items()
        if _run_scope_id(candidate_id, candidate) == root_run_id
    )


def _scoped_run_params(
    state: _MemoryState,
    run_id: str,
    params: RunCreateParams,
) -> RunCreateParams:
    root_run_id = params.root_run_id or run_id
    agent_id = params.agent_id or run_id
    if root_run_id != run_id:
        root = _require_run(state, root_run_id)
        if _run_scope_id(root_run_id, root) != root_run_id:
            raise ContractViolationError(
                "Run scope root is not a Root Run",
                code="run_scope_conflict",
            )
        if params.parent_run_id is None:
            raise ContractViolationError(
                "Child Run scope requires parent_run_id",
                code="run_scope_conflict",
            )
        parent = _require_run(state, params.parent_run_id)
        if _run_scope_id(params.parent_run_id, parent) != root_run_id:
            raise ContractViolationError(
                "Child Run parent belongs to a different Root scope",
                code="run_scope_conflict",
            )
        if (
            state.run_tree_authority is not None
            and params.lease_epoch is not None
        ):
            state.run_tree_authority.require_run_claim_unlocked(
                run_id,
                params.lease_owner_id,
                params.lease_epoch,
            )
    elif params.parent_run_id is not None:
        raise ContractViolationError(
            "Root Run cannot have parent_run_id",
            code="run_scope_conflict",
        )
    return replace(
        params,
        root_run_id=root_run_id,
        agent_id=agent_id,
    )


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
    if commit.execution_checkpoint is not None:
        checkpoint = commit.execution_checkpoint
        current = record.execution_checkpoint
        if current is not None:
            if checkpoint.next_round < current.next_round:
                raise ContractViolationError(
                    "Agent execution checkpoint cannot move backwards",
                    code="agent_execution_checkpoint_conflict",
                )
            if (
                checkpoint.next_round == current.next_round
                and checkpoint != current
                and not is_input_checkpoint_update(current, checkpoint)
            ):
                raise ContractViolationError(
                    "Agent execution checkpoint content conflicts",
                    code="agent_execution_checkpoint_conflict",
                )
        record.execution_checkpoint = checkpoint
        record.checkpoint_attempt_count = len(record.model_attempt_ids)
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


_PROVIDER_OUTPUT_BUDGET_KINDS = frozenset({
    OutputEventKind.PROVIDER_CONTENT_DELTA,
    OutputEventKind.PROVIDER_REASONING_DELTA,
    OutputEventKind.PROVIDER_TOOL_CALL_DELTA,
    OutputEventKind.PROVIDER_DELTA_BATCH,
    OutputEventKind.PLANNING_PROGRESS,
})


def _provider_output_cost(draft: AgentOutputEventDraft) -> tuple[int, int]:
    if draft.kind not in _PROVIDER_OUTPUT_BUDGET_KINDS:
        return 0, 0
    payload = json.dumps(
        thaw_json_mapping(draft.payload),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return 1, len(payload)


def _batched_provider_entries(event: AgentOutputEvent) -> tuple[Mapping[str, Any], ...]:
    if event.kind is not OutputEventKind.PROVIDER_DELTA_BATCH:
        return ()
    entries = event.payload.get("entries")
    return (
        tuple(entries)
        if isinstance(entries, Sequence)
        and not isinstance(entries, (str, bytes, bytearray))
        else ()
    )


def _require_provider_output_budget(
    state: _MemoryState,
    run_id: str,
    run: _RunRecord,
    event_count: int,
    payload_bytes: int,
) -> None:
    root_run_id, runs = _scope_runs(state, run_id, run)
    root = _require_run(state, root_run_id)
    limits = root.params.runtime_limits
    output_events = sum(item.provider_output_events for item in runs)
    output_bytes = sum(item.provider_output_bytes for item in runs)
    if output_events + event_count > limits.max_provider_output_events:
        raise ContractViolationError(
            "Root Run Provider output event budget was exceeded",
            code="runtime_budget_exceeded",
            details={"budgetKind": "provider_output_events"},
        )
    if output_bytes + payload_bytes > limits.max_provider_output_bytes:
        raise ContractViolationError(
            "Root Run Provider output byte budget was exceeded",
            code="runtime_budget_exceeded",
            details={"budgetKind": "provider_output_bytes"},
        )


def _validate_new_event(
    state: _MemoryState,
    draft: AgentOutputEventDraft,
    *,
    allow_committed_stream: bool,
) -> _RunRecord:
    run = _require_run_write(state, draft.run_id)
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
        if (stream.spec.output_protocol is not None and draft.visibility is OutputVisibility.PUBLIC
                and draft.kind is not OutputEventKind.PLANNING_PROGRESS):
            raise ContractViolationError("planning bytes cannot be published as ordinary text")
    if draft.kind is OutputEventKind.PLANNING_PROGRESS:
        if run.status is not RunStatus.RUNNING:
            raise ContractViolationError("terminal Run cannot accept planning progress")
        _validate_planning_projection(state, draft)
    is_domain_effect = draft.kind is OutputEventKind.DOMAIN_EFFECT
    if is_domain_effect != (draft.source is OutputSource.DOMAIN):
        raise ContractViolationError(
            "domain effect events require domain source and kind"
        )
    return run


def _validate_planning_projection(state: _MemoryState, draft: AgentOutputEventDraft) -> None:
    from purra.planning_stream import PLANNING_STREAM_SCHEMA, PlanningStreamParser
    from purra.errors import InvalidPlannerOutputError
    stream = state.streams.get(draft.output_stream_id)
    if stream is None or stream.spec.output_protocol != PLANNING_STREAM_SCHEMA:
        raise ContractViolationError("planning projection requires a planning stream")
    scope = stream.spec.planning_scope
    payload = draft.payload
    if (scope is None or payload.get("operationId") != scope.operation_id
            or payload.get("revision") != scope.revision
            or payload.get("attempt") != stream.spec.planning_attempt
            or draft.source_event_key != f"planning:{draft.invocation_id}:{payload.get('recordIndex')}"):
        raise ContractViolationError("planning projection scope mismatch")
    events = state.output_events.get(draft.run_id, [])
    stage = [e for e in events if e.payload.get("operationId") == scope.operation_id
             and e.kind in {OutputEventKind.OPERATION_STARTED, OutputEventKind.OPERATION_FINISHED}]
    if (not stage or stage[-1].kind is not OutputEventKind.OPERATION_STARTED
            or stage[-1].payload.get("kind") != "planning"):
        raise ContractViolationError("planning operation is not active")
    # ponytail: bounded replay (1 MiB, 16 projections); index record spans only if
    # profiling shows this small per-invocation verification dominates persistence.
    raw = "".join(str(entry["payload"].get("delta", ""))
        for event in events if event.invocation_id == draft.invocation_id
        for entry in _batched_provider_entries(event)
        if entry.get("kind") == OutputEventKind.PROVIDER_CONTENT_DELTA.value).encode("utf-8")
    start, end = payload.get("sourceStart"), payload.get("sourceEnd")
    if type(start) is not int or type(end) is not int or not 0 <= start < end <= len(raw):
        raise ContractViolationError("planning projection has invalid source span")
    try:
        records = PlanningStreamParser().feed(raw[:end].decode("utf-8"))
        expected = next(record for record in records if record.record_index == payload.get("recordIndex"))
        if any(payload.get(key) != value for key, value in expected.to_mapping().items()):
            raise ValueError("projection differs")
    except (ValueError, StopIteration, InvalidPlannerOutputError) as error:
        raise ContractViolationError("planning projection does not match Provider source") from error


def _append_event(
    state: _MemoryState,
    draft: AgentOutputEventDraft,
    *,
    allow_committed_stream: bool = False,
    budget_prechecked: bool = False,
) -> AgentOutputEvent:
    existing = _existing_event(state, draft)
    if existing is not None:
        return existing
    run = _validate_new_event(
        state,
        draft,
        allow_committed_stream=allow_committed_stream,
    )
    event_count, payload_bytes = _provider_output_cost(draft)
    if not budget_prechecked:
        _require_provider_output_budget(
            state,
            draft.run_id,
            run,
            event_count,
            payload_bytes,
        )
    sequence = state.sequences.get(draft.run_id, 0) + 1
    state.sequences[draft.run_id] = sequence
    root_run_id = _run_scope_id(draft.run_id, run)
    root_sequence = state.root_sequences.get(root_run_id, 0) + 1
    state.root_sequences[root_run_id] = root_sequence
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
        root_run_id=root_run_id,
        agent_id=run.params.agent_id,
        parent_run_id=run.params.parent_run_id,
        root_sequence=root_sequence,
        source_event_key=draft.source_event_key,
    )
    state.output_events.setdefault(draft.run_id, []).append(event)
    state.root_output_events.setdefault(root_run_id, []).append(event)
    state.events_by_source_key[draft.source_event_key] = event
    run.provider_output_events += event_count
    run.provider_output_bytes += payload_bytes
    return event


def _append_events(
    state: _MemoryState,
    drafts: tuple[AgentOutputEventDraft, ...],
) -> tuple[AgentOutputEvent, ...]:
    if not drafts:
        return ()
    run_ids = {draft.run_id for draft in drafts}
    if len(run_ids) != 1:
        raise ContractViolationError("one output batch cannot span Runs")
    source_keys = [draft.source_event_key for draft in drafts]
    if len(source_keys) != len(set(source_keys)):
        raise ContractViolationError("output batch source keys must be unique")

    pending = []
    for draft in drafts:
        if _existing_event(state, draft) is None:
            _validate_new_event(state, draft, allow_committed_stream=False)
            pending.append(draft)
    run = _require_run(state, drafts[0].run_id)
    costs = tuple(_provider_output_cost(draft) for draft in pending)
    _require_provider_output_budget(
        state,
        drafts[0].run_id,
        run,
        sum(count for count, _ in costs),
        sum(size for _, size in costs),
    )
    return tuple(
        _append_event(state, draft, budget_prechecked=True) for draft in drafts
    )


class _InMemoryRunRepository:
    def __init__(self, state: _MemoryState) -> None:
        self._state = state

    async def begin(
        self,
        params: RunCreateParams,
        started_event: AgentEvent,
    ) -> RunBeginResult:
        async with self._state.lock:
            run_id = params.requested_run_id or self._state.next_run_id()
            if run_id in self._state.runs:
                raise ContractViolationError(
                    "requested Run id already exists",
                    code="run_identity_conflict",
                )
            event = replace(started_event, run_id=run_id)
            self._state.runs[run_id] = _RunRecord(
                params=_scoped_run_params(self._state, run_id, params),
                events=[event],
            )
            return RunBeginResult(run_id=run_id, event=event)

    async def get(self, run_id: RunId) -> RunSnapshot:
        async with self._state.lock:
            return _run_snapshot(run_id, _require_run(self._state, run_id))

    async def commit(
        self,
        run_id: RunId,
        commit: RunCommit,
    ) -> tuple[AgentEvent, ...]:
        validate_run_commit_lifecycle(commit)
        if (
            commit.execution_checkpoint is not None
            and commit.execution_checkpoint.run_id != run_id
        ):
            raise ContractViolationError(
                "Agent execution checkpoint belongs to another Run",
                code="agent_execution_checkpoint_conflict",
            )
        async with self._state.lock:
            _apply_commit(_require_run_write(self._state, run_id), commit)
            return commit.events

    async def bind_conversation(
        self,
        run_id: RunId,
        conversation_id: int,
    ) -> None:
        async with self._state.lock:
            _require_run_write(
                self._state,
                run_id,
            ).conversation_id = conversation_id

    async def append_event(self, run_id: RunId, event: AgentEvent) -> None:
        async with self._state.lock:
            _require_run_write(self._state, run_id).events.append(event)

    async def reserve_model_attempt(
        self,
        run_id: RunId,
        invocation_id: str,
    ):
        invocation = required_text(invocation_id, "model invocation id")
        async with self._state.lock:
            run = _require_run_write(self._state, run_id)
            if invocation in run.model_attempt_ids:
                return _run_budget_snapshot(run)
            root_run_id, runs = _scope_runs(self._state, run_id, run)
            limit = _require_run(
                self._state,
                root_run_id,
            ).params.runtime_limits.max_model_invocation_attempts
            if sum(len(item.model_attempt_ids) for item in runs) >= limit:
                raise ContractViolationError(
                    "Root Run model invocation budget was exceeded",
                    code="runtime_budget_exceeded",
                    details={"budgetKind": "model_attempts"},
                )
            run.model_attempt_ids.add(invocation)
            return _run_budget_snapshot(run)

    async def settle_model_attempt(self, run_id, invocation_id, usage):
        invocation = required_text(invocation_id, "model invocation id")
        if usage is not None and not isinstance(usage, ModelTokenUsage):
            raise TypeError("model attempt usage must be ModelTokenUsage")
        async with self._state.lock:
            run = _require_run_write(self._state, run_id)
            if invocation not in run.model_attempt_ids:
                raise ContractViolationError(
                    "model attempt was not reserved",
                    code="model_attempt_not_reserved",
                )
            if invocation in run.model_usage_by_invocation:
                if run.model_usage_by_invocation[invocation] == usage:
                    snapshot = _run_budget_snapshot(run)
                    _require_root_token_budgets(self._state, run_id, run)
                    return snapshot
                raise ContractViolationError("model attempt usage conflicts")
            run.model_usage_by_invocation[invocation] = usage
            snapshot = _run_budget_snapshot(run)
            _require_root_token_budgets(self._state, run_id, run)
            return snapshot

    async def append_trace(self, run_id: RunId, trace: TraceRecord) -> None:
        async with self._state.lock:
            _require_run_write(self._state, run_id).traces.append(trace)


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
            _require_run_write(self._state, run_id)
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
                _require_run_write(self._state, key[0])
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
            run_id = params.requested_run_id or self._state.next_run_id()
            if run_id in self._state.runs:
                raise ContractViolationError(
                    "requested Run id already exists",
                    code="run_identity_conflict",
                )
            event = replace(started_event, run_id=run_id)
            self._state.runs[run_id] = _RunRecord(
                params=_scoped_run_params(self._state, run_id, params),
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
            run = _require_run_write(self._state, spec.run_id)
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
            if spec.planning_scope is not None:
                scope = spec.planning_scope
                stage = [e for e in self._state.output_events.get(spec.run_id, [])
                         if e.payload.get("operationId") == scope.operation_id
                         and e.kind in {OutputEventKind.OPERATION_STARTED, OutputEventKind.OPERATION_FINISHED}]
                if (scope.run_id != spec.run_id or not stage
                        or stage[-1].kind is not OutputEventKind.OPERATION_STARTED
                        or stage[-1].payload.get("kind") != "planning"):
                    raise ContractViolationError("planning operation is not active")
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

    async def append_batch(
        self,
        drafts: tuple[AgentOutputEventDraft, ...],
    ) -> tuple[AgentOutputEvent, ...]:
        async with self._state.lock:
            return _append_events(self._state, tuple(drafts))

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
            record = _require_run_write(self._state, run_id)
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
            operation_aborts = []
            for started in self._state.output_events.get(run_id, ()):
                if started.kind is not OutputEventKind.OPERATION_STARTED:
                    continue
                operation_id = started.payload["operationId"]
                if any(e.kind is OutputEventKind.OPERATION_FINISHED and e.payload.get("operationId") == operation_id
                       for e in self._state.output_events.get(run_id, ())):
                    continue
                operation_aborts.append(AgentOutputEventDraft(
                    run_id=run_id, turn_id=started.turn_id, output_stream_id=None,
                    invocation_id=started.invocation_id,
                    source_event_key=f"operation:{operation_id}:finished", source=OutputSource.RUNTIME,
                    kind=OutputEventKind.OPERATION_FINISHED, channel=OutputChannel.OPERATION,
                    visibility=started.visibility, occurred_at=draft.occurred_at,
                    payload={"operationId": operation_id, "parentOperationId": started.payload.get("parentOperationId"),
                             "status": "canceled" if expected_status is RunStatus.CANCELED else "failed",
                             "errorCode": "run_terminalized", "finishedAt": draft.occurred_at.isoformat(),
                             "durationMs": max(0, round((draft.occurred_at - started.occurred_at).total_seconds() * 1000)),
                             "timingSource": "recovery_wall_clock", "display": started.payload.get("display", {})},
                ))
            atomic_drafts = (
                *operation_aborts,
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
            if stream.spec.output_protocol is not None:
                raise ContractViolationError("planning streams cannot be promoted to commentary")
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
                or any(
                    entry.get("kind")
                    == OutputEventKind.PROVIDER_TOOL_CALL_DELTA.value
                    for entry in _batched_provider_entries(event)
                )
                for event in scoped
            ):
                raise ContractViolationError(
                    "commentary publication requires a Provider tool call"
                )
            content_events = [
                event for event in scoped
                if event.source is OutputSource.PROVIDER
                and event.channel is OutputChannel.DIAGNOSTIC
                and event.visibility is OutputVisibility.PRIVATE
                and (
                    event.kind is OutputEventKind.PROVIDER_CONTENT_DELTA
                    or event.kind is OutputEventKind.PROVIDER_DELTA_BATCH
                )
            ]
            content_parts = []
            for event in content_events:
                if event.kind is OutputEventKind.PROVIDER_CONTENT_DELTA:
                    content_parts.append(str(event.payload.get("delta") or ""))
                else:
                    content_parts.extend(
                        str(entry.get("payload", {}).get("delta") or "")
                        for entry in _batched_provider_entries(event)
                        if entry.get("kind")
                        == OutputEventKind.PROVIDER_CONTENT_DELTA.value
                    )
            content = "".join(content_parts)
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

    async def publish_stream_content_as_final(
        self,
        output_stream_id: str,
    ) -> tuple[AgentOutputEvent, ...]:
        async with self._state.lock:
            stream = self._require_stream(output_stream_id)
            if stream.spec.output_protocol is not None:
                raise ContractViolationError(
                    "planning streams cannot be promoted to final"
                )
            if stream.status != "committed":
                raise ContractViolationError(
                    "only a committed model stream can publish final output"
                )
            if stream.spec.intent is not AgentOutputIntent.STRUCTURED_PRIVATE:
                raise ContractViolationError(
                    "only private model content can be promoted to final"
                )
            scoped = [
                event
                for event in self._state.output_events.get(
                    stream.spec.run_id, ()
                )
                if event.output_stream_id == output_stream_id
            ]
            if any(
                event.kind is OutputEventKind.PROVIDER_TOOL_CALL_DELTA
                or any(
                    entry.get("kind")
                    == OutputEventKind.PROVIDER_TOOL_CALL_DELTA.value
                    for entry in _batched_provider_entries(event)
                )
                for event in scoped
            ):
                raise ContractViolationError(
                    "final publication rejects Provider tool calls"
                )
            content_events = [
                event
                for event in scoped
                if event.source is OutputSource.PROVIDER
                and event.channel is OutputChannel.DIAGNOSTIC
                and event.visibility is OutputVisibility.PRIVATE
                and event.kind
                in {
                    OutputEventKind.PROVIDER_CONTENT_DELTA,
                    OutputEventKind.PROVIDER_DELTA_BATCH,
                }
            ]
            content_parts = []
            for event in content_events:
                if event.kind is OutputEventKind.PROVIDER_CONTENT_DELTA:
                    content_parts.append(str(event.payload.get("delta") or ""))
                else:
                    content_parts.extend(
                        str(entry.get("payload", {}).get("delta") or "")
                        for entry in _batched_provider_entries(event)
                        if entry.get("kind")
                        == OutputEventKind.PROVIDER_CONTENT_DELTA.value
                    )
            content = "".join(content_parts)
            if not content.strip():
                return ()
            final = AgentOutputEventDraft.public_text(
                run_id=stream.spec.run_id,
                turn_id=stream.spec.turn_id,
                output_stream_id=output_stream_id,
                invocation_id=stream.spec.invocation_id,
                source_event_key=f"provider:{stream.spec.invocation_id}:final",
                source=OutputSource.PROVIDER,
                channel=OutputChannel.FINAL,
                delta=content,
                occurred_at=content_events[0].occurred_at,
            )
            committed = AgentOutputEventDraft(
                run_id=stream.spec.run_id,
                turn_id=stream.spec.turn_id,
                output_stream_id=output_stream_id,
                invocation_id=stream.spec.invocation_id,
                source_event_key=f"stream:{output_stream_id}:final:committed",
                source=OutputSource.RUNTIME,
                kind=OutputEventKind.STREAM_COMMITTED,
                channel=OutputChannel.FINAL,
                visibility=OutputVisibility.PUBLIC,
                payload={
                    "finishReason": (
                        stream.finish_reason.value
                        if stream.finish_reason is not None
                        else ""
                    )
                },
                occurred_at=_now(),
            )
            return (
                _append_event(self._state, final, allow_committed_stream=True),
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

    async def list_root_events(
        self,
        root_run_id: RunId,
        *,
        after_root_sequence: int,
        limit: int = 200,
    ) -> tuple[AgentOutputEvent, ...]:
        if after_root_sequence < 0:
            raise ValueError("after root sequence must be non-negative")
        if limit <= 0:
            raise ValueError("limit must be positive")
        async with self._state.lock:
            root = _require_run(self._state, root_run_id)
            if _run_scope_id(root_run_id, root) != root_run_id:
                raise ContractViolationError(
                    "Root journal query requires a Root Run",
                    code="run_scope_conflict",
                )
            return tuple(
                event
                for event in self._state.root_output_events.get(root_run_id, ())
                if (
                    event.root_sequence is not None
                    and event.root_sequence > after_root_sequence
                )
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


def _run_budget_snapshot(run: _RunRecord):
    from purra.ports.run_lifecycle import RunBudgetSnapshot

    usages = tuple(
        usage
        for usage in run.model_usage_by_invocation.values()
        if usage is not None
    )
    return RunBudgetSnapshot(
        model_attempts=len(run.model_attempt_ids),
        unreported_usage_attempts=sum(
            usage is None for usage in run.model_usage_by_invocation.values()
        ),
        unreported_reasoning_attempts=sum(
            usage is not None and usage.reasoning_tokens is None
            for usage in run.model_usage_by_invocation.values()
        ),
        input_tokens=sum(usage.input_tokens for usage in usages),
        generation_tokens=sum(usage.generation_tokens for usage in usages),
        reasoning_tokens=sum(
            usage.reasoning_tokens
            for usage in usages
            if usage.reasoning_tokens is not None
        ),
    )


def _require_root_token_budgets(
    state: _MemoryState,
    run_id: str,
    run: _RunRecord,
) -> None:
    root_run_id, runs = _scope_runs(state, run_id, run)
    limits = _require_run(state, root_run_id).params.runtime_limits
    snapshots = tuple(_run_budget_snapshot(item) for item in runs)
    if sum(item.unreported_usage_attempts for item in snapshots) and any(
        limit is not None
        for limit in (
            limits.max_input_tokens,
            limits.max_run_generation_tokens,
            limits.max_reasoning_tokens,
        )
    ):
        raise ContractViolationError(
            "Root Run Provider usage was not reported",
            code="runtime_budget_exceeded",
            details={"budgetKind": "provider_usage_unreported"},
        )
    if (
        limits.max_reasoning_tokens is not None
        and sum(item.unreported_reasoning_attempts for item in snapshots)
    ):
        raise ContractViolationError(
            "Root Run Provider reasoning usage was not reported",
            code="runtime_budget_exceeded",
            details={"budgetKind": "reasoning_tokens_unreported"},
        )
    for kind, value, limit in (
        (
            "input_tokens",
            sum(item.input_tokens for item in snapshots),
            limits.max_input_tokens,
        ),
        (
            "generation_tokens",
            sum(item.generation_tokens for item in snapshots),
            limits.max_run_generation_tokens,
        ),
        (
            "reasoning_tokens",
            sum(item.reasoning_tokens for item in snapshots),
            limits.max_reasoning_tokens,
        ),
    ):
        if limit is not None and value > limit:
            raise ContractViolationError(
                "Root Run Provider token budget was exceeded",
                code="runtime_budget_exceeded",
                details={"budgetKind": kind},
            )


class InMemoryAgentAdapters:
    """Compose process-local implementations of PurrA's host storage ports."""

    def __init__(self, *, agent_tree_clock_ms=None) -> None:
        state = _MemoryState()
        from purra.agent_tree import InMemoryRunTreeRepository

        self.run_tree = InMemoryRunTreeRepository(
            transaction_lock=state.lock,
            clock_ms=agent_tree_clock_ms,
        )
        state.run_tree_authority = self.run_tree
        durable = InMemoryDurableAdapters()
        self.runs: RunRepository = _InMemoryRunRepository(state)
        self.outputs: AgentOutputRepository = _InMemoryAgentOutputRepository(state)
        self.publisher: AgentOutputPublisher = _InMemoryAgentOutputPublisher(state)
        self.idempotency: ToolIdempotencyGateway = (
            _InMemoryToolIdempotencyGateway(state)
        )
        self.artifacts = durable.artifacts
        self.artifact_claims = durable.artifact_claims
        self.artifact_maintenance = durable.artifact_maintenance
        self.long_tasks = durable.long_tasks


__all__ = ["InMemoryAgentAdapters"]
