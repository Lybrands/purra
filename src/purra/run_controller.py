"""Port-backed persistence and event orchestration for Core Run state."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import replace
from typing import Awaitable, TypeVar

from purra.contracts import (
    ExecutionTransition,
    RunCreateParams,
    RunStatus,
    StepStatus,
    ExecutionPlan,
    TaskStep,
    ToolBatchOutcome,
    TraceRecord,
)
from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.json_values import freeze_json_mapping, thaw_json_mapping
from purra.events import AgentEvent, CoreEventType
from purra.errors import RunCancellationConflictError
from purra.ports import EventSink, RunCommit, RunRepository
from purra.run_state import RunSnapshot, RunStateMachine, RunTransition


_ReceiptT = TypeVar("_ReceiptT")


class AgentRunController:
    """Own one live run while depending only on Core ports."""

    def __init__(
        self,
        *,
        repository: RunRepository,
        event_sink: EventSink,
    ):
        self._repository = repository
        self._event_sink = event_sink
        self._snapshot: RunSnapshot | None = None
        self._mutation_lock = asyncio.Lock()

    @property
    def run_id(self) -> str | None:
        return self._snapshot.run_id if self._snapshot is not None else None

    @property
    def status(self) -> RunStatus | None:
        return self._snapshot.status if self._snapshot is not None else None

    @property
    def error(self) -> str | None:
        return self._snapshot.error if self._snapshot is not None else None

    @property
    def snapshot(self) -> RunSnapshot | None:
        return self._snapshot

    async def start(
        self,
        params: RunCreateParams,
        plan: ExecutionPlan | None = None,
    ) -> RunSnapshot:
        async with self._mutation_lock:
            await self._begin_unlocked(params)
            if plan is not None:
                return await self._install_plan_unlocked(plan)
            return self._require_started()

    async def begin(self, params: RunCreateParams) -> RunSnapshot:
        """Atomically create the Run and its first persisted outbox event."""

        async with self._mutation_lock:
            return await self._begin_unlocked(params)

    async def attach(self, snapshot: RunSnapshot) -> RunSnapshot:
        """Attach a replacement executor to an existing non-terminal Run."""

        if not isinstance(snapshot, RunSnapshot):
            raise TypeError("run controller attachment requires RunSnapshot")
        if snapshot.terminal:
            raise RuntimeError("cannot attach a terminal run")
        async with self._mutation_lock:
            if self._snapshot is not None:
                raise RuntimeError("run controller has already started")
            self._snapshot = snapshot
            return snapshot

    async def save_execution_checkpoint(
        self,
        checkpoint: AgentExecutionCheckpoint,
    ) -> None:
        """Atomically persist a private model-ready cursor and journal fact."""

        state = self._require_started()
        if checkpoint.run_id != state.run_id:
            raise ContractViolationError(
                "Agent execution checkpoint belongs to another Run",
                code="agent_execution_checkpoint_conflict",
            )
        event = AgentEvent(
            type=CoreEventType.AGENT_EXECUTION_CHECKPOINTED,
            run_id=state.run_id,
            payload={
                "schemaVersion": checkpoint.schema_version,
                "phase": checkpoint.phase,
                "executionProfile": checkpoint.execution_profile,
                "nextRound": checkpoint.next_round,
            },
        )
        persisted, canceled = await _await_repository_receipt(
            self._repository.commit(
                state.run_id,
                RunCommit(
                    execution_checkpoint=checkpoint,
                    events=(event,),
                ),
            )
        )
        self._snapshot = replace(
            state,
            execution_checkpoint=checkpoint,
        )
        _raise_if_canceled(canceled)
        await self._publish(persisted)

    async def _begin_unlocked(self, params: RunCreateParams) -> RunSnapshot:
        if self._snapshot is not None:
            raise RuntimeError("run controller has already started")
        event_template = AgentEvent(type=CoreEventType.RUN_STARTED, payload={
            "status": RunStatus.RUNNING.value,
            "title": "To-dos",
            "goal": None,
            **(
                {
                    "agentPreset": thaw_json_mapping(
                        params.agent_preset_snapshot
                    )
                }
                if params.agent_preset_snapshot
                else {}
            ),
        })
        begun, canceled = await _await_repository_receipt(
            self._repository.begin(params, event_template)
        )
        if (
            params.requested_run_id is not None
            and begun.run_id != params.requested_run_id
        ):
            raise ContractViolationError(
                "Run repository did not honor the requested Run id",
                code="run_identity_conflict",
            )
        snapshot = replace(
            RunStateMachine.initialize(begun.run_id),
            agent_preset_snapshot=params.agent_preset_snapshot,
        )
        self._snapshot = snapshot
        _raise_if_canceled(canceled)
        await self._publish((begun.event,))
        return snapshot

    async def bind_conversation(self, conversation_id: int) -> None:
        state = self._require_started()
        await self._repository.bind_conversation(state.run_id, conversation_id)

    async def install_plan(self, plan: ExecutionPlan) -> RunSnapshot:
        """Install the validated plan after the run has become observable.

        Starting before model planning lets a host receive ``run.started`` and
        persist a canceled/failed terminal state even when the planner never
        returns. A plan is write-once so its tool grants cannot be replaced
        while a run is executing.
        """

        async with self._mutation_lock:
            return await self._install_plan_unlocked(plan)

    async def revise_plan(
        self,
        plan: ExecutionPlan,
        *,
        revision_metadata: Mapping[str, object] | None = None,
    ) -> RunSnapshot:
        """Atomically replace tentative steps after observing runtime evidence."""

        async with self._mutation_lock:
            return await self._revise_plan_unlocked(
                plan,
                revision_metadata=revision_metadata,
            )

    async def _revise_plan_unlocked(
        self,
        plan: ExecutionPlan,
        *,
        revision_metadata: Mapping[str, object] | None,
    ) -> RunSnapshot:
        state = self._require_started()
        revised = RunStateMachine.revise_plan(state, plan)
        metadata = freeze_json_mapping(revision_metadata or {})
        event = AgentEvent(
            type=CoreEventType.RUN_TODOS_UPDATED,
            run_id=state.run_id,
            payload={
                **_todos_payload(revised),
                **({"planRevision": metadata} if metadata else {}),
            },
        )
        persisted, canceled = await _await_repository_receipt(
            self._repository.commit(
                state.run_id,
                RunCommit(replace_plan=_require_plan(revised), events=(event,)),
            )
        )
        if self._snapshot is not state:
            raise RuntimeError("stale run plan revision")
        self._snapshot = revised
        _raise_if_canceled(canceled)
        await self._publish(persisted)
        return revised

    async def _install_plan_unlocked(self, plan: ExecutionPlan) -> RunSnapshot:
        state = self._require_started()
        if state.terminal:
            raise RuntimeError("cannot install a plan on a terminal run")
        if state.steps:
            raise RuntimeError("run plan has already been installed")
        planned = RunStateMachine.initialize(state.run_id, plan)
        event = AgentEvent(
            type=CoreEventType.RUN_TODOS_UPDATED,
            run_id=state.run_id,
            payload=_todos_payload(planned),
        )
        persisted, canceled = await _await_repository_receipt(
            self._repository.commit(
                state.run_id,
                RunCommit(replace_plan=_require_plan(planned), events=(event,)),
            )
        )
        self._snapshot = planned
        _raise_if_canceled(canceled)
        await self._publish(persisted)
        return planned

    def current_execution_transition(self) -> ExecutionTransition | None:
        state = self._snapshot
        if state is None:
            raise RuntimeError("execution transition requires a live run")
        return RunStateMachine.execution_transition(state)

    def allowed_tool_names(self) -> frozenset[str]:
        state = self._snapshot
        if state is None:
            return frozenset()
        return RunStateMachine.allowed_tool_names(state)

    async def record_trace(self, trace: TraceRecord) -> None:
        state = self._require_started()
        await self._repository.append_trace(state.run_id, trace)

    async def record_event(
        self,
        event_type: CoreEventType,
        payload: dict[str, object],
    ) -> None:
        state = self._require_started()
        event = AgentEvent(
            type=event_type,
            run_id=state.run_id,
            payload=payload,
        )
        await self._repository.append_event(state.run_id, event)
        await self._event_sink.emit(event)

    async def on_model_delta(self) -> None:
        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(RunStateMachine.on_model_delta(state))

    async def on_tool_calls_started(self, tool_names: tuple[str, ...]) -> None:
        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(
                RunStateMachine.on_tool_calls_started(state, tool_names)
            )

    async def on_tool_round_completed(
        self,
        outcome: ToolBatchOutcome = ToolBatchOutcome.COMPLETED,
    ) -> None:
        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(
                RunStateMachine.on_tool_round_completed(state, outcome)
            )

    async def on_tool_round_failed(
        self,
        error: str = "tool_execution_failed",
    ) -> None:
        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(RunStateMachine.on_tool_round_failed(state, error))

    async def complete(self, final_response: str = "") -> None:
        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(RunStateMachine.complete(state, final_response))

    async def complete_validated_result(
        self,
        validated_result: str,
        *,
        final_response: str = "",
    ) -> None:
        """Commit a private validated result without publishing it as text."""

        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(
                RunStateMachine.complete(state, final_response),
                validated_result=str(validated_result),
            )

    async def complete_durable_execution(
        self,
        final_response: str = "",
        *,
        covered_step_ids: tuple[str, ...],
    ) -> None:
        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(
                RunStateMachine.complete_durable_execution(
                    state,
                    final_response,
                    covered_step_ids=covered_step_ids,
                )
            )

    async def sync_durable_execution(
        self,
        statuses: Mapping[str, StepStatus],
    ) -> None:
        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(
                RunStateMachine.sync_durable_execution(
                    state,
                    dict(statuses),
                )
            )

    async def fail(self, error: str) -> None:
        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(RunStateMachine.fail(state, error))

    async def cancel(self, reason: str = "request_canceled") -> None:
        async with self._mutation_lock:
            state = self._require_started()
            await self._apply(RunStateMachine.cancel(state, reason))

    async def current_steps(self) -> tuple[TaskStep, ...]:
        state = self._require_started()
        return state.steps

    async def _apply(
        self,
        transition: RunTransition,
        *,
        validated_result: str | None = None,
    ) -> None:
        if not transition.changed:
            return
        before = self._require_started()
        if before is not transition.before:
            raise RuntimeError("stale run transition")

        events = _transition_events(transition)
        terminal_status = (
            transition.after.status
            if transition.after.status is not before.status
            else None
        )
        try:
            persisted, canceled = await _await_repository_receipt(
                self._repository.commit(
                    before.run_id,
                    RunCommit(
                        step_updates=transition.step_updates,
                        terminal_status=terminal_status,  # type: ignore[arg-type]
                        final_response=(
                            transition.after.final_response
                            if terminal_status is RunStatus.DONE
                            else None
                        ),
                        validated_result=validated_result,
                        error=(
                            transition.after.error
                            if terminal_status is RunStatus.FAILED
                            else None
                        ),
                        events=events,
                    ),
                )
            )
        except RunCancellationConflictError:
            await self._apply(
                RunStateMachine.cancel(before, "request_canceled")
            )
            return
        if self._snapshot is not before:
            raise RuntimeError("stale run transition")
        self._snapshot = transition.after
        _raise_if_canceled(canceled)
        await self._publish(persisted)

    async def _publish(self, events: tuple[AgentEvent, ...]) -> None:
        for event in events:
            await self._event_sink.emit(event)

    def _require_started(self) -> RunSnapshot:
        if self._snapshot is None:
            raise RuntimeError("run controller has not started")
        return self._snapshot


async def _await_repository_receipt(
    operation: Awaitable[_ReceiptT],
) -> tuple[_ReceiptT, bool]:
    """Settle a cancellation-linearizable repository operation.

    A canceled child means the port guarantees no durable write. If the child
    returns a receipt after ``cancel()``, the durable write won the race and
    the controller must apply that receipt before propagating caller cancel.
    """

    task = asyncio.create_task(operation)
    caller_canceled = False
    while True:
        try:
            return await asyncio.shield(task), caller_canceled
        except asyncio.CancelledError:
            # A canceled repository child is authoritative: the port promises
            # that no durable write exists, so propagate its cancellation.
            if task.cancelled():
                raise

            # Cancellation of the controller is forwarded to the repository
            # exactly once. The repository may then either roll back and
            # cancel, or finish a commit and return its durable receipt.
            # Continue through shield so repeated caller cancel() requests
            # cannot interrupt that settlement or cancel the child again.
            if not caller_canceled:
                caller_canceled = True
                task.cancel()

            # The receipt may have won the same event-loop tick as caller
            # cancellation. Read it synchronously rather than awaiting again.
            if task.done():
                return task.result(), True


def _raise_if_canceled(canceled: bool) -> None:
    if canceled:
        raise asyncio.CancelledError


def _require_plan(snapshot: RunSnapshot) -> ExecutionPlan:
    plan = snapshot.execution_plan
    if plan is None:
        raise RuntimeError("planned run snapshot has no ExecutionPlan")
    return plan


def _todos_payload(state: RunSnapshot) -> dict[str, object]:
    return {
        "title": state.title,
        "goal": state.goal,
        "status": state.status.value,
        **(
            {"taskSpec": state.task_spec.to_mapping()}
            if state.task_spec is not None
            else {}
        ),
        "steps": [
            _step_payload(step, state.work_step_ids)
            for step in state.steps
            if step.id in state.work_step_ids
        ],
    }


def _transition_events(transition: RunTransition) -> tuple[AgentEvent, ...]:
    state = transition.after
    events = [
        AgentEvent(
            type=CoreEventType.RUN_TODO_UPDATED,
            run_id=state.run_id,
            payload={
                "step_id": step.id,
                "step": _step_payload(step, state.work_step_ids),
                "status": state.status.value,
            },
        )
        for update in transition.step_updates
        if update.step_id in state.work_step_ids
        for step in state.steps
        if step.id == update.step_id
    ]
    if state.status is transition.before.status:
        return tuple(events)

    event_type = {
        RunStatus.DONE: CoreEventType.RUN_COMPLETED,
        RunStatus.BLOCKED: CoreEventType.RUN_BLOCKED,
        RunStatus.FAILED: CoreEventType.RUN_FAILED,
        RunStatus.CANCELED: CoreEventType.RUN_CANCELED,
    }[state.status]
    payload: dict[str, object] = {"status": state.status.value}
    if state.status is RunStatus.DONE:
        payload["final_response"] = state.final_response
    if state.status is RunStatus.FAILED:
        payload["error"] = state.error
    if state.status is RunStatus.CANCELED:
        payload["reason"] = state.error
    events.append(AgentEvent(
        type=event_type,
        run_id=state.run_id,
        payload=payload,
    ))
    return tuple(events)


def _step_payload(
    step: TaskStep,
    work_step_ids: tuple[str, ...],
) -> dict[str, object]:
    return {
        "id": step.id,
        "title": step.title,
        "type": step.type.value,
        "executor": step.executor.value,
        "status": step.status.value,
        "risk_level": step.risk_level.value if step.risk_level else None,
        "depends_on": [
            dependency
            for dependency in step.depends_on
            if dependency in work_step_ids
        ],
        "description": step.description,
        "result_summary": step.result_summary,
        "error": step.error,
    }
