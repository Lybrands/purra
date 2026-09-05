from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import replace

import pytest

from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    DomainContext,
    ExecutionRecipe,
    ExecutionRecipeStep,
    MessageRole,
    ModelRequest,
    RunCreateParams,
    RunStatus,
    StepExecutor,
    StepStatus,
    StepType,
    ExecutionPlan,
    TaskSpec,
    TaskStep,
    ToolRiskLevel,
)
from purra.engine.durable_execution import (
    _validate_durable_plan_revision,
    complete_admitted_task,
)
from purra.errors import ContractViolationError
from purra.events import AgentEvent, CoreEventType
from purra.ports import RunBeginResult
from purra.run_controller import AgentRunController
from purra.task_admission import (
    ExecutionMode,
    LongTaskDispatchReceipt,
    LongTaskExecutionResult,
    LongTaskExecutionStatus,
    LongTaskExecutionUpdate,
    TaskAdmissionDecision,
)


class _Repository:
    def __init__(self) -> None:
        self.events: list[AgentEvent] = []
        self.commits = []
        self.steps: list[TaskStep] = []
        self.status = RunStatus.RUNNING
        self.error: str | None = None

    async def begin(self, params, started_event):
        del params
        event = AgentEvent(
            type=started_event.type,
            run_id="run-root",
            payload=started_event.payload,
        )
        self.events.append(event)
        return RunBeginResult(run_id="run-root", event=event)

    async def commit(self, run_id, commit):
        assert run_id == "run-root"
        self.commits.append(commit)
        if commit.replace_plan is not None:
            self.steps = list(commit.replace_plan.steps)
        for update in commit.step_updates:
            self.steps = [
                replace(
                    step,
                    status=update.status,
                    result_summary=(
                        update.result_summary
                        if update.result_summary is not None
                        else step.result_summary
                    ),
                    error=(
                        update.error if update.error is not None else step.error
                    ),
                )
                if step.id == update.step_id
                else step
                for step in self.steps
            ]
        if commit.terminal_status is not None:
            self.status = commit.terminal_status
            self.error = commit.error
        self.events.extend(commit.events)
        return commit.events

    async def append_event(self, run_id, event):
        assert run_id == "run-root"
        self.events.append(event)

    async def append_trace(self, run_id, trace):
        del run_id, trace

    async def bind_conversation(self, run_id, conversation_id):
        del run_id, conversation_id


class _Sink:
    def __init__(self) -> None:
        self.buffer: list[AgentEvent] = []

    async def emit(self, event: AgentEvent) -> None:
        self.buffer.append(event)

    def drain(self) -> tuple[AgentEvent, ...]:
        events = tuple(self.buffer)
        self.buffer.clear()
        return events


def _plan(*, revised: bool = False) -> ExecutionPlan:
    completed_summary = (
        "Durable execution completed this Planner step."
        if revised
        else None
    )
    return ExecutionPlan(
        title="Durable plan",
        steps=(
            TaskStep(
                id="gather",
                title="Gather evidence",
                type=StepType.ANALYZE,
                executor=StepExecutor.MODEL,
                status=StepStatus.DONE if revised else StepStatus.PENDING,
                result_summary=completed_summary,
            ),
            TaskStep(
                id="deliver",
                title=(
                    "Deliver the revised result"
                    if revised
                    else "Deliver the result"
                ),
                type=StepType.WRITE,
                executor=StepExecutor.MODEL,
                depends_on=("gather",),
                description="Use the latest checkpoint" if revised else None,
            ),
        ),
    )


def _history_plan() -> ExecutionPlan:
    return ExecutionPlan(
        title="History plan",
        steps=(
            TaskStep(
                id="source",
                title="Read source",
                type=StepType.ANALYZE,
                executor=StepExecutor.MODEL,
            ),
            TaskStep(
                id="gather",
                title="Gather evidence",
                type=StepType.ANALYZE,
                executor=StepExecutor.MODEL,
                depends_on=("source",),
            ),
            TaskStep(
                id="deliver",
                title="Deliver result",
                type=StepType.WRITE,
                executor=StepExecutor.MODEL,
                depends_on=("gather",),
            ),
        ),
    )


def _request() -> AgentRunRequest:
    return AgentRunRequest(
        messages=(AgentMessage(role=MessageRole.USER, content="Do the work"),),
        model=ModelRequest(provider="fixture", model="scripted"),
        domain_context=DomainContext(namespace="fixture.durable"),
        session_id="session-1",
        mode="agent",
        context_window=32_000,
        tools_enabled=True,
    )


def _admission() -> TaskAdmissionDecision:
    return TaskAdmissionDecision(
        mode=ExecutionMode.DURABLE,
        reason_code="fixture_durable",
        covered_step_ids=("gather", "deliver"),
        execution_recipe=ExecutionRecipe(
            kind="fixture.durable",
            steps=(
                ExecutionRecipeStep(id="gather", kind="fixture"),
                ExecutionRecipeStep(
                    id="deliver",
                    kind="fixture",
                    depends_on=("gather",),
                ),
            ),
        ),
    )


async def _started() -> tuple[AgentRunController, _Repository, _Sink]:
    repository = _Repository()
    sink = _Sink()
    controller = AgentRunController(repository=repository, event_sink=sink)
    await controller.start(
        RunCreateParams(session_id="session-1", prompt="Do the work", mode="agent"),
        _plan(),
    )
    sink.drain()
    return controller, repository, sink


@pytest.mark.asyncio
async def test_todo_projection_excludes_private_execution_authority():
    repository = _Repository()
    sink = _Sink()
    controller = AgentRunController(repository=repository, event_sink=sink)
    await controller.start(
        RunCreateParams(session_id="session-1", prompt="work", mode="agent"),
        ExecutionPlan(
            title="Work",
            steps=(
                TaskStep(
                    id="private-prepare",
                    title="Prepare",
                    type=StepType.READ,
                    executor=StepExecutor.TOOL,
                    suggested_tools=("prepare",),
                    protocol_private=True,
                ),
                TaskStep(
                    id="deliver",
                    title="Deliver",
                    type=StepType.WRITE,
                    executor=StepExecutor.TOOL,
                    suggested_tools=("deliver",),
                    depends_on=("private-prepare",),
                ),
            ),
        ),
    )

    todos = next(
        event
        for event in repository.events
        if event.type == CoreEventType.RUN_TODOS_UPDATED
    )

    assert [step.id for step in repository.steps] == [
        "private-prepare",
        "deliver",
    ]
    assert todos.payload["steps"] == [{
        "id": "deliver",
        "title": "Deliver",
        "type": "write",
        "executor": "tool",
        "status": "pending",
        "risk_level": None,
        "depends_on": [],
        "description": None,
        "result_summary": None,
        "error": None,
    }]


def _progress_event(*, status: str) -> AgentEvent:
    return AgentEvent(
        type=CoreEventType.LONG_TASK_PROGRESS,
        payload={
            "taskId": "task-1",
            "units": [{"plannerStepId": "gather", "status": status}],
        },
    )


@pytest.mark.asyncio
async def test_durable_checkpoint_atomically_revises_root_plan_before_evidence():
    controller, repository, sink = await _started()

    class _Dispatcher:
        async def dispatch(self, *args, **kwargs):
            del args, kwargs
            return LongTaskDispatchReceipt(
                task_id="task-1",
                message="Dispatched",
                admission=_admission(),
            )

        async def execute(self, task_id, *, observer, **kwargs):
            del kwargs
            await observer(LongTaskExecutionUpdate(
                event=_progress_event(status="completed"),
            ))
            await observer(LongTaskExecutionUpdate(
                event=AgentEvent(
                    type="long_task.checkpoint",
                    payload={"taskId": task_id, "checkpoint": "episode-1"},
                ),
                plan_revision=_plan(revised=True),
            ))
            return LongTaskExecutionResult(
                task_id=task_id,
                status=LongTaskExecutionStatus.COMPLETED,
                final_response="Finished",
            )

    yielded = [
        event
        async for event in complete_admitted_task(
            controller=controller,
            request=_request(),
            plan=_plan(),
            admission=_admission(),
            dispatcher=_Dispatcher(),
            sink=sink,
            signal=None,
        )
    ]

    assert controller.status is RunStatus.DONE
    assert repository.status is RunStatus.DONE
    assert [step.title for step in repository.steps] == [
        "Gather evidence",
        "Deliver the revised result",
    ]
    assert repository.steps[1].depends_on == ("gather",)
    checkpoint_index = next(
        index
        for index, event in enumerate(repository.events)
        if event.type == "long_task.checkpoint"
    )
    revision_index = max(
        index
        for index, event in enumerate(repository.events[:checkpoint_index])
        if event.type == CoreEventType.RUN_TODOS_UPDATED
    )
    assert revision_index < checkpoint_index
    revision_commit = next(
        commit
        for commit in repository.commits
        if commit.replace_plan is not None
        and commit.events
        and commit.events[0].type == CoreEventType.RUN_TODOS_UPDATED
        and commit.replace_plan.steps[1].title == "Deliver the revised result"
    )
    assert revision_commit.events[0].payload["steps"][1]["title"] == (
        "Deliver the revised result"
    )
    assert any(event.type == "long_task.checkpoint" for event in yielded)


@pytest.mark.asyncio
async def test_failed_long_task_fails_root_with_the_original_error_code():
    controller, repository, sink = await _started()

    class _Dispatcher:
        async def dispatch(self, *args, **kwargs):
            del args, kwargs
            return LongTaskDispatchReceipt(
                task_id="task-output-truncated",
                message="Dispatched",
                admission=_admission(),
            )

        async def execute(self, task_id, *, observer, **kwargs):
            del observer, kwargs
            return LongTaskExecutionResult(
                task_id=task_id,
                status=LongTaskExecutionStatus.FAILED,
                error="model_output_truncated",
            )

    yielded = [
        event
        async for event in complete_admitted_task(
            controller=controller,
            request=_request(),
            plan=_plan(),
            admission=_admission(),
            dispatcher=_Dispatcher(),
            sink=sink,
            signal=None,
        )
    ]

    assert controller.status is RunStatus.FAILED
    assert repository.status is RunStatus.FAILED
    assert repository.error == "model_output_truncated"
    assert yielded[-1].type == CoreEventType.RUN_FAILED
    assert yielded[-1].payload["error"] == "model_output_truncated"


@pytest.mark.asyncio
async def test_continuation_executes_existing_receipt_without_redispatch():
    controller, repository, sink = await _started()

    class _Dispatcher:
        async def dispatch(self, *args, **kwargs):
            del args, kwargs
            raise AssertionError("continuation must not dispatch a second task")

        async def execute(self, task_id, *, run_id, observer, signal=None):
            del signal
            assert task_id == "task-existing"
            assert run_id == "run-root"
            await observer(LongTaskExecutionUpdate(
                event=AgentEvent(
                    type=CoreEventType.LONG_TASK_PROGRESS,
                    payload={"taskId": task_id, "units": []},
                ),
            ))
            return LongTaskExecutionResult(
                task_id=task_id,
                status=LongTaskExecutionStatus.COMPLETED,
                final_response="Continued",
            )

    yielded = [
        event
        async for event in complete_admitted_task(
            controller=controller,
            request=_request(),
            plan=_plan(),
            admission=_admission(),
            dispatcher=_Dispatcher(),
            sink=sink,
            signal=None,
            existing_receipt=LongTaskDispatchReceipt(
                task_id="task-existing",
                message="Resumed existing durable task",
                admission=_admission(),
            ),
        )
    ]

    assert repository.status is RunStatus.DONE
    assert all(event.type != CoreEventType.LONG_TASK_DISPATCHED for event in yielded)


@pytest.mark.asyncio
async def test_invalid_durable_revision_fails_root_and_cancels_old_recipe():
    controller, repository, sink = await _started()
    canceled = asyncio.Event()
    continued = False
    invalid = _plan(revised=True)
    invalid = replace(
        invalid,
        steps=(
            replace(invalid.steps[0], title="Rewrite completed history"),
            invalid.steps[1],
        ),
    )

    class _Dispatcher:
        async def dispatch(self, *args, **kwargs):
            del args, kwargs
            return LongTaskDispatchReceipt(
                task_id="task-1",
                message="Dispatched",
                admission=_admission(),
            )

        async def execute(self, task_id, *, observer, **kwargs):
            nonlocal continued
            del task_id, kwargs
            try:
                await observer(LongTaskExecutionUpdate(
                    event=_progress_event(status="completed"),
                ))
                await observer(LongTaskExecutionUpdate(
                    event=AgentEvent(
                        type="long_task.checkpoint",
                        payload={"checkpoint": "invalid"},
                    ),
                    plan_revision=invalid,
                ))
                continued = True
                await asyncio.Event().wait()
            finally:
                canceled.set()

    yielded = [
        event
        async for event in complete_admitted_task(
            controller=controller,
            request=_request(),
            plan=_plan(),
            admission=_admission(),
            dispatcher=_Dispatcher(),
            sink=sink,
            signal=None,
        )
    ]

    assert canceled.is_set()
    assert not continued
    assert controller.status is RunStatus.FAILED
    assert repository.status is RunStatus.FAILED
    assert repository.error == "durable_plan_revision_contract_violation"
    assert all(event.type != "long_task.checkpoint" for event in repository.events)
    assert yielded[-1].type == CoreEventType.RUN_FAILED


@pytest.mark.asyncio
async def test_revision_contract_rejection_settles_observer_with_same_error():
    controller, repository, sink = await _started()
    observer_rejected = asyncio.Event()
    cleaned = asyncio.Event()
    invalid = replace(
        _plan(revised=True),
        steps=(
            replace(_plan(revised=True).steps[0], title="Rewrite history"),
            _plan(revised=True).steps[1],
        ),
    )

    class Dispatcher:
        async def dispatch(self, *args, **kwargs):
            del args, kwargs
            return LongTaskDispatchReceipt(
                task_id="task-ack",
                message="ok",
                admission=_admission(),
            )

        async def execute(self, task_id, *, observer, **kwargs):
            del task_id, kwargs
            try:
                with pytest.raises(ContractViolationError):
                    await observer(LongTaskExecutionUpdate(
                        event=_progress_event(status="completed"),
                        plan_revision=invalid,
                    ))
                observer_rejected.set()
                raise AssertionError("the rejected dispatcher must be canceled")
            finally:
                cleaned.set()

    async def collect():
        return [
            event
            async for event in complete_admitted_task(
                controller=controller,
                request=_request(),
                plan=_plan(),
                admission=_admission(),
                dispatcher=Dispatcher(),
                sink=sink,
                signal=None,
            )
        ]

    yielded = await asyncio.wait_for(collect(), 1)

    assert observer_rejected.is_set()
    assert cleaned.is_set()
    assert repository.status is RunStatus.FAILED
    assert yielded[-1].type == CoreEventType.RUN_FAILED


@pytest.mark.asyncio
async def test_caller_cancel_settles_inflight_revision_observer_and_dispatcher():
    controller, repository, sink = await _started()
    commit_started = asyncio.Event()
    cleaned = asyncio.Event()
    original_commit = repository.commit

    async def blocked_commit(run_id, commit):
        if commit.replace_plan is not None and len(repository.commits) > 0:
            commit_started.set()
            await asyncio.Event().wait()
        return await original_commit(run_id, commit)

    repository.commit = blocked_commit  # type: ignore[method-assign]

    class Dispatcher:
        async def dispatch(self, *args, **kwargs):
            del args, kwargs
            return LongTaskDispatchReceipt(
                task_id="task-cancel",
                message="ok",
                admission=_admission(),
            )

        async def execute(self, task_id, *, observer, **kwargs):
            del task_id, kwargs
            try:
                await observer(LongTaskExecutionUpdate(
                    event=_progress_event(status="completed"),
                ))
                await observer(LongTaskExecutionUpdate(
                    event=AgentEvent(
                        type="long_task.checkpoint",
                        payload={"checkpoint": "cancel"},
                    ),
                    plan_revision=_plan(revised=True),
                ))
            finally:
                cleaned.set()

    async def collect():
        return [
            event
            async for event in complete_admitted_task(
                controller=controller,
                request=_request(),
                plan=_plan(),
                admission=_admission(),
                dispatcher=Dispatcher(),
                sink=sink,
                signal=None,
            )
        ]

    execution = asyncio.create_task(collect())
    await asyncio.wait_for(commit_started.wait(), 1)
    execution.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(execution, 1)

    assert cleaned.is_set()


@pytest.mark.asyncio
async def test_durable_update_cannot_forge_todo_replacement_event():
    controller, repository, sink = await _started()
    canceled = asyncio.Event()

    class _Dispatcher:
        async def dispatch(self, *args, **kwargs):
            del args, kwargs
            return LongTaskDispatchReceipt(
                task_id="task-1",
                message="Dispatched",
                admission=_admission(),
            )

        async def execute(self, task_id, *, observer, **kwargs):
            del task_id, kwargs
            try:
                await observer(LongTaskExecutionUpdate(
                    event=AgentEvent(
                        type=CoreEventType.RUN_TODOS_UPDATED,
                        payload={"steps": []},
                    ),
                    plan_revision=_plan(revised=True),
                ))
                await asyncio.Event().wait()
            finally:
                canceled.set()

    yielded = [
        event
        async for event in complete_admitted_task(
            controller=controller,
            request=_request(),
            plan=_plan(),
            admission=_admission(),
            dispatcher=_Dispatcher(),
            sink=sink,
            signal=None,
        )
    ]

    assert canceled.is_set()
    assert controller.status is RunStatus.FAILED
    assert sum(
        event.type == CoreEventType.RUN_TODOS_UPDATED
        for event in repository.events
    ) == 1
    assert yielded[-1].payload["error"] == (
        "durable_plan_revision_contract_violation"
    )


def test_task_plan_still_rejects_unknown_and_cyclic_revision_dependencies():
    with pytest.raises(ValueError, match="reference earlier steps"):
        ExecutionPlan(
            title="Unknown dependency",
            steps=(replace(_plan().steps[0], depends_on=("missing",)),),
        )

    with pytest.raises(ValueError, match="reference earlier steps"):
        ExecutionPlan(
            title="Cyclic dependency",
            steps=(
                replace(_plan().steps[0], depends_on=("deliver",)),
                _plan().steps[1],
            ),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    (
        lambda step: replace(step, title="Rewrite history"),
        lambda step: replace(step, type=StepType.REVIEW),
        lambda step: replace(
            step,
            executor=StepExecutor.TOOL,
            suggested_tools=("forged_tool",),
        ),
        lambda step: replace(step, depends_on=()),
        lambda step: replace(step, status=StepStatus.PENDING),
        lambda step: replace(step, description="Changed detail"),
        lambda step: replace(step, result_summary="Changed receipt"),
        lambda step: replace(step, error="changed_error"),
    ),
    ids=(
        "title",
        "type",
        "executor",
        "dependencies",
        "done-status",
        "description",
        "result-summary",
        "error",
    ),
)
async def test_durable_revision_rejects_changes_to_completed_step_contract(
    mutate,
):
    repository = _Repository()
    sink = _Sink()
    controller = AgentRunController(repository=repository, event_sink=sink)
    plan = _history_plan()
    await controller.start(
        RunCreateParams(session_id="session-1", prompt="work", mode="agent"),
        plan,
    )
    await controller.sync_durable_execution({
        "source": StepStatus.DONE,
        "gather": StepStatus.DONE,
    })
    assert controller.snapshot is not None
    completed_source = next(
        step
        for step in controller.snapshot.steps
        if step.id == "source"
    )
    completed_gather = next(
        step
        for step in controller.snapshot.steps
        if step.id == "gather"
    )
    revised = replace(
        plan,
        steps=(
            completed_source,
            mutate(completed_gather),
            plan.steps[2],
        ),
    )

    with pytest.raises(
        ContractViolationError,
        match="cannot change completed steps",
    ):
        _validate_durable_plan_revision(
            controller,
            revised,
            ("source", "gather", "deliver"),
        )


@pytest.mark.asyncio
async def test_durable_revision_rejects_added_or_removed_step_ids():
    controller, _repository, _sink = await _started()
    revision = replace(
        _plan(),
        steps=(_plan().steps[0],),
        work_step_ids=("gather",),
    )

    with pytest.raises(
        ContractViolationError,
        match="preserve admitted step ids",
    ):
        _validate_durable_plan_revision(
            controller,
            revision,
            ("gather", "deliver"),
        )

    added = replace(
        _plan(),
        steps=(
            *_plan().steps,
            TaskStep(
                id="extra",
                title="Extra work",
                type=StepType.REVIEW,
                executor=StepExecutor.MODEL,
                depends_on=("deliver",),
            ),
        ),
    )
    with pytest.raises(
        ContractViolationError,
        match="preserve admitted step ids",
    ):
        _validate_durable_plan_revision(
            controller,
            added,
            ("gather", "deliver"),
        )


@pytest.mark.asyncio
async def test_durable_revision_cannot_hide_an_admitted_work_step():
    controller, _repository, _sink = await _started()
    revision = replace(_plan(), work_step_ids=("gather",))

    with pytest.raises(
        ContractViolationError,
        match="WorkStep lineage",
    ):
        _validate_durable_plan_revision(
            controller,
            revision,
            ("gather", "deliver"),
            original_plan=_plan(),
        )


@pytest.mark.asyncio
async def test_non_persisted_durable_revision_fails_before_checkpoint_emission():
    controller, repository, sink = await _started()
    canceled = asyncio.Event()

    class _Dispatcher:
        async def dispatch(self, *args, **kwargs):
            del args, kwargs
            return LongTaskDispatchReceipt(
                task_id="task-1",
                message="Dispatched",
                admission=_admission(),
            )

        async def execute(self, task_id, *, observer, **kwargs):
            del task_id, kwargs
            try:
                await observer(LongTaskExecutionUpdate(
                    event=AgentEvent(
                        type="long_task.checkpoint",
                        payload={"checkpoint": "ephemeral"},
                    ),
                    persist=False,
                    plan_revision=_plan(revised=True),
                ))
            finally:
                canceled.set()

    yielded = [
        event
        async for event in complete_admitted_task(
            controller=controller,
            request=_request(),
            plan=_plan(),
            admission=_admission(),
            dispatcher=_Dispatcher(),
            sink=sink,
            signal=None,
        )
    ]

    assert canceled.is_set()
    assert controller.status is RunStatus.FAILED
    assert all(event.type != "long_task.checkpoint" for event in repository.events)
    assert yielded[-1].payload["error"] == (
        "durable_plan_revision_contract_violation"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    (
        lambda step: replace(step, type=StepType.REVIEW),
        lambda step: replace(
            step,
            executor=StepExecutor.TOOL,
            suggested_tools=("forged_tool",),
        ),
        lambda step: replace(step, status=StepStatus.DONE),
        lambda step: replace(step, risk_level=ToolRiskLevel.WRITE),
        lambda step: replace(step, suggested_tools=("forged_tool",)),
        lambda step: replace(step, result_summary="forged result"),
        lambda step: replace(step, error="forged_error"),
    ),
    ids=(
        "type",
        "executor",
        "status",
        "risk-level",
        "suggested-tools",
        "result-summary",
        "error",
    ),
)
async def test_durable_revision_rejects_non_whitelisted_future_step_changes(
    mutate,
):
    controller, _repository, _sink = await _started()
    assert controller.snapshot is not None
    current = controller.snapshot.steps
    revised = ExecutionPlan(
        title=controller.snapshot.title,
        goal=controller.snapshot.goal,
        steps=(current[0], mutate(current[1])),
    )

    with pytest.raises(
        ContractViolationError,
        match="only change title, description, and dependencies",
    ):
        _validate_durable_plan_revision(
            controller,
            revised,
            ("gather", "deliver"),
        )


@pytest.mark.asyncio
async def test_durable_revision_allows_future_copy_and_dependency_reordering():
    repository = _Repository()
    sink = _Sink()
    controller = AgentRunController(repository=repository, event_sink=sink)
    plan = ExecutionPlan(
        title="Reorder future",
        steps=(
            TaskStep(
                id="source-a",
                title="Source A",
                type=StepType.ANALYZE,
                executor=StepExecutor.MODEL,
            ),
            TaskStep(
                id="source-b",
                title="Source B",
                type=StepType.ANALYZE,
                executor=StepExecutor.MODEL,
            ),
            TaskStep(
                id="deliver",
                title="Deliver",
                type=StepType.WRITE,
                executor=StepExecutor.MODEL,
                depends_on=("source-a", "source-b"),
            ),
        ),
    )
    await controller.start(
        RunCreateParams(session_id="session-1", prompt="work", mode="agent"),
        plan,
    )
    assert controller.snapshot is not None
    current = controller.snapshot.steps
    revision = replace(
        plan,
        steps=(
            current[1],
            current[0],
            replace(
                current[2],
                title="Deliver revised",
                description="Use checkpoint evidence",
                depends_on=("source-b", "source-a"),
            ),
        ),
    )

    _validate_durable_plan_revision(
        controller,
        revision,
        ("source-a", "source-b", "deliver"),
    )
    revised_snapshot = await controller.revise_plan(revision)
    assert [step.id for step in revised_snapshot.steps] == [
        "source-b",
        "source-a",
        "deliver",
    ]
    assert {
        step.id: step.status
        for step in revised_snapshot.steps
    } == {
        "source-a": StepStatus.RUNNING,
        "source-b": StepStatus.PENDING,
        "deliver": StepStatus.PENDING,
    }


@pytest.mark.asyncio
async def test_durable_revision_persists_generic_checkpoint_identity_on_todo_event():
    controller, repository, sink = await _started()

    class Dispatcher:
        async def dispatch(self, *_args, **_kwargs):
            return LongTaskDispatchReceipt(
                task_id="task-checkpoint-identity",
                message="started",
                admission=_admission(),
                metadata={"stepIds": ["gather", "deliver"]},
            )

        async def execute(self, task_id, *, observer, **kwargs):
            del kwargs
            await observer(LongTaskExecutionUpdate(
                event=_progress_event(status="completed"),
            ))
            await observer(LongTaskExecutionUpdate(
                event=AgentEvent(
                    type="long_task.checkpoint",
                    payload={"taskId": task_id},
                ),
                plan_revision=_plan(revised=True),
                plan_revision_metadata={
                    "identity": "episode:4",
                    "digest": "sha256:" + "a" * 64,
                },
            ))
            return LongTaskExecutionResult(
                task_id=task_id,
                status=LongTaskExecutionStatus.COMPLETED,
                final_response="Finished",
            )

    async for _event in complete_admitted_task(
        controller=controller,
        request=_request(),
        plan=_plan(),
        admission=_admission(),
        dispatcher=Dispatcher(),
        sink=sink,
        signal=None,
    ):
        pass

    revision_event = next(
        event for event in repository.events
        if event.type == CoreEventType.RUN_TODOS_UPDATED
        and event.payload.get("planRevision")
    )
    assert revision_event.payload["planRevision"] == {
        "identity": "episode:4",
        "digest": "sha256:" + "a" * 64,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    (
        {"title": "Forged plan title"},
        {"goal": "Forged plan goal"},
        {"task_spec": TaskSpec(goal="Forged task contract")},
    ),
    ids=("title", "goal", "task-spec"),
)
async def test_durable_revision_freezes_top_level_plan_contract(changes):
    controller, _repository, _sink = await _started()
    assert controller.snapshot is not None
    revision = replace(_plan(), **changes)

    with pytest.raises(
        ContractViolationError,
        match="cannot change plan title, goal, or task spec",
    ):
        _validate_durable_plan_revision(
            controller,
            revision,
            ("gather", "deliver"),
            original_plan=_plan(),
        )


class _CloseAwareDispatcher:
    def __init__(self, *, cleanup_error: bool = False) -> None:
        self.started = asyncio.Event()
        self.second_observe_started = asyncio.Event()
        self.cleaned = asyncio.Event()
        self.task: asyncio.Task | None = None
        self.cleanup_error = cleanup_error

    async def dispatch(self, *args, **kwargs):
        del args, kwargs
        return LongTaskDispatchReceipt(
            task_id="task-1",
            message="Dispatched",
            admission=_admission(),
        )

    async def execute(self, task_id, *, observer, **kwargs):
        del task_id, kwargs
        self.task = asyncio.current_task()
        self.started.set()
        try:
            await observer(LongTaskExecutionUpdate(
                event=_progress_event(status="running"),
            ))
            self.second_observe_started.set()
            await observer(LongTaskExecutionUpdate(
                event=_progress_event(status="running"),
            ))
            await asyncio.Event().wait()
        finally:
            self.cleaned.set()
            if self.cleanup_error:
                raise RuntimeError("dispatcher_cleanup_failed")


async def _close_leaked_dispatcher(dispatcher: _CloseAwareDispatcher) -> None:
    task = dispatcher.task
    if task is not None and not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError, RuntimeError):
            await task


@pytest.mark.asyncio
async def test_consumer_aclose_cancels_dispatcher_waiting_on_observer_ack():
    controller, _repository, sink = await _started()
    dispatcher = _CloseAwareDispatcher(cleanup_error=True)
    stream = complete_admitted_task(
        controller=controller,
        request=_request(),
        plan=_plan(),
        admission=_admission(),
        dispatcher=dispatcher,
        sink=sink,
        signal=None,
    )
    try:
        assert (await anext(stream)).type == CoreEventType.LONG_TASK_DISPATCHED
        assert (await anext(stream)).type == CoreEventType.LONG_TASK_PROGRESS
        await asyncio.wait_for(dispatcher.second_observe_started.wait(), 1)

        await asyncio.wait_for(stream.aclose(), 1)
        await asyncio.wait_for(dispatcher.cleaned.wait(), 1)

        assert dispatcher.task is not None and dispatcher.task.done()
        assert controller.status is RunStatus.RUNNING
    finally:
        await _close_leaked_dispatcher(dispatcher)


@pytest.mark.asyncio
async def test_contract_violation_cleanup_error_does_not_replace_root_failure():
    controller, repository, sink = await _started()
    dispatcher = _CloseAwareDispatcher(cleanup_error=True)

    async def invalid_execute(task_id, *, observer, **kwargs):
        del task_id, kwargs
        dispatcher.task = asyncio.current_task()
        try:
            await observer(LongTaskExecutionUpdate(
                event=AgentEvent(
                    type=CoreEventType.RUN_TODOS_UPDATED,
                    payload={"steps": []},
                ),
            ))
        finally:
            dispatcher.cleaned.set()
            raise RuntimeError("dispatcher_cleanup_failed")

    dispatcher.execute = invalid_execute  # type: ignore[method-assign]

    yielded = [
        event
        async for event in complete_admitted_task(
            controller=controller,
            request=_request(),
            plan=_plan(),
            admission=_admission(),
            dispatcher=dispatcher,
            sink=sink,
            signal=None,
        )
    ]

    assert dispatcher.cleaned.is_set()
    assert repository.status is RunStatus.FAILED
    assert repository.error == "durable_plan_revision_contract_violation"
    assert yielded[-1].type == CoreEventType.RUN_FAILED


@pytest.mark.asyncio
async def test_caller_cancellation_survives_dispatcher_cleanup_error():
    controller, _repository, sink = await _started()
    dispatcher = _CloseAwareDispatcher(cleanup_error=True)

    async def waiting_execute(task_id, *, observer, **kwargs):
        del task_id, observer, kwargs
        dispatcher.task = asyncio.current_task()
        dispatcher.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            dispatcher.cleaned.set()
            raise RuntimeError("dispatcher_cleanup_failed")

    dispatcher.execute = waiting_execute  # type: ignore[method-assign]
    stream = complete_admitted_task(
        controller=controller,
        request=_request(),
        plan=_plan(),
        admission=_admission(),
        dispatcher=dispatcher,
        sink=sink,
        signal=None,
    )
    assert (await anext(stream)).type == CoreEventType.LONG_TASK_DISPATCHED
    pending = asyncio.create_task(anext(stream))
    await asyncio.wait_for(dispatcher.started.wait(), 1)
    pending.cancel()

    with pytest.raises(asyncio.CancelledError):
        await pending

    assert dispatcher.cleaned.is_set()
    assert controller.status is RunStatus.RUNNING
