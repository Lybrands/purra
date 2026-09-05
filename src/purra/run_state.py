"""Pure, immutable Agent Run and todo state transitions."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Mapping

from purra.agent_execution_checkpoint import AgentExecutionCheckpoint

from purra.contracts import (
    ExecutionTransition,
    RunId,
    RunStatus,
    StepExecutor,
    StepStatus,
    StepType,
    ExecutionPlan,
    TaskSpec,
    TaskStep,
    TaskStepUpdate,
    ToolBatchOutcome,
)
from purra.normalization import (
    optional_positive_int,
    optional_text as _optional_text,
    required_text,
)
from purra.json_values import freeze_json_mapping
from purra.errors import ContractViolationError


_TERMINAL_STATUSES = frozenset({
    RunStatus.DONE,
    RunStatus.BLOCKED,
    RunStatus.FAILED,
    RunStatus.CANCELED,
})
_FINISHED_STEP_STATUSES = frozenset({
    StepStatus.DONE,
    StepStatus.BLOCKED,
    StepStatus.FAILED,
})


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    """Detached state of one run at a single logical instant."""

    run_id: RunId
    title: str
    goal: str | None
    status: RunStatus
    task_spec: TaskSpec | None = None
    steps: tuple[TaskStep, ...] = ()
    work_step_ids: tuple[str, ...] | None = None
    final_response: str = ""
    error: str | None = None
    execution_checkpoint: AgentExecutionCheckpoint | None = None
    agent_preset_snapshot: Mapping[str, Any] = field(default_factory=dict)
    deadline_at_ms: int | None = None
    requested_user_max_generation_tokens: int | None = None
    result_capacity_target_tokens: int | None = None
    selected_context_window_tokens: int | None = None

    def __post_init__(self) -> None:
        run_id = required_text(self.run_id, "run snapshot run id")
        title = required_text(self.title, "run snapshot title")
        steps = tuple(self.steps)
        status = RunStatus(self.status)
        if len({step.id for step in steps}) != len(steps):
            raise ValueError("run snapshot step ids must be unique")
        running_count = sum(step.status is StepStatus.RUNNING for step in steps)
        if running_count > 1:
            raise ValueError("a Run may execute only one plan transition at a time")
        status_by_id = {step.id: step.status for step in steps}
        for step in steps:
            if step.status is not StepStatus.RUNNING or not step.depends_on:
                continue
            if any(
                status_by_id.get(dependency) is not StepStatus.DONE
                for dependency in step.depends_on
            ):
                raise ValueError(
                    "a running step requires all Planner dependencies to be done"
                )
        if status in _TERMINAL_STATUSES and running_count:
            raise ValueError("terminal run snapshot cannot have a running step")
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "goal", _optional_text(self.goal))
        object.__setattr__(self, "status", status)
        if self.task_spec is not None and not isinstance(self.task_spec, TaskSpec):
            raise TypeError("run snapshot task_spec must be a TaskSpec")
        object.__setattr__(self, "steps", steps)
        work_step_ids = (
            tuple(step.id for step in steps if not step.protocol_private)
            if self.work_step_ids is None
            else tuple(self.work_step_ids)
        )
        if set(work_step_ids) - {step.id for step in steps}:
            raise ValueError("run snapshot work_step_ids must name known steps")
        object.__setattr__(self, "work_step_ids", work_step_ids)
        object.__setattr__(self, "final_response", str(self.final_response or ""))
        object.__setattr__(self, "error", _optional_text(self.error))
        object.__setattr__(
            self,
            "agent_preset_snapshot",
            freeze_json_mapping(self.agent_preset_snapshot or {}),
        )
        for name in (
            "requested_user_max_generation_tokens",
            "result_capacity_target_tokens",
            "selected_context_window_tokens",
        ):
            object.__setattr__(
                self,
                name,
                optional_positive_int(
                    getattr(self, name),
                    f"run snapshot {name}",
                ),
            )
        if self.execution_checkpoint is not None:
            if not isinstance(
                self.execution_checkpoint,
                AgentExecutionCheckpoint,
            ):
                raise TypeError("Run execution checkpoint is invalid")
            if self.execution_checkpoint.run_id != run_id:
                raise ValueError("Run execution checkpoint belongs to another Run")

    @property
    def terminal(self) -> bool:
        return self.status in _TERMINAL_STATUSES

    @property
    def execution_plan(self) -> ExecutionPlan | None:
        """Return the complete persisted plan authority for this snapshot."""

        if not self.steps:
            return None
        return ExecutionPlan(
            title=self.title,
            goal=self.goal,
            task_spec=self.task_spec,
            steps=self.steps,
            work_step_ids=self.work_step_ids,
        )


@dataclass(frozen=True, slots=True)
class RunTransition:
    """Pure reducer output; callers decide how and when to persist it."""

    before: RunSnapshot
    after: RunSnapshot
    step_updates: tuple[TaskStepUpdate, ...] = ()

    def __post_init__(self) -> None:
        updates = tuple(self.step_updates)
        if self.before.run_id != self.after.run_id:
            raise ValueError("run transition cannot change run id")
        object.__setattr__(self, "step_updates", updates)

    @property
    def changed(self) -> bool:
        return self.before != self.after


class RunStateMachine:
    """Stateless reducer for sequential work and Planner-authored Agent DAGs."""

    @staticmethod
    def initialize(
        run_id: RunId,
        plan: ExecutionPlan | None = None,
        *,
        default_title: str = "To-dos",
    ) -> RunSnapshot:
        if plan is None:
            return RunSnapshot(
                run_id=run_id,
                title=default_title,
                goal=None,
                status=RunStatus.RUNNING,
            )
        if not isinstance(plan, ExecutionPlan):
            raise TypeError("run initialization requires an ExecutionPlan")

        steps = tuple(_initial_step(step) for step in plan.steps)
        for step in steps:
            _validate_step_execution_contract(step)
        first_runnable = next(
            (
                index
                for index, step in enumerate(steps)
                if step.status is not StepStatus.DONE
                and step.type is not StepType.CONFIRM
            ),
            -1,
        )
        if first_runnable >= 0:
            steps = _replace_at(
                steps,
                first_runnable,
                replace(steps[first_runnable], status=StepStatus.RUNNING),
            )
        return RunSnapshot(
            run_id=run_id,
            title=plan.title,
            goal=plan.goal,
            status=RunStatus.RUNNING,
            task_spec=plan.task_spec,
            steps=steps,
            work_step_ids=plan.work_step_ids,
        )

    @staticmethod
    def revise_plan(state: RunSnapshot, plan: ExecutionPlan) -> RunSnapshot:
        """Replace tentative work while preserving immutable execution history."""

        if not isinstance(plan, ExecutionPlan):
            raise TypeError("run revision requires an ExecutionPlan")
        if state.terminal:
            raise ContractViolationError("cannot revise a terminal run plan")
        history = tuple(
            step for step in state.steps if step.status in _FINISHED_STEP_STATUSES
        )
        history_ids = frozenset(step.id for step in history)
        future = tuple(step for step in plan.steps if step.id not in history_ids)
        if not future:
            fallback_id = _unique_step_id("respond", history_ids)
            future = (TaskStep(
                id=fallback_id,
                title="Respond",
                type=StepType.REVIEW,
                executor=StepExecutor.MODEL,
            ),)
        future_dependencies = {
            step.id: step.depends_on
            for step in future
        }
        current_future = {
            step.id: step
            for step in state.steps
            if step.id not in history_ids
            and step.status is StepStatus.RUNNING
        }
        preserve_running_step = any(
            step.id in current_future
            for step in future
        )
        revised_future = RunStateMachine.initialize(
            state.run_id,
            ExecutionPlan(
                title=plan.title,
                goal=plan.goal,
                task_spec=plan.task_spec,
                steps=tuple(
                    replace(
                        step,
                        depends_on=tuple(
                            dependency
                            for dependency in step.depends_on
                            if dependency not in history_ids
                        ),
                    )
                    for step in future
                ),
            ),
        )
        revised_future_steps = tuple(
            replace(
                step,
                depends_on=future_dependencies[step.id],
                status=(
                    current_future[step.id].status
                    if step.id in current_future
                    else StepStatus.PENDING
                    if preserve_running_step
                    else step.status
                ),
                result_summary=(
                    current_future[step.id].result_summary
                    if step.id in current_future
                    else step.result_summary
                ),
                error=(
                    current_future[step.id].error
                    if step.id in current_future
                    else step.error
                ),
            )
            for step in revised_future.steps
        )
        return replace(
            state,
            title=plan.title,
            goal=plan.goal,
            task_spec=plan.task_spec,
            steps=history + revised_future_steps,
            work_step_ids=tuple(dict.fromkeys((
                *(
                    step_id
                    for step_id in state.work_step_ids
                    if step_id in history_ids
                ),
                *plan.work_step_ids,
            ))),
        )

    @staticmethod
    def on_model_delta(state: RunSnapshot) -> RunTransition:
        if state.terminal or any(
            step.status is StepStatus.RUNNING and _is_tool_step(step)
            for step in state.steps
        ):
            return _unchanged(state)
        index = _next_incomplete_index(state.steps)
        if index < 0 or not _is_model_step(state.steps[index]):
            return _unchanged(state)
        if state.steps[index].status is StepStatus.RUNNING:
            return _unchanged(state)
        return _with_step_changes(
            state,
            ((index, replace(state.steps[index], status=StepStatus.RUNNING)),),
        )

    @staticmethod
    def on_tool_calls_started(
        state: RunSnapshot,
        tool_names: frozenset[str] | set[str] | tuple[str, ...],
    ) -> RunTransition:
        if state.terminal:
            return _unchanged(state)
        requested = frozenset(
            str(name).strip() for name in tool_names if str(name).strip()
        )
        allowed = RunStateMachine.allowed_tool_names_for_current_transition(state)
        if not requested or not requested.issubset(allowed):
            raise ContractViolationError(
                "tool calls are outside the current plan transition"
            )

        steps = state.steps
        changes: list[tuple[int, TaskStep]] = []
        running_model = next(
            (
                index
                for index, step in enumerate(steps)
                if step.status is StepStatus.RUNNING and _is_model_step(step)
            ),
            -1,
        )
        if running_model >= 0:
            completed = replace(
                steps[running_model],
                status=StepStatus.DONE,
                result_summary=(
                    steps[running_model].result_summary
                    or "Model reasoning completed before tool execution."
                ),
            )
            changes.append((running_model, completed))
            steps = _replace_at(steps, running_model, completed)

        tool_index = next(
            (
                index
                for index, step in enumerate(steps)
                if step.status is StepStatus.RUNNING and _is_tool_step(step)
            ),
            -1,
        )
        if tool_index < 0:
            tool_index = _next_incomplete_index(steps)
        if tool_index < 0 or not _is_tool_step(steps[tool_index]):
            raise ContractViolationError("current plan transition is not a tool step")
        if steps[tool_index].status is not StepStatus.RUNNING:
            changes.append((
                tool_index,
                replace(steps[tool_index], status=StepStatus.RUNNING),
            ))
        return _with_step_changes(state, tuple(changes))

    @staticmethod
    def on_tool_round_completed(
        state: RunSnapshot,
        outcome: ToolBatchOutcome = ToolBatchOutcome.COMPLETED,
    ) -> RunTransition:
        if state.terminal:
            return _unchanged(state)
        normalized_outcome = ToolBatchOutcome(outcome)
        if normalized_outcome not in {
            ToolBatchOutcome.PROGRESSED,
            ToolBatchOutcome.COMPLETED,
            ToolBatchOutcome.DECLINED,
        }:
            raise ContractViolationError(
                "only progressed, completed, or declined tool rounds may "
                "advance todo state"
            )
        tool_index = next(
            (
                index
                for index, step in enumerate(state.steps)
                if step.status is StepStatus.RUNNING and _is_tool_step(step)
            ),
            -1,
        )
        if tool_index < 0:
            return _unchanged(state)

        if normalized_outcome is ToolBatchOutcome.DECLINED:
            declined = replace(
                state.steps[tool_index],
                status=StepStatus.BLOCKED,
                result_summary=(
                    "User declined approval; the planned tool was not executed."
                ),
                error="approval_rejected",
            )
            # A rejected approval ends this tool transition.  Do not activate a
            # later tool grant; the following model round may still provide the
            # protocol-compatible final explanation to the user.
            return _with_step_changes(state, ((tool_index, declined),))

        if normalized_outcome is ToolBatchOutcome.PROGRESSED:
            # The host committed a valid bounded portion of this operation,
            # but explicitly did not satisfy the Planner step. Keep the same
            # transition running; ordinary batch progress follows the existing
            # plan and does not itself justify another Planner model call.
            return _unchanged(state)

        completed = replace(
            state.steps[tool_index],
            status=StepStatus.DONE,
            result_summary=(
                state.steps[tool_index].result_summary
                or "Planned tool step completed."
            ),
        )
        steps = _replace_at(state.steps, tool_index, completed)
        changes: list[tuple[int, TaskStep]] = [(tool_index, completed)]
        next_index = _next_incomplete_index(steps)
        if next_index >= 0 and steps[next_index].type is not StepType.CONFIRM:
            running = replace(steps[next_index], status=StepStatus.RUNNING)
            changes.append((next_index, running))
        return _with_step_changes(state, tuple(changes))

    @staticmethod
    def on_tool_round_failed(
        state: RunSnapshot,
        error: str = "tool_execution_failed",
    ) -> RunTransition:
        """Close the current tool step as failed before a recovery replan."""

        if state.terminal:
            return _unchanged(state)
        normalized_error = _optional_text(error) or "tool_execution_failed"
        tool_index = next(
            (
                index
                for index, step in enumerate(state.steps)
                if step.status is StepStatus.RUNNING and _is_tool_step(step)
            ),
            -1,
        )
        if tool_index < 0:
            raise ContractViolationError(
                "failed tool round requires a running tool step"
            )
        failed = replace(
            state.steps[tool_index],
            status=StepStatus.FAILED,
            result_summary="Tool execution failed; runtime replanning requested.",
            error=normalized_error,
        )
        return _with_step_changes(state, ((tool_index, failed),))

    @staticmethod
    def complete(state: RunSnapshot, final_response: str = "") -> RunTransition:
        if state.terminal:
            return _unchanged(state)
        has_incomplete_non_model = any(
            step.status in {StepStatus.PENDING, StepStatus.RUNNING}
            and not _is_model_step(step)
            for step in state.steps
        )
        changes: list[tuple[int, TaskStep]] = []
        for index, step in enumerate(state.steps):
            if step.status not in {StepStatus.PENDING, StepStatus.RUNNING}:
                continue
            if _is_model_step(step) and (
                step.status is StepStatus.RUNNING or not has_incomplete_non_model
            ):
                updated = replace(
                    step,
                    status=StepStatus.DONE,
                    result_summary=(
                        step.result_summary
                        or "Final response covered this model step."
                    ),
                )
            else:
                updated = replace(
                    step,
                    status=StepStatus.BLOCKED,
                    result_summary=(
                        step.result_summary
                        or "This planned step was not executed in the run."
                    ),
                )
            changes.append((index, updated))
        return _with_step_changes(
            state,
            tuple(changes),
            status=(RunStatus.BLOCKED if has_incomplete_non_model else RunStatus.DONE),
            final_response=final_response,
        )

    @staticmethod
    def complete_durable_execution(
        state: RunSnapshot,
        final_response: str = "",
        *,
        covered_step_ids: tuple[str, ...],
    ) -> RunTransition:
        """Complete a root run after its durable execution has finished."""

        if state.terminal:
            return _unchanged(state)
        covered = frozenset(covered_step_ids)
        planned = frozenset(step.id for step in state.steps)
        if planned != covered:
            raise RuntimeError(
                "durable completion must match the admitted plan steps"
            )
        changes = tuple(
            (
                index,
                replace(
                    step,
                    status=StepStatus.DONE,
                    result_summary=(
                        step.result_summary
                        or "Durable execution fulfilled this admitted step."
                    ),
                ),
            )
            for index, step in enumerate(state.steps)
            if step.id in covered and step.status is not StepStatus.DONE
        )
        return _with_step_changes(
            state,
            changes,
            status=RunStatus.DONE,
            final_response=final_response,
        )

    @staticmethod
    def finish_durable_execution(
        state: RunSnapshot,
        *,
        covered_step_ids: tuple[str, ...],
    ) -> RunTransition:
        """Finish durable steps while leaving the Run open for presentation."""

        if state.terminal:
            return _unchanged(state)
        covered = frozenset(covered_step_ids)
        planned = frozenset(step.id for step in state.steps)
        if planned != covered:
            raise RuntimeError(
                "durable completion must match the admitted plan steps"
            )
        changes = tuple(
            (
                index,
                replace(
                    step,
                    status=StepStatus.DONE,
                    result_summary=(
                        step.result_summary
                        or "Durable execution fulfilled this admitted step."
                    ),
                ),
            )
            for index, step in enumerate(state.steps)
            if step.id in covered and step.status is not StepStatus.DONE
        )
        return _with_step_changes(state, changes)

    @staticmethod
    def sync_durable_execution(
        state: RunSnapshot,
        statuses: dict[str, StepStatus],
    ) -> RunTransition:
        """Project durable unit states onto their Planner-authored steps."""

        if state.terminal:
            return _unchanged(state)
        by_id = {step.id: index for index, step in enumerate(state.steps)}
        unknown = set(statuses) - set(by_id)
        if unknown:
            raise ContractViolationError(
                "durable progress names unknown plan steps: "
                + ", ".join(sorted(unknown))
            )
        changes: list[tuple[int, TaskStep]] = []
        for step_id, raw_status in statuses.items():
            index = by_id[step_id]
            step = state.steps[index]
            status = StepStatus(raw_status)
            if step.status is status:
                continue
            if step.status is StepStatus.DONE and status is not StepStatus.DONE:
                continue
            changes.append((
                index,
                replace(
                    step,
                    status=status,
                    result_summary=(
                        "Durable execution completed this Planner step."
                        if status is StepStatus.DONE
                        else step.result_summary
                    ),
                    error=(
                        "durable_execution_failed"
                        if status is StepStatus.FAILED
                        else None
                    ),
                ),
            ))
        return _with_step_changes(state, tuple(changes))

    @staticmethod
    def fail(state: RunSnapshot, error: str) -> RunTransition:
        if state.terminal:
            return _unchanged(state)
        normalized_error = _optional_text(error)
        if normalized_error is None:
            raise ContractViolationError("failed run requires a non-empty error")
        changes = tuple(
            (
                index,
                replace(step, status=StepStatus.FAILED, error=normalized_error),
            )
            for index, step in enumerate(state.steps)
            if step.status is StepStatus.RUNNING
        )
        return _with_step_changes(
            state,
            changes,
            status=RunStatus.FAILED,
            error=normalized_error,
        )

    @staticmethod
    def cancel(
        state: RunSnapshot,
        reason: str = "request_canceled",
    ) -> RunTransition:
        if state.terminal:
            return _unchanged(state)
        normalized_reason = _optional_text(reason) or "request_canceled"
        changes = tuple(
            (
                index,
                replace(
                    step,
                    status=StepStatus.BLOCKED,
                    result_summary=(
                        step.result_summary
                        or "Run was canceled before this step completed."
                    ),
                ),
            )
            for index, step in enumerate(state.steps)
            if step.status in {StepStatus.PENDING, StepStatus.RUNNING}
        )
        return _with_step_changes(
            state,
            changes,
            status=RunStatus.CANCELED,
            error=normalized_reason,
        )

    @staticmethod
    def allowed_tool_names(state: RunSnapshot) -> frozenset[str]:
        return frozenset(
            tool
            for step in state.steps
            if _is_tool_step(step)
            for tool in step.suggested_tools
        )

    @staticmethod
    def execution_transition(
        state: RunSnapshot,
    ) -> ExecutionTransition | None:
        """Compile the live Run snapshot into one bounded runtime grant."""

        if state.terminal:
            return None
        running = next(
            (
                step
                for step in state.steps
                if step.status is StepStatus.RUNNING
            ),
            None,
        )
        if running is None:
            return None
        return ExecutionTransition(
            step_id=running.id,
            executor=running.executor,
            allowed_tool_names=(
                RunStateMachine.allowed_tool_names_for_current_transition(state)
            ),
            future_tool_names=RunStateMachine.future_allowed_tool_names(state),
        )

    @staticmethod
    def allowed_tool_names_for_current_transition(
        state: RunSnapshot,
    ) -> frozenset[str]:
        if state.terminal:
            return frozenset()
        running_index = next(
            (
                index
                for index, step in enumerate(state.steps)
                if step.status is StepStatus.RUNNING
            ),
            -1,
        )
        if running_index < 0:
            return frozenset()
        running = state.steps[running_index]
        if _is_tool_step(running):
            return frozenset(running.suggested_tools)
        if not _is_model_step(running):
            return frozenset()
        for step in state.steps[running_index + 1:]:
            if step.status in _FINISHED_STEP_STATUSES:
                continue
            return (
                frozenset(step.suggested_tools)
                if _is_tool_step(step)
                else frozenset()
            )
        return frozenset()

    @staticmethod
    def future_allowed_tool_names(state: RunSnapshot) -> frozenset[str]:
        """Return unfinished tool grants strictly after the current transition."""

        if state.terminal:
            return frozenset()
        running_index = next(
            (
                index
                for index, step in enumerate(state.steps)
                if step.status is StepStatus.RUNNING
            ),
            -1,
        )
        if running_index < 0:
            return frozenset()

        current_tool_index = -1
        running = state.steps[running_index]
        if _is_tool_step(running):
            current_tool_index = running_index
        elif _is_model_step(running):
            for index in range(running_index + 1, len(state.steps)):
                step = state.steps[index]
                if step.status in _FINISHED_STEP_STATUSES:
                    continue
                if _is_tool_step(step):
                    current_tool_index = index
                break

        start_index = (
            current_tool_index + 1
            if current_tool_index >= 0
            else running_index + 1
        )
        return frozenset(
            tool
            for step in state.steps[start_index:]
            if step.status not in _FINISHED_STEP_STATUSES
            and _is_tool_step(step)
            for tool in step.suggested_tools
        )


def _initial_step(step: TaskStep) -> TaskStep:
    return replace(
        step,
        status=(
            StepStatus.DONE
            if step.status is StepStatus.DONE
            else StepStatus.PENDING
        ),
        result_summary=(step.result_summary if step.status is StepStatus.DONE else None),
        error=None,
    )


def _validate_step_execution_contract(step: TaskStep) -> None:
    if step.type is StepType.CONFIRM:
        raise ContractViolationError(
            "confirm steps are owned by the host approval policy"
        )
    if step.type is StepType.READ and step.executor is not StepExecutor.TOOL:
        raise ContractViolationError("read step must use the tool executor")
    if (
        step.executor is StepExecutor.TOOL
        and len(step.suggested_tools) != 1
    ):
        raise ContractViolationError(
            "tool step requires exactly one expected tool"
        )
    if step.executor is StepExecutor.MODEL and step.suggested_tools:
        raise ContractViolationError("model step cannot grant tool access")


def _is_tool_step(step: TaskStep) -> bool:
    return step.executor is StepExecutor.TOOL or step.type is StepType.READ


def _is_model_step(step: TaskStep) -> bool:
    return (
        step.executor is StepExecutor.MODEL
        and step.type not in {StepType.READ, StepType.CONFIRM}
    )


def _next_incomplete_index(steps: tuple[TaskStep, ...]) -> int:
    return next(
        (
            index
            for index, step in enumerate(steps)
            if step.status not in _FINISHED_STEP_STATUSES
        ),
        -1,
    )


def _with_step_changes(
    state: RunSnapshot,
    changes: tuple[tuple[int, TaskStep], ...],
    *,
    status: RunStatus | None = None,
    final_response: str | None = None,
    error: str | None = None,
) -> RunTransition:
    steps = state.steps
    updates: list[TaskStepUpdate] = []
    for index, step in changes:
        steps = _replace_at(steps, index, step)
        updates.append(TaskStepUpdate(
            step_id=step.id,
            status=step.status,
            result_summary=step.result_summary,
            error=step.error,
        ))
    after = replace(
        state,
        steps=steps,
        status=(state.status if status is None else status),
        final_response=(
            state.final_response if final_response is None else final_response
        ),
        error=(state.error if error is None else error),
    )
    return RunTransition(before=state, after=after, step_updates=tuple(updates))


def _replace_at(
    steps: tuple[TaskStep, ...],
    index: int,
    step: TaskStep,
) -> tuple[TaskStep, ...]:
    return steps[:index] + (step,) + steps[index + 1:]


def _unchanged(state: RunSnapshot) -> RunTransition:
    return RunTransition(before=state, after=state)


def _unique_step_id(base: str, used: frozenset[str]) -> str:
    if base not in used:
        return base
    suffix = 2
    while f"{base}-{suffix}" in used:
        suffix += 1
    return f"{base}-{suffix}"


def canonicalize_execution_plan(plan: ExecutionPlan) -> ExecutionPlan:
    """Apply the same initial step-state rules used when Core opens a Run."""

    snapshot = RunStateMachine.initialize("canonical-plan", plan)
    return ExecutionPlan(
        title=snapshot.title,
        goal=snapshot.goal,
        task_spec=snapshot.task_spec,
        steps=snapshot.steps,
        work_step_ids=snapshot.work_step_ids,
    )
