"""Durable admission execution isolated from the high-level pipeline."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from typing import Protocol

from purra.cancellation import await_with_cancellation
from purra.contracts import (
    AgentRunRequest,
    RunId,
    StepStatus,
    ExecutionPlan,
)
from purra.errors import ContractViolationError
from purra.events import AgentEvent, CoreEventType
from purra.engine.options import DurableTaskContinuation
from purra.json_values import thaw_json_mapping
from purra.ports import CancellationSignal
from purra.run_controller import AgentRunController
from purra.task_admission import (
    ExecutionMode,
    LongTaskDispatcher,
    LongTaskDispatchReceipt,
    LongTaskExecutionResult,
    LongTaskExecutionStatus,
    LongTaskExecutionUpdate,
    TaskAdmissionDecision,
)


class BufferedEventSink(Protocol):
    def drain(self) -> tuple[AgentEvent, ...]: ...


@dataclass(frozen=True, slots=True)
class DurableExecutionCompletion:
    """Completed durable work awaiting Core's terminal response transaction."""

    result: LongTaskExecutionResult


async def complete_durable_continuation(
    controller: AgentRunController,
    request: AgentRunRequest,
    continuation: DurableTaskContinuation,
    dispatcher: LongTaskDispatcher | None,
    sink: BufferedEventSink,
    signal: CancellationSignal | None,
    *,
    defer_successful_completion: bool = False,
) -> AsyncIterator[AgentEvent | DurableExecutionCompletion]:
    """Resume an admitted durable receipt without re-planning or dispatching."""

    plan = continuation.source.execution_plan
    if plan is None:
        raise ContractViolationError("durable continuation source has no plan")
    admission = continuation.receipt.admission
    validate_task_admission_coverage(plan, admission)
    await controller.record_event(
        CoreEventType.TASK_ADMISSION_DECIDED,
        {
            **admission.to_event_payload(),
            "continuation": True,
            "sourceRootRunId": continuation.source.run_id,
            "continuationCommand": continuation.continuation_command,
        },
    )
    await controller.install_plan(plan)
    async for event in complete_admitted_task(
        controller=controller,
        request=request,
        plan=plan,
        admission=admission,
        dispatcher=dispatcher,
        sink=sink,
        signal=signal,
        existing_receipt=continuation.receipt,
        defer_successful_completion=defer_successful_completion,
    ):
        yield event


async def complete_admitted_task(
    *,
    controller: AgentRunController,
    request: AgentRunRequest,
    plan: ExecutionPlan,
    admission: TaskAdmissionDecision,
    dispatcher: LongTaskDispatcher | None,
    sink: BufferedEventSink,
    signal: CancellationSignal | None,
    existing_receipt: LongTaskDispatchReceipt | None = None,
    defer_successful_completion: bool = False,
) -> AsyncIterator[AgentEvent | DurableExecutionCompletion]:
    """Run durable work under the originating Run and its event stream."""

    if admission.mode is ExecutionMode.INLINE:
        raise ContractViolationError(
            "inline admission cannot use the durable handoff path"
        )
    if admission.mode is ExecutionMode.DURABLE:
        if admission.requires_confirmation:
            await controller.complete(
                admission.message
                or "This long-running task requires confirmation."
            )
            for event in sink.drain():
                yield event
            return
        if dispatcher is None:
            raise ContractViolationError(
                "durable task admission requires a dispatcher"
            )
        receipt = existing_receipt
        if receipt is None:
            receipt = await await_with_cancellation(
                dispatcher.dispatch(
                    request,
                    plan,
                    admission,
                    run_id=controller.run_id,
                    signal=signal,
                ),
                signal,
            )
            await controller.record_event(
                CoreEventType.LONG_TASK_DISPATCHED,
                {
                    "taskId": receipt.task_id,
                    "message": receipt.message,
                    **thaw_json_mapping(receipt.metadata),
                },
            )
            for event in sink.drain():
                yield event
        if receipt.admission != admission:
            raise ContractViolationError(
                "durable dispatcher changed the admitted execution contract"
            )

        durable_step_aliases = _durable_step_aliases(receipt, admission)
        updates: asyncio.Queue[
            tuple[LongTaskExecutionUpdate, asyncio.Future[None]]
        ] = asyncio.Queue()
        acknowledgements: set[asyncio.Future[None]] = set()

        async def observe(update: LongTaskExecutionUpdate) -> None:
            acknowledged = asyncio.get_running_loop().create_future()
            acknowledgements.add(acknowledged)
            try:
                await updates.put((update, acknowledged))
                await acknowledged
            finally:
                acknowledgements.discard(acknowledged)

        execution = asyncio.create_task(dispatcher.execute(
            receipt.task_id,
            run_id=str(controller.run_id or ""),
            observer=observe,
            signal=signal,
        ))
        pending_update: asyncio.Task[
            tuple[LongTaskExecutionUpdate, asyncio.Future[None]]
        ] | None = None
        execution_drained = False
        contract_violation = False
        try:
            while not execution.done() or not updates.empty():
                pending_update = asyncio.create_task(updates.get())
                done, _ = await asyncio.wait(
                    (execution, pending_update),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if pending_update in done:
                    update, acknowledged = pending_update.result()
                    pending_update = None
                    try:
                        if update.event.type == CoreEventType.RUN_TODOS_UPDATED:
                            raise ContractViolationError(
                                "durable updates cannot publish run.todos_updated"
                            )
                        if update.plan_revision is not None and not update.persist:
                            raise ContractViolationError(
                                "durable plan revisions require persisted "
                                "checkpoint evidence"
                            )
                        event = bind_event_to_run(
                            update.event,
                            controller.run_id,
                        )
                        if update.persist:
                            event = _bind_durable_progress_to_plan(
                                event,
                                durable_step_aliases,
                            )
                            durable_statuses = _durable_plan_step_statuses(event)
                            if durable_statuses and update.plan_revision is None:
                                await controller.sync_durable_execution(
                                    durable_statuses
                                )
                            if update.plan_revision is not None:
                                _validate_durable_plan_revision(
                                    controller,
                                    update.plan_revision,
                                    admission.covered_step_ids,
                                    original_plan=plan,
                                )
                                if durable_statuses:
                                    await controller.sync_durable_execution(
                                        durable_statuses
                                    )
                                await controller.revise_plan(
                                    update.plan_revision,
                                    revision_metadata=thaw_json_mapping(
                                        update.plan_revision_metadata
                                    ),
                                )
                            await controller.record_event(
                                event.type,
                                thaw_json_mapping(event.payload),
                            )
                            persisted = sink.drain()
                        else:
                            persisted = (event,)
                    except BaseException as error:
                        if not acknowledged.done():
                            if isinstance(error, asyncio.CancelledError):
                                acknowledged.cancel()
                            else:
                                acknowledged.set_exception(error)
                        # Let the dispatcher consume the same terminal ACK
                        # before cleanup cancels its task. This prevents the
                        # producer and consumer from waiting on each other.
                        await asyncio.sleep(0)
                        raise
                    else:
                        if not acknowledged.done():
                            acknowledged.set_result(None)
                    for emitted in persisted:
                        yield emitted
                else:
                    pending_update.cancel()
                    with suppress(asyncio.CancelledError):
                        await pending_update
                    pending_update = None
            result = await execution
            execution_drained = True
        except ContractViolationError:
            contract_violation = True
        finally:
            if not execution_drained:
                await _cleanup_durable_execution(
                    execution,
                    pending_update,
                    acknowledgements,
                )

        if contract_violation:
            await controller.fail(
                "durable_plan_revision_contract_violation"
            )
            for event in sink.drain():
                yield event
            return

        completion: DurableExecutionCompletion | None = None
        if result.status is LongTaskExecutionStatus.COMPLETED:
            # Durable execution has now genuinely completed. This transition
            # closes the Planner steps atomically at the end of the work.
            if defer_successful_completion:
                await controller.finish_durable_execution(
                    covered_step_ids=admission.covered_step_ids,
                )
                completion = DurableExecutionCompletion(result)
            else:
                await controller.complete_durable_execution(
                    result.final_response,
                    covered_step_ids=admission.covered_step_ids,
                )
        elif result.status is LongTaskExecutionStatus.FAILED:
            await controller.fail(result.error or "long_task_execution_failed")
        else:
            await controller.cancel(
                "long_task_paused"
                if result.status is LongTaskExecutionStatus.PAUSED
                else "long_task_canceled"
            )
        for event in sink.drain():
            yield event
        if completion is not None:
            yield completion
        return
    await controller.complete(
        admission.message
        or (
            "The task needs clarification before it can run."
            if admission.mode is ExecutionMode.CLARIFY
            else "The task was not admitted for execution."
        )
    )
    for event in sink.drain():
        yield event


async def _cleanup_durable_execution(
    execution: asyncio.Task[object],
    pending_update: asyncio.Task[object] | None,
    acknowledgements: set[asyncio.Future[None]],
) -> None:
    if pending_update is not None and not pending_update.done():
        pending_update.cancel()
    execution.cancel()
    for acknowledged in tuple(acknowledgements):
        if not acknowledged.done():
            acknowledged.cancel()
    if pending_update is not None:
        with suppress(asyncio.CancelledError, Exception):
            await pending_update
    with suppress(asyncio.CancelledError, Exception):
        await execution


def validate_task_admission_coverage(
    plan: ExecutionPlan,
    admission: TaskAdmissionDecision,
) -> None:
    """Require a durable executor to own every step it bypasses."""

    if admission.mode is not ExecutionMode.DURABLE:
        return
    planned_step_ids = {step.id for step in plan.steps}
    covered_step_ids = set(admission.covered_step_ids)
    unknown = covered_step_ids - planned_step_ids
    uncovered = planned_step_ids - covered_step_ids
    if unknown or uncovered:
        details: list[str] = []
        if unknown:
            details.append("unknown=" + ",".join(sorted(unknown)))
        if uncovered:
            details.append("uncovered=" + ",".join(sorted(uncovered)))
        raise ContractViolationError(
            "durable task admission must cover every planned step: "
            + "; ".join(details)
        )


def _validate_durable_plan_revision(
    controller: AgentRunController,
    revision: ExecutionPlan,
    covered_step_ids: Sequence[str],
    *,
    original_plan: ExecutionPlan | None = None,
) -> None:
    revised_by_id = {step.id: step for step in revision.steps}
    if set(revised_by_id) != set(covered_step_ids):
        raise ContractViolationError(
            "durable plan revision must preserve admitted step ids"
        )
    snapshot = controller.snapshot
    if snapshot is None:
        raise ContractViolationError(
            "durable plan revision requires an active root run"
        )
    baseline = original_plan or snapshot.execution_plan
    if baseline is None:
        raise ContractViolationError(
            "durable plan revision requires a persisted ExecutionPlan"
        )
    if (
        revision.title,
        revision.goal,
        revision.task_spec,
    ) != (
        baseline.title,
        baseline.goal,
        baseline.task_spec,
    ):
        raise ContractViolationError(
            "durable plan revision cannot change plan title, goal, or task spec"
        )
    if revision.work_step_ids != baseline.work_step_ids:
        raise ContractViolationError(
            "durable plan revision cannot change WorkStep lineage"
        )
    for current in snapshot.steps:
        revised = revised_by_id[current.id]
        if current.status is StepStatus.DONE and revised != current:
            raise ContractViolationError(
                "durable plan revision cannot change completed steps"
            )
        if current.status is StepStatus.DONE:
            continue
        immutable_revision = replace(
            revised,
            title=current.title,
            description=current.description,
            depends_on=current.depends_on,
        )
        if immutable_revision != current:
            raise ContractViolationError(
                "durable plan revision may only change title, description, "
                "and dependencies for incomplete steps"
            )


def _durable_plan_step_statuses(event: AgentEvent) -> dict[str, StepStatus]:
    if event.type != CoreEventType.LONG_TASK_PROGRESS:
        return {}
    units = event.payload.get("units")
    if not isinstance(units, Sequence) or isinstance(
        units,
        (str, bytes, bytearray),
    ):
        return {}
    status_map = {
        "pending": StepStatus.PENDING,
        "claimed": StepStatus.RUNNING,
        "running": StepStatus.RUNNING,
        "completed": StepStatus.DONE,
        "failed": StepStatus.FAILED,
        "canceled": StepStatus.BLOCKED,
    }
    grouped: dict[str, list[StepStatus]] = {}
    for raw in units:
        if not isinstance(raw, Mapping):
            continue
        step_id = str(raw.get("plannerStepId") or "").strip()
        status = status_map.get(str(raw.get("status") or "").strip())
        if step_id and status is not None:
            grouped.setdefault(step_id, []).append(status)
    return {
        step_id: _aggregate_durable_unit_statuses(statuses)
        for step_id, statuses in grouped.items()
    }


def _aggregate_durable_unit_statuses(
    statuses: Sequence[StepStatus],
) -> StepStatus:
    values = frozenset(statuses)
    if StepStatus.FAILED in values:
        return StepStatus.FAILED
    if values == {StepStatus.DONE}:
        return StepStatus.DONE
    if StepStatus.BLOCKED in values:
        return StepStatus.BLOCKED
    if StepStatus.RUNNING in values or StepStatus.DONE in values:
        return StepStatus.RUNNING
    return StepStatus.PENDING


def _durable_step_aliases(
    receipt: LongTaskDispatchReceipt,
    admission: TaskAdmissionDecision,
) -> dict[str, str]:
    raw = receipt.metadata.get("durableStepAliases")
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ContractViolationError("durable step aliases must be a mapping")
    aliases = {
        str(source or "").strip(): str(target or "").strip()
        for source, target in raw.items()
    }
    if (
        any(not source or not target for source, target in aliases.items())
        or len(set(aliases.values())) != len(aliases)
        or not set(aliases.values()).issubset(set(admission.covered_step_ids))
    ):
        raise ContractViolationError(
            "durable step aliases must bind uniquely to admitted plan steps"
        )
    return aliases


def _bind_durable_progress_to_plan(
    event: AgentEvent,
    aliases: Mapping[str, str],
) -> AgentEvent:
    if event.type != CoreEventType.LONG_TASK_PROGRESS or not aliases:
        return event
    payload = thaw_json_mapping(event.payload)
    units = payload.get("units")
    if not isinstance(units, list):
        return event
    rebound_units: list[object] = []
    for raw in units:
        if not isinstance(raw, Mapping):
            rebound_units.append(raw)
            continue
        unit = dict(raw)
        persisted_step_id = str(unit.get("plannerStepId") or "").strip()
        if persisted_step_id in aliases:
            unit["plannerStepId"] = aliases[persisted_step_id]
        rebound_units.append(unit)
    payload["units"] = rebound_units
    return AgentEvent(
        type=event.type,
        run_id=event.run_id,
        payload=payload,
    )


def bind_event_to_run(event: AgentEvent, run_id: RunId | None) -> AgentEvent:
    if run_id is None:
        raise ContractViolationError("active run has no id")
    if event.run_id is None:
        return AgentEvent(type=event.type, run_id=run_id, payload=event.payload)
    if event.run_id != run_id:
        raise ContractViolationError("runtime event belongs to another run")
    return event
