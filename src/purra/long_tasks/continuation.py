"""Continuation binding validation for Recipe tasks."""

from __future__ import annotations

from purra.errors import ContractViolationError
from purra.long_tasks.contracts import LongTaskRunRelation, LongTaskStatus
from purra.long_tasks.ports import LongTaskRepository
from purra.normalization import required_text


async def prepare_continuation(
    repository: LongTaskRepository,
    task_id: str,
    *,
    source_run_id: str,
    run_id: str,
) -> None:
    """Bind one already-authorized continuation Root to its Recipe task."""

    task_id = required_text(task_id, "durable continuation task id")
    source_run_id = required_text(
        source_run_id,
        "durable continuation source Run id",
    )
    run_id = required_text(run_id, "durable continuation Run id")
    if source_run_id == run_id:
        raise ContractViolationError(
            "durable continuation requires a new Root Run",
            code="durable_continuation_binding_conflict",
        )
    task = await repository.load(task_id)
    if task is None:
        raise ContractViolationError(
            "durable continuation task does not exist",
            code="durable_continuation_binding_conflict",
        )
    bindings = await repository.list_run_bindings(task_id)
    source = next(
        (binding for binding in bindings if binding.run_id == source_run_id),
        None,
    )
    if (
        task.created_by_run_id != source_run_id
        or source is None
        or source.relation is not LongTaskRunRelation.CREATED
    ):
        raise ContractViolationError(
            "durable continuation source does not own the Recipe task",
            code="durable_continuation_binding_conflict",
        )
    current = next(
        (binding for binding in bindings if binding.run_id == run_id),
        None,
    )
    if current is not None:
        if current.relation is not LongTaskRunRelation.CONTINUATION:
            raise ContractViolationError(
                "durable continuation Run has a conflicting task relation",
                code="durable_continuation_binding_conflict",
            )
        return
    if task.status not in {LongTaskStatus.PAUSED, LongTaskStatus.RUNNING}:
        raise ContractViolationError(
            "durable continuation task is not resumable",
            code="durable_continuation_binding_conflict",
        )
    await repository.bind_run(
        task_id,
        run_id,
        relation=LongTaskRunRelation.CONTINUATION,
    )
