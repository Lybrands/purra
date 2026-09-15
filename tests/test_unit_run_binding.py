import pytest

from purra.adapters import InMemoryDurableAdapters
from purra.errors import ContractViolationError
from purra.long_tasks import LongTaskCreateCommand, LongTaskUnitSpec, LongTaskUnitResult


@pytest.mark.asyncio
async def test_unit_attempt_binding_is_immutable_and_completion_cannot_replace_it():
    repository = InMemoryDurableAdapters().long_tasks
    task = await repository.create("binding-task", LongTaskCreateCommand(
        namespace="test.binding", kind="test", owner_id="test", created_by_run_id="root",
        max_parallelism=2, units=(LongTaskUnitSpec(id="a", position=0), LongTaskUnitSpec(id="b", position=1)),
    ))
    await repository.start(task.id, expected_revision=task.revision)
    first = await repository.claim_ready_unit(task.id, worker_id="worker", lease_duration_ms=30000)
    second = await repository.claim_ready_unit(task.id, worker_id="worker", lease_duration_ms=30000)
    claim = dict(worker_id="worker", lease_epoch=first.lease_epoch)
    bound = await repository.bind_unit_run(task.id, first.id, run_id="child-a", **claim)
    revision = (await repository.load(task.id)).revision
    assert await repository.bind_unit_run(task.id, first.id, run_id="child-a", **claim) == bound
    assert (await repository.load(task.id)).revision == revision
    for identity in (first.id, second.id):
        with pytest.raises(ContractViolationError) as rejected:
            await repository.bind_unit_run(task.id, identity,
                run_id="different-child" if identity == first.id else "child-a", **claim)
        assert rejected.value.code == "long_task_unit_run_conflict"
    for identity in (first.id, second.id):
        with pytest.raises(ContractViolationError) as rejected:
            await repository.complete_unit(task.id, identity, **claim,
                result=LongTaskUnitResult(output_ref="test://result", run_id="different-child" if identity == first.id else "child-a"))
        assert rejected.value.code == "long_task_unit_run_conflict"
    assert (await repository.load(task.id)).revision == revision
    result = LongTaskUnitResult(output_ref="test://result", run_id="child-a")
    completed = await repository.complete_unit(task.id, first.id, result=result, **claim)
    assert await repository.complete_unit(task.id, first.id, result=result, **claim) == completed
    with pytest.raises(ContractViolationError):
        await repository.complete_unit(task.id, first.id, **claim,
            result=LongTaskUnitResult(output_ref="test://result", run_id="forged-child"))
    assert (await repository.list_units(task.id))[0].run_id == "child-a"


@pytest.mark.asyncio
async def test_reclaimed_attempt_can_bind_new_run_but_old_lease_cannot():
    from purra.adapters.durable_memory import InMemoryLongTaskRepository
    now = 100
    repository = InMemoryLongTaskRepository(clock_ms=lambda: now)
    task = await repository.create("retry-task", LongTaskCreateCommand(
        namespace="test.binding", kind="test", owner_id="test", created_by_run_id="root",
        units=(LongTaskUnitSpec(id="unit", position=0, max_attempts=2),),
    ))
    await repository.start(task.id, expected_revision=task.revision)
    first = await repository.claim_ready_unit(task.id, worker_id="worker", lease_duration_ms=10)
    await repository.bind_unit_run(task.id, first.id, worker_id="worker", lease_epoch=first.lease_epoch, run_id="first")
    now = 110
    second = await repository.claim_ready_unit(task.id, worker_id="worker", lease_duration_ms=10)
    with pytest.raises(ContractViolationError) as stale:
        await repository.bind_unit_run(task.id, first.id, worker_id="worker", lease_epoch=first.lease_epoch, run_id="stale")
    assert stale.value.code == "long_task_unit_lease_lost"
    bound = await repository.bind_unit_run(task.id, second.id, worker_id="worker", lease_epoch=second.lease_epoch, run_id="second")
    assert bound.run_id == "second"
