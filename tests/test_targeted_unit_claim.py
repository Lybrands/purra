import asyncio

import pytest

from purra.long_tasks import LongTaskCreateCommand, LongTaskUnitSpec, LongTaskUnitResult
from purra.storage import StorageSession


@pytest.fixture
def repository():
    return StorageSession().long_tasks


async def create(repository, **options):
    task = await repository.create("targeted", LongTaskCreateCommand(
        namespace="test.targeted", kind="test", owner_id="synthetic", created_by_run_id="root",
        max_parallelism=2, units=(
            LongTaskUnitSpec(id="first", position=0),
            LongTaskUnitSpec(id="second", position=1),
            LongTaskUnitSpec(id="dependent", position=2, dependencies=("second",)),
        ), **options,
    ))
    await repository.start(task.id, expected_revision=task.revision)
    return task


async def claim(repository, unit, worker="worker"):
    return await repository.claim_unit("targeted", unit, worker_id=worker, lease_duration_ms=30000)


@pytest.mark.asyncio
async def test_targeted_claim_does_not_substitute_a_ready_sibling(repository):
    await create(repository)
    revision = (await repository.load("targeted")).revision
    assert await claim(repository, "missing") is None
    assert await claim(repository, "dependent") is None
    assert (await repository.load("targeted")).revision == revision
    selected = await claim(repository, "second")
    assert selected.id == "second"
    assert (await repository.list_units("targeted"))[0].attempt == 0
    assert await claim(repository, "second") is None
    await repository.complete_unit("targeted", selected.id, worker_id="worker",
        lease_epoch=selected.lease_epoch, result=LongTaskUnitResult(output_ref="test://second"))
    assert (await claim(repository, "dependent")).id == "dependent"


@pytest.mark.asyncio
async def test_targeted_claim_has_one_winner_and_preserves_parallelism(repository):
    await create(repository)
    claims = await asyncio.gather(*(claim(repository, "second", f"worker-{n}") for n in range(8)))
    assert sum(item is not None for item in claims) == 1
    assert (await claim(repository, "first")).id == "first"
    assert await claim(repository, "dependent") is None
    units = await repository.list_units("targeted")
    assert [unit.attempt for unit in units] == [1, 1, 0]


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["pause", "request_cancel"])
async def test_targeted_claim_cannot_bypass_task_stop(repository, transition):
    await create(repository)
    await getattr(repository, transition)("targeted")
    assert await claim(repository, "second") is None
    assert all(unit.attempt == 0 for unit in await repository.list_units("targeted"))


@pytest.mark.asyncio
async def test_targeted_claim_keeps_budget_gate(repository):
    from purra.long_tasks import LongTaskBudgetLimits, LongTaskUsage
    await create(repository, budget_limits=LongTaskBudgetLimits(max_invocation_attempts=1))
    await repository.record_usage("targeted", run_id="usage-run", usage=LongTaskUsage(invocation_count=1),
        expected_revision=(await repository.load("targeted")).revision)
    assert await claim(repository, "second") is None
    assert all(unit.attempt == 0 for unit in await repository.list_units("targeted"))
