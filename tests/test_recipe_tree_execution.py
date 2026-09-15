import asyncio

import pytest

from purra.agent_tree import AgentCapabilityGrant, BeginRootAgentCommand

from purra.agent_tree_execution import AgentTreeRunSupervisor, RunCommandService
from purra.errors import ContractViolationError
from purra.long_tasks import (
    DurableExecutorRegistry, LongTaskCreateCommand, LongTaskUnitResult,
    LongTaskRunRelation, LongTaskUnitSpec, RecipeLongTaskDispatcher,
)
from purra.storage import StorageSession


async def setup(executor, *, deliver=None, preflight=None):
    session = StorageSession()
    tree, tasks = session.run_tree, session.long_tasks
    await tree.begin_root(BeginRootAgentCommand(
        run_id="root", agent_id="root-agent", name="root", title="Root", instruction="Own",
        objective="Synthetic Recipe", capability_grant=AgentCapabilityGrant(can_spawn_agents=True),
        idempotency_key="root",
    ))
    await tasks.create("recipe", LongTaskCreateCommand(
        namespace="test", kind="test", owner_id="synthetic", created_by_run_id="root",
        max_parallelism=2, units=tuple(LongTaskUnitSpec(
            id=name, position=position, dependencies=("fast",) if name == "dependent" else (),
            metadata={"executor": "fixture"},
        ) for position, name in enumerate(("fast", "slow", "dependent"))),
    ))
    dispatcher = RecipeLongTaskDispatcher(long_task_repository=tasks,
        descriptor_resolver=None, executor_registry=DurableExecutorRegistry({"fixture": executor}),
        worker_id="fixture-worker")
    return tree, tasks, dispatcher


@pytest.mark.asyncio
async def test_recipe_operations_share_run_and_complete_dependencies_while_other_work_runs():
    release = asyncio.Event()
    delivered = asyncio.Event()
    calls, deliveries, updates = [], [], []

    class Executor:
        async def execute(self, context, signal=None):
            assert context.run_id == "root"
            assert context.tree_run is None and context.tree_agent is None
            assert context.unit.run_id is None
            await context.bind_run("root")
            calls.append(context.unit.id)
            if context.unit.id == "slow":
                await release.wait()
            if context.unit.id == "dependent":
                assert context.dependency_outputs == {"fast": "test://fast"}
            return LongTaskUnitResult(output_ref=f"test://{context.unit.id}",
                metadata={"finalResponse": f"Result {context.unit.id}"})

    async def deliver(root, results, signal):
        deliveries.extend(results)
        delivered.set()

    async def observe(update):
        updates.append(update)
        if "dependent" in calls:
            delivered.set()

    tree, tasks, dispatcher = await setup(Executor(), deliver=deliver)
    execution = asyncio.create_task(dispatcher.execute("recipe", run_id="root", observer=observe))
    try:
        await asyncio.wait_for(delivered.wait(), 2)
        assert not execution.done()
        assert (await tree.get_run("root")).status.value == "running"
        descendants = await tree.list_descendants("root")
        assert descendants == () or descendants == []
        release.set()
        result = await asyncio.wait_for(execution, 2)
        assert result.status.value == "completed"
        assert calls.index("fast") < calls.index("dependent")
        assert not deliveries
        assert (await tree.get_run("root")).status.value == "running"
        assert all(unit.run_id is None for unit in await tasks.list_units("recipe"))
        assert result.final_response == "Result dependent"
        assert updates
    finally:
        release.set()
        if not execution.done():
            execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)


@pytest.mark.asyncio
async def test_explicit_continuation_executes_only_unfinished_operations():
    calls = []

    class Executor:
        async def execute(self, context, signal=None):
            calls.append(context.unit.id)
            assert context.run_id == "continued-root"
            return LongTaskUnitResult(
                output_ref=f"test://{context.unit.id}",
                metadata={"finalResponse": f"Result {context.unit.id}"},
            )

    async def observe(update):
        pass

    tree, tasks, dispatcher = await setup(Executor())
    task = await tasks.load("recipe")
    task = await tasks.start(task.id, expected_revision=task.revision)
    fast = await tasks.claim_unit(
        task.id, "fast", worker_id="prior-worker", lease_duration_ms=30_000,
    )
    fast = await tasks.bind_unit_run(
        task.id, fast.id, worker_id="prior-worker",
        lease_epoch=fast.lease_epoch, run_id="prior-fast-child",
    )
    await tasks.complete_unit(
        task.id, fast.id, worker_id="prior-worker",
        lease_epoch=fast.lease_epoch,
        result=LongTaskUnitResult(
            output_ref="test://fast", run_id="prior-fast-child",
        ),
    )
    await tasks.pause(task.id)
    await tree.begin_root(BeginRootAgentCommand(
        run_id="continued-root", agent_id="continued-root-agent",
        name="continued-root", title="Continued Root", instruction="Resume",
        objective="Finish the remaining Recipe Units.",
        capability_grant=AgentCapabilityGrant(
            can_spawn_agents=True,
        ),
        idempotency_key="continued-root",
    ))
    await dispatcher.prepare_continuation(
        task.id,
        source_run_id="root",
        run_id="continued-root",
    )
    await tasks.resume(task.id)

    result = await dispatcher.execute(
        task.id, run_id="continued-root", observer=observe,
    )

    assert result.status.value == "completed"
    assert set(calls) == {"slow", "dependent"}
    assert "fast" not in calls
    descendants = await tree.list_descendants("continued-root")
    assert not descendants


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["unbound", "legacy"])
async def test_recipe_rejects_unbound_or_legacy_execution_before_work(gate):
    class Executor:
        async def execute(self, context, signal=None):
            pytest.fail("No operation may execute")
    tree, tasks, dispatcher = await setup(Executor())
    if gate == "legacy":
        task = await tasks.load("recipe")
        await tasks.start(task.id, expected_revision=task.revision)
        unit = await tasks.claim_unit(task.id, "fast", worker_id="previous", lease_duration_ms=30000)
        await tasks.bind_unit_run(task.id, unit.id, worker_id="previous", lease_epoch=unit.lease_epoch, run_id="legacy-child")
    revision = (await tasks.load("recipe")).revision
    async def observe(update):
        pytest.fail("No progress before validation")
    with pytest.raises(ContractViolationError) as error:
        await dispatcher.execute("recipe", run_id="unbound" if gate == "unbound" else "root", observer=observe)
    assert error.value.code == ("recipe_root_binding_conflict" if gate == "unbound" else "recipe_tree_reconciliation_required")
    assert not await tree.list_descendants("root")
    assert (await tasks.load("recipe")).revision == revision


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted", [False, True])
async def test_cancel_recipe_stops_children_and_preserves_task_checkpoint(persisted):
    started, signal = asyncio.Event(), asyncio.Event()
    class Executor:
        async def execute(self, context, signal=None):
            started.set()
            await asyncio.Event().wait()
    async def observe(update):
        pass
    tree, tasks, dispatcher = await setup(Executor())
    execution = asyncio.create_task(dispatcher.execute("recipe", run_id="root", observer=observe, signal=signal))
    try:
        await asyncio.wait_for(started.wait(), 2)
        if persisted:
            await tasks.request_cancel("recipe")
        else:
            signal.set()
        result = await asyncio.wait_for(execution, 2)
        assert result.status.value == ("canceled" if persisted else "paused")
        assert all(child.terminal for child in await tree.list_descendants("root"))
        assert (await tasks.load("recipe")).status.value == ("canceled" if persisted else "paused")
    finally:
        if not execution.done():
            execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)


@pytest.mark.asyncio
async def test_recipe_cannot_replace_child_identity_or_retry_implicitly():
    calls = []
    class Executor:
        async def execute(self, context, signal=None):
            calls.append(context.unit.id)
            if context.unit.id == "fast":
                await context.bind_run("independent-root")
            return LongTaskUnitResult(output_ref=f"test://{context.unit.id}")
    async def observe(update):
        pass
    tree, tasks, dispatcher = await setup(Executor())
    with pytest.raises(ContractViolationError) as failed:
        await dispatcher.execute("recipe", run_id="root", observer=observe)
    assert failed.value.code == "long_task_unit_run_conflict"
    assert (await tasks.load("recipe")).status.value == "paused"
    assert calls.count("fast") == 1 and "dependent" not in calls
    children = await tree.list_descendants("root")
    assert not children
    assert all(unit.run_id != "independent-root" for unit in await tasks.list_units("recipe"))
    repeated = await dispatcher.execute("recipe", run_id="root", observer=observe)
    assert repeated.status.value == "paused"
    assert calls.count("fast") == 1
    assert not await tree.list_descendants("root")
