import asyncio

import pytest

from purra.agent_tree import (
    AgentCapabilityGrant, BeginRootAgentCommand, ChildAgentSpec,
    SpawnAgentsCommand,
)

from purra.agent_tree_execution import AgentTreeExecutionResult, AgentTreeRunSupervisor, RunCommandService
from purra.errors import ContractViolationError
from purra.storage import StorageSession, dump_storage_value, load_storage_value


def child(name, dependencies=()):
    return ChildAgentSpec(name=name, title=name, instruction=name, objective=name, depends_on=dependencies)


async def setup():
    session = StorageSession()
    repository = session.run_tree
    await repository.begin_root(BeginRootAgentCommand(
        run_id="root", agent_id="root-agent", name="root", title="Root", instruction="Own",
        objective="Analyze", capability_grant=AgentCapabilityGrant(can_spawn_agents=True), idempotency_key="root",
    ))
    command = SpawnAgentsCommand(parent_run_id="root", idempotency_key="plan",
        children=(child("analysis", ("read",)), child("read"), child("slow")))
    receipt = await repository.spawn_agents(command)
    assert load_storage_value(dump_storage_value(receipt)) == receipt
    ids = {item.agent.name: item.run.run_id for item in receipt.items}
    restored = StorageSession(session.export_snapshot())
    return restored.run_tree, command, ids


@pytest.mark.asyncio
async def test_dependency_wait_and_root_delivery_share_the_existing_scheduler():
    repository, command, ids = await setup()
    release_analysis, release_slow = asyncio.Event(), asyncio.Event()
    calls, deliveries = [], []

    class Executor:
        async def execute(self, run, agent, checkpoint, signal=None):
            calls.append(agent.name)
            if agent.name == "analysis":
                assert (await repository.get_run(ids["read"])).status.value == "done"
                await release_analysis.wait()
            if agent.name == "slow":
                await release_slow.wait()
            return AgentTreeExecutionResult(status="done", result=agent.name, content_ref=agent.name, fingerprint=agent.name)

    async def deliver(root, results, signal):
        deliveries.append({item["runId"] for item in results})
        if ids["read"] in deliveries[-1]:
            assert (await repository.get_run(ids["slow"])).status.value == "running"
            release_analysis.set()
        if ids["analysis"] in deliveries[-1]:
            release_slow.set()

    supervisor = AgentTreeRunSupervisor(repository=repository, executor=Executor(), deliver_results=deliver)
    assert await repository.claim_run(ids["analysis"]) is None
    with pytest.raises(ContractViolationError) as incomplete:
        await supervisor.execute_and_join("root", (ids["analysis"],))
    assert incomplete.value.code == "agent_dependency_join_incomplete"
    assert (await repository.get_run("root")).status.value == "running"
    commands = RunCommandService(repository, supervisor)
    received = []
    try:
        while True:
            result = await asyncio.wait_for(commands.receive_runs("root", tuple(ids.values()), after_run_ids=received), 2)
            received.extend(item["runId"] for item in result.results)
            if not result.pending_run_ids:
                break
        await commands.results.wait("root")
    finally:
        release_analysis.set()
        release_slow.set()
        await commands.results.close()
    assert result.state == "ready"
    assert calls.index("read") < calls.index("analysis")
    assert deliveries == [{ids["read"]}, {ids["analysis"]}, {ids["slow"]}]
    assert (await repository.spawn_agents(command)).replayed
    changed = SpawnAgentsCommand(parent_run_id="root", idempotency_key="plan",
        children=(child("analysis"), child("read"), child("slow")))
    with pytest.raises(ContractViolationError) as conflict:
        await repository.spawn_agents(changed)
    assert conflict.value.code == "child_spawn_idempotency_conflict"


@pytest.mark.asyncio
async def test_failed_predecessor_blocks_dependent_without_executing_it():
    repository, _, ids = await setup()
    calls = []

    class Executor:
        async def execute(self, run, agent, checkpoint, signal=None):
            calls.append(agent.name)
            if agent.name == "read":
                raise RuntimeError("read failed")
            return AgentTreeExecutionResult(status="done", result=agent.name, content_ref=agent.name, fingerprint=agent.name)

    async def deliver(*args):
        pass

    supervisor = AgentTreeRunSupervisor(repository=repository, executor=Executor(), deliver_results=deliver)
    result = await supervisor.execute_and_join("root", tuple(ids.values()))
    assert result.state == "blocked"
    assert "analysis" not in calls
    assert (await repository.get_run(ids["analysis"])).error_code == "agent_dependency_failed"


@pytest.mark.parametrize("children", [
    (child("a", ("missing",)),), (child("a", ("a",)),),
    (child("a", ("b",)), child("b", ("a",))),
])
def test_invalid_dependency_graph_rejected_before_spawn(children):
    with pytest.raises(ValueError):
        SpawnAgentsCommand(parent_run_id="root", idempotency_key="invalid", children=children)


@pytest.mark.asyncio
async def test_suspended_dependency_keeps_consumer_pending_without_stalling():
    from purra.interaction import UserInputRequired
    repository, _, ids = await setup()
    calls = []

    class Executor:
        async def execute(self, run, agent, checkpoint, signal=None):
            calls.append(agent.name)
            if agent.name == "read":
                raise UserInputRequired(run.run_id, "input")
            return AgentTreeExecutionResult(status="done", result=agent.name, content_ref=agent.name, fingerprint=agent.name)

    async def deliver(*args):
        pass

    supervisor = AgentTreeRunSupervisor(repository=repository, executor=Executor(), deliver_results=deliver)
    result = await asyncio.wait_for(supervisor.execute_and_join("root", tuple(ids.values())), 2)
    assert result.state == "pending"
    assert set(result.pending_run_ids) == {ids["read"], ids["analysis"]}
    assert "analysis" not in calls


@pytest.mark.asyncio
async def test_cancel_stops_active_and_dependency_blocked_runs():
    repository, _, ids = await setup()
    started, signal = asyncio.Event(), asyncio.Event()
    calls = []

    class Executor:
        async def execute(self, run, agent, checkpoint, signal=None):
            calls.append(agent.name)
            started.set()
            await asyncio.Event().wait()

    async def deliver(*args):
        raise AssertionError("No result should be presented")

    supervisor = AgentTreeRunSupervisor(repository=repository, executor=Executor(), deliver_results=deliver)
    execution = asyncio.create_task(supervisor.execute_and_join("root", tuple(ids.values()), signal))
    await asyncio.wait_for(started.wait(), 1)
    signal.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(execution, 1)
    assert "analysis" not in calls
    for identity in ids.values():
        assert (await repository.get_run(identity)).status.value == "canceled"


@pytest.mark.asyncio
@pytest.mark.parametrize("dependency", ["missing", "analysis"])
async def test_stored_dependency_graph_rejects_missing_nodes_and_cycles(dependency):
    from dataclasses import replace
    from purra.agent_tree import validate_stored_dependencies
    repository, _, ids = await setup()
    records = {"root": await repository.get_run("root")}
    records.update({identity: await repository.get_run(identity) for identity in ids.values()})
    records[ids["read"]] = replace(records[ids["read"]], dependency_run_ids=(ids.get(dependency, dependency),))
    with pytest.raises(ValueError):
        validate_stored_dependencies(records)
