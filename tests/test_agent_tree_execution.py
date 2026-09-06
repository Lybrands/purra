from __future__ import annotations

import asyncio
import hashlib

import pytest

from purra.agent_tree import (
    AgentCapabilityGrant,
    AgentTreeRunStatus,
    BeginRootAgentCommand,
    ChildAgentSpec,
    ContinueAgentCommand,
    InMemoryRunTreeRepository,
    SpawnAgentsCommand,
)
from purra.agent_tree_execution import (
    AgentTreeExecutionResult,
    AgentTreeRunSupervisor,
    RunCommandService,
)


class _RecursiveExecutor:
    def __init__(self) -> None:
        self.service: RunCommandService | None = None
        self.executed: list[str] = []

    async def execute(self, run, agent, checkpoint, signal=None):
        del checkpoint
        self.executed.append(agent.name)
        if agent.name == "recursive":
            assert self.service is not None
            receipt = await self.service.spawn_agents(SpawnAgentsCommand(
                parent_run_id=run.run_id,
                idempotency_key="nested-call",
                lease_owner_id=run.lease_owner_id,
                lease_epoch=run.lease_epoch,
                children=(ChildAgentSpec(
                    name="grandchild",
                    title="Grandchild",
                    instruction="Finish the nested task.",
                    objective="Nested objective",
                ),),
            ))
            aggregate = await self.service.join_runs(
                run.run_id,
                tuple(item.run.run_id for item in receipt.items),
                signal,
                lease_owner_id=run.lease_owner_id,
                lease_epoch=run.lease_epoch,
            )
            assert aggregate.state == "ready"
        content = f"completed:{agent.name}"
        return AgentTreeExecutionResult(
            status=AgentTreeRunStatus.DONE,
            result={"content": content},
            content_ref=f"memory://{run.run_id}",
            fingerprint=hashlib.sha256(content.encode()).hexdigest(),
        )


async def _root(repository: InMemoryRunTreeRepository):
    return await repository.begin_root(BeginRootAgentCommand(
        run_id="root-run",
        agent_id="root-agent",
        name="root",
        title="Root",
        instruction="Own the task.",
        objective="Complete the task.",
        capability_grant=AgentCapabilityGrant(
            can_spawn_agents=True,
            max_depth=3,
            max_children_per_call=3,
            max_agents_per_root=16,
            max_parallel_runs=1,
        ),
        idempotency_key="root-call",
    ))


@pytest.mark.asyncio
async def test_supervisor_releases_waiting_slots_for_recursive_runs():
    repository = InMemoryRunTreeRepository()
    root = await _root(repository)
    executor = _RecursiveExecutor()
    supervisor = AgentTreeRunSupervisor(
        repository=repository,
        executor=executor,
    )
    service = RunCommandService(repository, supervisor)
    executor.service = service
    receipt = await service.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="root-spawn",
        children=(
            ChildAgentSpec(
                name="recursive",
                title="Recursive",
                instruction="Delegate once.",
                objective="Recursive objective",
                priority=10,
            ),
            ChildAgentSpec(
                name="sibling",
                title="Sibling",
                instruction="Finish directly.",
                objective="Sibling objective",
            ),
        ),
    ))

    aggregate = await service.join_runs(
        root.run_id,
        tuple(item.run.run_id for item in receipt.items),
    )

    assert aggregate.state == "ready"
    assert executor.executed == ["recursive", "grandchild", "sibling"]
    assert (await repository.get_run(root.run_id)).status is AgentTreeRunStatus.RUNNING
    for item in receipt.items:
        assert (await repository.get_run(item.run.run_id)).status is AgentTreeRunStatus.DONE
        assert (await repository.get_agent(item.agent.agent_id)).context_version == 1

    original = receipt.items[0]
    continued = await service.continue_agent(ContinueAgentCommand(
        requester_run_id=root.run_id,
        idempotency_key="continue-recursive",
        agent_id=original.agent.agent_id,
        expected_context_version=1,
        message="Run the same specialist again.",
    ))
    continued_aggregate = await service.join_runs(
        root.run_id,
        (continued.run.run_id,),
    )
    assert continued_aggregate.state == "ready"
    assert continued.run.previous_run_id == original.run.run_id
    assert (
        await repository.get_agent(original.agent.agent_id)
    ).context_version == 2


@pytest.mark.asyncio
async def test_executor_failure_settles_the_child_and_blocks_required_join():
    class _FailingExecutor:
        async def execute(self, run, agent, checkpoint, signal=None):
            del run, agent, checkpoint, signal
            raise RuntimeError("boom")

    repository = InMemoryRunTreeRepository()
    root = await _root(repository)
    supervisor = AgentTreeRunSupervisor(
        repository=repository,
        executor=_FailingExecutor(),
    )
    service = RunCommandService(repository, supervisor)
    receipt = await service.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="failing-call",
        children=(ChildAgentSpec(
            name="failing",
            title="Failing",
            instruction="Fail.",
            objective="Fail safely.",
        ),),
    ))

    aggregate = await service.join_runs(
        root.run_id,
        (receipt.items[0].run.run_id,),
    )

    assert aggregate.state == "blocked"
    assert aggregate.required_failures == (receipt.items[0].run.run_id,)
    assert (
        await repository.get_run(receipt.items[0].run.run_id)
    ).error_code == "RuntimeError"


@pytest.mark.asyncio
async def test_supervisor_renews_child_lease_while_executor_is_active():
    now = [100]
    renewed = asyncio.Event()

    class _CountingRepository(InMemoryRunTreeRepository):
        def __init__(self):
            super().__init__(clock_ms=lambda: now[0])
            self.renewals = 0

        async def renew_run_lease(self, *args, **kwargs):
            # The second renewal crosses the original nine-millisecond lease.
            now[0] += 6
            result = await super().renew_run_lease(*args, **kwargs)
            self.renewals += 1
            if self.renewals >= 2:
                renewed.set()
            return result

    class _SlowExecutor:
        async def execute(self, run, agent, checkpoint, signal=None):
            del agent, checkpoint, signal
            await asyncio.wait_for(renewed.wait(), timeout=2)
            return AgentTreeExecutionResult(
                status=AgentTreeRunStatus.DONE,
                result="done",
                content_ref=f"memory://{run.run_id}",
                fingerprint="done",
            )

    repository = _CountingRepository()
    root = await _root(repository)
    supervisor = AgentTreeRunSupervisor(
        repository=repository,
        executor=_SlowExecutor(),
        lease_duration_ms=9,
    )
    service = RunCommandService(repository, supervisor)
    child = await service.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="slow-child",
        children=(ChildAgentSpec(
            name="slow",
            title="Slow",
            instruction="Wait.",
            objective="Complete after renewal.",
        ),),
    ))

    result = await service.join_runs(
        root.run_id,
        (child.items[0].run.run_id,),
    )

    assert result.state == "ready"
    assert repository.renewals >= 2


@pytest.mark.asyncio
async def test_new_supervisor_reclaims_committed_child_once_after_worker_crash():
    now = [100]
    repository = InMemoryRunTreeRepository(clock_ms=lambda: now[0])
    root = await _root(repository)
    child = (await repository.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="crash-before-execute",
        children=(ChildAgentSpec(
            name="recovered",
            title="Recovered",
            instruction="Run after restart.",
            objective="Execute exactly once.",
        ),),
    ))).items[0]
    await repository.mark_waiting(root.run_id)
    abandoned = await repository.claim_run(
        child.run.run_id,
        owner_id="crashed-worker",
        lease_duration_ms=10,
    )
    assert abandoned is not None
    assert abandoned.lease_epoch == 1

    executions: list[tuple[str, int]] = []

    class _RecoveredExecutor:
        async def execute(self, run, agent, checkpoint, signal=None):
            del agent, checkpoint, signal
            executions.append((run.run_id, run.lease_epoch))
            return AgentTreeExecutionResult(
                status=AgentTreeRunStatus.DONE,
                result={"content": "recovered"},
                content_ref=f"memory://{run.run_id}",
                fingerprint="recovered",
            )

    now[0] = 110
    recovered = RunCommandService(
        repository,
        AgentTreeRunSupervisor(
            repository=repository,
            executor=_RecoveredExecutor(),
            owner_id="recovery-worker",
            lease_duration_ms=10,
        ),
    )
    aggregate = await recovered.join_runs(root.run_id, (child.run.run_id,))

    assert aggregate.state == "ready"
    assert executions == [(child.run.run_id, 2)]
    settled = await repository.get_run(child.run.run_id)
    assert settled.status is AgentTreeRunStatus.DONE
    assert settled.lease_epoch == 2

    replay = await recovered.join_runs(root.run_id, (child.run.run_id,))
    assert replay == aggregate
    assert executions == [(child.run.run_id, 2)]


@pytest.mark.asyncio
async def test_join_cancellation_wakes_even_when_executor_ignores_the_signal():
    class _IgnoringExecutor:
        async def execute(self, run, agent, checkpoint, signal=None):
            del run, agent, checkpoint, signal
            await asyncio.Event().wait()

    repository = InMemoryRunTreeRepository()
    root = await _root(repository)
    service = RunCommandService(
        repository,
        AgentTreeRunSupervisor(
            repository=repository,
            executor=_IgnoringExecutor(),
        ),
    )
    receipt = await service.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="cancel-call",
        children=(ChildAgentSpec(
            name="stuck",
            title="Stuck",
            instruction="Wait.",
            objective="Wait forever.",
        ),),
    ))
    signal = asyncio.Event()
    joined = asyncio.create_task(service.join_runs(
        root.run_id,
        (receipt.items[0].run.run_id,),
        signal,
    ))
    await asyncio.sleep(0)
    signal.set()

    with pytest.raises(asyncio.CancelledError) as error:
        await asyncio.wait_for(joined, timeout=1)
    assert error.value.code == "child_run_join_canceled"
    assert (
        await repository.get_run(receipt.items[0].run.run_id)
    ).status is AgentTreeRunStatus.CANCELED
    assert (await repository.get_run(root.run_id)).status is AgentTreeRunStatus.RUNNING
