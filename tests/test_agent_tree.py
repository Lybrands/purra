from __future__ import annotations

import json
from pathlib import Path

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
from purra.errors import ContractViolationError


FIXTURE = json.loads(
    (
        Path(__file__).resolve().parents[1]
        / "conformance"
        / "fixtures"
        / "agent_tree_protocol.json"
    ).read_text(encoding="utf-8")
)


def _grant(**changes) -> AgentCapabilityGrant:
    values = {
        "can_spawn_agents": True,
        "max_depth": 3,
        "max_children_per_call": 3,
        "max_agents_per_root": 16,
        "max_parallel_runs": 3,
        "allowed_tools": ("readDocs", "search"),
        "allowed_models": ("test:model",),
    }
    values.update(changes)
    return AgentCapabilityGrant(**values)


async def _root(
    repository: InMemoryRunTreeRepository,
    *,
    run_id: str = "root-run-1",
    agent_id: str = "root-agent",
    grant: AgentCapabilityGrant | None = None,
):
    return await repository.begin_root(BeginRootAgentCommand(
        run_id=run_id,
        agent_id=agent_id,
        name="root",
        title="Root",
        instruction="Own the task.",
        objective="Complete the request.",
        capability_grant=grant or _grant(),
        idempotency_key=f"begin:{run_id}",
    ))


def _spec(
    name: str,
    *,
    required: bool = True,
    priority: int = 0,
    grant: AgentCapabilityGrant | None = None,
) -> ChildAgentSpec:
    return ChildAgentSpec(
        name=name,
        title=f"{name} title",
        instruction=f"Act as {name}.",
        objective=f"Complete {name}.",
        required=required,
        priority=priority,
        capability_grant=grant,
    )


async def _complete(
    repository: InMemoryRunTreeRepository,
    run_id: str,
    *,
    expected_context_version: int = 0,
    result: object = "done",
):
    run = await repository.get_run(run_id)
    return await repository.complete_run(
        run_id,
        expected_context_version=expected_context_version,
        result=result,
        content_ref=f"context://{run_id}",
        fingerprint=f"fingerprint:{run_id}",
        lease_owner_id=run.lease_owner_id,
        lease_epoch=run.lease_epoch if run.lease_owner_id is not None else None,
    )


def test_shared_agent_tree_protocol_matches_python_contracts():
    assert FIXTURE["protocolVersion"] == 1
    assert FIXTURE["agentPresetSnapshotVersion"] == 5
    assert FIXTURE["policyDefaults"] == AgentCapabilityGrant().to_mapping()
    assert FIXTURE["agentNodeStates"] == ["active", "closed"]
    assert FIXTURE["agentRunStatuses"] == [
        "queued",
        "running",
        "waiting",
        "done",
        "failed",
        "canceled",
    ]
    assert "child_run_join_canceled" in FIXTURE["stableErrorCodes"]
    assert "agent_run_resume_checkpoint_missing" in FIXTURE["stableErrorCodes"]
    assert "agent_execution_checkpoint_conflict" in FIXTURE["stableErrorCodes"]
    assert "agent_preset_mismatch" in FIXTURE["stableErrorCodes"]
    assert FIXTURE["recovery"] == {
        "executionCheckpointSchemaVersion": 1,
        "resumablePhase": "model_ready",
        "resumableExecutionProfile": "reactive",
        "inFlightProviderOrToolPolicy": "fail_stop",
    }
    assert "run_not_found" in FIXTURE["stableErrorCodes"]
    assert FIXTURE["authority"]["rootBudgetDimensions"] == [
        "model_attempts",
        "input_tokens",
        "output_tokens",
        "reasoning_tokens",
        "provider_output_events",
        "provider_output_bytes",
    ]
    assert FIXTURE["authority"]["canonicalJournalFields"] == [
        "root_run_id",
        "run_id",
        "agent_id",
        "parent_run_id",
        "root_sequence",
        "source_key",
        "kind",
        "channel",
        "visibility",
        "payload",
    ]
    assert FIXTURE["lease"] == {
        "initialEpoch": 1,
        "reclaimIncrementsEpoch": True,
        "expiryBoundary": "now_greater_than_or_equal_expires_at",
        "fences": [
            "spawn",
            "continue",
            "checkpoint",
            "budget",
            "output",
            "terminal",
        ],
    }
    with pytest.raises(TypeError, match="boolean"):
        AgentCapabilityGrant(can_spawn_agents="yes")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_spawn_is_atomic_idempotent_and_globally_scheduled():
    repository = InMemoryRunTreeRepository()
    root = await _root(repository)
    command = SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="call-1",
        children=(
            _spec("low", priority=0),
            _spec("high", priority=10),
            _spec("middle", priority=5),
        ),
    )

    created = await repository.spawn_agents(command)
    replayed = await repository.spawn_agents(command)

    assert replayed.replayed
    assert [item.run.run_id for item in replayed.items] == [
        item.run.run_id for item in created.items
    ]
    assert [run.priority for run in await repository.list_runnable(root.run_id)] == [
        10,
        5,
        0,
    ]

    claimed = [
        await repository.claim_run(item.run.run_id)
        for item in created.items
    ]
    assert sum(item is not None for item in claimed) == 2
    await repository.mark_waiting(root.run_id)
    assert await repository.claim_run(created.items[2].run.run_id) is not None

    with pytest.raises(ContractViolationError) as conflict:
        await repository.spawn_agents(SpawnAgentsCommand(
            parent_run_id=root.run_id,
            idempotency_key="call-1",
            children=(_spec("different"),),
        ))
    assert conflict.value.code == "child_spawn_idempotency_conflict"

    with pytest.raises(ContractViolationError) as root_conflict:
        await repository.begin_root(BeginRootAgentCommand(
            run_id=root.run_id,
            agent_id="root-agent",
            name="root",
            title="Changed",
            instruction="Own the task.",
            objective="Complete the request.",
            capability_grant=_grant(),
            idempotency_key=f"begin:{root.run_id}",
        ))
    assert root_conflict.value.code == "child_spawn_idempotency_conflict"


@pytest.mark.asyncio
async def test_spawn_replay_survives_parent_terminal_state():
    repository = InMemoryRunTreeRepository()
    root = await _root(repository)
    command = SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="durable-call",
        children=(_spec("child"),),
    )
    created = await repository.spawn_agents(command)
    await repository.mark_waiting(root.run_id)
    await repository.claim_run(created.items[0].run.run_id)
    await _complete(repository, created.items[0].run.run_id)
    await repository.release_waiting(root.run_id)
    await _complete(repository, root.run_id)

    replayed = await repository.spawn_agents(command)
    assert replayed.replayed
    assert replayed.items == created.items


@pytest.mark.asyncio
async def test_recursive_spawn_narrows_authority_and_enforces_depth():
    repository = InMemoryRunTreeRepository()
    root = await _root(repository, grant=_grant(max_depth=2))
    child = (await repository.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="child",
        children=(_spec(
            "child",
            grant=_grant(
                max_depth=2,
                allowed_tools=("readDocs",),
                allowed_models=("test:model",),
            ),
        ),),
    ))).items[0]
    await repository.mark_waiting(root.run_id)
    claimed_child = await repository.claim_run(child.run.run_id)
    assert claimed_child is not None

    grandchild = (await repository.spawn_agents(SpawnAgentsCommand(
        parent_run_id=child.run.run_id,
        idempotency_key="grandchild",
        lease_owner_id=claimed_child.lease_owner_id,
        lease_epoch=claimed_child.lease_epoch,
        children=(_spec("grandchild"),),
    ))).items[0]
    assert grandchild.agent.depth == 2

    await repository.mark_waiting(
        child.run.run_id,
        lease_owner_id=claimed_child.lease_owner_id,
        lease_epoch=claimed_child.lease_epoch,
    )
    claimed_grandchild = await repository.claim_run(grandchild.run.run_id)
    assert claimed_grandchild is not None
    with pytest.raises(ContractViolationError) as depth:
        await repository.spawn_agents(SpawnAgentsCommand(
            parent_run_id=grandchild.run.run_id,
            idempotency_key="too-deep",
            lease_owner_id=claimed_grandchild.lease_owner_id,
            lease_epoch=claimed_grandchild.lease_epoch,
            children=(_spec("great-grandchild"),),
        ))
    assert depth.value.code == "agent_depth_exceeded"

    escalating = _grant(allowed_tools=("readDocs", "search"))
    with pytest.raises(ContractViolationError) as authority:
        await repository.spawn_agents(SpawnAgentsCommand(
            parent_run_id=child.run.run_id,
            idempotency_key="escalate",
            lease_owner_id=claimed_child.lease_owner_id,
            lease_epoch=claimed_child.lease_epoch,
            children=(_spec("escalating", grant=escalating),),
        ))
    assert authority.value.code == "agent_capability_escalation"


@pytest.mark.asyncio
async def test_continue_uses_stable_agent_identity_and_context_cas():
    repository = InMemoryRunTreeRepository()
    root = await _root(repository)
    child = (await repository.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="child",
        children=(_spec("child"),),
    ))).items[0]
    await repository.mark_waiting(root.run_id)
    await repository.claim_run(child.run.run_id)
    await _complete(repository, child.run.run_id, result={"answer": 1})
    child_agent = await repository.get_agent(child.agent.agent_id)
    assert child_agent.context_version == 1

    await repository.release_waiting(root.run_id)
    await _complete(repository, root.run_id)
    await _root(repository, run_id="root-run-2", agent_id="root-agent")

    continued = await repository.continue_agent(ContinueAgentCommand(
        requester_run_id="root-run-2",
        idempotency_key="continue-1",
        agent_id=child.agent.agent_id,
        expected_context_version=1,
        message="Check the answer again.",
    ))
    replayed = await repository.continue_agent(ContinueAgentCommand(
        requester_run_id="root-run-2",
        idempotency_key="continue-1",
        agent_id=child.agent.agent_id,
        expected_context_version=1,
        message="Check the answer again.",
    ))

    assert replayed.replayed
    assert continued.agent.agent_id == child.agent.agent_id
    assert continued.run.previous_run_id == child.run.run_id
    assert continued.run.root_run_id == "root-run-2"

    with pytest.raises(ContractViolationError) as busy:
        await repository.continue_agent(ContinueAgentCommand(
            requester_run_id="root-run-2",
            idempotency_key="continue-2",
            agent_id=child.agent.agent_id,
            expected_context_version=1,
            message="Race.",
        ))
    assert busy.value.code == "agent_busy"

    await repository.claim_run(continued.run.run_id)
    await _complete(
        repository,
        continued.run.run_id,
        expected_context_version=1,
    )
    await _complete(repository, "root-run-2", expected_context_version=1)
    replay_after_terminal = await repository.continue_agent(
        ContinueAgentCommand(
            requester_run_id="root-run-2",
            idempotency_key="continue-1",
            agent_id=child.agent.agent_id,
            expected_context_version=1,
            message="Check the answer again.",
        )
    )
    assert replay_after_terminal.replayed


@pytest.mark.asyncio
async def test_cancel_subtree_preserves_agent_identity_and_join_attribution():
    repository = InMemoryRunTreeRepository()
    root = await _root(repository)
    receipt = await repository.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="children",
        children=(
            _spec("required", required=True),
            _spec("optional", required=False),
        ),
    ))
    required, optional = receipt.items
    await repository.mark_waiting(root.run_id)
    await repository.claim_run(required.run.run_id)
    await repository.claim_run(optional.run.run_id)
    await _complete(repository, optional.run.run_id, result={"ok": True})
    canceled = await repository.cancel_subtree(required.run.run_id)

    aggregation = await repository.aggregate_runs(
        root.run_id,
        (required.run.run_id, optional.run.run_id),
    )

    assert canceled == (required.run.run_id,)
    assert aggregation.state == "blocked"
    assert aggregation.required_failures == (required.run.run_id,)
    assert {
        row["agentId"] for row in aggregation.results
    } == {required.agent.agent_id, optional.agent.agent_id}
    assert (await repository.get_agent(required.agent.agent_id)).state.value == "active"


@pytest.mark.asyncio
async def test_root_cannot_finish_until_direct_children_are_terminal():
    repository = InMemoryRunTreeRepository()
    root = await _root(repository)
    await repository.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="child",
        children=(_spec("child"),),
    ))

    with pytest.raises(ContractViolationError) as error:
        await _complete(repository, root.run_id)
    assert error.value.code == "root_run_not_quiescent"


@pytest.mark.asyncio
async def test_failed_run_cancels_non_terminal_descendants():
    repository = InMemoryRunTreeRepository()
    root = await _root(repository)
    child = (await repository.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="child",
        children=(_spec("child"),),
    ))).items[0]
    await repository.mark_waiting(root.run_id)
    claimed_child = await repository.claim_run(child.run.run_id)
    assert claimed_child is not None
    grandchild = (await repository.spawn_agents(SpawnAgentsCommand(
        parent_run_id=child.run.run_id,
        idempotency_key="grandchild",
        lease_owner_id=claimed_child.lease_owner_id,
        lease_epoch=claimed_child.lease_epoch,
        children=(_spec("grandchild"),),
    ))).items[0]

    await repository.fail_run(
        child.run.run_id,
        "child_failed",
        lease_owner_id=claimed_child.lease_owner_id,
        lease_epoch=claimed_child.lease_epoch,
    )

    assert (await repository.get_run(child.run.run_id)).status is AgentTreeRunStatus.FAILED
    assert (
        await repository.get_run(grandchild.run.run_id)
    ).status is AgentTreeRunStatus.CANCELED


@pytest.mark.asyncio
async def test_expired_lease_fences_old_commands_and_terminal_commit():
    now = [100]
    repository = InMemoryRunTreeRepository(clock_ms=lambda: now[0])
    root = await _root(repository)
    child = (await repository.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="lease-child",
        children=(_spec("lease-child"),),
    ))).items[0]
    await repository.mark_waiting(root.run_id)
    first = await repository.claim_run(
        child.run.run_id,
        owner_id="worker-a",
        lease_duration_ms=10,
    )
    assert first is not None
    assert first.lease_epoch == 1

    now[0] = 110
    with pytest.raises(ContractViolationError) as stale_spawn:
        await repository.spawn_agents(SpawnAgentsCommand(
            parent_run_id=first.run_id,
            idempotency_key="stale-spawn",
            lease_owner_id=first.lease_owner_id,
            lease_epoch=first.lease_epoch,
            children=(_spec("stale"),),
        ))
    assert stale_spawn.value.code == "agent_run_lease_lost"
    with pytest.raises(ContractViolationError) as stale_terminal:
        await _complete(repository, first.run_id)
    assert stale_terminal.value.code == "agent_run_lease_lost"

    assert [run.run_id for run in await repository.list_runnable(root.run_id)] == [
        first.run_id
    ]
    reclaimed = await repository.claim_run(
        first.run_id,
        owner_id="worker-b",
        lease_duration_ms=10,
    )
    assert reclaimed is not None
    assert reclaimed.lease_epoch == 2
    with pytest.raises(ContractViolationError) as old_epoch:
        await repository.complete_run(
            first.run_id,
            expected_context_version=0,
            result="stale",
            content_ref="context://stale",
            fingerprint="stale",
            lease_owner_id=first.lease_owner_id,
            lease_epoch=first.lease_epoch,
        )
    assert old_epoch.value.code == "agent_run_lease_lost"
    await _complete(repository, reclaimed.run_id)
