"""All Agents share one admission path; task payloads have no authority."""
from dataclasses import replace

import pytest
from purra.agent_tree import AgentCapabilityGrant, BeginRootAgentCommand, ChildAgentSpec, SpawnAgentsCommand
from purra.agent_tree_policy import AgentTreePolicy
from purra.errors import ContractViolationError
from purra.storage import StorageSession


def command(key, count, payload=None):
    return SpawnAgentsCommand(parent_run_id="root", idempotency_key=key,
        children=tuple(ChildAgentSpec(name=f"{key}-{n}", title="Synthetic",
            instruction="Own synthetic evidence", objective="Inspect evidence",
            input_payload=payload or {}) for n in range(count)))


@pytest.mark.parametrize("constructor", [AgentCapabilityGrant, AgentTreePolicy])
def test_retired_recipe_quota_is_not_an_option(constructor):
    with pytest.raises(TypeError):
        constructor(max_recipe_children=5)


@pytest.mark.asyncio
async def test_task_payload_cannot_expand_agent_capacity_and_replay_survives_restore():
    session = StorageSession()
    await session.run_tree.begin_root(BeginRootAgentCommand(
        run_id="root", agent_id="root-agent", name="root", title="Root",
        instruction="Coordinate evidence", objective="Synthetic", idempotency_key="root",
        capability_grant=AgentCapabilityGrant(can_spawn_agents=True, max_agents_per_root=4)))
    receipt = await session.run_tree.spawn_agents(command("workers", 3, {"recipeTaskId": "task"}))
    restored = StorageSession(session.export_snapshot()).run_tree
    assert (await restored.spawn_agents(command("workers", 3, {"recipeTaskId": "task"}))).replayed
    assert all(not hasattr(item.agent, "recipe_task_id") for item in receipt.items)
    with pytest.raises(ContractViolationError, match="capacity"):
        await restored.spawn_agents(command("more", 1, {"recipeTaskId": "another-task"}))
    with pytest.raises(ContractViolationError) as conflict:
        await restored.spawn_agents(command("workers", 3, {"recipeTaskId": "different"}))
    assert conflict.value.code == "child_spawn_idempotency_conflict"
    with pytest.raises(TypeError):
        replace(command("retired", 1), recipe_task_id="task")
