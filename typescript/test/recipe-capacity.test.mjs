import assert from "node:assert/strict";
import test from "node:test";
import { AgentCapabilityGrant, InMemoryRunTreeRepository } from "purra";

const command = (key, count, task) => ({parentRunId: "root", idempotencyKey: key,
  children: Array.from({length: count}, (_, n) => ({
    name: `${key}-${n}`, title: "Synthetic", instruction: "Own evidence",
    objective: "Inspect evidence", input: {recipeTaskId: task},
  }))});

test("task payload cannot expand Agent capacity; restore retains one admission path", async () => {
  const tree = new InMemoryRunTreeRepository();
  await tree.beginRoot({runId: "root", agentId: "root-agent", name: "root", title: "Root",
    instruction: "Coordinate", objective: "Synthetic", idempotencyKey: "root",
    capabilityGrant: new AgentCapabilityGrant({canSpawnAgents: true, maxAgentsPerRoot: 4})});
  const receipt = await tree.spawnAgents(command("workers", 3, "task"));
  const restored = new InMemoryRunTreeRepository();
  restored.importState(tree.exportState());
  assert.equal((await restored.spawnAgents(command("workers", 3, "task"))).replayed, true);
  for (const item of receipt.items) assert.equal("recipeTaskId" in item.agent, false);
  const before = restored.exportState();
  await assert.rejects(restored.spawnAgents(command("more", 1, "other")), {code: "agent_capacity_exceeded"});
  assert.equal(restored.exportState(), before);
  await assert.rejects(restored.spawnAgents(command("workers", 3, "other")), {code: "child_spawn_idempotency_conflict"});
});
