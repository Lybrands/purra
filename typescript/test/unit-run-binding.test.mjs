import assert from "node:assert/strict";
import test from "node:test";
import { InMemoryLongTaskRepository, claimFromUnit } from "purra";

test("Unit Run identity is immutable, unique, and lease fenced", async () => {
  let now = 100;
  const repository = new InMemoryLongTaskRepository({ clockMs: () => now });
  await repository.create("task", {
    namespace: "binding", kind: "test", ownerId: "test", createdByRunId: "root",
    idempotencyKey: "binding", deadlineAtMs: null,
    budgets: { maxInvocationAttempts: 10, maxInputTokens: 1000,
      maxRunGenerationTokens: 1000, maxReasoningTokens: 1000 },
    units: ["a", "b"].map((id, position) => ({
      id, position, executor: "test", planStepId: id, maxAttempts: 2,
    })),
  });
  await repository.start("task");
  const first = claimFromUnit(await repository.claimReadyUnit("task", "worker", 10));
  const second = claimFromUnit(await repository.claimReadyUnit("task", "worker", 10));
  const bound = await repository.bindUnitRun(first, "child-a");
  const revision = (await repository.load("task")).revision;
  assert.deepEqual(await repository.bindUnitRun(first, "child-a"), bound);
  const conflict = (error) => error.code === "long_task_unit_run_conflict";
  await assert.rejects(repository.bindUnitRun(first, "other"), conflict);
  await assert.rejects(repository.bindUnitRun(second, "child-a"), conflict);
  await assert.rejects(repository.completeUnit(first, { runId: "other" }, "bad"), conflict);
  await assert.rejects(repository.completeUnit(second, { runId: "child-a" }, "bad"), conflict);
  assert.equal((await repository.load("task")).revision, revision);
  const result = { runId: "child-a", outputRef: "test://result" };
  const completed = await repository.completeUnit(first, result, "settled");
  assert.deepEqual(await repository.completeUnit(first, result, "settled"), completed);
  await assert.rejects(repository.completeUnit(first, { runId: "forged" }, "settled"), conflict);
  await repository.bindUnitRun(second, "child-b");
  now = 110;
  const retry = claimFromUnit(await repository.claimReadyUnit("task", "worker", 10));
  await assert.rejects(repository.bindUnitRun(second, "stale"),
    (error) => error.code === "long_task_unit_lease_lost");
  assert.equal((await repository.bindUnitRun(retry, "child-b-retry")).runId, "child-b-retry");
});
