import assert from "node:assert/strict";
import test from "node:test";
import { InMemoryLongTaskRepository, claimFromUnit } from "purra";

async function setup() {
  const repository = new InMemoryLongTaskRepository();
  await repository.create("task", {
    namespace: "targeted", kind: "test", ownerId: "test", createdByRunId: "root",
    idempotencyKey: "targeted", deadlineAtMs: null,
    budgets: { maxInvocationAttempts: 10, maxInputTokens: 1000,
      maxRunGenerationTokens: 1000, maxReasoningTokens: 1000 },
    units: ["first", "second", "dependent"].map((id, position) => ({
      id, position, executor: "test", planStepId: id, maxAttempts: 2,
      dependencies: id === "dependent" ? ["second"] : [],
    })),
  });
  await repository.start("task");
  return repository;
}

test("targeted Unit claims never substitute a ready sibling", async () => {
  const repository = await setup();
  const claim = (id) => repository.claimUnit("task", id, "worker", 30000);
  const revision = (await repository.load("task")).revision;
  assert.equal(await claim("missing"), undefined);
  assert.equal(await claim("dependent"), undefined);
  assert.equal((await repository.load("task")).revision, revision);
  const selected = await claim("second");
  assert.equal(selected.id, "second");
  assert.equal((await repository.listUnits("task"))[0].attempt, 0);
  assert.equal(await claim("second"), undefined);
  await repository.completeUnit(claimFromUnit(selected), { outputRef: "test://second" }, "settled");
  assert.equal((await claim("dependent")).id, "dependent");
});

test("concurrent claims on one Unit have exactly one winner", async () => {
  const repository = await setup();
  const claims = await Promise.all(Array.from({ length: 8 }, (_, n) =>
    repository.claimUnit("task", "second", `worker-${n}`, 30000)));
  assert.equal(claims.filter(Boolean).length, 1);
  assert.deepEqual((await repository.listUnits("task")).map(unit => unit.attempt), [0, 1, 0]);
});

for (const transition of ["pause", "requestCancel"]) {
  test(`targeted claims preserve ${transition}`, async () => {
    const repository = await setup();
    await repository[transition]("task");
    assert.equal(await repository.claimUnit("task", "second", "worker", 30000), undefined);
    assert.equal(await repository.claimReadyUnit("task", "worker", 30000), undefined);
    assert.ok((await repository.listUnits("task")).every(unit => unit.attempt === 0));
  });
}
