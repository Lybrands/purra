import assert from "node:assert/strict";
import test from "node:test";

import { AgentError, AgentOperationController } from "purra";

test("Operation lifecycle persists one start and one monotonic terminal event", async () => {
  const events = [];
  const wallTimes = [new Date("2026-08-24T00:00:00.000Z"), new Date("2026-08-24T00:00:01.000Z")];
  const monotonicTimes = [10, 2355];
  const controller = new AgentOperationController({
    acceptOperationEvent(event) { events.push(event); },
  }, {
    idFactory: () => "operation-fixture",
    wallClock: () => wallTimes.shift(),
    monotonicClock: () => monotonicTimes.shift(),
  });

  const receipt = await controller.start("model", {
    runId: "run-1",
    invocationId: "invocation-1",
    display: { labelKey: "agent.model", labelParams: { attempt: 1 } },
  });
  assert.deepEqual(controller.runningOperationIds, ["operation-fixture"]);
  const finished = await controller.succeed(receipt.operationId);

  assert.equal(finished.status, "succeeded");
  assert.equal(finished.durationMs, 2345);
  assert.deepEqual(controller.runningOperationIds, []);
  assert.deepEqual(events.map((event) => event.type), ["operation.started", "operation.finished"]);
  await rejectsContract(controller.succeed(receipt.operationId));
});

test("Operation persistence failure does not advance lifecycle authority", async () => {
  let failTerminal = true;
  const events = [];
  const controller = new AgentOperationController({
    acceptOperationEvent(event) {
      if (event.type === "operation.finished" && failTerminal) throw new Error("storage failed");
      events.push(event);
    },
  }, { idFactory: () => "operation-retry" });
  const receipt = await controller.start("validation", { runId: "run-1" });

  await assert.rejects(controller.fail(receipt.operationId, "invalid_output"), /storage failed/);
  assert.deepEqual(controller.runningOperationIds, [receipt.operationId]);
  failTerminal = false;
  assert.equal((await controller.fail(receipt.operationId, "invalid_output")).status, "failed");
  assert.deepEqual(events.map((event) => event.type), ["operation.started", "operation.finished"]);
});

test("Operation display cannot impersonate lifecycle authority", async () => {
  const controller = new AgentOperationController({ acceptOperationEvent() {} });
  await assert.rejects(
    controller.start("tool", {
      runId: "run-1",
      display: { labelParams: { duration_ms: 100 } },
    }),
    /cannot include a lifecycle field/,
  );
  await rejectsContract(controller.cancel("missing"));
});

async function rejectsContract(promise) {
  await assert.rejects(
    promise,
    (error) => error instanceof AgentError && error.code === "operation_contract_violation",
  );
}
