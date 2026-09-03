import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  Agent,
  AgentError,
  DurableExecutorRegistry,
  HmacRecoveryAuthenticator,
  InMemoryLongTaskRepository,
  RecipeLongTaskDispatcher,
  claimFromUnit,
  decideOrphanRun,
  OrphanRecoveryCoordinator,
  validateContinuation,
} from "purra";

const SECRET = "phase-six-recovery-secret-has-at-least-32-bytes";
const durableFixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/durable_protocol.json", import.meta.url),
  "utf8",
));

const RUN_OPTIONS = Object.freeze({
  budgets: Object.freeze({ maxRunOutputTokens: null }),
});

test("shared Durable classifications and exact lease boundary stay aligned", () => {
  assert.equal(durableFixture.protocolVersion, 4);
  assert.equal(durableFixture.agentPresetSnapshotVersion, 4);
  assert.deepEqual(durableFixture.stableErrorCodes, {
    activityDeadline: "model_activity_deadline_exceeded",
    progressDeadline: "model_progress_deadline_exceeded",
    invocationDeadline: "model_invocation_deadline_exceeded",
    runDeadline: "run_deadline_exceeded",
    taskDeadline: "long_task_deadline_exceeded",
    leaseLost: "long_task_unit_lease_lost",
    budgetExceeded: "runtime_budget_exceeded",
    streamLimit: "model_stream_limit_exceeded",
  });
  for (const row of durableFixture.budgetCases) {
    const { usage, limits } = row;
    const missing = usage.unreportedUsageAttempts > 0 && [
      limits.maxInputTokens,
      limits.maxRunOutputTokens,
      limits.maxReasoningTokens,
    ].some((value) => value !== null);
    const actual = missing ? "provider_usage_unreported" : [
      ["model_attempts", usage.invocationCount, limits.maxInvocationAttempts],
      ["input_tokens", usage.inputTokens, limits.maxInputTokens],
      ["output_tokens", usage.outputTokens, limits.maxRunOutputTokens],
      ["reasoning_tokens", usage.reasoningTokens, limits.maxReasoningTokens],
    ].find(([, used, maximum]) => (
      maximum !== null && (used > maximum || (row.inclusive && used >= maximum))
    ))?.[0];
    assert.equal(actual, row.budgetKind, row.name);
  }
  assert.equal(
    createHash("sha256")
      .update(JSON.stringify(sortJson(durableFixture.providerDeltaBatch.entries)))
      .digest("hex"),
    durableFixture.providerDeltaBatch.payloadDigest,
  );
  for (const row of durableFixture.leaseExpiryCases) {
    assert.equal(row.leaseExpiresAtMs <= row.nowMs, row.expired, row.name);
  }
  for (const row of durableFixture.orphanCases) {
    const decision = decideOrphanRun(row.candidate);
    assert.equal(decision.disposition, row.disposition, row.name);
    assert.equal(decision.reason, row.reason, row.name);
    assert.equal(decision.terminalStatus, row.terminalStatus, row.name);
  }
});

test("shared Runtime and output batch defaults stay aligned", async () => {
  const agent = new Agent({
    model: {
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        yield { contentDelta: "a" };
        await new Promise((resolve) => setTimeout(resolve, 75));
        yield { contentDelta: "b", finishReason: "stop" };
      },
    },
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "defaults" }] },
    RUN_OPTIONS,
  );
  await handle.result;
  const snapshot = await handle.snapshot();
  const events = [];
  for await (const event of handle.events({ visibility: "all" })) events.push(event);

  assert.equal(
    snapshot.budgets.maxOutputBytes,
    durableFixture.runtimeDefaults.maxProviderOutputBytes,
  );
  assert.deepEqual(snapshot.preset.runtimeLimits, {
    runTimeoutMs: 900_000,
    activityIdleTimeoutMs: durableFixture.runtimeDefaults.providerActivityIdleTimeoutMs,
    progressIdleTimeoutMs: durableFixture.runtimeDefaults.providerProgressIdleTimeoutMs,
    invocationTimeoutMs: durableFixture.runtimeDefaults.providerInvocationTimeoutMs,
    maxChunks: 100_000,
    maxContentChars: 1_000_000,
    maxReasoningChars: 1_000_000,
    maxToolArgumentChars: 1_000_000,
  });
  assert.deepEqual(durableFixture.outputBatchDefaults, {
    maxPayloadBytes: 16_384,
    maxFragments: 64,
    maxLatencyMs: 25,
    maxBackgroundLatencyMs: 250,
  });
  assert.equal(events.filter((event) => event.kind === "provider.delta_batch").length, 1);
});

test("repository fences an expired same-worker claim and settles replay once", async () => {
  let now = 1_000;
  const tokens = ["claim-a", "claim-b"];
  const repository = new InMemoryLongTaskRepository({
    clockMs: () => now,
    tokenFactory: () => tokens.shift(),
  });
  await repository.create("task-1", taskCommand([
    { id: "unit-1", position: 0, maxAttempts: 3 },
  ]));
  await repository.start("task-1");

  const first = claimFromUnit(await repository.claimReadyUnit("task-1", "worker", 10));
  await repository.markUnitRunning(first);
  now = 1_010;
  const secondUnit = await repository.claimReadyUnit("task-1", "worker", 10);
  const second = claimFromUnit(secondUnit);
  assert.equal(second.leaseEpoch, first.leaseEpoch + 1);
  assert.notEqual(second.claimToken, first.claimToken);
  await assert.rejects(
    repository.heartbeat(first, 10),
    (error) => error instanceof AgentError && error.code === "long_task_unit_lease_lost",
  );

  await repository.markUnitRunning(second);
  await repository.appendCheckpoint(second, { progress: 1 });
  await repository.appendCheckpoint(second, { progress: 2 });
  await repository.recordUsage(second, { inputTokens: 3, outputTokens: 2 });
  const completed = await repository.completeUnit(
    second,
    { outputRef: "artifact://one" },
    "settlement-1",
  );
  const replay = await repository.completeUnit(
    first,
    { outputRef: "must-not-replace" },
    "settlement-1",
  );
  assert.equal(replay.outputRef, completed.outputRef);
  assert.deepEqual(
    (await repository.listCheckpoints("task-1", "unit-1")).map((item) => item.sequence),
    [1, 2],
  );
  assert.equal((await repository.load("task-1")).usage.invocationCount, 1);

  await repository.bindRun("task-1", "run-2", "continuation");
  await repository.bindRun("task-1", "run-2", "continuation");
  assert.deepEqual(
    (await repository.listRunBindings("task-1")).map((item) => [item.runId, item.relation]),
    [["run-1", "created"], ["run-2", "continuation"]],
  );
});

test("Long Task budgets fail before the next claim and preserve unreported usage", async () => {
  const repository = new InMemoryLongTaskRepository({ tokenFactory: () => "budget-claim" });
  await repository.create("task-budget", {
    ...taskCommand([
      { id: "unit-1", position: 0 },
      { id: "unit-2", position: 1, dependencies: ["unit-1"] },
    ]),
    idempotencyKey: "budget-task",
    budgets: {
      maxInvocationAttempts: 1,
      maxInputTokens: null,
      maxRunOutputTokens: null,
      maxReasoningTokens: null,
    },
  });
  await repository.start("task-budget");
  const first = claimFromUnit(await repository.claimReadyUnit("task-budget", "worker", 60_000));
  await repository.markUnitRunning(first);
  await repository.recordUsage(first, { inputTokens: 1, outputTokens: 1 });
  await repository.completeUnit(first, { outputRef: "result:1" }, "budget-settlement");
  assert.equal(await repository.claimReadyUnit("task-budget", "worker", 60_000), undefined);
  assert.equal((await repository.load("task-budget")).status, "failed");
  assert.equal((await repository.listUnits("task-budget"))[1].errorCode, "runtime_budget_exceeded");

  const missing = new InMemoryLongTaskRepository({ tokenFactory: () => "missing-claim" });
  await missing.create("task-missing", {
    ...taskCommand([{ id: "unit-1", position: 0 }]),
    idempotencyKey: "missing-task",
    budgets: {
      maxInvocationAttempts: null,
      maxInputTokens: 10,
      maxRunOutputTokens: null,
      maxReasoningTokens: null,
    },
  });
  await missing.start("task-missing");
  const claim = claimFromUnit(await missing.claimReadyUnit("task-missing", "worker", 60_000));
  await missing.markUnitRunning(claim);
  await assert.rejects(
    missing.recordUsage(claim, null),
    (error) => error instanceof AgentError && error.code === "runtime_budget_exceeded",
  );
  assert.equal((await missing.load("task-missing")).usage.unreportedUsageAttempts, 1);
});

test("recipe dispatcher resumes a DAG without replaying completed units and retries safely", async () => {
  let now = 2_000;
  let taskSequence = 0;
  const calls = [];
  let secondAttempts = 0;
  const repository = new InMemoryLongTaskRepository({
    clockMs: () => now,
    tokenFactory: () => `claim-${++taskSequence}`,
  });
  const recipe = executionRecipe();
  const dispatcher = new RecipeLongTaskDispatcher({
    repository,
    descriptorResolver: {
      resolve: () => ({
        namespace: "tests",
        ownerId: "owner-1",
        idempotencyKey: "request-1",
      }),
    },
    executors: new DurableExecutorRegistry({
      fixture: {
        async execute(context) {
          calls.push(context.unit.id);
          if (context.unit.id === "unit-2" && secondAttempts++ === 0) {
            throw { code: "temporary", retryable: true };
          }
          await context.checkpoint({ unit: context.unit.id });
          await context.recordUsage({ inputTokens: 1, outputTokens: 1 });
          return { outputRef: `artifact://${context.unit.id}` };
        },
      },
    }),
    workerId: "worker-a",
    leaseDurationMs: 60_000,
    retryBackoffMs: [0],
    idFactory: () => "task-dag",
  });
  const receipt = await dispatcher.dispatch(dispatchInput(recipe));

  await repository.start(receipt.taskId);
  const first = claimFromUnit(await repository.claimReadyUnit(receipt.taskId, "worker-before-restart", 10));
  await repository.markUnitRunning(first);
  await repository.completeUnit(first, { outputRef: "artifact://unit-1" }, "unit-1-settlement");

  const result = await dispatcher.execute({ receipt, runId: "run-2" });
  assert.equal(result.status, "completed");
  assert.equal(result.finalResponse, "artifact://unit-2");
  assert.deepEqual(calls, ["unit-2", "unit-2"]);
  assert.equal((await repository.listUnits(receipt.taskId))[0].attempt, 1);
  assert.equal((await repository.listUnits(receipt.taskId))[1].attempt, 2);
  assert.equal((await repository.listCheckpoints(receipt.taskId, "unit-2")).length, 1);
  assert.equal((await repository.load(receipt.taskId)).usage.invocationCount, 1);
  now += 1;
});

test("repository pause, resume, and cancellation preserve terminal authority", async () => {
  const repository = new InMemoryLongTaskRepository();
  await repository.create("task-pause", taskCommand([
    { id: "unit-1", position: 0 },
  ]));
  await repository.start("task-pause");
  await repository.claimReadyUnit("task-pause", "worker", 1_000);
  assert.equal((await repository.pause("task-pause")).status, "paused");
  assert.equal((await repository.listUnits("task-pause"))[0].attempt, 0);
  assert.equal((await repository.resume("task-pause")).status, "running");
  assert.equal(
    (await repository.claimReadyUnit("task-pause", "worker", 1_000)).attempt,
    1,
  );
  await repository.requestCancel("task-pause", 123);
  assert.equal((await repository.cancel("task-pause")).status, "canceled");
  assert.equal((await repository.listUnits("task-pause"))[0].status, "canceled");
  await assert.rejects(
    repository.start("task-pause"),
    (error) => error instanceof AgentError && error.code === "long_task_not_startable",
  );
});

test("an exhausted orphan lease fails deterministically at the exact boundary", async () => {
  let now = 10;
  const repository = new InMemoryLongTaskRepository({
    clockMs: () => now,
    tokenFactory: () => "only-claim",
  });
  await repository.create("task-expired", taskCommand([
    { id: "unit-expired", position: 0 },
  ]));
  await repository.start("task-expired");
  await repository.claimReadyUnit("task-expired", "worker", 5);
  now = 15;
  assert.equal(await repository.claimReadyUnit("task-expired", "worker", 5), undefined);
  const unit = (await repository.listUnits("task-expired"))[0];
  assert.equal(unit.status, "failed");
  assert.equal(unit.errorCode, "long_task_lease_expired");
  assert.equal((await repository.load("task-expired")).failedUnits, 1);
  assert.equal((await repository.finalizeIfComplete("task-expired")).status, "failed");
});

test("orphan recovery claims, rechecks cancellation, and fences task revisions", async () => {
  const repository = new InMemoryLongTaskRepository();
  await repository.create("task-orphan", taskCommand([
    { id: "unit-1", position: 0, maxAttempts: 2 },
  ]));
  await repository.start("task-orphan");
  const revision = (await repository.load("task-orphan")).revision;
  const settled = [];
  const released = [];
  const candidate = {
    runId: "run-orphan",
    recoverableTasks: [{ taskId: "task-orphan", revision }],
  };
  const control = {
    async listOrphans() { return [candidate]; },
    async claimOrphan() { return true; },
    async loadExecutionLease() {
      return {
        runId: "run-orphan",
        status: "running",
        ownerId: "recovery-worker",
        cancellationRequestedAtMs: null,
      };
    },
    async release(runId) { released.push(runId); return true; },
  };
  const coordinator = new OrphanRecoveryCoordinator({
    control,
    longTasks: repository,
    ownerId: "recovery-worker",
    settle(decision, afterRestart) { settled.push([decision, afterRestart]); },
    clockMs: () => 100,
  });

  assert.deepEqual(await coordinator.recover({ afterRestart: true }), ["run-orphan"]);
  assert.equal((await repository.load("task-orphan")).status, "paused");
  assert.equal(settled[0][0].reason, "durable_task_interrupted");
  assert.equal(settled[0][1], true);
  assert.deepEqual(released, []);

  const staleControl = { ...control, async listOrphans() { return [candidate]; } };
  const stale = new OrphanRecoveryCoordinator({
    control: staleControl,
    longTasks: repository,
    ownerId: "recovery-worker",
    settle() { throw new Error("stale evidence must not settle"); },
  });
  assert.deepEqual(await stale.recover(), []);
  assert.deepEqual(released, ["run-orphan"]);

  let cancellationDecision;
  const canceled = new OrphanRecoveryCoordinator({
    control: {
      ...control,
      async loadExecutionLease() {
        return {
          runId: "run-orphan",
          status: "running",
          ownerId: "recovery-worker",
          cancellationRequestedAtMs: 101,
        };
      },
    },
    longTasks: repository,
    ownerId: "recovery-worker",
    settle(decision) { cancellationDecision = decision; },
  });
  assert.deepEqual(await canceled.recover(), ["run-orphan"]);
  assert.equal(cancellationDecision.disposition, "cancel");
  assert.equal(cancellationDecision.reason, "cancellation_requested");
});

test("Durable admission is fail-closed before Provider and dispatcher authority", async () => {
  let modelCalls = 0;
  let dispatchCalls = 0;
  let executeCalls = 0;
  let plannerCalls = 0;
  const dispatcher = {
    async dispatch({ admission }) {
      dispatchCalls += 1;
      return receipt(admission);
    },
    async execute() {
      executeCalls += 1;
      return { taskId: "task-agent", status: "completed", finalResponse: "durable done" };
    },
  };
  const agent = durableAgent({
    model() { modelCalls += 1; return finalTurn("provider"); },
    planner() { plannerCalls += 1; return workPlan(); },
    admission() {
      return {
        mode: "durable",
        reasonCode: "large",
        coveredStepIds: ["step-1"],
        executionRecipe: singleStepRecipe(),
      };
    },
    dispatcher,
  });

  const handle = await agent.submit(plannedUser("run durable"), RUN_OPTIONS);
  const result = await handle.result;
  assert.equal(result.output, "durable done");
  assert.equal(result.durable.status, "completed");
  assert.equal(result.durable.continuation, false);
  assert.equal(modelCalls, 0);
  assert.equal(plannerCalls, 1);
  assert.equal(dispatchCalls, 1);
  assert.equal(executeCalls, 1);

  const invalid = durableAgent({
    model() { modelCalls += 1; return finalTurn("must not run"); },
    planner: () => workPlan(),
    admission: () => ({
      mode: "durable",
      reasonCode: "bad-coverage",
      coveredStepIds: ["missing"],
      executionRecipe: singleStepRecipe(),
    }),
    dispatcher,
  });
  await rejectsCode(
    (await invalid.submit(plannedUser("invalid"), RUN_OPTIONS)).result,
    "durable_plan_coverage_invalid",
  );
  assert.equal(modelCalls, 0);
  assert.equal(dispatchCalls, 1);
});

test("inline, clarify, and reject admission modes keep their authority boundaries", async () => {
  for (const row of [
    { mode: "inline", expected: "provider", modelCalls: 1 },
    { mode: "clarify", expected: "clarify first", modelCalls: 0 },
    { mode: "reject", expected: "not allowed", modelCalls: 0 },
  ]) {
    let modelCalls = 0;
    let dispatchCalls = 0;
    const agent = durableAgent({
      model() { modelCalls += 1; return finalTurn("provider"); },
      planner: () => workPlan(),
      admission: () => ({
        mode: row.mode,
        reasonCode: `fixture-${row.mode}`,
        ...(row.mode === "inline" ? {} : { message: row.expected }),
      }),
      dispatcher: {
        async dispatch() {
          dispatchCalls += 1;
          throw new Error("non-durable admission must not dispatch");
        },
        async execute() { throw new Error("non-durable admission must not execute"); },
      },
    });
    const handle = await agent.submit(plannedUser(row.mode), RUN_OPTIONS);
    const result = await handle.result;
    const events = [];
    for await (const event of handle.events()) events.push(event);
    const phase = events.find((e) => e.kind === "operation.started" && e.payload.kind === "planning");
    const terminal = events.find((e) => e.kind === "operation.finished" && e.payload.operationId === phase.payload.operationId);
    assert.equal(terminal.payload.status, row.mode === "inline" ? "succeeded" : "failed");
    assert.equal(events.filter((e) => e.kind === "plan.updated").length, row.mode === "inline" ? 1 : 0);
    assert.equal(result.output, row.expected);
    assert.equal(modelCalls, row.modelCalls);
    assert.equal(dispatchCalls, 0);
  }
});

test("authenticated continuation skips planning and rejects incompatible authority first", async () => {
  let plannerCalls = 0;
  let modelCalls = 0;
  let dispatchCalls = 0;
  let executeCalls = 0;
  const dispatcher = {
    async dispatch({ admission }) {
      dispatchCalls += 1;
      return receipt(admission);
    },
    async execute() {
      executeCalls += 1;
      return { taskId: "task-agent", status: "completed", finalResponse: "resumed" };
    },
  };
  const agent = durableAgent({
    model() { modelCalls += 1; return finalTurn("must not run"); },
    planner() { plannerCalls += 1; return workPlan(); },
    admission: () => ({
      mode: "durable",
      reasonCode: "large",
      coveredStepIds: ["step-1"],
      executionRecipe: singleStepRecipe(),
    }),
    dispatcher,
  });
  const initial = await (await agent.submit(plannedUser("start"), RUN_OPTIONS)).result;
  const snapshot = initial.durable.recoverySnapshot;
  const resumed = await (await agent.submit(
    { messages: [user("continue")] },
    { durableContinuation: { snapshot, command: "resume" } },
  )).result;
  assert.equal(resumed.durable.continuation, true);
  assert.equal(plannerCalls, 1);
  assert.equal(dispatchCalls, 1);
  assert.equal(executeCalls, 2);
  assert.equal(modelCalls, 0);

  const v3Snapshot = {
    ...snapshot,
    preset: { ...snapshot.preset, schemaVersion: 3 },
  };
  await rejectsCode(
    agent.submit(
      { messages: [user("v3 unsupported")] },
      { durableContinuation: { snapshot: v3Snapshot, command: "resume" } },
    ),
    "agent_preset_snapshot_unsupported",
  );
  assert.equal(modelCalls, 0);

  const tampered = { ...snapshot, authorityProof: "0".repeat(64) };
  await rejectsCode(
    agent.submit(
      { messages: [user("tampered")] },
      { durableContinuation: { snapshot: tampered, command: "resume" } },
    ),
    "durable_recovery_authentication_failed",
  );
  assert.equal(plannerCalls, 1);
  assert.equal(dispatchCalls, 1);
  assert.equal(executeCalls, 2);

  const incompatible = durableAgent({
    presetRevision: "2",
    model() { modelCalls += 1; return finalTurn("must not run"); },
    planner() { plannerCalls += 1; return workPlan(); },
    admission: () => ({ mode: "inline", reasonCode: "small" }),
    dispatcher,
  });
  await rejectsCode(
    incompatible.submit(
      { messages: [user("mismatch")] },
      { durableContinuation: { snapshot, command: "resume" } },
    ),
    "agent_preset_snapshot_mismatch",
  );

  await rejectsCode(validateContinuation({
    continuation: {
      snapshot: await signedSnapshotWithDeadline("1970-01-01T00:00:01.000Z", snapshot.preset),
      command: "resume",
    },
    currentPreset: snapshot.preset,
    authenticator: new HmacRecoveryAuthenticator(SECRET),
    nowMs: 1_000,
  }), "run_deadline_exceeded");
});

function durableAgent({
  model,
  planner,
  admission,
  dispatcher,
  presetRevision = "1",
}) {
  return new Agent({
    model: { invoke: model },
    preset: { id: "durable-test", revision: presetRevision },
    planning: {
      binding: { id: "fixture-planner", revision: "1" },
      policy: {
        planningConstraints: () => ({}),
      },
      planner: { createPlan: planner },
    },
    durable: {
      binding: { id: "fixture-durable", revision: "1" },
      admission: { evaluate: admission },
      dispatcher,
      recoveryAuthenticator: new HmacRecoveryAuthenticator(SECRET),
    },
  });
}

function plannedUser(content) {
  return { messages: [user(content)], planningMode: "planned" };
}

function workPlan() {
  return {
    workPlan: {
      title: "Durable",
      goal: "finish safely",
      taskSpec: { goal: "finish safely" },
      steps: [{ id: "step-1", title: "Execute", type: "write", executor: "model" }],
    },
  };
}

function singleStepRecipe() {
  return {
    kind: "fixture",
    steps: [{
      id: "unit-1",
      kind: "execute",
      executor: "fixture",
      planStepId: "step-1",
    }],
  };
}

function executionRecipe() {
  return {
    kind: "fixture",
    steps: [
      {
        id: "unit-1",
        kind: "prepare",
        executor: "fixture",
        planStepId: "step-1",
      },
      {
        id: "unit-2",
        kind: "finish",
        dependsOn: ["unit-1"],
        executor: "fixture",
        planStepId: "step-2",
        maxAttempts: 2,
      },
    ],
  };
}

function dispatchInput(recipe) {
  return {
    plan: executionPlan(),
    admission: {
      mode: "durable",
      reasonCode: "large",
      coveredStepIds: ["step-1", "step-2"],
      executionRecipe: recipe,
    },
    runId: "run-1",
    deadlineAt: null,
    budgets: runBudgets(),
  };
}

function executionPlan() {
  return {
    title: "DAG",
    taskSpec: { goal: "test" },
    workStepIds: ["step-1", "step-2"],
    steps: [
      executionStep("step-1"),
      executionStep("step-2", ["step-1"]),
    ],
  };
}

function executionStep(id, dependsOn = []) {
  return {
    id,
    title: id,
    type: "write",
    executor: "model",
    dependsOn,
    status: "pending",
    runtimeToolNames: [],
    protocolPrivate: false,
  };
}

function receipt(admission) {
  return {
    schemaVersion: 1,
    taskId: "task-agent",
    message: "admitted",
    admission,
    recipeFingerprint: "recipe-v1",
    metadata: { recipeFingerprint: "recipe-v1" },
  };
}

function taskCommand(units) {
  return {
    namespace: "tests",
    kind: "fixture",
    ownerId: "owner-1",
    createdByRunId: "run-1",
    idempotencyKey: "request-1",
    units: units.map((unit) => ({
      ...unit,
      executor: "fixture",
      planStepId: unit.id,
    })),
    deadlineAtMs: null,
    budgets: taskBudgets(),
  };
}

function runBudgets() {
  return {
    maxModelAttempts: 10,
    maxInputTokens: 1_000,
    maxRunOutputTokens: 1_000,
    maxReasoningTokens: 1_000,
    maxOutputBytes: 100_000,
    maxOutputEvents: 1_000,
  };
}

function taskBudgets() {
  return {
    maxInvocationAttempts: 10,
    maxInputTokens: 1_000,
    maxRunOutputTokens: 1_000,
    maxReasoningTokens: 1_000,
  };
}

async function signedSnapshotWithDeadline(deadlineAt, preset) {
  const authenticator = new HmacRecoveryAuthenticator(SECRET);
  const base = {
    schemaVersion: 1,
    sourceRunId: "run-source",
    plan: executionPlan(),
    receipt: receipt({
      mode: "durable",
      reasonCode: "large",
      coveredStepIds: ["step-1", "step-2"],
      executionRecipe: executionRecipe(),
    }),
    preset,
    deadlineAt,
    remainingBudgets: runBudgets(),
  };
  return { ...base, authorityProof: await authenticator.sign(base) };
}

function user(content) {
  return { role: "user", content };
}

function finalTurn(content) {
  return { message: { role: "assistant", content }, finishReason: "stop" };
}

function sortJson(value) {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value).sort().map((key) => [key, sortJson(value[key])]),
    );
  }
  return value;
}

async function rejectsCode(promise, code) {
  await assert.rejects(
    promise,
    (error) => error instanceof AgentError && error.code === code,
  );
}
