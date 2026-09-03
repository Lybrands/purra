import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import {
  Agent,
  AgentCapabilityGrant,
  AgentTreeRunSupervisor,
  InMemoryAgentAdapters,
  InMemoryRunRepository,
  InMemoryRunTreeRepository,
  RunCommandService,
} from "../dist/index.js";

const fixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/agent_tree_protocol.json", import.meta.url),
  "utf8",
));

const RUN_OPTIONS = Object.freeze({
  budgets: Object.freeze({ maxRunOutputTokens: null }),
});

function grant(options = {}) {
  return new AgentCapabilityGrant({
    canSpawnAgents: true,
    maxDepth: 3,
    maxChildrenPerCall: 3,
    maxAgentsPerRoot: 16,
    maxParallelRuns: 3,
    allowedTools: ["readDocs", "search"],
    allowedModels: ["test:model"],
    ...options,
  });
}

async function root(repository, options = {}) {
  return repository.beginRoot({
    runId: options.runId ?? "root-run-1",
    agentId: options.agentId ?? "root-agent",
    name: "root",
    title: "Root",
    instruction: "Own the task.",
    objective: "Complete the request.",
    capabilityGrant: options.grant ?? grant(),
    idempotencyKey: `begin:${options.runId ?? "root-run-1"}`,
  });
}

function spec(name, options = {}) {
  return {
    name,
    title: `${name} title`,
    instruction: `Act as ${name}.`,
    objective: `Complete ${name}.`,
    required: options.required ?? true,
    priority: options.priority ?? 0,
    ...(options.grant === undefined ? {} : { capabilityGrant: options.grant }),
  };
}

async function complete(repository, runId, options = {}) {
  const run = await repository.getRun(runId);
  return repository.completeRun(runId, {
    expectedContextVersion: options.expectedContextVersion ?? 0,
    result: options.result ?? "done",
    contentRef: `context://${runId}`,
    fingerprint: `fingerprint:${runId}`,
    ...(run.leaseOwnerId === null ? {} : {
      leaseOwnerId: run.leaseOwnerId,
      leaseEpoch: run.leaseEpoch,
    }),
  });
}

async function rejectsCode(promise, code) {
  await assert.rejects(promise, (error) => error?.code === code);
}

function invocationInput(runId, invocationId) {
  return {
    schemaVersion: 1,
    runId,
    invocationId,
    messageFingerprint: "message",
    toolFingerprint: "tools",
    requestFingerprint: "request",
    evidenceFingerprint: "evidence",
    contextEvidence: [],
    capabilityProfileId: null,
    outputLimit: null,
  };
}

test("shared Agent tree protocol matches TypeScript contracts", () => {
  assert.equal(fixture.protocolVersion, 1);
  assert.equal(fixture.agentPresetSnapshotVersion, 5);
  assert.deepEqual(new AgentCapabilityGrant().toJSON(), fixture.policyDefaults);
  assert.deepEqual(fixture.agentNodeStates, ["active", "closed"]);
  assert.deepEqual(fixture.agentRunStatuses, [
    "queued",
    "running",
    "waiting",
    "done",
    "failed",
    "canceled",
  ]);
  assert.ok(fixture.stableErrorCodes.includes("child_run_join_canceled"));
  assert.ok(fixture.stableErrorCodes.includes("agent_run_resume_checkpoint_missing"));
  assert.ok(fixture.stableErrorCodes.includes("agent_execution_checkpoint_conflict"));
  assert.ok(fixture.stableErrorCodes.includes("agent_preset_mismatch"));
  assert.ok(fixture.stableErrorCodes.includes("run_not_found"));
  assert.deepEqual(fixture.authority.rootBudgetDimensions, [
    "model_attempts",
    "input_tokens",
    "output_tokens",
    "reasoning_tokens",
    "provider_output_events",
    "provider_output_bytes",
  ]);
  assert.deepEqual(fixture.authority.canonicalJournalFields, [
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
  ]);
  assert.deepEqual(fixture.lease, {
    initialEpoch: 1,
    reclaimIncrementsEpoch: true,
    expiryBoundary: "now_greater_than_or_equal_expires_at",
    fences: ["spawn", "continue", "checkpoint", "budget", "output", "terminal"],
  });
  assert.deepEqual(fixture.recovery, {
    executionCheckpointSchemaVersions: { python: 1, typescript: 2 },
    resumablePhase: "model_ready",
    resumableExecutionProfile: "reactive",
    inFlightProviderOrToolPolicy: "fail_stop",
  });
  assert.throws(
    () => new AgentCapabilityGrant({ canSpawnAgents: "yes" }),
    /boolean/,
  );
});

test("Agent tree rejects the legacy delegation write authority", () => {
  assert.throws(() => new Agent({
    model: {
      async invoke() {
        return { message: { role: "assistant", content: "done" }, finishReason: "stop" };
      },
    },
    delegation: {},
    agentTree: { repository: new InMemoryRunTreeRepository() },
  }), /mutually exclusive/);
});

test("Agent executes delegateToAgents through canonical Child Runs", async () => {
  const adapters = new InMemoryAgentAdapters();
  let modelCalls = 0;
  const agent = new Agent({
    model: {
      async invoke(request) {
        modelCalls += 1;
        if (request.messages.at(-1)?.attributes?.publicPresentation === true) {
          throw new Error("Agent Tree unexpectedly entered public presentation");
        }
        if (request.messages.some((message) => message.content === "Review evidence.")) {
          return {
            message: { role: "assistant", content: "child result" },
            finishReason: "stop",
          };
        }
        if (request.messages.at(-1)?.role === "tool") {
          return {
            message: { role: "assistant", content: "root done" },
            finishReason: "stop",
          };
        }
        return {
          message: {
            role: "assistant",
            content: "",
            toolCalls: [{
              id: "delegate-review",
              name: "delegateToAgents",
              arguments: {
                delegations: [{
                  agentName: "reviewer",
                  title: "Reviewer",
                  instruction: "Review evidence.",
                  objective: "Check the evidence.",
                }],
              },
            }],
          },
          finishReason: "tool_calls",
        };
      },
    },
    runRepository: adapters.runs,
    outputPublisher: adapters.outputs,
    agentTree: {
      repository: adapters.runTree,
      rootAgentId: "typescript-root-agent",
    },
  });

  const handle = await agent.submit({
    messages: [{ role: "user", content: "Use a reviewer." }],
    enabledTools: ["delegateToAgents"],
  }, RUN_OPTIONS);
  const result = await handle.result;
  const descendants = await adapters.runTree.listDescendants(handle.runId);
  const journal = await adapters.runs.listRootEvents(handle.runId, 0);

  assert.equal(result.output, "root done");
  assert.equal(modelCalls, 3);
  assert.equal((await handle.snapshot()).preset.schemaVersion, 5);
  assert.equal(descendants.length, 1);
  assert.equal(descendants[0].status, "done");
  assert.equal(descendants[0].parentRunId, handle.runId);
  assert.deepEqual(
    journal.map((event) => event.rootSequence),
    Array.from({ length: journal.length }, (_item, index) => index + 1),
  );
  assert.equal(journal.every((event) => event.rootRunId === handle.runId), true);
  assert.equal(journal.some((event) => event.runId === descendants[0].runId), true);
  assert.equal(
    journal
      .filter((event) => event.runId === descendants[0].runId && event.kind === "final")
      .every((event) => event.visibility === "private"),
    true,
  );
});

test("Agent permits bounded recursive Child Runs", async () => {
  const adapters = new InMemoryAgentAdapters();
  const agent = new Agent({
    model: {
      async invoke(request) {
        const system = request.messages.find((message) => message.role === "system")?.content;
        const afterTool = request.messages.at(-1)?.role === "tool";
        if (request.messages.at(-1)?.attributes?.publicPresentation === true) {
          throw new Error("Agent Tree unexpectedly entered public presentation");
        }
        if (system === "Nested worker.") {
          return {
            message: { role: "assistant", content: "nested done" },
            finishReason: "stop",
          };
        }
        if (system === "Recursive worker." && afterTool) {
          return {
            message: { role: "assistant", content: "recursive done" },
            finishReason: "stop",
          };
        }
        if (afterTool) {
          return {
            message: { role: "assistant", content: "root done" },
            finishReason: "stop",
          };
        }
        const nested = system === "Recursive worker.";
        return {
          message: {
            role: "assistant",
            content: "",
            toolCalls: [{
              id: nested ? "delegate-nested" : "delegate-recursive",
              name: "delegateToAgents",
              arguments: {
                delegations: [{
                  agentName: nested ? "nested" : "recursive",
                  title: nested ? "Nested" : "Recursive",
                  instruction: nested ? "Nested worker." : "Recursive worker.",
                  objective: nested ? "Finish nested work." : "Delegate once.",
                }],
              },
            }],
          },
          finishReason: "tool_calls",
        };
      },
    },
    runRepository: adapters.runs,
    outputPublisher: adapters.outputs,
    agentTree: {
      repository: adapters.runTree,
      rootAgentId: "recursive-typescript-root",
      policy: {
        allowRecursiveDelegation: true,
        maxDepth: 2,
        maxParallel: 1,
      },
    },
  });

  const handle = await agent.submit({
    messages: [{ role: "user", content: "Delegate recursively." }],
    enabledTools: ["delegateToAgents"],
  }, RUN_OPTIONS);
  const result = await handle.result;
  const descendants = await adapters.runTree.listDescendants(handle.runId);

  assert.equal(result.output, "root done");
  assert.deepEqual(descendants.map((run) => run.status), ["done", "done"]);
  assert.deepEqual(
    await Promise.all(descendants.map(async (run) => (
      (await adapters.runTree.getAgent(run.agentId)).depth
    ))),
    [1, 2],
  );
  assert.equal(descendants[1].parentRunId, descendants[0].runId);
  const recursiveCheckpoint = (
    await adapters.runs.get(descendants[0].runId)
  ).executionCheckpoint;
  assert.equal(recursiveCheckpoint?.phase, "model_ready");
  assert.equal(recursiveCheckpoint?.nextRound, 2);
  const checkpointEvents = (
    await adapters.runs.listRootEvents(handle.runId, 0)
  ).filter((event) => event.kind === "agent.execution_checkpoint");
  assert.equal(checkpointEvents.length, 1);
  assert.equal(checkpointEvents[0].visibility, "private");
  assert.equal(checkpointEvents[0].runId, descendants[0].runId);
});

test("Agent host commands continue a canonical Child Agent", async () => {
  const adapters = new InMemoryAgentAdapters();
  let releaseRoot;
  const rootGate = new Promise((resolve) => { releaseRoot = resolve; });
  const agent = new Agent({
    model: {
      async invoke(request) {
        if (request.messages.some((message) => message.content === "Host child.")) {
          return {
            message: { role: "assistant", content: "host child done" },
            finishReason: "stop",
          };
        }
        await rootGate;
        return {
          message: { role: "assistant", content: "host root done" },
          finishReason: "stop",
        };
      },
    },
    runRepository: adapters.runs,
    outputPublisher: adapters.outputs,
    agentTree: {
      repository: adapters.runTree,
      rootAgentId: "host-command-root-agent",
      policy: { maxParallel: 1 },
    },
  });
  const handle = await agent.submit({
    messages: [{ role: "user", content: "Wait for host commands." }],
  }, RUN_OPTIONS);
  const child = (await agent.spawnAgents({
    parentRunId: handle.runId,
    idempotencyKey: "host-spawn",
    children: [{
      name: "host-child",
      title: "Host child",
      instruction: "Host child.",
      objective: "Run once.",
    }],
  })).items[0];
  assert.equal((await agent.joinAgentRuns(handle.runId, [child.run.runId])).state, "ready");
  const continued = await agent.continueAgent({
    requesterRunId: handle.runId,
    idempotencyKey: "host-continue",
    agentId: child.agent.agentId,
    expectedContextVersion: 1,
    message: "Run again.",
  });
  assert.equal(
    (await agent.joinAgentRuns(handle.runId, [continued.run.runId])).state,
    "ready",
  );
  assert.equal((await adapters.runTree.getAgent(child.agent.agentId)).contextVersion, 2);
  releaseRoot();
  assert.equal((await handle.result).output, "host root done");
});

test("Root completion is rejected before final output while a Child Run is pending", async () => {
  const adapters = new InMemoryAgentAdapters();
  let releaseRoot;
  const rootGate = new Promise((resolve) => { releaseRoot = resolve; });
  const agent = new Agent({
    model: {
      async invoke() {
        await rootGate;
        return {
          message: { role: "assistant", content: "must not become final" },
          finishReason: "stop",
        };
      },
    },
    runRepository: adapters.runs,
    outputPublisher: adapters.outputs,
    agentTree: {
      repository: adapters.runTree,
      rootAgentId: "quiescence-root-agent",
      policy: { maxParallel: 1 },
    },
  });
  const handle = await agent.submit({
    messages: [{ role: "user", content: "Do not finish early." }],
  }, RUN_OPTIONS);
  const child = (await agent.spawnAgents({
    parentRunId: handle.runId,
    idempotencyKey: "pending-child",
    children: [{
      name: "pending",
      title: "Pending",
      instruction: "Remain queued.",
      objective: "Prevent premature Root completion.",
    }],
  })).items[0];
  releaseRoot();

  await rejectsCode(handle.result, "root_run_not_quiescent");
  const events = await adapters.runs.listEvents(handle.runId, 0);
  assert.equal((await handle.snapshot()).status, "failed");
  assert.equal((await adapters.runTree.getRun(handle.runId)).status, "failed");
  assert.equal((await adapters.runTree.getRun(child.run.runId)).status, "canceled");
  assert.equal(events.some((event) => event.kind === "final"), false);
  assert.equal(events.some((event) => event.kind === "run.completed"), false);
});

test("new Agent instance rebinds and executes one committed Child Run", async () => {
  let now = 100;
  const adapters = new InMemoryAgentAdapters({ agentTreeClockMs: () => now });
  const rootRunId = "rebound-root";
  await adapters.runs.begin({
    requestedRunId: rootRunId,
    agentId: "rebound-root-agent",
    preset: {
      schemaVersion: 4,
      presetId: "rebound",
      presetRevision: "1",
      promptFingerprint: "prompt",
      toolFingerprint: "tools",
      capabilityProfileId: null,
      compositionFingerprint: "composition",
      runtimeLimits: {
        runTimeoutMs: 900_000,
        activityIdleTimeoutMs: 30_000,
        progressIdleTimeoutMs: 60_000,
        invocationTimeoutMs: 300_000,
        maxChunks: 100_000,
        maxContentChars: 1_000_000,
        maxReasoningChars: 1_000_000,
        maxToolArgumentChars: 1_000_000,
      },
    },
    deadlineAt: null,
    budgets: {
      maxModelAttempts: 4,
      maxInputTokens: null,
      maxRunOutputTokens: null,
      maxReasoningTokens: null,
      maxOutputBytes: 10_000,
      maxOutputEvents: 100,
    },
    metadata: {},
  });
  await adapters.runTree.beginRoot({
    runId: rootRunId,
    agentId: "rebound-root-agent",
    name: "root",
    title: "Root",
    instruction: "Own recovery.",
    objective: "Recover a Child Run.",
    capabilityGrant: grant({
      maxParallelRuns: 1,
      allowedTools: [],
      allowedModels: ["configured:model"],
    }),
    idempotencyKey: "rebound-root-begin",
  });
  const child = (await adapters.runTree.spawnAgents({
    parentRunId: rootRunId,
    idempotencyKey: "committed-before-crash",
    children: [{
      name: "rebound-child",
      title: "Rebound child",
      instruction: "Recover safely.",
      objective: "Execute once.",
    }],
  })).items[0];
  let executions = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        executions += 1;
        return {
          message: { role: "assistant", content: "rebound child done" },
          finishReason: "stop",
        };
      },
    },
    runRepository: adapters.runs,
    outputPublisher: adapters.outputs,
    agentTree: {
      repository: adapters.runTree,
      rootAgentId: "rebound-root-agent",
      policy: { maxParallel: 1 },
    },
  });
  const recoveryRequest = {
    messages: [{ role: "user", content: "Recover." }],
  };
  const originalComplete = adapters.runTree.completeRun.bind(adapters.runTree);
  let crashed = false;
  adapters.runTree.completeRun = async (runId, options) => {
    if (runId === child.run.runId && !crashed) {
      crashed = true;
      throw new Error("worker stopped before Tree terminal commit");
    }
    return originalComplete(runId, options);
  };
  await assert.rejects(agent.recoverAgentTreeRoot(rootRunId, recoveryRequest, RUN_OPTIONS));
  assert.equal((await adapters.runs.get(child.run.runId)).status, "completed");
  assert.equal((await adapters.runTree.getRun(rootRunId)).status, "waiting");
  now = 30_100;
  adapters.runTree.completeRun = originalComplete;
  const aggregate = await agent.recoverAgentTreeRoot(rootRunId, recoveryRequest, RUN_OPTIONS);
  const replay = await agent.recoverAgentTreeRoot(rootRunId, recoveryRequest, RUN_OPTIONS);
  const continued = await agent.continueAgent({
    requesterRunId: rootRunId,
    idempotencyKey: "resume-without-cursor",
    agentId: child.agent.agentId,
    expectedContextVersion: 1,
    message: "This execution loses its in-flight cursor.",
  });
  await adapters.runTree.markWaiting(rootRunId);
  const abandoned = await adapters.runTree.claimRun(continued.run.runId, {
    ownerId: "crashed-worker",
    leaseDurationMs: 10,
  });
  assert.notEqual(abandoned, undefined);
  const rootSnapshot = await adapters.runs.get(rootRunId);
  await adapters.runs.begin({
    requestedRunId: abandoned.runId,
    rootRunId,
    agentId: abandoned.agentId,
    parentRunId: abandoned.parentRunId,
    leaseOwnerId: abandoned.leaseOwnerId,
    leaseEpoch: abandoned.leaseEpoch,
    preset: rootSnapshot.preset,
    deadlineAt: null,
    budgets: rootSnapshot.budgets,
    metadata: {},
  });
  now += 10;
  const gap = await agent.recoverAgentTreeRoot(rootRunId, recoveryRequest, RUN_OPTIONS);
  const resumable = await agent.continueAgent({
    requesterRunId: rootRunId,
    idempotencyKey: "resume-from-model-ready-checkpoint",
    agentId: child.agent.agentId,
    expectedContextVersion: 1,
    message: "Continue from the committed tool boundary.",
  });
  await adapters.runTree.markWaiting(rootRunId);
  const checkpointed = await adapters.runTree.claimRun(resumable.run.runId, {
    ownerId: "checkpointed-worker",
    leaseDurationMs: 10,
  });
  assert.notEqual(checkpointed, undefined);
  const childPreset = (await adapters.runs.get(child.run.runId)).preset;
  await adapters.runs.begin({
    requestedRunId: checkpointed.runId,
    rootRunId,
    agentId: checkpointed.agentId,
    parentRunId: checkpointed.parentRunId,
    leaseOwnerId: checkpointed.leaseOwnerId,
    leaseEpoch: checkpointed.leaseEpoch,
    preset: childPreset,
    deadlineAt: null,
    budgets: rootSnapshot.budgets,
    metadata: {},
  });
  await adapters.runs.saveExecutionCheckpoint(checkpointed.runId, {
    schemaVersion: 2,
    runId: checkpointed.runId,
    phase: "model_ready",
    executionProfile: "reactive",
    initialPlanningOpen: false,
    nextRound: 2,
    messages: [
      { role: "system", content: "Recover safely." },
      { role: "user", content: "Continue from the committed tool boundary." },
      {
        role: "assistant",
        content: null,
        toolCalls: [{ id: "committed-call", name: "readDocs", arguments: {} }],
      },
      { role: "tool", content: "committed result", toolCallId: "committed-call" },
    ],
    context: null,
    contextEvidence: [],
    responseAttempts: 0,
    recoveryAttempts: [],
  }, {
    leaseOwnerId: checkpointed.leaseOwnerId,
    leaseEpoch: checkpointed.leaseEpoch,
  });
  now += 10;
  const resumed = await agent.recoverAgentTreeRoot(rootRunId, recoveryRequest, RUN_OPTIONS);
  await rejectsCode(agent.bindAgentTreeRoot(rootRunId, {
    ...recoveryRequest,
    metadata: { binding: "different" },
  }, RUN_OPTIONS), "run_identity_conflict");
  const recoveredRun = await adapters.runTree.getRun(child.run.runId);

  assert.equal(
    aggregate.state,
    "ready",
    JSON.stringify({ aggregate, recoveredRun }),
  );
  assert.deepEqual(replay, aggregate);
  assert.equal(gap.state, "blocked");
  assert.equal(resumed.state, "blocked");
  assert.equal(executions, 2);
  assert.equal(recoveredRun.status, "done");
  assert.equal((await adapters.runs.get(continued.run.runId)).status, "failed");
  assert.equal(
    (await adapters.runs.get(continued.run.runId)).errorCode,
    "agent_run_resume_checkpoint_missing",
  );
  assert.equal((await adapters.runTree.getRun(continued.run.runId)).status, "failed");
  assert.equal(
    (await adapters.runTree.getRun(continued.run.runId)).errorCode,
    "agent_run_resume_checkpoint_missing",
  );
  assert.equal((await adapters.runs.get(resumable.run.runId)).status, "completed");
  assert.equal((await adapters.runTree.getRun(resumable.run.runId)).status, "done");
});

test("spawn is idempotent and globally scheduled by priority", async () => {
  const repository = new InMemoryRunTreeRepository();
  const rootRun = await root(repository);
  const command = {
    parentRunId: rootRun.runId,
    idempotencyKey: "call-1",
    children: [
      spec("low"),
      spec("high", { priority: 10 }),
      spec("middle", { priority: 5 }),
    ],
  };
  const created = await repository.spawnAgents(command);
  const replayed = await repository.spawnAgents(command);

  assert.equal(replayed.replayed, true);
  assert.deepEqual(
    replayed.items.map((item) => item.run.runId),
    created.items.map((item) => item.run.runId),
  );
  assert.deepEqual(
    (await repository.listRunnable(rootRun.runId)).map((run) => run.priority),
    [10, 5, 0],
  );
  const claimed = [];
  for (const item of created.items) claimed.push(await repository.claimRun(item.run.runId));
  assert.equal(claimed.filter(Boolean).length, 2);
  await repository.markWaiting(rootRun.runId);
  assert.ok(await repository.claimRun(created.items[2].run.runId));

  await rejectsCode(repository.spawnAgents({
    parentRunId: rootRun.runId,
    idempotencyKey: "call-1",
    children: [spec("different")],
  }), "child_spawn_idempotency_conflict");
  await rejectsCode(repository.beginRoot({
    runId: rootRun.runId,
    agentId: "root-agent",
    name: "root",
    title: "Changed",
    instruction: "Own the task.",
    objective: "Complete the request.",
    capabilityGrant: grant(),
    idempotencyKey: `begin:${rootRun.runId}`,
  }), "child_spawn_idempotency_conflict");
});

test("spawn replay survives the parent terminal state", async () => {
  const repository = new InMemoryRunTreeRepository();
  const rootRun = await root(repository);
  const command = {
    parentRunId: rootRun.runId,
    idempotencyKey: "durable-call",
    children: [spec("child")],
  };
  const created = await repository.spawnAgents(command);
  await repository.markWaiting(rootRun.runId);
  await repository.claimRun(created.items[0].run.runId);
  await complete(repository, created.items[0].run.runId);
  await repository.releaseWaiting(rootRun.runId);
  await complete(repository, rootRun.runId);

  const replayed = await repository.spawnAgents(command);
  assert.equal(replayed.replayed, true);
  assert.deepEqual(replayed.items, created.items);
});

test("recursive spawn narrows authority and enforces depth", async () => {
  const repository = new InMemoryRunTreeRepository();
  const rootRun = await root(repository, { grant: grant({ maxDepth: 2 }) });
  const child = (await repository.spawnAgents({
    parentRunId: rootRun.runId,
    idempotencyKey: "child",
    children: [spec("child", {
      grant: grant({
        maxDepth: 2,
        allowedTools: ["readDocs"],
        allowedModels: ["test:model"],
      }),
    })],
  })).items[0];
  await repository.markWaiting(rootRun.runId);
  const claimedChild = await repository.claimRun(child.run.runId);
  const grandchild = (await repository.spawnAgents({
    parentRunId: child.run.runId,
    idempotencyKey: "grandchild",
    leaseOwnerId: claimedChild.leaseOwnerId,
    leaseEpoch: claimedChild.leaseEpoch,
    children: [spec("grandchild")],
  })).items[0];
  assert.equal(grandchild.agent.depth, 2);
  await repository.markWaiting(child.run.runId, {
    leaseOwnerId: claimedChild.leaseOwnerId,
    leaseEpoch: claimedChild.leaseEpoch,
  });
  const claimedGrandchild = await repository.claimRun(grandchild.run.runId);
  await rejectsCode(repository.spawnAgents({
    parentRunId: grandchild.run.runId,
    idempotencyKey: "too-deep",
    leaseOwnerId: claimedGrandchild.leaseOwnerId,
    leaseEpoch: claimedGrandchild.leaseEpoch,
    children: [spec("great-grandchild")],
  }), "agent_depth_exceeded");
  await rejectsCode(repository.spawnAgents({
    parentRunId: child.run.runId,
    idempotencyKey: "escalate",
    leaseOwnerId: claimedChild.leaseOwnerId,
    leaseEpoch: claimedChild.leaseEpoch,
    children: [spec("escalating", { grant: grant() })],
  }), "agent_capability_escalation");
});

test("continuation preserves Agent identity and uses context CAS", async () => {
  const repository = new InMemoryRunTreeRepository();
  const rootRun = await root(repository);
  const child = (await repository.spawnAgents({
    parentRunId: rootRun.runId,
    idempotencyKey: "child",
    children: [spec("child")],
  })).items[0];
  await repository.markWaiting(rootRun.runId);
  await repository.claimRun(child.run.runId);
  await complete(repository, child.run.runId, { result: { answer: 1 } });
  await repository.releaseWaiting(rootRun.runId);
  await complete(repository, rootRun.runId);
  await root(repository, { runId: "root-run-2", agentId: "root-agent" });

  const command = {
    requesterRunId: "root-run-2",
    idempotencyKey: "continue-1",
    agentId: child.agent.agentId,
    expectedContextVersion: 1,
    message: "Check the answer again.",
  };
  const continued = await repository.continueAgent(command);
  const replayed = await repository.continueAgent(command);

  assert.equal(replayed.replayed, true);
  assert.equal(continued.agent.agentId, child.agent.agentId);
  assert.equal(continued.run.previousRunId, child.run.runId);
  assert.equal(continued.run.rootRunId, "root-run-2");
  await rejectsCode(repository.continueAgent({
    ...command,
    idempotencyKey: "continue-2",
  }), "agent_busy");

  await repository.claimRun(continued.run.runId);
  await complete(repository, continued.run.runId, { expectedContextVersion: 1 });
  await complete(repository, "root-run-2", { expectedContextVersion: 1 });
  assert.equal((await repository.continueAgent(command)).replayed, true);
});

test("canceling a subtree preserves Agent identity and join attribution", async () => {
  const repository = new InMemoryRunTreeRepository();
  const rootRun = await root(repository);
  const [required, optional] = (await repository.spawnAgents({
    parentRunId: rootRun.runId,
    idempotencyKey: "children",
    children: [
      spec("required"),
      spec("optional", { required: false }),
    ],
  })).items;
  await repository.markWaiting(rootRun.runId);
  await repository.claimRun(required.run.runId);
  await repository.claimRun(optional.run.runId);
  await complete(repository, optional.run.runId, { result: { ok: true } });
  assert.deepEqual(
    await repository.cancelSubtree(required.run.runId),
    [required.run.runId],
  );

  const aggregate = await repository.aggregateRuns(rootRun.runId, [
    required.run.runId,
    optional.run.runId,
  ]);
  assert.equal(aggregate.state, "blocked");
  assert.deepEqual(aggregate.requiredFailures, [required.run.runId]);
  assert.deepEqual(
    new Set(aggregate.results.map((row) => row.agentId)),
    new Set([required.agent.agentId, optional.agent.agentId]),
  );
  assert.equal((await repository.getAgent(required.agent.agentId)).state, "active");
});

test("failed runs cancel non-terminal descendants", async () => {
  const repository = new InMemoryRunTreeRepository();
  const rootRun = await root(repository);
  const child = (await repository.spawnAgents({
    parentRunId: rootRun.runId,
    idempotencyKey: "child",
    children: [spec("child")],
  })).items[0];
  await repository.markWaiting(rootRun.runId);
  const claimedChild = await repository.claimRun(child.run.runId);
  const grandchild = (await repository.spawnAgents({
    parentRunId: child.run.runId,
    idempotencyKey: "grandchild",
    leaseOwnerId: claimedChild.leaseOwnerId,
    leaseEpoch: claimedChild.leaseEpoch,
    children: [spec("grandchild")],
  })).items[0];

  await repository.failRun(child.run.runId, "child_failed", {
    leaseOwnerId: claimedChild.leaseOwnerId,
    leaseEpoch: claimedChild.leaseEpoch,
  });

  assert.equal((await repository.getRun(child.run.runId)).status, "failed");
  assert.equal((await repository.getRun(grandchild.run.runId)).status, "canceled");
});

test("expired lease fences stale commands and terminal commits", async () => {
  let now = 100;
  const repository = new InMemoryRunTreeRepository({ clockMs: () => now });
  const rootRun = await root(repository);
  const child = (await repository.spawnAgents({
    parentRunId: rootRun.runId,
    idempotencyKey: "lease-child",
    children: [spec("lease-child")],
  })).items[0];
  await repository.markWaiting(rootRun.runId);
  const first = await repository.claimRun(child.run.runId, {
    ownerId: "worker-a",
    leaseDurationMs: 10,
  });
  assert.equal(first.leaseEpoch, 1);

  now = 110;
  await rejectsCode(repository.spawnAgents({
    parentRunId: first.runId,
    idempotencyKey: "stale-spawn",
    leaseOwnerId: first.leaseOwnerId,
    leaseEpoch: first.leaseEpoch,
    children: [spec("stale")],
  }), "agent_run_lease_lost");
  await rejectsCode(repository.completeRun(first.runId, {
    expectedContextVersion: 0,
    result: "stale",
    contentRef: "context://stale",
    fingerprint: "stale",
    leaseOwnerId: first.leaseOwnerId,
    leaseEpoch: first.leaseEpoch,
  }), "agent_run_lease_lost");
  assert.deepEqual(
    (await repository.listRunnable(rootRun.runId)).map((run) => run.runId),
    [first.runId],
  );
  const reclaimed = await repository.claimRun(first.runId, {
    ownerId: "worker-b",
    leaseDurationMs: 10,
  });
  assert.equal(reclaimed.leaseEpoch, 2);
  await rejectsCode(repository.completeRun(first.runId, {
    expectedContextVersion: 0,
    result: "stale",
    contentRef: "context://stale",
    fingerprint: "stale",
    leaseOwnerId: first.leaseOwnerId,
    leaseEpoch: first.leaseEpoch,
  }), "agent_run_lease_lost");
  await complete(repository, reclaimed.runId);
});

test("expired Agent tree lease fences canonical Run budget and output writes", async () => {
  let now = 100;
  const tree = new InMemoryRunTreeRepository({ clockMs: () => now });
  const runs = new InMemoryRunRepository({
    leaseValidator: (runId, claim) => tree.requireRunClaim(runId, claim),
  });
  const preset = {
    schemaVersion: 4,
    presetId: "fenced",
    presetRevision: "1",
    promptFingerprint: "prompt",
    toolFingerprint: "tools",
    capabilityProfileId: null,
    compositionFingerprint: "composition",
    runtimeLimits: {
      runTimeoutMs: 900_000,
      activityIdleTimeoutMs: 30_000,
      progressIdleTimeoutMs: 60_000,
      invocationTimeoutMs: 300_000,
      maxChunks: 100_000,
      maxContentChars: 1_000_000,
      maxReasoningChars: 1_000_000,
      maxToolArgumentChars: 1_000_000,
    },
  };
  const budgets = {
    maxModelAttempts: 2,
    maxInputTokens: null,
    maxRunOutputTokens: null,
    maxReasoningTokens: null,
    maxOutputBytes: 1_000,
    maxOutputEvents: 2,
  };
  const rootRun = await runs.begin({
    requestedRunId: "fenced-root",
    agentId: "fenced-root-agent",
    preset,
    deadlineAt: null,
    budgets,
    metadata: {},
  });
  await tree.beginRoot({
    runId: rootRun.snapshot.runId,
    agentId: "fenced-root-agent",
    name: "root",
    title: "Root",
    instruction: "Own the test.",
    objective: "Fence writes.",
    capabilityGrant: grant(),
    idempotencyKey: "fenced-root-begin",
  });
  const child = (await tree.spawnAgents({
    parentRunId: rootRun.snapshot.runId,
    idempotencyKey: "fenced-child",
    children: [spec("child")],
  })).items[0];
  await tree.markWaiting(rootRun.snapshot.runId);
  const claim = await tree.claimRun(child.run.runId, {
    ownerId: "worker-a",
    leaseDurationMs: 10,
  });
  await runs.begin({
    requestedRunId: claim.runId,
    rootRunId: rootRun.snapshot.runId,
    agentId: claim.agentId,
    parentRunId: rootRun.snapshot.runId,
    leaseOwnerId: claim.leaseOwnerId,
    leaseEpoch: claim.leaseEpoch,
    preset,
    deadlineAt: null,
    budgets,
    metadata: {},
  });

  now = 110;
  await rejectsCode(runs.openInvocation(
    claim.runId,
    invocationInput(claim.runId, "stale-invocation"),
    { leaseOwnerId: claim.leaseOwnerId, leaseEpoch: claim.leaseEpoch },
  ), "agent_run_lease_lost");
  await rejectsCode(runs.appendEvent(claim.runId, {
    sourceKey: "stale-output",
    kind: "provider.delta_batch",
    channel: "model",
    visibility: "private",
    payload: {},
  }, {
    leaseOwnerId: claim.leaseOwnerId,
    leaseEpoch: claim.leaseEpoch,
  }), "agent_run_lease_lost");
  await rejectsCode(runs.saveExecutionCheckpoint(claim.runId, {
    schemaVersion: 2,
    runId: claim.runId,
    phase: "model_ready",
    executionProfile: "reactive",
    initialPlanningOpen: false,
    nextRound: 2,
    messages: [{ role: "user", content: "resume" }],
    context: null,
    contextEvidence: [],
    responseAttempts: 0,
    recoveryAttempts: [],
  }, {
    leaseOwnerId: claim.leaseOwnerId,
    leaseEpoch: claim.leaseEpoch,
  }), "agent_run_lease_lost");
});

test("Run command service releases waiting slots for recursive execution", async () => {
  const repository = new InMemoryRunTreeRepository();
  const rootRun = await root(repository, {
    grant: grant({ maxParallelRuns: 1 }),
  });
  const executed = [];
  let commands;
  const executor = {
    async execute(run, agent) {
      executed.push(agent.name);
      if (agent.name === "recursive") {
        const nested = await commands.spawnAgents({
          parentRunId: run.runId,
          idempotencyKey: "nested-call",
          leaseOwnerId: run.leaseOwnerId,
          leaseEpoch: run.leaseEpoch,
          children: [spec("grandchild")],
        });
        assert.equal((await commands.joinRuns(
          run.runId,
          nested.items.map((item) => item.run.runId),
          undefined,
          { leaseOwnerId: run.leaseOwnerId, leaseEpoch: run.leaseEpoch },
        )).state, "ready");
      }
      return {
        status: "done",
        result: { content: `completed:${agent.name}` },
        contentRef: `memory://${run.runId}`,
        fingerprint: `fingerprint:${run.runId}`,
      };
    },
  };
  const supervisor = new AgentTreeRunSupervisor({ repository, executor });
  commands = new RunCommandService(repository, supervisor);
  const receipt = await commands.spawnAgents({
    parentRunId: rootRun.runId,
    idempotencyKey: "root-call",
    children: [
      spec("recursive", { priority: 10 }),
      spec("sibling"),
    ],
  });

  const aggregate = await commands.joinRuns(
    rootRun.runId,
    receipt.items.map((item) => item.run.runId),
  );

  assert.equal(aggregate.state, "ready");
  assert.deepEqual(executed, ["recursive", "grandchild", "sibling"]);
  assert.equal((await repository.getRun(rootRun.runId)).status, "running");
  assert.equal((await repository.listDescendants(rootRun.runId)).length, 3);

  const original = receipt.items[0];
  const continued = await commands.continueAgent({
    requesterRunId: rootRun.runId,
    idempotencyKey: "continue-recursive",
    agentId: original.agent.agentId,
    expectedContextVersion: 1,
    message: "Run the same specialist again.",
  });
  assert.equal((await commands.joinRuns(
    rootRun.runId,
    [continued.run.runId],
  )).state, "ready");
  assert.equal(continued.run.previousRunId, original.run.runId);
  assert.equal((await repository.getAgent(original.agent.agentId)).contextVersion, 2);
});

test("new supervisor reclaims one committed Child Run after worker crash", async () => {
  let now = 100;
  const repository = new InMemoryRunTreeRepository({ clockMs: () => now });
  const rootRun = await root(repository);
  const child = (await repository.spawnAgents({
    parentRunId: rootRun.runId,
    idempotencyKey: "crash-before-execute",
    children: [spec("recovered")],
  })).items[0];
  await repository.markWaiting(rootRun.runId);
  const abandoned = await repository.claimRun(child.run.runId, {
    ownerId: "crashed-worker",
    leaseDurationMs: 10,
  });
  assert.equal(abandoned.leaseEpoch, 1);

  const executions = [];
  now = 110;
  const recovered = new RunCommandService(repository, new AgentTreeRunSupervisor({
    repository,
    ownerId: "recovery-worker",
    leaseDurationMs: 10,
    executor: {
      async execute(run) {
        executions.push([run.runId, run.leaseEpoch]);
        return {
          status: "done",
          result: { content: "recovered" },
          contentRef: `memory://${run.runId}`,
          fingerprint: "recovered",
        };
      },
    },
  }));
  const aggregate = await recovered.joinRuns(rootRun.runId, [child.run.runId]);

  assert.equal(aggregate.state, "ready");
  assert.deepEqual(executions, [[child.run.runId, 2]]);
  const settled = await repository.getRun(child.run.runId);
  assert.equal(settled.status, "done");
  assert.equal(settled.leaseEpoch, 2);
  assert.deepEqual(
    await recovered.joinRuns(rootRun.runId, [child.run.runId]),
    aggregate,
  );
  assert.deepEqual(executions, [[child.run.runId, 2]]);
});

test("join cancellation wakes when an executor ignores AbortSignal", async () => {
  const repository = new InMemoryRunTreeRepository();
  const rootRun = await root(repository, {
    grant: grant({ maxParallelRuns: 1 }),
  });
  const supervisor = new AgentTreeRunSupervisor({
    repository,
    executor: {
      async execute() {
        return new Promise(() => undefined);
      },
    },
  });
  const commands = new RunCommandService(repository, supervisor);
  const child = (await commands.spawnAgents({
    parentRunId: rootRun.runId,
    idempotencyKey: "cancel-call",
    children: [spec("stuck")],
  })).items[0];
  const controller = new AbortController();
  const joined = commands.joinRuns(rootRun.runId, [child.run.runId], controller.signal);
  await Promise.resolve();
  controller.abort();

  await rejectsCode(joined, "child_run_join_canceled");
  assert.equal((await repository.getRun(child.run.runId)).status, "canceled");
  assert.equal((await repository.getRun(rootRun.runId)).status, "running");
});
