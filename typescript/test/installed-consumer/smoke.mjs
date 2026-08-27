import assert from "node:assert/strict";

import {
  Agent,
  AgentCapabilityGrant,
  AgentTreeRunSupervisor,
  AgentOperationController,
  ArtifactAccessController,
  ArtifactLifecycle,
  DurableExecutorRegistry,
  evaluateAgentRun,
  InMemoryAgentAdapters,
  InMemoryRunTreeRepository,
  InMemoryLongTaskRepository,
  InMemoryArtifactStore,
  ModelResponseJudge,
  ModelWorkPlanner,
  RecipeLongTaskDispatcher,
  RecoveryPolicy,
  RunCommandService,
  ToolPlanningPolicy,
} from "purra";

const RUN_OPTIONS = Object.freeze({
  budgets: Object.freeze({ maxRunOutputTokens: null }),
});

let round = 0;
const events = [];
const handle = await new Agent({
  recovery: new RecoveryPolicy(),
  model: {
    capabilities: capabilities(),
    async invoke() {
      throw new Error("stream should be used");
    },
    async *stream() {
      round += 1;
      if (round === 1) {
        yield {
          toolCallDeltas: [{
            index: 0,
            id: "call-1",
            name: "lookup",
            argumentsFragment: "{\"key\":\"installed\"}",
          }],
          finishReason: "tool_calls",
        };
        return;
      }
      yield { contentDelta: "installed", finishReason: "stop" };
    },
  },
  tools: [{
    name: "lookup",
    description: "Look up a value",
    inputSchema: {
      type: "object",
      properties: { key: { type: "string" } },
      required: ["key"],
      additionalProperties: false,
    },
    policy: { mode: "read", title: "Look up" },
    run(input) {
      assert.deepEqual(input, { key: "installed" });
      return { content: { found: true }, effectState: "not_started" };
    },
  }],
  context: {
    provider: {
      buildContext() {
        return {
          blocks: [{
            name: "installed",
            content: "installed context",
            evidence: [{ evidenceId: "installed-1", source: "smoke", itemId: "context" }],
          }],
        };
      },
    },
  },
  planning: {
    policy: new ToolPlanningPolicy(),
    planner: {
      createPlan() {
        return {
          workPlan: {
            title: "Installed plan",
            taskSpec: { goal: "Look up the installed fixture" },
            steps: [{
              id: "lookup",
              title: "Look up",
              type: "read",
              executor: "tool",
              capabilityNames: ["lookup"],
            }],
          },
        };
      },
    },
  },
}).submit({ messages: [{ role: "user", content: "hello" }] }, RUN_OPTIONS);
const result = await handle.result;
for await (const event of handle.events()) events.push(event);
const allEvents = [];
for await (const event of handle.events({ visibility: "all" })) allEvents.push(event);

assert.deepEqual(events.map((event) => event.kind), [
  "run.started",
  "plan.updated",
  "tool.started",
  "tool.completed",
  "final",
  "run.completed",
]);
assert.equal(result.output, "installed");
assert.equal(events.at(-2).payload.output, "installed");
const completedSnapshot = await handle.snapshot();
assert.equal(completedSnapshot.status, "completed");
assert.equal(completedSnapshot.preset.schemaVersion, 4);
assert.deepEqual(completedSnapshot.preset.runtimeLimits, {
  runTimeoutMs: 900_000,
  activityIdleTimeoutMs: 30_000,
  progressIdleTimeoutMs: 60_000,
  invocationTimeoutMs: 300_000,
  maxChunks: 100_000,
  maxContentChars: 1_000_000,
  maxReasoningChars: 1_000_000,
  maxToolArgumentChars: 1_000_000,
});
const receipt = allEvents.find((event) => event.kind === "invocation.started").payload.receipt;
assert.deepEqual(receipt.contextEvidence, [{
  evidenceId: "installed-1",
  contextBlock: "installed",
  source: "smoke",
  itemId: "context",
}]);

const longTasks = new InMemoryLongTaskRepository();
const durableDispatcher = new RecipeLongTaskDispatcher({
  repository: longTasks,
  descriptorResolver: {
    resolve() {
      return { namespace: "installed", ownerId: "smoke", idempotencyKey: "durable-smoke" };
    },
  },
  executors: new DurableExecutorRegistry({
    installed: {
      execute() { return { outputRef: "installed durable" }; },
    },
  }),
  workerId: "installed-worker",
  idFactory: () => "installed-task",
});
const durableReceipt = await durableDispatcher.dispatch(durableInput());
const durableResult = await durableDispatcher.execute({
  receipt: durableReceipt,
  runId: "installed-run",
});
assert.equal(durableResult.status, "completed");
assert.equal(durableResult.finalResponse, "installed durable");

const artifacts = new InMemoryArtifactStore();
const artifactLifecycle = new ArtifactLifecycle({
  repository: artifacts,
  idFactory: () => "installed-artifact",
});
const artifact = await artifactLifecycle.begin({
  namespace: "installed",
  kind: "report",
  ownerId: "smoke",
  ownerRef: { kind: "run", id: "installed-artifact-run" },
  createdByRunId: "installed-artifact-run",
  expectedItemCount: 1,
});
const artifactClaim = (await new ArtifactAccessController(artifacts).authorize({
  artifactId: artifact.id,
  namespace: artifact.namespace,
  kind: artifact.kind,
  ownerId: artifact.ownerId,
  ownerRef: artifact.ownerRef,
  createdByRunId: artifact.createdByRunId,
  status: artifact.status,
  revision: artifact.revision,
}, {
  artifactId: artifact.id,
  runId: artifact.createdByRunId,
  mode: "write",
  expectedRevision: artifact.revision,
}, 30_000)).writeClaim;
const artifactReceipt = await artifactLifecycle.append({
  artifactId: artifact.id,
  expectedRevision: artifact.revision,
  sequence: artifact.nextSequence,
  batchId: "installed-batch",
  idempotencyKey: "installed-append",
  items: [{ installed: true }],
  coverageKeys: ["installed"],
  writeLease: {
    runId: artifactClaim.runId,
    claimToken: artifactClaim.claimToken,
  },
});
const installedArtifact = await artifactLifecycle.finalize({
  artifactId: artifact.id,
  expectedRevision: artifactReceipt.committedRevision,
  expectedItemCount: 1,
  expectedCoverageKeys: ["installed"],
  resourceRef: "memory://installed-artifact",
  writeLease: {
    runId: artifactClaim.runId,
    claimToken: artifactClaim.claimToken,
  },
});
assert.equal(installedArtifact.status, "finalized");

const referenceAdapters = new InMemoryAgentAdapters();
assert.equal(referenceAdapters.runs.constructor.name, "InMemoryRunRepository");
const diagnostics = evaluateAgentRun({ id: "installed", status: "done" }, [
  { eventType: "agentRunTrace", payload: { stage: "planning", outcome: "skipped", prompt: "must-not-leak" } },
  { eventType: "agentRunTrace", payload: { stage: "context_budget", outcome: "within_budget" } },
  { eventType: "run.completed", payload: { output: "must-not-leak" } },
]);
assert.equal(diagnostics.verdict, "pass");
assert.equal(JSON.stringify(diagnostics).includes("must-not-leak"), false);

let delegationRootRounds = 0;
const delegationHandle = await new Agent({
  model: {
    async invoke(request) {
      if (request.messages[0]?.attributes?.delegatedAgentDefinition === "model") {
        assert.deepEqual(request.tools, []);
        return {
          message: { role: "assistant", content: "installed delegation" },
          finishReason: "stop",
        };
      }
      delegationRootRounds += 1;
      if (delegationRootRounds === 1) {
        return {
          message: {
            role: "assistant",
            content: "",
            toolCalls: [{
              id: "installed-delegation-call",
              name: "delegateToAgents",
              arguments: {
                delegations: [{
                  agentName: "installed",
                  title: "Installed delegate",
                  instruction: "Return the installed delegation result.",
                  objective: "verify installed delegation",
                }],
              },
            }],
          },
          finishReason: "tool_calls",
        };
      }
      return { message: { role: "assistant", content: "installed root" }, finishReason: "stop" };
    },
  },
  delegation: { policy: { maxAgentsPerCall: 1, maxParallel: 1 } },
}).submit({ messages: [{ role: "user", content: "delegate" }] }, RUN_OPTIONS);
assert.equal((await delegationHandle.result).output, "installed root");
const delegationEvents = [];
for await (const event of delegationHandle.events()) delegationEvents.push(event);
assert.deepEqual(
  delegationEvents
    .filter((event) => event.kind === "delegation.status")
    .map((event) => event.payload.status),
  ["queued", "running", "done"],
);

const installedTreeAdapters = new InMemoryAgentAdapters();
const installedTreeAgent = new Agent({
  model: {
    async invoke(request) {
      const system = request.messages.find((message) => message.role === "system")?.content;
      const afterTool = request.messages.at(-1)?.role === "tool";
      if (system === "Installed nested worker.") {
        return finalTurn("installed nested done");
      }
      if (system === "Installed recursive worker." && afterTool) {
        return finalTurn("installed child done");
      }
      if (afterTool) return finalTurn("installed tree root done");
      const nested = system === "Installed recursive worker.";
      return {
        message: {
          role: "assistant",
          content: "",
          toolCalls: [{
            id: nested ? "installed-nested-call" : "installed-recursive-call",
            name: "delegateToAgents",
            arguments: {
              delegations: [{
                agentName: nested ? "nested" : "recursive",
                title: nested ? "Nested" : "Recursive",
                instruction: nested
                  ? "Installed nested worker."
                  : "Installed recursive worker.",
                objective: nested ? "Finish nested work." : "Delegate once.",
              }],
            },
          }],
        },
        finishReason: "tool_calls",
      };
    },
  },
  runRepository: installedTreeAdapters.runs,
  outputPublisher: installedTreeAdapters.outputs,
  agentTree: {
    repository: installedTreeAdapters.runTree,
    rootAgentId: "installed-tree-root-agent",
    policy: {
      allowRecursiveDelegation: true,
      maxDepth: 2,
      maxParallel: 1,
    },
  },
});
const installedTreeHandle = await installedTreeAgent.submit({
  messages: [{ role: "user", content: "Run the installed Agent tree." }],
  enabledTools: ["delegateToAgents"],
}, RUN_OPTIONS);
assert.equal((await installedTreeHandle.result).output, "installed tree root done");
assert.equal((await installedTreeHandle.snapshot()).preset.schemaVersion, 5);
assert.deepEqual(
  (await installedTreeAdapters.runTree.listDescendants(installedTreeHandle.runId))
    .map((run) => run.status),
  ["done", "done"],
);

const installedContinuationRepository = new InMemoryRunTreeRepository();
let installedParallelActive = 0;
let installedParallelPeak = 0;
let releaseInstalledParallel;
const installedParallelStarted = new Promise((resolve) => {
  releaseInstalledParallel = resolve;
});
const installedContinuationCommands = new RunCommandService(
  installedContinuationRepository,
  new AgentTreeRunSupervisor({
    repository: installedContinuationRepository,
    executor: {
      async execute(run, childAgent) {
        if (
          run.previousRunId === null
          && (childAgent.name === "continued" || childAgent.name === "peer")
        ) {
          installedParallelActive += 1;
          installedParallelPeak = Math.max(installedParallelPeak, installedParallelActive);
          if (installedParallelActive === 2) releaseInstalledParallel();
          await installedParallelStarted;
          await Promise.resolve();
          installedParallelActive -= 1;
        }
        return {
          status: "done",
          result: { agent: childAgent.name },
          contentRef: `memory://${run.runId}`,
          fingerprint: `installed:${run.runId}`,
        };
      },
    },
  }),
);
const installedContinuationRoot = await installedContinuationCommands.beginRoot({
  runId: "installed-continuation-root",
  agentId: "installed-continuation-root-agent",
  name: "root",
  title: "Root",
  instruction: "Own continuation smoke.",
  objective: "Continue one Child Agent.",
  capabilityGrant: new AgentCapabilityGrant({
    canSpawnAgents: true,
    maxParallelRuns: 2,
  }),
  idempotencyKey: "installed-continuation-begin",
});
const installedContinuationChildren = await installedContinuationCommands.spawnAgents({
  parentRunId: installedContinuationRoot.runId,
  idempotencyKey: "installed-continuation-spawn",
  children: [
    {
      name: "continued",
      title: "Continued",
      instruction: "Run twice.",
      objective: "First run.",
    },
    {
      name: "peer",
      title: "Peer",
      instruction: "Run beside Continued.",
      objective: "Prove parallel execution.",
    },
  ],
});
const installedContinuationChild = installedContinuationChildren.items[0];
assert.equal((await installedContinuationCommands.joinRuns(
  installedContinuationRoot.runId,
  installedContinuationChildren.items.map((item) => item.run.runId),
)).state, "ready");
assert.equal(installedParallelPeak, 2);
const installedContinuedRun = await installedContinuationCommands.continueAgent({
  requesterRunId: installedContinuationRoot.runId,
  idempotencyKey: "installed-continuation-second",
  agentId: installedContinuationChild.agent.agentId,
  expectedContextVersion: 1,
  message: "Run again.",
});
assert.equal((await installedContinuationCommands.joinRuns(
  installedContinuationRoot.runId,
  [installedContinuedRun.run.runId],
)).state, "ready");
assert.equal((await installedContinuationRepository.getAgent(
  installedContinuationChild.agent.agentId,
)).contextVersion, 2);

const managedOperationEvents = [];
let managedOperationSequence = 0;
const managedFactoryRunIds = [];
const managedCalls = [];
const managedHandle = await new Agent({
  model: {
    capabilities: managedCapabilities(),
    async invoke(request) {
      managedCalls.push(request);
      if (request.messages.some((message) => message.attributes?.planningContract === true)) {
        return finalTurn(JSON.stringify({
          workPlan: {
            title: "Installed managed plan",
            steps: [{ id: "respond", title: "Respond", type: "review", executor: "model" }],
          },
        }));
      }
      if (String(request.messages[0]?.content).startsWith("installed-judge:")) {
        return finalTurn("accept");
      }
      if (request.messages[0]?.content === "installed-context-task") {
        return finalTurn("managed context");
      }
      return finalTurn("installed managed response");
    },
  },
  operations: new AgentOperationController({
    acceptOperationEvent(event) {
      managedOperationEvents.push(event);
    },
  }, {
    idFactory: () => `installed-operation-${++managedOperationSequence}`,
  }),
  context: {
    providerFactory(modelTasks) {
      managedFactoryRunIds.push(modelTasks.runId);
      return {
        async buildContext() {
          const completion = await modelTasks.complete([
            { role: "user", content: "installed-context-task" },
          ]);
          return {
            blocks: [{ name: "managed", content: completion.turn.message.content }],
          };
        },
      };
    },
  },
  planning: {
    policy: {
      shouldPlan() { return true; },
      planningConstraints() { return { maxSteps: 2 }; },
    },
    plannerFactory(modelTasks) {
      managedFactoryRunIds.push(modelTasks.runId);
      return new ModelWorkPlanner(modelTasks);
    },
  },
  responseValidation: {
    judgeFactories: [(modelTasks) => {
      managedFactoryRunIds.push(modelTasks.runId);
      return new ModelResponseJudge(modelTasks, {
        buildMessages({ content }) {
          return [{ role: "user", content: `installed-judge:${String(content)}` }];
        },
        evaluate({ judgmentContent }) {
          return judgmentContent === "accept"
            ? {}
            : { violationCode: "installed_rejection", repairGuidance: "Retry." };
        },
      });
    }],
    maxAttempts: 1,
  },
}).submit({ messages: [{ role: "user", content: "managed" }] }, RUN_OPTIONS);
assert.equal((await managedHandle.result).output, "installed managed response");
assert.deepEqual(managedFactoryRunIds, [
  managedHandle.runId,
  managedHandle.runId,
  managedHandle.runId,
]);
assert.equal(managedCalls.length, 4);
assert.deepEqual(
  managedOperationEvents.map((event) => [event.type, event.status ?? event.kind]),
  [
    ["operation.started", "model"],
    ["operation.finished", "succeeded"],
    ["operation.started", "model"],
    ["operation.finished", "succeeded"],
    ["operation.started", "model"],
    ["operation.finished", "succeeded"],
  ],
);
const managedEvents = [];
for await (const event of managedHandle.events({ visibility: "all" })) managedEvents.push(event);
assert.equal(managedEvents.filter((event) => event.kind === "invocation.started").length, 4);

function capabilities() {
  return {
    schemaVersion: 1,
    profileId: "installed",
    providerProtocol: "custom",
    contextWindowTokens: 16_000,
    maxCallOutputTokens: 512,
    thinkingTokenAccounting: "unknown",
    protocol: {
      reasoningControl: "selectable",
      reasoningReplay: "ignored",
      toolCalling: "supported",
      requiredToolChoice: "supported",
      parallelToolCalls: "supported",
      streaming: "supported",
      cancellation: "supported",
      assistantContentWithToolCalls: "optional",
      jsonSchemaLevel: "unknown",
      streamFinishSemantics: "normalized",
      usageSemantics: "normalized",
    },
  };
}

function managedCapabilities() {
  return {
    ...capabilities(),
    profileId: "installed-managed",
    protocol: { ...capabilities().protocol, streaming: "unavailable" },
  };
}

function finalTurn(content) {
  return { message: { role: "assistant", content }, finishReason: "stop" };
}

function durableInput() {
  const step = {
    id: "durable-step",
    title: "Durable step",
    type: "write",
    executor: "model",
    status: "pending",
    runtimeToolNames: [],
    protocolPrivate: false,
  };
  return {
    plan: {
      title: "Installed durable plan",
      taskSpec: { goal: "Run installed durable task" },
      steps: [step],
      workStepIds: [step.id],
    },
    admission: {
      mode: "durable",
      reasonCode: "installed",
      coveredStepIds: [step.id],
      executionRecipe: {
        kind: "installed",
        steps: [{
          id: "installed-unit",
          kind: "execute",
          executor: "installed",
          planStepId: step.id,
        }],
      },
    },
    runId: "installed-run",
    deadlineAt: null,
    budgets: {
      maxModelAttempts: 2,
      maxInputTokens: 100,
      maxRunOutputTokens: 100,
      maxReasoningTokens: 100,
      maxOutputBytes: 1_000,
      maxOutputEvents: 100,
    },
  };
}
