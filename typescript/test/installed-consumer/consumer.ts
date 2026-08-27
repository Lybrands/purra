import {
  Agent,
  AgentCapabilityGrant,
  AgentTreeRunSupervisor,
  AgentOperationController,
  ArtifactAccessController,
  ArtifactLifecycle,
  DurableExecutorRegistry,
  DelegationPolicy,
  evaluateAgentRun,
  InMemoryAgentAdapters,
  InMemoryRunTreeRepository,
  InMemoryDelegationRepository,
  InMemoryLongTaskRepository,
  InMemoryArtifactStore,
  ModelResponseJudge,
  ModelTaskRunner,
  ModelWorkPlanner,
  RecipeLongTaskDispatcher,
  RecoveryPolicy,
  RunCommandService,
  ToolPlanningPolicy,
  type JsonValue,
  type AgentRuntimeLimits,
  type AgentRuntimeLimitSnapshot,
  type AgentExecutionCheckpoint,
  type LongTaskDispatchReceipt,
  type ModelGateway,
  type ModelStreamActivity,
  type ModelStreamActivityKind,
  type ModelStreamActivitySupport,
  type ModelStreamItem,
  type ModelStreamLimits,
  type DelegationRepository,
  type OutputEvent,
  type RunHandle,
  type ToolDefinition,
} from "purra";

const typedActivityKind = "working" satisfies ModelStreamActivityKind;
"working" satisfies ModelStreamActivitySupport;
const typedActivity = {
  type: "activity",
  kind: typedActivityKind,
} satisfies ModelStreamActivity;
typedActivity satisfies ModelStreamItem;
const typedStreamLimits = {
  activityIdleTimeoutMs: 30_000,
  progressIdleTimeoutMs: 60_000,
  invocationTimeoutMs: 300_000,
  maxChunks: 100_000,
  maxContentChars: 1_000_000,
  maxReasoningChars: 1_000_000,
  maxToolArgumentChars: 1_000_000,
} satisfies ModelStreamLimits;
const typedRuntimeLimits = {
  ...typedStreamLimits,
  runTimeoutMs: 900_000,
} satisfies AgentRuntimeLimits;
typedRuntimeLimits satisfies AgentRuntimeLimitSnapshot;
const typedExecutionCheckpoint = {
  schemaVersion: 1,
  runId: "typed-checkpoint",
  phase: "model_ready",
  executionProfile: "reactive",
  nextRound: 2,
  messages: [{ role: "user", content: "resume" }],
  responseAttempts: 0,
  recoveryAttempts: [],
} satisfies AgentExecutionCheckpoint;
typedExecutionCheckpoint satisfies AgentExecutionCheckpoint;

const typedOperationEvents: unknown[] = [];
const typedOperations = new AgentOperationController({
  acceptOperationEvent(event) { typedOperationEvents.push(event); },
});
const typedModelTasks = new ModelTaskRunner({
  model: {
    capabilities: capabilities(),
    async invoke() {
      return { message: { role: "assistant", content: "typed" }, finishReason: "stop" };
    },
  },
  runId: "typed-managed-run",
  operations: typedOperations,
});
new ModelWorkPlanner(typedModelTasks) satisfies ModelWorkPlanner;
new ModelResponseJudge(typedModelTasks, {
  buildMessages({ content }) {
    return [{ role: "user", content: `judge:${String(content)}` }];
  },
  evaluate() { return {}; },
}) satisfies ModelResponseJudge;

const referenceAdapters = new InMemoryAgentAdapters();
referenceAdapters.outputs.publishCommitted satisfies Function;
evaluateAgentRun({ id: "typed", status: "done" }, []) satisfies Readonly<Record<string, unknown>>;

let round = 0;
const model: ModelGateway = {
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
};

const tools: readonly ToolDefinition[] = [{
  name: "lookup",
  description: "Look up a value",
  inputSchema: {
    type: "object",
    properties: { key: { type: "string" } },
    required: ["key"],
    additionalProperties: false,
  },
  policy: { mode: "read", title: "Look up" },
  run() { return { content: { found: true }, effectState: "not_started" }; },
}];

const handle: RunHandle = await new Agent({
  model,
  recovery: new RecoveryPolicy(),
  tools,
  context: {
    provider: {
      buildContext() {
        return { blocks: [{ name: "installed", content: "installed context" }] };
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
}).submit({
  messages: [{ role: "user", content: "hello" }],
}, {
  budgets: { maxRunOutputTokens: null },
});
const result = await handle.result;
result.output satisfies JsonValue;
for await (const event of handle.events()) {
  event satisfies OutputEvent;
}

const durableDispatcher = new RecipeLongTaskDispatcher({
  repository: new InMemoryLongTaskRepository(),
  descriptorResolver: {
    resolve() {
      return { namespace: "installed", ownerId: "consumer", idempotencyKey: "typed-durable" };
    },
  },
  executors: new DurableExecutorRegistry({
    installed: {
      execute() { return { outputRef: "typed durable" }; },
    },
  }),
  workerId: "typed-worker",
  idFactory: () => "typed-task",
});
const durableReceipt: LongTaskDispatchReceipt = await durableDispatcher.dispatch({
  plan: {
    title: "Typed durable",
    taskSpec: { goal: "Compile Durable consumer" },
    workStepIds: ["step"],
    steps: [{
      id: "step",
      title: "Step",
      type: "write",
      executor: "model",
      status: "pending",
      runtimeToolNames: [],
      protocolPrivate: false,
    }],
  },
  admission: {
    mode: "durable",
    reasonCode: "typed",
    coveredStepIds: ["step"],
    executionRecipe: {
      kind: "typed",
      steps: [{
        id: "unit",
        kind: "execute",
        executor: "installed",
        planStepId: "step",
      }],
    },
  },
  runId: "typed-run",
  deadlineAt: null,
  budgets: {
    maxModelAttempts: 2,
    maxInputTokens: 100,
    maxRunOutputTokens: 100,
    maxReasoningTokens: 100,
    maxOutputBytes: 1_000,
    maxOutputEvents: 100,
  },
});
(await durableDispatcher.execute({
  receipt: durableReceipt,
  runId: "typed-run",
})).status satisfies "completed" | "failed" | "canceled" | "paused";

const artifacts = new InMemoryArtifactStore();
const artifactLifecycle = new ArtifactLifecycle({
  repository: artifacts,
  idFactory: () => "typed-artifact",
});
const artifact = await artifactLifecycle.begin({
  namespace: "installed",
  kind: "report",
  ownerId: "consumer",
  ownerRef: { kind: "run", id: "typed-artifact-run" },
  createdByRunId: "typed-artifact-run",
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
}, 30_000)).writeClaim!;
const artifactReceipt = await artifactLifecycle.append({
  artifactId: artifact.id,
  expectedRevision: artifact.revision,
  sequence: artifact.nextSequence,
  batchId: "typed-batch",
  idempotencyKey: "typed-append",
  items: [{ typed: true }],
  coverageKeys: ["typed"],
  writeLease: {
    runId: artifactClaim.runId,
    claimToken: artifactClaim.claimToken,
  },
});
const finalizedArtifact = await artifactLifecycle.finalize({
  artifactId: artifact.id,
  expectedRevision: artifactReceipt.committedRevision,
  expectedItemCount: 1,
  expectedCoverageKeys: ["typed"],
  writeLease: {
    runId: artifactClaim.runId,
    claimToken: artifactClaim.claimToken,
  },
});
if (finalizedArtifact.status !== "finalized") {
  throw new Error(`unexpected artifact status: ${finalizedArtifact.status}`);
}

const delegationRepository: DelegationRepository = new InMemoryDelegationRepository();
new DelegationPolicy({ maxAgentsPerCall: 1, maxParallel: 1 }).snapshot().contextMode satisfies "isolated";
const delegationBatch = await delegationRepository.createBatch({
  runId: "typed-root-run",
  batchId: "typed-delegation-batch",
  idempotencyKey: "typed-delegation-call",
  delegations: [{
    agentName: "typed",
    title: "Typed delegate",
    instruction: "Verify the installed TypeScript contracts.",
    objective: "compile delegation",
  }],
});
delegationBatch.delegations[0]!.contextMode satisfies "isolated";

const treeRepository = new InMemoryRunTreeRepository();
let treeCommands: RunCommandService;
const treeSupervisor = new AgentTreeRunSupervisor({
  repository: treeRepository,
  executor: {
    async execute(run, agent) {
      if (agent.name === "child") {
        const nested = await treeCommands.spawnAgents({
          parentRunId: run.runId,
          idempotencyKey: "typed-nested",
          leaseOwnerId: run.leaseOwnerId!,
          leaseEpoch: run.leaseEpoch,
          children: [{
            name: "grandchild",
            title: "Grandchild",
            instruction: "Finish.",
            objective: "Finish nested work.",
          }],
        });
        if ((await treeCommands.joinRuns(
          run.runId,
          [nested.items[0]!.run.runId],
          undefined,
          {
            leaseOwnerId: run.leaseOwnerId!,
            leaseEpoch: run.leaseEpoch,
          },
        )).state !== "ready") {
          throw new Error("nested Agent tree did not settle");
        }
      }
      return {
        status: "done",
        result: { agent: agent.name },
        contentRef: `memory://${run.runId}`,
        fingerprint: `fingerprint:${run.runId}`,
      };
    },
  },
});
treeCommands = new RunCommandService(treeRepository, treeSupervisor);
const treeRoot = await treeCommands.beginRoot({
  runId: "typed-tree-root",
  agentId: "typed-tree-agent",
  name: "root",
  title: "Root",
  instruction: "Own the smoke test.",
  objective: "Run two levels.",
  capabilityGrant: new AgentCapabilityGrant({
    canSpawnAgents: true,
    maxParallelRuns: 1,
  }),
  idempotencyKey: "typed-tree-begin",
});
const treeChild = await treeCommands.spawnAgents({
  parentRunId: treeRoot.runId,
  idempotencyKey: "typed-tree-spawn",
  children: [{
    name: "child",
    title: "Child",
    instruction: "Delegate once.",
    objective: "Run child work.",
  }],
});
if ((await treeCommands.joinRuns(
  treeRoot.runId,
  [treeChild.items[0]!.run.runId],
)).state !== "ready") {
  throw new Error("installed Agent tree did not settle");
}
const continuedTreeChild = await treeCommands.continueAgent({
  requesterRunId: treeRoot.runId,
  idempotencyKey: "typed-tree-continue",
  agentId: treeChild.items[0]!.agent.agentId,
  expectedContextVersion: 1,
  message: "Run the installed Child Agent again.",
});
if ((await treeCommands.joinRuns(
  treeRoot.runId,
  [continuedTreeChild.run.runId],
)).state !== "ready") {
  throw new Error("installed Agent continuation did not settle");
}
if ((await treeRepository.listDescendants(treeRoot.runId)).length !== 4) {
  throw new Error("installed Agent tree did not recurse and continue");
}

function capabilities() {
  return {
    schemaVersion: 1,
    profileId: "installed",
    providerProtocol: "custom",
    contextWindowTokens: 16_000,
    maxCallOutputTokens: 512,
    thinkingTokenAccounting: "unknown" as const,
    protocol: {
      reasoningControl: "selectable" as const,
      reasoningReplay: "ignored" as const,
      toolCalling: "supported" as const,
      requiredToolChoice: "supported" as const,
      parallelToolCalls: "supported" as const,
      streaming: "supported" as const,
      cancellation: "supported" as const,
      assistantContentWithToolCalls: "optional" as const,
      jsonSchemaLevel: "unknown",
      streamFinishSemantics: "normalized",
      usageSemantics: "normalized",
    },
  };
}
