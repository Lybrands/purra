import {
  Agent,
  AgentOperationController,
  ArtifactAccessController,
  ArtifactLifecycle,
  DurableExecutorRegistry,
  DelegationPolicy,
  evaluateAgentRun,
  InMemoryAgentAdapters,
  InMemoryDelegationRepository,
  InMemoryLongTaskRepository,
  InMemoryArtifactStore,
  ModelResponseJudge,
  ModelTaskRunner,
  ModelWorkPlanner,
  RecipeLongTaskDispatcher,
  RecoveryPolicy,
  ToolPlanningPolicy,
  type JsonValue,
  type LongTaskDispatchReceipt,
  type ModelGateway,
  type DelegationRepository,
  type OutputEvent,
  type RunHandle,
  type ToolDefinition,
} from "@lybrands/purra";

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
    maxTotalTokens: 100,
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

function capabilities() {
  return {
    schemaVersion: 1,
    profileId: "installed",
    providerProtocol: "custom",
    contextWindowTokens: 16_000,
    maxOutputTokens: 512,
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
