import {
  Agent,
  allocateContextBudget,
  assertRunRepositoryConforms,
  InMemoryRunRepository,
  ModelTaskRunner,
  ModelResponseJudge,
  ModelWorkPlanner,
  RetrievalError,
  RetrieverTool,
  type JsonValue,
  type ContextProvider,
  type ModelGateway,
  type ModelCapabilitySnapshot,
  type ModelTokenUsage,
  type OutputEvent,
  type RetrievalHit,
  type RetrievalRequest,
  type Retriever,
  type RunHandle,
  type ToolDefinition,
  type ToolPlanningRequirement,
} from "purra";

// @ts-expect-error generation usage is mandatory; only reasoning detail may be unknown
const incompleteUsage: ModelTokenUsage = { inputTokens: 1 };
void incompleteUsage;

const typedCapabilities = {
  schemaVersion: 2,
  profileId: "typed-consumer",
  providerProtocol: "custom",
  contextWindowTokens: 16_000,
  maxGenerationTokens: 512,
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
} satisfies ModelCapabilitySnapshot;

const budget = allocateContextBudget({
  windowTokens: 5_000,
  outputReserveTokens: 1_000,
  reserves: { safetyTokens: 500, runtimeTokens: 500, minimumMessageTokens: 500 },
});
budget.providerInputTokens satisfies number;
const contextProvider: ContextProvider = {
  buildContext() { return { blocks: [] }; },
};

const model: ModelGateway = {
  capabilities: typedCapabilities,
  async invoke(request) {
    return {
      message: { role: "assistant", content: "typed" },
      finishReason: "stop",
      appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
    };
  },
  stream(request) {
    return {
      appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
      async *[Symbol.asyncIterator]() {
        yield { contentDelta: "typed", finishReason: "stop" as const };
      },
    };
  },
};
const modelTasks = new ModelTaskRunner({ model, runId: "typed-model-task" });
(await modelTasks.streamText([{ role: "user", content: "typed model task" }])).content satisfies string;
// @ts-expect-error generation authority is bound when the runner is constructed, never per call
await modelTasks.complete([{ role: "user", content: "typed model task" }], { maxGenerationTokens: 64 });
new ModelWorkPlanner(modelTasks).createPlan satisfies Function;
new ModelResponseJudge(modelTasks, {
  buildMessages({ content }) { return [{ role: "user", content }]; },
  evaluate() { return {}; },
}).judge satisfies Function;
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
  planningRequirement: "optional" satisfies ToolPlanningRequirement,
  run(input) {
    input satisfies JsonValue;
    return { content: { found: true }, effectState: "not_started" };
  },
}];
const retriever: Retriever = {
  async retrieve(request: RetrievalRequest, signal?: AbortSignal) {
    request.query satisfies string;
    request.limit satisfies number;
    signal?.aborted satisfies boolean | undefined;
    const hits: readonly RetrievalHit[] = [{
      id: "typed-hit",
      content: "typed content",
      source: "typed-fixture",
      untrusted: true,
      metadata: {},
    }];
    return hits;
  },
};
const retrieverTool = new RetrieverTool({
  retriever,
  name: "search_knowledge",
  description: "Search configured knowledge.",
  scope: { namespace: "typed-project" },
});
retrieverTool.definition satisfies ToolDefinition;
new RetrievalError(
  "retrieval_timeout",
  "retrieval timed out",
  { retryable: true },
).retryable satisfies boolean;
const agent = new Agent({ model, tools });
const result = await agent.invoke({ messages: [{ role: "user", content: "hello" }] });
result.output satisfies JsonValue;
for await (const event of agent.stream({ messages: [{ role: "user", content: "hello" }] })) {
  event.type satisfies "agent_progress" | "model_delta" | "tool_started" | "tool_completed" | "final";
}

await assertRunRepositoryConforms(new InMemoryRunRepository());
const handle: RunHandle = await agent.submit({
  messages: [{ role: "user", content: "hello" }],
}, {
  budgets: { maxRunGenerationTokens: null },
});
(await handle.result).output satisfies JsonValue;
for await (const event of handle.events()) {
  event satisfies OutputEvent;
}
contextProvider satisfies ContextProvider;

import { PlanningStreamParser, PLANNING_STREAM_SCHEMA,
  type PlanningScope, type PlanningProgress, type ModelTaskPlanOptions, type ModelTransportDiagnostics,
} from "purra";
const planningScope: PlanningScope = { runId: "typed", operationId: "phase", revision: 0 };
const planningProgress: PlanningProgress = { text: "Intent", recordIndex: 1, sourceStart: 0, sourceEnd: 44 };
const planOptions: ModelTaskPlanOptions = { scope: planningScope, attempt: 0, validatePlan: () => undefined };
const transport: ModelTransportDiagnostics = { requestSentAtMs: 1000, firstByteAtMs: 1001, httpAttempts: 1 };
PLANNING_STREAM_SCHEMA satisfies "purra.planning-stream/v1";
new PlanningStreamParser().feed("") satisfies readonly PlanningProgress[];
void [planningProgress, planOptions, transport];

import { StructuredOutputContract, type StructuredOutputLimits } from "purra";
const structuredOutput = await StructuredOutputContract.create({
  schemaId: "typed", schemaVersion: "1",
  schema: { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"], additionalProperties: false },
  limits: { outputBytes: 1024 } satisfies Partial<StructuredOutputLimits>,
});
const structuredValue = structuredOutput.parse('{"ok":true}');
structuredValue satisfies Readonly<Record<string, JsonValue>>;
// @ts-expect-error Validated JSON objects are immutable.
structuredValue.ok = false;
// @ts-expect-error The JSON contract does not assert an application-specific shape.
structuredValue satisfies { readonly ok: boolean };
