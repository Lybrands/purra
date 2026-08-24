import {
  Agent,
  allocateContextBudget,
  assertRunRepositoryConforms,
  InMemoryRunRepository,
  ModelTaskRunner,
  ModelResponseJudge,
  ModelWorkPlanner,
  type JsonValue,
  type ContextProvider,
  type ModelGateway,
  type OutputEvent,
  type RunHandle,
  type ToolDefinition,
} from "@lybrands/purra";

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
  async invoke() {
    return {
      message: { role: "assistant", content: "typed" },
      finishReason: "stop",
    };
  },
  async *stream() {
    yield { contentDelta: "typed", finishReason: "stop" };
  },
};
const modelTasks = new ModelTaskRunner({ model, runId: "typed-model-task" });
(await modelTasks.streamText([{ role: "user", content: "typed model task" }])).content satisfies string;
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
  run(input) {
    input satisfies JsonValue;
    return { content: { found: true }, effectState: "not_started" };
  },
}];
const agent = new Agent({ model, tools });
const result = await agent.invoke({ messages: [{ role: "user", content: "hello" }] });
result.output satisfies JsonValue;
for await (const event of agent.stream({ messages: [{ role: "user", content: "hello" }] })) {
  event.type satisfies "model_delta" | "tool_started" | "tool_completed" | "final";
}

await assertRunRepositoryConforms(new InMemoryRunRepository());
const handle: RunHandle = await agent.submit({
  messages: [{ role: "user", content: "hello" }],
});
(await handle.result).output satisfies JsonValue;
for await (const event of handle.events()) {
  event satisfies OutputEvent;
}
contextProvider satisfies ContextProvider;
