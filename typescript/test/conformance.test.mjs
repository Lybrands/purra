import assert from "node:assert/strict";
import test from "node:test";

import {
  AgentError,
  allocateContextBudget,
  assertArtifactRepositoryConforms,
  assertContextProviderConforms,
  assertDelegationRepositoryConforms,
  assertLongTaskRepositoryConforms,
  assertModelGatewayConforms,
  assertOutputPublisherConforms,
  assertRunRepositoryConforms,
  assertToolDefinitionConforms,
  InMemoryAgentAdapters,
} from "purra";

const readTool = {
  name: "lookup",
  description: "Read a fixture",
  inputSchema: {
    type: "object",
    properties: { key: { type: "string" } },
    required: ["key"],
    additionalProperties: false,
  },
  policy: { mode: "read", title: "Lookup" },
  run(input) { return { content: input, effectState: "not_started" }; },
};

test("in-memory host adapters pass every public repository probe", async () => {
  const adapters = new InMemoryAgentAdapters();
  await assertRunRepositoryConforms(adapters.runs);
  await assertOutputPublisherConforms(adapters.outputs);
  await assertDelegationRepositoryConforms(adapters.delegations);
  await assertLongTaskRepositoryConforms(adapters.longTasks);
  await assertArtifactRepositoryConforms(adapters.artifacts);
});

test("model, context, and tool adapter probes use the real public boundaries", async () => {
  await assertModelGatewayConforms({
    gateway: { async invoke() { return { message: { role: "assistant", content: "ok" }, finishReason: "stop" }; } },
  });
  await assertToolDefinitionConforms({
    definition: readTool,
    validInput: { key: "value" },
    invalidInput: { key: 1 },
  });
  await assertContextProviderConforms({
    provider: { buildContext() { return { blocks: [] }; } },
    request: { messages: [{ role: "user", content: "probe" }] },
    budget: budget(),
  });
});

test("conformance probes reject adapters with removed safety guarantees", async () => {
  await assert.rejects(
    assertModelGatewayConforms({
      gateway: { async invoke() { return { message: { role: "user", content: "wrong" }, finishReason: "stop" }; } },
    }),
  );
  await assert.rejects(assertToolDefinitionConforms({
    definition: { ...readTool, inputSchema: { type: "object", additionalProperties: true } },
    validInput: {},
    invalidInput: { unexpected: true },
  }), nonconforming("tool_definition_nonconforming"));
  await assert.rejects(assertToolDefinitionConforms({
    definition: { ...readTool, run() { return { content: "invalid" }; } },
    validInput: { key: "value" },
    invalidInput: { key: 1 },
  }), nonconforming("tool_definition_nonconforming"));
  await assert.rejects(assertOutputPublisherConforms({
    async publishCommitted() {},
    async waitForSequence() {},
  }), nonconforming("output_publisher_nonconforming"));

  const adapters = new InMemoryAgentAdapters();
  await assert.rejects(assertRunRepositoryConforms(proxy(adapters.runs, {
    async listEvents(target, ...args) { return (await target.listEvents(...args)).slice(0, -1); },
  })));
  await assert.rejects(assertContextProviderConforms({
    provider: { buildContext() { return { blocks: [{ name: "same", content: "a" }, { name: "same", content: "b" }] }; } },
    request: { messages: [{ role: "user", content: "probe" }] },
    budget: budget(),
  }));
  await assert.rejects(assertDelegationRepositoryConforms(proxy(adapters.delegations, {
    async createBatch(target, ...args) { return { ...(await target.createBatch(...args)), replayed: false }; },
  })), nonconforming("delegation_repository_nonconforming"));
  await assert.rejects(assertLongTaskRepositoryConforms(proxy(adapters.longTasks, {
    async create(target, ...args) {
      const result = await target.create(...args);
      return args[0].endsWith("-replay") ? { ...result, id: args[0] } : result;
    },
  })), nonconforming("long_task_repository_nonconforming"));
  await assert.rejects(assertArtifactRepositoryConforms(proxy(adapters.artifacts, {
    async replayReceipt() { return undefined; },
  })), nonconforming("artifact_repository_nonconforming"));
});

function budget() {
  return allocateContextBudget({
    windowTokens: 5_000,
    outputReserveTokens: 1_000,
    reserves: { safetyTokens: 500, runtimeTokens: 500, minimumMessageTokens: 500 },
  });
}

function proxy(target, overrides) {
  return new Proxy(target, {
    get(value, property) {
      const override = overrides[property];
      if (override !== undefined) return (...args) => override(value, ...args);
      const member = value[property];
      return typeof member === "function" ? member.bind(value) : member;
    },
  });
}

function nonconforming(code) {
  return (error) => error instanceof AgentError && error.code === code;
}
