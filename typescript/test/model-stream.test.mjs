import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  Agent,
  AgentCanceledError,
  AgentError,
  constrainOutputBudgetToContext,
  maxGenerationTokensForContext,
  resolveInvocationOutputBudget,
} from "purra";
import { testGateway } from "./support/model-gateway.mjs";

const fixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/model_protocol.json", import.meta.url),
  "utf8",
));

test("Agent consumes model chunks, tool deltas, usage, and output limits", async () => {
  const requests = [];
  let closed = 0;
  const agent = new Agent({
    model: {
      capabilities: capabilities(1000),
      async invoke() {
        throw new Error("stream should be used");
      },
      stream(request) {
        requests.push(request);
        const stream = (async function* () {
          try {
            if (request.tools.length === 0) {
              yield { contentDelta: "Weather: " };
              yield {
                contentDelta: "sunny",
                finishReason: "stop",
                usage: { inputTokens: 12, generationTokens: 2 },
              };
              return;
            }
            if (request.messages.at(-1).role === "tool") {
              yield { contentDelta: "private weather " };
              yield {
                contentDelta: "candidate",
                finishReason: "stop",
                usage: { inputTokens: 12, generationTokens: 2 },
              };
              return;
            }
            yield {
              contentDelta: "Checking",
              reasoningDelta: "private reasoning",
              toolCallDeltas: [{
                index: 0,
                id: "call-1",
                name: "weather",
                argumentsFragment: "{\"city\":",
              }],
            };
            yield {
              toolCallDeltas: [{ index: 0, argumentsFragment: "\"Hangzhou\"}" }],
              finishReason: "stop",
              providerData: { opaque_fixture: "encrypted-state" },
            };
          } finally {
            closed += 1;
          }
        })();
        return Object.assign(stream, {
          appliedGenerationLimit: request.outputBudget?.maxGenerationTokens,
        });
      },
    },
    tools: [readTool("weather", (input) => {
        assert.deepEqual(input, { city: "Hangzhou" });
        return { content: { condition: "sunny" }, effectState: "not_started" };
      }, { inputSchema: objectSchema({ city: { type: "string" } }, ["city"]) },
    )],
  });

  const result = await agent.invoke({
    messages: [{ role: "user", content: "Weather?" }],
    maxGenerationTokens: 200,
  });

  assert.equal(result.output, "Weather: sunny");
  assert.equal(result.rounds, 3);
  assert.equal(closed, 3);
  assert.deepEqual(requests[0].outputBudget, {
    maxGenerationTokens: 200,
    generationSource: "user",
    profileMaxGenerationTokens: 1000,
    requestedUserMaxGenerationTokens: 200,
    resultCapacityTargetTokens: null,
    resultCapacitySource: null,
    nonResultHeadroomTokens: null,
  });
  assert.equal(requests[0].capabilitySnapshot.profileId, "fixture");
  assert.deepEqual(requests[2].tools, []);
  assert.deepEqual(result.messages[1].toolCalls[0].arguments, { city: "Hangzhou" });
  assert.equal(requests[1].messages[1].reasoning, "private reasoning");
  assert.deepEqual(requests[1].messages[1].providerData, { opaque_fixture: "encrypted-state" });
  assert.equal(JSON.stringify(result.messages).includes("encrypted-state"), false);
  assert.equal(JSON.stringify(result.messages).includes("private weather candidate"), false);
  assert.equal(result.messages[1].reasoning, undefined);
});

test("Agent retries one private interrupted stream and closes both attempts", async () => {
  let closed = 0;
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        try {
          yield { contentDelta: "partial" };
        } finally {
          closed += 1;
        }
      },
    }),
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Hello" }] }),
    (error) => error instanceof AgentError && error.code === "upstream_stream_interrupted",
  );
  assert.equal(closed, 2);
});

test("Agent.stream does not retry after a public delta was emitted", async () => {
  let closed = 0;
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        try {
          yield { contentDelta: "visible" };
        } finally {
          closed += 1;
        }
      },
    }),
  });

  const stream = agent.stream({ messages: [{ role: "user", content: "Hello" }] })[Symbol.asyncIterator]();
  assert.deepEqual(await stream.next(), {
    value: { type: "model_delta", delta: "visible" },
    done: false,
  });
  await assert.rejects(
    stream.next(),
    (error) => error instanceof AgentError && error.code === "upstream_stream_interrupted",
  );
  assert.equal(closed, 1);
});

test("Agent.stream emits native public progress separately from answer text", async () => {
  const agent = new Agent({
    model: {
      capabilities: capabilities(1000),
      async invoke() { throw new Error("stream should be used"); },
      stream(request) {
        return Object.assign((async function* () {
          yield { progressDelta: "正在核对人物动机" };
          yield { contentDelta: "分析完成", finishReason: "stop" };
        })(), { appliedGenerationLimit: request.outputBudget?.maxGenerationTokens });
      },
    },
  });

  const events = [];
  for await (const event of agent.stream({
    messages: [{ role: "user", content: "分析" }],
  })) events.push(event);

  assert.deepEqual(events.map((event) => event.type), [
    "agent_progress",
    "model_delta",
    "final",
  ]);
  assert.equal(events[0].text, "正在核对人物动机");
  assert.equal(events.at(-1).result.output, "分析完成");
})

test("Agent closes a pending model stream when canceled", async () => {
  const controller = new AbortController();
  let returned = 0;
  let started;
  const nextStarted = new Promise((resolve) => { started = resolve; });
  const iterator = {
    next() {
      started();
      return new Promise(() => {});
    },
    return() {
      returned += 1;
      return Promise.resolve({ done: true });
    },
  };
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return { [Symbol.asyncIterator]: () => iterator };
      },
    }),
  });

  const running = agent.invoke({
    messages: [{ role: "user", content: "Wait" }],
    signal: controller.signal,
  });
  await nextStarted;
  controller.abort();

  await assert.rejects(running, AgentCanceledError);
  assert.equal(returned, 1);
});

test("Agent stops waiting for a completion gateway when canceled", async () => {
  const controller = new AbortController();
  let started;
  const invocationStarted = new Promise((resolve) => { started = resolve; });
  const agent = new Agent({
    model: testGateway({
      invoke() {
        started();
        return new Promise(() => {});
      },
    }),
  });

  const running = agent.invoke({
    messages: [{ role: "user", content: "Wait" }],
    signal: controller.signal,
  });
  await invocationStarted;
  controller.abort();

  await assert.rejects(running, AgentCanceledError);
});

test("Agent enforces one invocation deadline for a never-returning gateway", async () => {
  const agent = new Agent({
    model: testGateway({ invoke: () => new Promise(() => {}) }),
    runtimeLimits: { invocationTimeoutMs: 100 },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Wait" }] }),
    (error) => error instanceof AgentError
      && error.code === "model_invocation_deadline_exceeded",
  );
});

test("semantic-only streams ignore idle limits and keep the absolute fuse", async () => {
  let closed = 0;
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return streamWithSupport("semantic_only", async function* () {
          try {
            await delay(30);
            yield { contentDelta: "done", finishReason: "stop" };
          } finally {
            closed += 1;
          }
        });
      },
    }),
    runtimeLimits: {
      activityIdleTimeoutMs: 5,
      progressIdleTimeoutMs: 10,
      invocationTimeoutMs: 80,
    },
  });

  const result = await agent.invoke({ messages: [{ role: "user", content: "Wait" }] });
  assert.equal(result.output, "done");
  assert.equal(closed, 1);
});

test("working activity renews both leases and never enters stream budgets", async () => {
  let closed = 0;
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return streamWithSupport("working", async function* () {
          try {
            for (let index = 0; index < 3; index += 1) {
              await delay(4);
              yield { type: "activity", kind: "working" };
            }
            await delay(4);
            yield { contentDelta: "ok", finishReason: "stop" };
          } finally {
            closed += 1;
          }
        });
      },
    }),
    runtimeLimits: {
      activityIdleTimeoutMs: 7,
      progressIdleTimeoutMs: 7,
      invocationTimeoutMs: 80,
      maxChunks: 1,
    },
  });

  const result = await agent.invoke({ messages: [{ role: "user", content: "Work" }] });
  assert.equal(result.output, "ok");
  assert.equal(closed, 1);
});

test("undeclared activity fails before output with the stable code", async () => {
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return streamWithSupport("semantic_only", async function* () {
          yield { type: "activity", kind: "transport" };
        });
      },
    }),
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Wait" }] }),
    (error) => error instanceof AgentError
      && error.code === "model_stream_activity_unsupported",
  );
});

test("Transport-only activity expires progress with the stable code", async () => {
  let closed = 0;
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return streamWithSupport("transport", async function* () {
          try {
            for (let index = 0; index < 20; index += 1) {
              await delay(4);
              yield { type: "activity", kind: "transport" };
            }
          } finally {
            closed += 1;
          }
        });
      },
    }),
    runtimeLimits: {
      activityIdleTimeoutMs: 8,
      progressIdleTimeoutMs: 18,
      invocationTimeoutMs: 100,
    },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Wait" }] }),
    (error) => error instanceof AgentError
      && error.code === "model_progress_deadline_exceeded",
  );
  await delay(10);
  assert.equal(closed, 1);
});

test("declared stream silence expires activity with the stable code", async () => {
  let closed = 0;
  let stoppedBeforeClose = false;
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream(_request, signal) {
        signal.addEventListener("abort", () => { stoppedBeforeClose = closed === 0; }, { once: true });
        return streamWithSupport("transport", async function* () {
          try {
            await delay(100);
          } finally {
            closed += 1;
          }
        });
      },
    }),
    runtimeLimits: {
      activityIdleTimeoutMs: 12,
      progressIdleTimeoutMs: 40,
      invocationTimeoutMs: 100,
    },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Wait" }] }),
    (error) => error instanceof AgentError
      && error.code === "model_activity_deadline_exceeded",
  );
  assert.equal(stoppedBeforeClose, true);
  assert.equal(closed, 1);
});

test("continuous semantic progress still stops at the absolute fuse", async () => {
  let closed = 0;
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return streamWithSupport("working", async function* () {
          try {
            for (let index = 0; index < 30; index += 1) {
              await delay(4);
              yield { contentDelta: "x" };
            }
          } finally {
            closed += 1;
          }
        });
      },
    }),
    runtimeLimits: {
      activityIdleTimeoutMs: 10,
      progressIdleTimeoutMs: 10,
      invocationTimeoutMs: 30,
    },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Stream" }] }),
    (error) => error instanceof AgentError
      && error.code === "model_invocation_deadline_exceeded",
  );
  await delay(10);
  assert.equal(closed, 1);
});

test("Agent rejects the first oversized stream fragment before materializing it", async () => {
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        yield { contentDelta: "ab" };
        yield { contentDelta: "cd", finishReason: "stop" };
      },
    }),
    runtimeLimits: { maxContentChars: 3 },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Oversize" }] }),
    (error) => error instanceof AgentError
      && error.code === "model_stream_limit_exceeded",
  );
});

test("Agent rejects malformed streamed tool calls before execution", async () => {
  let executions = 0;
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        yield {
          toolCallDeltas: [{ index: 0, id: "call-1", argumentsFragment: "{}" }],
          finishReason: "stop",
        };
      },
    }),
    tools: [readTool("known", () => {
      executions += 1;
      return { content: null, effectState: "not_started" };
    })],
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Run" }] }),
    (error) => error instanceof AgentError && error.code === "malformed_tool_call_batch",
  );
  assert.equal(executions, 0);
});

test("Agent accepts detached, immutable JSON messages", async () => {
  const input = { role: "developer", content: { rules: ["safe", 1] } };
  const agent = new Agent({
    model: testGateway({
      async invoke(request) {
        assert.notEqual(request.messages[0].content, input.content);
        assert.equal(Object.isFrozen(request.messages[0].content), true);
        assert.equal(Object.isFrozen(request.messages[0].content.rules), true);
        return {
          message: { role: "assistant", content: { accepted: true } },
          finishReason: "stop",
        };
      },
    }),
  });

  const result = await agent.invoke({ messages: [input] });
  assert.deepEqual(result.output, { accepted: true });
  assert.equal(Object.isFrozen(result.output), true);
});

test("shared message and tool-call cases preserve the Python semantics", async () => {
  let received;
  const agent = new Agent({
    model: testGateway({
      async invoke(request) {
        received = request.messages;
        return { message: { role: "assistant", content: "done" }, finishReason: "stop" };
      },
    }),
  });

  await agent.invoke({ messages: fixture.messageCases });

  assert.deepEqual(received, fixture.messageCases);
  assert.equal(Object.isFrozen(received[0].content), true);
  assert.equal(Object.isFrozen(received[0].toolCalls[0].arguments), true);
  assert.equal(Object.isFrozen(received[1].attributes), true);
});

test("shared termination cases produce the Python safety outcomes", async () => {
  for (const row of fixture.terminationCases) {
    let invocation = 0;
    let executions = 0;
    const agent = new Agent({
      model: testGateway({
        async invoke() {
          invocation += 1;
          if (invocation > 1) {
            return { message: { role: "assistant", content: "done" }, finishReason: "stop" };
          }
          const toolCalls = Array.from({ length: row.toolCallCount }, (_item, index) => ({
            id: `call-${index}`,
            name: "known",
            arguments: {},
          }));
          return {
            message: {
              role: "assistant",
              content: "first",
              ...(toolCalls.length === 0 ? {} : { toolCalls }),
            },
            finishReason: row.finishReason,
          };
        },
      }),
      tools: [readTool("known", () => {
        executions += 1;
        return { content: null, effectState: "not_started" };
      })],
    });
    const running = agent.invoke({ messages: [{ role: "user", content: "Run" }] });
    if (row.errorCode !== null) {
      await assert.rejects(
        running,
        (error) => error instanceof AgentError && error.code === row.errorCode,
      );
      assert.equal(executions, 0);
      continue;
    }
    await running;
    assert.equal(executions, row.authorizesToolCalls ? row.toolCallCount : 0);
  }
});

test("shared output-budget cases produce the Python contract or error", () => {
  for (const row of fixture.outputBudgetCases) {
    const snapshot = capabilities(row.profileMaxGenerationTokens, "unavailable");
    const options = {
      ...(row.maxGenerationTokens === null
        ? {}
        : { maxGenerationTokens: row.maxGenerationTokens }),
      ...(row.generationSource === null ? {} : { generationSource: row.generationSource }),
      ...(row.resultCapacityTargetTokens === null
        ? {}
        : { resultCapacityTargetTokens: row.resultCapacityTargetTokens }),
      ...(row.resultCapacitySource === null
        ? {}
        : { resultCapacitySource: row.resultCapacitySource }),
    };
    if (row.errorCode !== null) {
      assert.throws(
        () => resolveInvocationOutputBudget(snapshot, options),
        (error) => error instanceof AgentError && error.code === row.errorCode,
      );
      continue;
    }
    assert.deepEqual(resolveInvocationOutputBudget(snapshot, options), row.outputBudget);
  }
});

test("context ceilings clamp generation without reusing workflow capacity", () => {
  const snapshot = capabilities(393_216, "unavailable");
  const base = resolveInvocationOutputBudget(snapshot);
  const large = constrainOutputBudgetToContext(
    base,
    maxGenerationTokensForContext({ windowTokens: 1_000_000 }),
  );
  assert.equal(large, base);
  assert.equal(large.generationSource, "model_profile");

  const medium = constrainOutputBudgetToContext(
    base,
    maxGenerationTokensForContext({ windowTokens: 256_000 }),
  );
  assert.equal(medium.maxGenerationTokens, 230_400);
  assert.equal(medium.generationSource, "context_capacity");

  const small = constrainOutputBudgetToContext(
    base,
    maxGenerationTokensForContext({ windowTokens: 32_000 }),
  );
  assert.equal(small.maxGenerationTokens, 24_832);
  assert.equal(small.generationSource, "context_capacity");

  const targeted = resolveInvocationOutputBudget(snapshot, {
    resultCapacityTargetTokens: 25_000,
    resultCapacitySource: "workflow_policy",
  });
  assert.throws(
    () => constrainOutputBudgetToContext(
      targeted,
      maxGenerationTokensForContext({ windowTokens: 32_000 }),
    ),
    (error) => error instanceof AgentError && error.code === "model_result_capacity_incompatible",
  );
});

test("Model gateways must acknowledge the exact applied output limit", async () => {
  const missing = new Agent({
    model: {
      capabilities: capabilities(200),
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return (async function* () {
          yield { contentDelta: "unsafe", finishReason: "stop" };
        })();
      },
    },
  });
  await assert.rejects(
    missing.invoke({ messages: [{ role: "user", content: "run" }] }),
    (error) => error instanceof AgentError && error.code === "model_gateway_contract_violation",
  );

  for (const turn of [
    {
      message: { role: "assistant", content: "wrong limit" },
      finishReason: "stop",
      appliedGenerationLimit: 199,
    },
    {
      message: { role: "assistant", content: "impossible usage" },
      finishReason: "stop",
      appliedGenerationLimit: 200,
      usage: { inputTokens: 1, generationTokens: 201 },
    },
  ]) {
    const falseAcknowledgment = new Agent({
      model: {
        capabilities: capabilities(200, "unavailable"),
        async invoke() { return turn; },
      },
    });
    await assert.rejects(
      falseAcknowledgment.invoke({ messages: [{ role: "user", content: "run" }] }),
      (error) => error instanceof AgentError && error.code === "model_gateway_contract_violation",
    );
  }
});

test("streamed LENGTH failures retain validated Provider usage", async () => {
  const agent = new Agent({
    model: {
      capabilities: capabilities(200),
      async invoke() { throw new Error("stream should be used"); },
      stream(request) {
        return Object.assign((async function* () {
          yield {
            contentDelta: "partial",
            finishReason: "length",
            usage: { inputTokens: 3, generationTokens: 7 },
          };
        })(), { appliedGenerationLimit: request.outputBudget.maxGenerationTokens });
      },
    },
  });
  const run = await agent.submit(
    { messages: [{ role: "user", content: "write" }] },
    { budgets: { maxRunGenerationTokens: 100 } },
  );
  await assert.rejects(
    run.result,
    (error) => error instanceof AgentError && error.code === "model_output_truncated",
  );
  const snapshot = await run.snapshot();
  assert.equal(snapshot.usage.inputTokens, 3);
  assert.equal(snapshot.usage.generationTokens, 7);
  assert.equal(snapshot.usage.unreportedUsageAttempts, 0);
});

test("reasoning usage stays unknown and accounting semantics govern its relationship", async () => {
  const invokeWith = async (thinkingTokenAccounting, reasoningUsageDetail, usage) => {
    const base = capabilities(200, "unavailable");
    const agent = new Agent({
      model: {
        capabilities: { ...base, thinkingTokenAccounting, reasoningUsageDetail },
        async invoke(request) {
          return {
            message: { role: "assistant", content: "done" },
            finishReason: "stop",
            appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
            usage,
          };
        },
      },
    });
    return agent.invoke({ messages: [{ role: "user", content: "run" }] });
  };

  await assert.rejects(
    invokeWith("included", "required", { inputTokens: 1, generationTokens: 10 }),
    (error) => error instanceof AgentError && error.code === "model_gateway_contract_violation",
  );
  await assert.rejects(
    invokeWith("included", "optional", {
      inputTokens: 1,
      generationTokens: 10,
      reasoningTokens: 11,
    }),
    (error) => error instanceof AgentError && error.code === "model_gateway_contract_violation",
  );
  assert.equal((await invokeWith("separate", "optional", {
    inputTokens: 1,
    generationTokens: 10,
    reasoningTokens: 11,
  })).output, "done");
});

test("capability snapshots reject the pre-budget-contract schema", () => {
  assert.throws(
    () => new Agent({
      model: {
        capabilities: { ...capabilities(1000), schemaVersion: 1 },
        async invoke() {
          return { message: { role: "assistant", content: "unsafe" }, finishReason: "stop" };
        },
      },
    }),
    { message: "Unsupported model capability schema version" },
  );
});

test("Agent admission requires an actionable exact generation capability", () => {
  let calls = 0;
  assert.throws(
    () => new Agent({
      model: {
        async invoke() { calls += 1; throw new Error("unreachable"); },
      },
    }),
    (error) => error instanceof AgentError && error.code === "model_generation_limit_unknown",
  );
  assert.throws(
    () => new Agent({
      model: {
        capabilities: { ...capabilities(100), maxGenerationTokens: null },
        async invoke() { calls += 1; throw new Error("unreachable"); },
      },
    }),
    (error) => error instanceof AgentError && error.code === "model_generation_limit_unknown",
  );
  assert.equal(calls, 0);
});

test("legacy or missing generation usage fails closed after one Provider call", async () => {
  for (const usage of [
    { inputTokens: 1, outputTokens: 2 },
    { inputTokens: 1 },
    { inputTokens: 1, generationTokens: 2, outputTokens: 2 },
  ]) {
    let calls = 0;
    const agent = new Agent({
      model: {
        capabilities: capabilities(100, "unavailable"),
        async invoke(request) {
          calls += 1;
          return {
            message: { role: "assistant", content: "invalid usage" },
            finishReason: "stop",
            appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
            usage,
          };
        },
      },
    });
    await assert.rejects(
      agent.invoke({ messages: [{ role: "user", content: "run" }] }),
      (error) => error instanceof AgentError && error.code === "invalid_model_response",
    );
    assert.equal(calls, 1);
  }

  let streamCalls = 0;
  const streamed = new Agent({
    model: {
      capabilities: capabilities(100),
      async invoke() { throw new Error("stream should be used"); },
      stream(request) {
        streamCalls += 1;
        return Object.assign((async function* () {
          yield {
            finishReason: "stop",
            usage: { inputTokens: 1, outputTokens: 2 },
          };
        })(), { appliedGenerationLimit: request.outputBudget.maxGenerationTokens });
      },
    },
  });
  await assert.rejects(
    streamed.invoke({ messages: [{ role: "user", content: "run" }] }),
    (error) => error instanceof AgentError && error.code === "invalid_model_response",
  );
  assert.equal(streamCalls, 1);
});

function capabilities(maxGenerationTokens, streaming = "supported") {
  return {
    schemaVersion: 2,
    profileId: "fixture",
    providerProtocol: "custom",
    contextWindowTokens: 16_000,
    maxGenerationTokens,
    thinkingTokenAccounting: "unknown",
    protocol: {
      reasoningControl: "selectable",
      reasoningReplay: "ignored",
      toolCalling: "supported",
      requiredToolChoice: "supported",
      parallelToolCalls: "supported",
      streaming,
      cancellation: "supported",
      publicProgress: "supported",
      assistantContentWithToolCalls: "optional",
      jsonSchemaLevel: "unknown",
      streamFinishSemantics: "normalized",
      usageSemantics: "normalized",
    },
  };
}

function readTool(name, run, overrides = {}) {
  return {
    name,
    description: `${name} tool`,
    inputSchema: objectSchema(),
    policy: { mode: "read", title: name },
    run,
    ...overrides,
  };
}

function objectSchema(properties = {}, required = []) {
  return {
    type: "object",
    properties,
    required,
    additionalProperties: Object.keys(properties).length === 0,
  };
}

function streamWithSupport(activitySupport, factory) {
  return Object.assign(factory(), { activitySupport });
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
