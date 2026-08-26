import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { Agent, AgentCanceledError, AgentError } from "purra";

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
        return (async function* () {
          try {
            if (request.messages.at(-1).role === "tool") {
              yield { contentDelta: "Weather: " };
              yield {
                contentDelta: "sunny",
                finishReason: "stop",
                usage: { inputTokens: 12, outputTokens: 2 },
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
            };
          } finally {
            closed += 1;
          }
        })();
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
    maxOutputTokens: 200,
  });

  assert.equal(result.output, "Weather: sunny");
  assert.equal(result.rounds, 2);
  assert.equal(closed, 2);
  assert.deepEqual(requests[0].outputLimit, {
    maxTokens: 200,
    source: "user_override",
    profileMaxTokens: 1000,
  });
  assert.equal(requests[0].capabilitySnapshot.profileId, "fixture");
  assert.deepEqual(result.messages[1].toolCalls[0].arguments, { city: "Hangzhou" });
  assert.equal(requests[1].messages[1].reasoning, "private reasoning");
  assert.equal(result.messages[1].reasoning, undefined);
});

test("Agent retries one private interrupted stream and closes both attempts", async () => {
  let closed = 0;
  const agent = new Agent({
    model: {
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        try {
          yield { contentDelta: "partial" };
        } finally {
          closed += 1;
        }
      },
    },
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
    model: {
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        try {
          yield { contentDelta: "visible" };
        } finally {
          closed += 1;
        }
      },
    },
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
    model: {
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return { [Symbol.asyncIterator]: () => iterator };
      },
    },
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
    model: {
      invoke() {
        started();
        return new Promise(() => {});
      },
    },
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
    model: { invoke: () => new Promise(() => {}) },
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
    model: {
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
    },
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
    model: {
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
    },
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
    model: {
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return streamWithSupport("semantic_only", async function* () {
          yield { type: "activity", kind: "transport" };
        });
      },
    },
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
    model: {
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
    },
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
  const agent = new Agent({
    model: {
      async invoke() { throw new Error("stream should be used"); },
      stream() {
        return streamWithSupport("transport", async function* () {
          try {
            await delay(100);
          } finally {
            closed += 1;
          }
        });
      },
    },
    runtimeLimits: {
      activityIdleTimeoutMs: 12,
      progressIdleTimeoutMs: 40,
      invocationTimeoutMs: 100,
    },
  });

  const startedAt = performance.now();
  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Wait" }] }),
    (error) => error instanceof AgentError
      && error.code === "model_activity_deadline_exceeded",
  );
  assert.ok(performance.now() - startedAt < 60);
  assert.equal(closed, 0);
  await delay(100);
  assert.equal(closed, 1);
});

test("continuous semantic progress still stops at the absolute fuse", async () => {
  let closed = 0;
  const agent = new Agent({
    model: {
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
    },
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
    model: {
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        yield { contentDelta: "ab" };
        yield { contentDelta: "cd", finishReason: "stop" };
      },
    },
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
    model: {
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        yield {
          toolCallDeltas: [{ index: 0, id: "call-1", argumentsFragment: "{}" }],
          finishReason: "stop",
        };
      },
    },
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
    model: {
      async invoke(request) {
        assert.notEqual(request.messages[0].content, input.content);
        assert.equal(Object.isFrozen(request.messages[0].content), true);
        assert.equal(Object.isFrozen(request.messages[0].content.rules), true);
        return {
          message: { role: "assistant", content: { accepted: true } },
          finishReason: "stop",
        };
      },
    },
  });

  const result = await agent.invoke({ messages: [input] });
  assert.deepEqual(result.output, { accepted: true });
  assert.equal(Object.isFrozen(result.output), true);
});

test("shared message and tool-call cases preserve the Python semantics", async () => {
  let received;
  const agent = new Agent({
    model: {
      async invoke(request) {
        received = request.messages;
        return { message: { role: "assistant", content: "done" }, finishReason: "stop" };
      },
    },
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
      model: {
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
      },
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

test("shared output-limit cases produce the Python request or error", async () => {
  for (const row of fixture.outputLimitCases) {
    const requests = [];
    const agent = new Agent({
      model: {
        capabilities: capabilities(row.profileMaxTokens, "unavailable"),
        async invoke(request) {
          requests.push(request);
          return { message: { role: "assistant", content: "done" }, finishReason: "stop" };
        },
      },
    });
    const input = {
      messages: [{ role: "user", content: "Run" }],
      ...(row.userOverride === null ? {} : { maxOutputTokens: row.userOverride }),
    };
    if (row.errorCode !== null) {
      await assert.rejects(
        agent.invoke(input),
        (error) => error instanceof AgentError && error.code === row.errorCode,
      );
      assert.equal(requests.length, 0);
      continue;
    }
    await agent.invoke(input);
    assert.deepEqual(requests[0].outputLimit, row.outputLimit);
  }
});

function capabilities(maxOutputTokens, streaming = "supported") {
  return {
    schemaVersion: 1,
    profileId: "fixture",
    providerProtocol: "custom",
    contextWindowTokens: 4000,
    maxOutputTokens,
    thinkingTokenAccounting: "unknown",
    protocol: {
      reasoningControl: "selectable",
      reasoningReplay: "ignored",
      toolCalling: "supported",
      requiredToolChoice: "supported",
      parallelToolCalls: "supported",
      streaming,
      cancellation: "supported",
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
