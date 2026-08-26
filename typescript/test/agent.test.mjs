import assert from "node:assert/strict";
import test from "node:test";

import { Agent, AgentCanceledError, AgentError } from "purra";

test("Agent completes one validated model/tool loop", async () => {
  const requests = [];
  const agent = new Agent({
    model: {
      async invoke(request) {
        requests.push(request);
        if (request.tools.length === 0) {
          return {
            message: { role: "assistant", content: "Weather: sunny" },
            finishReason: "stop",
          };
        }
        if (request.messages.at(-1).role === "tool") {
          return {
            message: { role: "assistant", content: "private weather candidate" },
            finishReason: "stop",
          };
        }
        return {
          message: {
            role: "assistant",
            content: "",
            toolCalls: [{ id: "call-1", name: "weather", arguments: { city: "Hangzhou" } }],
          },
          finishReason: "stop",
        };
      },
    },
    tools: [readTool("weather",
      (input) => {
        assert.deepEqual(input, { city: "Hangzhou" });
        return { content: { condition: "sunny" }, effectState: "not_started" };
      },
      { inputSchema: objectSchema({ city: { type: "string" } }, ["city"]) },
    )],
  });

  const result = await agent.invoke({ messages: [{ role: "user", content: "Weather?" }] });

  assert.equal(result.output, "Weather: sunny");
  assert.equal(result.rounds, 3);
  assert.equal(requests.length, 3);
  assert.deepEqual(requests[0].tools, [{
    name: "weather",
    description: "weather tool",
    inputSchema: objectSchema({ city: { type: "string" } }, ["city"]),
  }]);
  assert.equal(Object.hasOwn(requests[0].tools[0], "run"), false);
  assert.deepEqual(requests[2].tools, []);
  assert.equal(
    requests[2].messages.some((message) => message.content === "private weather candidate"),
    true,
  );
  assert.equal(
    requests[2].messages.at(-1).content.includes("tool-free response"),
    true,
  );
  assert.equal(JSON.stringify(result.messages).includes("private weather candidate"), false);
  assert.deepEqual(result.messages.at(-2), {
    role: "tool",
    content: { condition: "sunny" },
    toolCallId: "call-1",
  });
});

test("Agent rejects an invalid tool batch before any handler runs", async () => {
  let executions = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        return {
          message: {
            role: "assistant",
            content: "",
            toolCalls: [
              { id: "call-1", name: "known", arguments: {} },
              { id: "call-2", name: "missing", arguments: {} },
            ],
          },
          finishReason: "tool_calls",
        };
      },
    },
    tools: [readTool("known", () => {
      executions += 1;
      return { content: null, effectState: "not_started" };
    })],
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Run tools" }] }),
    (error) => error instanceof AgentError && error.code === "unknown_tool",
  );
  assert.equal(executions, 0);
});

test("Agent stops before calling the model when already canceled", async () => {
  const controller = new AbortController();
  controller.abort();
  const agent = new Agent({
    model: { invoke() { throw new Error("must not run"); } },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Stop" }], signal: controller.signal }),
    AgentCanceledError,
  );
});

test("Agent rejects an empty tool-call turn", async () => {
  const agent = new Agent({
    model: {
      async invoke() {
        return {
          message: { role: "assistant", content: "" },
          finishReason: "tool_calls",
        };
      },
    },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Run a tool" }] }),
    (error) => error instanceof AgentError && error.code === "invalid_model_response",
  );
});

test("Agent converts malformed Provider messages into a coded boundary error", async () => {
  const agent = new Agent({
    model: {
      async invoke() {
        return {
          message: { role: "tool", content: "invalid", toolCallId: "call-1" },
          finishReason: "stop",
        };
      },
    },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Hello" }] }),
    (error) => error instanceof AgentError && error.code === "invalid_model_response",
  );
});

test("internal architecture paths are not public package subpaths", async () => {
  await assert.rejects(
    import("purra/model/validation"),
    (error) => error?.code === "ERR_PACKAGE_PATH_NOT_EXPORTED",
  );
});

test("Agent rejects incomplete model turns with stable error codes", async () => {
  for (const [finishReason, code] of [
    ["length", "model_output_truncated"],
    ["filtered", "model_output_filtered"],
    ["other", "unsupported_model_finish_reason"],
  ]) {
    const agent = new Agent({
      model: {
        async invoke() {
          return { message: { role: "assistant", content: "partial" }, finishReason };
        },
      },
    });

    await assert.rejects(
      agent.invoke({ messages: [{ role: "user", content: "Hello" }] }),
      (error) => error instanceof AgentError && error.code === code,
    );
  }
});

test("Agent never executes a truncated tool call", async () => {
  let executions = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        return {
          message: {
            role: "assistant",
            content: "",
            toolCalls: [{ id: "call-1", name: "known", arguments: {} }],
          },
          finishReason: "length",
        };
      },
    },
    tools: [readTool("known", () => {
      executions += 1;
      return { content: null, effectState: "not_started" };
    })],
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "Run a tool" }] }),
    (error) => error instanceof AgentError && error.code === "tool_call_truncated",
  );
  assert.equal(executions, 0);
});

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
