import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { Agent, AgentCanceledError, AgentError, RecoveryPolicy } from "purra";

const sharedFixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/tool_security.json", import.meta.url),
  "utf8",
));

test("Tool Catalog rejects unsupported nested schemas at assembly", () => {
  assert.throws(
    () => new Agent({
      model: completion("done"),
      tools: [readTool("broken", () => readResult(null), {
        inputSchema: objectSchema({ nested: { type: "object", patternProperties: {} } }),
      })],
    }),
    (error) => error instanceof AgentError && error.code === "invalid_tool_schema",
  );
});

test("an invalid argument rejects the whole batch before any handler", async () => {
  let executions = 0;
  const schema = objectSchema({
    rows: {
      type: "array",
      minItems: 1,
      items: objectSchema({ score: { type: "number", minimum: 0, maximum: 1 } }, ["score"]),
    },
  }, ["rows"]);
  const agent = new Agent({
    model: calls([
      { id: "one", name: "bounded", arguments: { rows: [{ score: 0.5 }] } },
      { id: "two", name: "bounded", arguments: { rows: [{ score: 2 }] } },
    ]),
    tools: [readTool("bounded", () => {
      executions += 1;
      return readResult(null);
    }, { inputSchema: schema })],
  });

  await rejectsCode(agent.invoke(runInput()), "invalid_tool_arguments_schema");
  assert.equal(executions, 0);
});

test("shared schema cases produce the Python admission outcomes", async () => {
  for (const row of sharedFixture.argumentSchemaCases) {
    let round = 0;
    let executions = 0;
    const agent = new Agent({
      recovery: new RecoveryPolicy({}),
      model: {
        async invoke() {
          round += 1;
          return round === 1
            ? callsTurn([{ id: "fixture-call", name: row.name, arguments: row.arguments }])
            : finalTurn("done");
        },
      },
      tools: [readTool(row.name, () => {
        executions += 1;
        return readResult(null);
      }, { inputSchema: row.schema })],
    });
    const running = agent.invoke(runInput());
    if (row.errorCode === null) {
      await running;
      assert.equal(executions, 1);
    } else {
      await rejectsCode(running, row.errorCode);
      assert.equal(executions, 0);
    }
  }
});

test("disabled tools stay absent from the model request and cannot execute", async () => {
  let executions = 0;
  const requests = [];
  const agent = new Agent({
    model: {
      async invoke(request) {
        requests.push(request);
        return callsTurn([{ id: "one", name: "hidden", arguments: {} }]);
      },
    },
    tools: [readTool("hidden", () => {
      executions += 1;
      return readResult(null);
    }, { enabled: false })],
  });

  await rejectsCode(agent.invoke(runInput()), "tool_not_enabled");
  assert.deepEqual(requests[0].tools, []);
  assert.equal(executions, 0);
});

test("all scope checks complete before the first handler starts", async () => {
  let executions = 0;
  const agent = new Agent({
    model: calls([
      { id: "one", name: "allowed", arguments: {} },
      { id: "two", name: "denied", arguments: {} },
    ]),
    tools: [
      readTool("allowed", () => { executions += 1; return readResult(null); }, { scope: () => true }),
      readTool("denied", () => { executions += 1; return readResult(null); }, { scope: () => "outside project" }),
    ],
  });

  await rejectsCode(agent.invoke(runInput()), "tool_scope_violation");
  assert.equal(executions, 0);
});

test("all approvals complete before the first handler starts", async () => {
  let executions = 0;
  const statuses = ["approved", "rejected"];
  const agent = new Agent({
    model: calls([
      { id: "one", name: "write_one", arguments: {} },
      { id: "two", name: "write_two", arguments: {} },
    ]),
    approval: { request() { return statuses.shift(); } },
    idempotency: memoryIdempotency(),
    tools: [
      confirmTool("write_one", () => { executions += 1; return committed("one"); }),
      confirmTool("write_two", () => { executions += 1; return committed("two"); }),
    ],
  });

  await rejectsCode(agent.invoke(runInput()), "tool_approval_rejected");
  assert.equal(executions, 0);
});

test("approval timeout rejects before a handler starts", async () => {
  let executions = 0;
  const agent = new Agent({
    model: calls([{ id: "one", name: "write", arguments: {} }]),
    approval: { request() { return new Promise(() => {}); } },
    idempotency: memoryIdempotency(),
    toolLimits: { approvalTimeoutMs: 5 },
    tools: [confirmTool("write", () => { executions += 1; return committed(null); })],
  });

  await rejectsCode(agent.invoke(runInput()), "tool_approval_timed_out");
  assert.equal(executions, 0);
});

test("side-effecting tools require idempotency or host-managed durability", () => {
  assert.throws(
    () => new Agent({
      model: completion("done"),
      tools: [proposeTool("write", () => committed("done"))],
    }),
    (error) => error instanceof AgentError && error.code === "tool_idempotency_unavailable",
  );
});

test("idempotency prevents a repeated model call id from repeating an effect", async () => {
  let modelRound = 0;
  let executions = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        modelRound += 1;
        if (modelRound === 3) return finalTurn("done");
        return callsTurn([{ id: "same", name: "write", arguments: { value: 1 } }]);
      },
    },
    idempotency: memoryIdempotency(),
    tools: [proposeTool("write", () => {
      executions += 1;
      return committed({ saved: true });
    }, { inputSchema: objectSchema({ value: { type: "integer" } }, ["value"]) })],
  });

  const result = await agent.invoke(runInput());
  assert.equal(result.output, "done");
  assert.equal(executions, 1);
});

test("an uncertain side effect stops recovery and is never replayed", async () => {
  let executions = 0;
  let modelCalls = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        modelCalls += 1;
        return callsTurn([{ id: "one", name: "write", arguments: {} }]);
      },
    },
    idempotency: memoryIdempotency(),
    tools: [proposeTool("write", () => {
      executions += 1;
      throw new Error("database response was lost");
    })],
  });

  await rejectsCode(agent.invoke(runInput()), "tool_effect_unknown");
  assert.equal(executions, 1);
  assert.equal(modelCalls, 1);
});

test("cancellation reports unknown for a non-linearizable side effect", async () => {
  const controller = new AbortController();
  let started;
  const runningTool = new Promise((resolve) => { started = resolve; });
  const agent = new Agent({
    model: calls([{ id: "one", name: "write", arguments: {} }]),
    idempotency: memoryIdempotency(),
    tools: [proposeTool("write", async () => {
      started();
      await new Promise(() => {});
      return committed(null);
    })],
  });
  const running = agent.invoke({ ...runInput(), signal: controller.signal });
  await runningTool;
  controller.abort();

  await rejectsCode(running, "tool_effect_unknown");
});

test("linearizable tool cancellation stays a normal cancellation", async () => {
  const controller = new AbortController();
  let started;
  const runningTool = new Promise((resolve) => { started = resolve; });
  const agent = new Agent({
    model: calls([{ id: "one", name: "write", arguments: {} }]),
    idempotency: memoryIdempotency(),
    tools: [proposeTool("write", async () => {
      started();
      await new Promise(() => {});
      return committed(null);
    }, { cancellationLinearizable: true })],
  });
  const running = agent.invoke({ ...runInput(), signal: controller.signal });
  await runningTool;
  controller.abort();

  await assert.rejects(running, AgentCanceledError);
});

test("read failures become sanitized tool receipts for bounded recovery", async () => {
  const requests = [];
  const agent = new Agent({
    model: {
      async invoke(request) {
        requests.push(request);
        return requests.length === 1
          ? callsTurn([{ id: "one", name: "read", arguments: {} }])
          : finalTurn("recovered");
      },
    },
    tools: [readTool("read", () => { throw new Error("secret-token-/private/path"); })],
  });

  const result = await agent.invoke(runInput());
  const receipt = requests[1].messages.at(-1).content;
  assert.equal(result.output, "recovered");
  assert.deepEqual(receipt, {
    ok: false,
    error: {
      code: "tool_execution_failed",
      message: "Tool execution did not complete successfully.",
    },
    effectState: "not_started",
  });
  assert.equal(JSON.stringify(receipt).includes("secret-token"), false);
  assert.equal(JSON.stringify(receipt).includes("private/path"), false);
});

test("oversized tool results are replaced with a bounded receipt", async () => {
  const requests = [];
  const agent = new Agent({
    model: {
      async invoke(request) {
        requests.push(request);
        return requests.length === 1
          ? callsTurn([{ id: "one", name: "read", arguments: {} }])
          : finalTurn("bounded");
      },
    },
    toolLimits: { maxResultChars: 20 },
    tools: [readTool("read", () => readResult("x".repeat(100)))],
  });

  const result = await agent.invoke(runInput());
  assert.equal(result.output, "bounded");
  assert.equal(requests[1].messages.at(-1).content.error.code, "tool_result_too_large");
});

test("Agent.stream emits public deltas, tool lifecycle, and one final result", async () => {
  let round = 0;
  const agent = new Agent({
    model: {
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        round += 1;
        if (round === 1) {
          yield { contentDelta: "working", reasoningDelta: "private" };
          yield {
            toolCallDeltas: [{ index: 0, id: "one", name: "read", argumentsFragment: "{}" }],
            finishReason: "tool_calls",
          };
          return;
        }
        yield { contentDelta: "done", reasoningDelta: "still private", finishReason: "stop" };
      },
    },
    tools: [readTool("read", () => readResult({ ok: true }))],
  });

  const events = [];
  for await (const event of agent.stream(runInput())) events.push(event);

  assert.deepEqual(events.map((event) => event.type), [
    "model_delta",
    "tool_started",
    "tool_completed",
    "model_delta",
    "final",
  ]);
  assert.equal(events.some((event) => JSON.stringify(event).includes("private")), false);
  assert.equal(events.at(-1).result.output, "done");
  assert.equal(events.at(-1).result.messages.some((message) => "reasoning" in message), false);
});

test("closing Agent.stream cancels and closes the upstream iterator", async () => {
  let returned = 0;
  let nextCount = 0;
  const iterator = {
    next() {
      nextCount += 1;
      return nextCount === 1
        ? Promise.resolve({ value: { contentDelta: "partial" }, done: false })
        : new Promise(() => {});
    },
    return() {
      returned += 1;
      return Promise.resolve({ done: true });
    },
  };
  const agent = new Agent({
    model: {
      async invoke() { throw new Error("stream should be used"); },
      stream() { return { [Symbol.asyncIterator]: () => iterator }; },
    },
  });

  const stream = agent.stream(runInput())[Symbol.asyncIterator]();
  assert.equal((await stream.next()).value.type, "model_delta");
  await stream.return();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(returned, 1);
});

test("empty final output and exhausted model rounds fail with stable codes", async () => {
  await rejectsCode(new Agent({ model: completion("   ") }).invoke(runInput()), "empty_model_response");

  let executions = 0;
  const agent = new Agent({
    model: calls([{ id: "one", name: "read", arguments: {} }]),
    maxRounds: 1,
    tools: [readTool("read", () => { executions += 1; return readResult(null); })],
  });
  await rejectsCode(agent.invoke(runInput()), "max_rounds_exceeded");
  assert.equal(executions, 1);
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

function proposeTool(name, run, overrides = {}) {
  return {
    name,
    description: `${name} tool`,
    inputSchema: objectSchema(),
    policy: { mode: "propose", title: name, riskLevel: "write" },
    run,
    ...overrides,
  };
}

function confirmTool(name, run, overrides = {}) {
  return proposeTool(name, run, {
    policy: { mode: "confirm", title: name, riskLevel: "destructive" },
    ...overrides,
  });
}

function objectSchema(properties = {}, required = []) {
  return {
    type: "object",
    properties,
    required,
    additionalProperties: Object.keys(properties).length === 0,
  };
}

function readResult(content) {
  return { content, effectState: "not_started" };
}

function committed(content) {
  return { content, effectState: "committed" };
}

function completion(content) {
  return { async invoke() { return finalTurn(content); } };
}

function calls(toolCalls) {
  return { async invoke() { return callsTurn(toolCalls); } };
}

function callsTurn(toolCalls) {
  return {
    message: { role: "assistant", content: "", toolCalls },
    finishReason: "tool_calls",
  };
}

function finalTurn(content) {
  return { message: { role: "assistant", content }, finishReason: "stop" };
}

function runInput() {
  return { messages: [{ role: "user", content: "run" }] };
}

function memoryIdempotency() {
  const results = new Map();
  return {
    executeOnce(key, operation) {
      const existing = results.get(key);
      if (existing !== undefined) return existing;
      const running = Promise.resolve().then(operation);
      results.set(key, running);
      return running;
    },
  };
}

async function rejectsCode(promise, code) {
  await assert.rejects(
    promise,
    (error) => error instanceof AgentError && error.code === code,
  );
}
