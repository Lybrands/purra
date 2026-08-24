import assert from "node:assert/strict";
import test from "node:test";

import {
  Agent,
  AgentCanceledError,
  AgentError,
  assertRunRepositoryConforms,
  InMemoryOutputPublisher,
  InMemoryRunRepository,
} from "@lybrands/purra";

test("in-memory Run repository passes the public conformance probe", async () => {
  await assertRunRepositoryConforms(new InMemoryRunRepository());
});

test("submitted Run persists private model evidence and public output in order", async () => {
  const repository = new InMemoryRunRepository();
  let round = 0;
  const agent = new Agent({
    preset: {
      id: "fixture",
      revision: "3",
      promptSections: [{ id: "policy", role: "system", content: "private-prompt" }],
    },
    runRepository: repository,
    model: {
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        round += 1;
        if (round === 1) {
          yield { contentDelta: "Checking", reasoningDelta: "private-reasoning" };
          yield {
            toolCallDeltas: [{
              index: 0,
              id: "call-1",
              name: "lookup",
              argumentsFragment: "{\"key\":\"purr\"}",
            }],
            finishReason: "tool_calls",
            usage: { inputTokens: 10, outputTokens: 2, totalTokens: 12 },
          };
          return;
        }
        yield {
          contentDelta: "Found it",
          reasoningDelta: "another-secret",
          finishReason: "stop",
          usage: { inputTokens: 12, outputTokens: 2, totalTokens: 14 },
        };
      },
    },
    tools: [readTool("lookup", (input) => readResult({ value: input.key }))],
  });

  const handle = await agent.submit({
    messages: [{ role: "user", content: "find" }],
    contextEvidence: [{ evidenceId: "ev-1", source: "fixture", itemId: "item-1" }],
  });
  const result = await handle.result;
  const all = await collect(handle.events({ visibility: "all" }));
  const publicEvents = await collect(handle.events());

  assert.equal(result.output, "Found it");
  assert.equal(JSON.stringify(result.messages).includes("private-prompt"), false);
  assert.equal(JSON.stringify(result.messages).includes("private-reasoning"), false);
  assert.deepEqual(all.map((event) => event.sequence), all.map((_event, index) => index + 1));
  assert.equal(all[0].kind, "run.started");
  assert.deepEqual(all.slice(-2).map((event) => event.kind), ["final", "run.completed"]);
  assert.equal(all.filter((event) => event.kind === "invocation.started").length, 2);
  assert.equal(all.filter((event) => event.kind === "invocation.completed").length, 2);
  assert.equal(all.some((event) => event.kind === "reasoning.delta"), true);
  assert.equal(all.some((event) => event.kind === "commentary"), true);
  assert.equal(all.some((event) => event.kind === "tool.started"), true);
  assert.equal(all.some((event) => event.kind === "tool.completed"), true);
  assert.equal(publicEvents.some((event) => event.channel === "reasoning"), false);
  assert.equal(publicEvents.some((event) => event.channel === "model"), false);
  assert.deepEqual(publicEvents.map((event) => event.kind), [
    "run.started",
    "commentary",
    "tool.started",
    "tool.completed",
    "final",
    "run.completed",
  ]);
  assert.deepEqual(
    (await collect(handle.events({ afterSequence: publicEvents[1].sequence })))
      .map((event) => event.kind),
    publicEvents.slice(2).map((event) => event.kind),
  );
  const receipt = all.find((event) => event.kind === "invocation.started").payload.receipt;
  assert.match(receipt.messageFingerprint, /^[a-f0-9]{64}$/);
  assert.match(receipt.toolFingerprint, /^[a-f0-9]{64}$/);
  assert.match(receipt.evidenceFingerprint, /^[a-f0-9]{64}$/);
  assert.equal(JSON.stringify(receipt).includes("private-prompt"), false);
  assert.equal((await handle.snapshot()).usage.knownTokens, 26);
  assert.equal((await handle.cancel()).accepted, false);
});

test("failed terminal commit does not expose a final answer or completed state", async () => {
  const base = new InMemoryRunRepository();
  const repository = proxyRepository(base, {
    settleRun(runId, status, options) {
      if (status === "completed") {
        throw new AgentError("repository_failed", "terminal commit failed");
      }
      return base.settleRun(runId, status, options);
    },
  });
  const agent = new Agent({
    runRepository: repository,
    model: { async invoke() { return finalTurn("uncommitted"); } },
  });
  const handle = await agent.submit({ messages: [{ role: "user", content: "run" }] });

  await rejectsCode(handle.result, "repository_failed");
  const events = await collect(handle.events({ visibility: "all" }));
  assert.equal(events.some((event) => event.kind === "final"), false);
  assert.equal(events.some((event) => event.kind === "run.completed"), false);
  assert.equal(events.at(-1).kind, "run.failed");
  assert.equal((await handle.snapshot()).status, "failed");
});

test("Run begin failure prevents Provider and tool execution", async () => {
  let modelCalls = 0;
  let toolCalls = 0;
  const repository = failingRepository("begin");
  const agent = new Agent({
    runRepository: repository,
    model: { async invoke() { modelCalls += 1; return finalTurn("no"); } },
    tools: [readTool("read", () => { toolCalls += 1; return readResult(null); })],
  });

  await assert.rejects(agent.submit({ messages: [{ role: "user", content: "run" }] }));
  assert.equal(modelCalls, 0);
  assert.equal(toolCalls, 0);
});

test("invocation receipt persistence failure prevents the Provider call", async () => {
  let modelCalls = 0;
  const base = new InMemoryRunRepository();
  const repository = proxyRepository(base, {
    openInvocation() { throw new AgentError("repository_failed", "open failed"); },
  });
  const agent = new Agent({
    runRepository: repository,
    model: { async invoke() { modelCalls += 1; return finalTurn("no"); } },
  });

  const handle = await agent.submit({ messages: [{ role: "user", content: "run" }] });
  await rejectsCode(handle.result, "repository_failed");
  assert.equal(modelCalls, 0);
  assert.equal((await handle.snapshot()).status, "failed");
});

test("committed invocation receipt is published before Provider execution", async () => {
  const repository = new InMemoryRunRepository();
  let runId;
  let providerCalls = 0;
  const publisher = {
    waiter: new InMemoryOutputPublisher(),
    async publishCommitted(event) {
      runId = event.runId;
      const persisted = await repository.listEvents(event.runId, event.sequence - 1, 1);
      assert.equal(persisted[0].eventId, event.eventId);
      await this.waiter.publishCommitted(event);
    },
    waitForSequence(id, sequence, signal) {
      return this.waiter.waitForSequence(id, sequence, signal);
    },
  };
  const agent = new Agent({
    runRepository: repository,
    outputPublisher: publisher,
    model: {
      async invoke() {
        providerCalls += 1;
        const events = await repository.listEvents(runId, 0);
        assert.equal(events.at(-1).kind, "invocation.started");
        return finalTurn("done");
      },
    },
  });

  const handle = await agent.submit({ messages: [{ role: "user", content: "run" }] });
  assert.equal((await handle.result).output, "done");
  assert.equal(providerCalls, 1);
});

test("tool-start persistence failure prevents the handler", async () => {
  let executions = 0;
  const base = new InMemoryRunRepository();
  const repository = proxyRepository(base, {
    appendEvent(runId, draft) {
      if (draft.kind === "tool.started") throw new AgentError("repository_failed", "tool event failed");
      return base.appendEvent(runId, draft);
    },
  });
  const agent = new Agent({
    runRepository: repository,
    model: calls([{ id: "call-1", name: "read", arguments: {} }]),
    tools: [readTool("read", () => { executions += 1; return readResult(null); })],
  });

  const handle = await agent.submit({ messages: [{ role: "user", content: "run" }] });
  await rejectsCode(handle.result, "repository_failed");
  assert.equal(executions, 0);
  assert.equal((await handle.snapshot()).status, "failed");
});

test("cancellation atomically aborts an open invocation and wins the terminal race", async () => {
  let started;
  const providerStarted = new Promise((resolve) => { started = resolve; });
  let returned = 0;
  const iterator = {
    next() { started(); return new Promise(() => {}); },
    return() { returned += 1; return Promise.resolve({ done: true }); },
  };
  const agent = new Agent({
    model: {
      async invoke() { throw new Error("stream should be used"); },
      stream() { return { [Symbol.asyncIterator]: () => iterator }; },
    },
  });
  const handle = await agent.submit({ messages: [{ role: "user", content: "wait" }] });
  await providerStarted;
  const receipt = await handle.cancel();

  assert.equal(receipt.accepted, true);
  await assert.rejects(handle.result, AgentCanceledError);
  assert.equal((await handle.snapshot()).status, "canceled");
  const events = await collect(handle.events({ visibility: "all" }));
  assert.deepEqual(events.slice(-2).map((event) => event.kind), [
    "invocation.aborted",
    "run.canceled",
  ]);
  assert.equal(events.filter((event) => event.kind === "run.canceled").length, 1);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(returned, 1);
  assert.equal((await handle.cancel()).accepted, false);
});

test("attempt budget prevents a second Provider call", async () => {
  let modelCalls = 0;
  let toolCalls = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        modelCalls += 1;
        return callsTurn([{ id: "call-1", name: "read", arguments: {} }]);
      },
    },
    tools: [readTool("read", () => { toolCalls += 1; return readResult(null); })],
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "loop" }] },
    { budgets: { maxModelAttempts: 1 } },
  );

  await rejectsCode(handle.result, "run_attempt_budget_exceeded");
  assert.equal(modelCalls, 1);
  assert.equal(toolCalls, 1);
  assert.equal((await handle.snapshot()).status, "failed");
});

test("reported token budget and final-output budget fail before completion commit", async () => {
  const tokenAgent = new Agent({
    model: {
      async invoke() {
        return {
          message: { role: "assistant", content: "answer" },
          finishReason: "stop",
          usage: { inputTokens: 8, outputTokens: 2, totalTokens: 10 },
        };
      },
    },
  });
  const tokenRun = await tokenAgent.submit(
    { messages: [{ role: "user", content: "run" }] },
    { budgets: { maxTotalTokens: 5 } },
  );
  await rejectsCode(tokenRun.result, "run_token_budget_exceeded");
  assert.equal((await tokenRun.snapshot()).usage.knownTokens, 10);
  assert.equal((await tokenRun.snapshot()).status, "failed");

  const outputAgent = new Agent({ model: { async invoke() { return finalTurn("answer"); } } });
  const outputRun = await outputAgent.submit(
    { messages: [{ role: "user", content: "run" }] },
    { budgets: { maxOutputEvents: 1 } },
  );
  await rejectsCode(outputRun.result, "run_output_budget_exceeded");
  const outputEvents = await collect(outputRun.events({ visibility: "all" }));
  assert.equal(outputEvents.some((event) => event.kind === "final"), false);
  assert.equal(outputEvents.some((event) => event.kind === "run.completed"), false);
  assert.equal(outputEvents.at(-1).kind, "run.failed");
});

test("absolute deadline aborts Provider work and commits failed exactly once", async () => {
  let returned = 0;
  const iterator = {
    next() { return new Promise(() => {}); },
    return() { returned += 1; return Promise.resolve({ done: true }); },
  };
  const agent = new Agent({
    model: {
      async invoke() { throw new Error("stream should be used"); },
      stream() { return { [Symbol.asyncIterator]: () => iterator }; },
    },
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "wait" }] },
    { deadlineAt: new Date(Date.now() + 10).toISOString() },
  );

  await rejectsCode(handle.result, "run_deadline_exceeded");
  assert.equal((await handle.snapshot()).status, "failed");
  const events = await collect(handle.events({ visibility: "all" }));
  assert.equal(events.filter((event) => event.kind === "run.failed").length, 1);
  assert.equal(events.at(-1).payload.errorCode, "run_deadline_exceeded");
  assert.equal(returned, 1);
});

test("output policy cannot promote private reasoning", async () => {
  const agent = new Agent({
    outputPolicy: {
      authorize(event) {
        return event.channel === "reasoning" ? { ...event, visibility: "public" } : event;
      },
    },
    model: {
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        yield { reasoningDelta: "secret", contentDelta: "answer", finishReason: "stop" };
      },
    },
  });
  const handle = await agent.submit({ messages: [{ role: "user", content: "run" }] });

  await rejectsCode(handle.result, "output_policy_violation");
  const events = await collect(handle.events({ visibility: "all" }));
  assert.equal(events.some((event) => event.visibility === "public" && event.channel === "reasoning"), false);
  assert.equal(events.at(-1).kind, "run.failed");
});

test("output policy authorizes final output before the atomic terminal commit", async () => {
  let sawFinal = false;
  const agent = new Agent({
    outputPolicy: {
      authorize(event) {
        if (event.kind === "final") sawFinal = true;
        return event;
      },
    },
    model: { async invoke() { return finalTurn("private answer"); } },
  });
  const handle = await agent.submit({ messages: [{ role: "user", content: "run" }] });

  assert.equal((await handle.result).output, "private answer");
  const events = await collect(handle.events());
  assert.equal(sawFinal, true);
  assert.equal(events.find((event) => event.kind === "final").payload.output, "private answer");
  assert.deepEqual(events.slice(-2).map((event) => event.kind), ["final", "run.completed"]);
});

function readTool(name, run) {
  return {
    name,
    description: `${name} tool`,
    inputSchema: {
      type: "object",
      properties: { key: { type: "string" } },
      additionalProperties: true,
    },
    policy: { mode: "read", title: name },
    run,
  };
}

function readResult(content) {
  return { content, effectState: "not_started" };
}

function calls(toolCalls) {
  return { async invoke() { return callsTurn(toolCalls); } };
}

function callsTurn(toolCalls) {
  return { message: { role: "assistant", content: "", toolCalls }, finishReason: "tool_calls" };
}

function finalTurn(content) {
  return { message: { role: "assistant", content }, finishReason: "stop" };
}

async function collect(iterable) {
  const rows = [];
  for await (const row of iterable) rows.push(row);
  return rows;
}

async function rejectsCode(promise, code) {
  await assert.rejects(
    promise,
    (error) => error instanceof AgentError && error.code === code,
  );
}

function proxyRepository(base, overrides = {}) {
  return {
    begin: overrides.begin ?? ((...args) => base.begin(...args)),
    openInvocation: overrides.openInvocation ?? ((...args) => base.openInvocation(...args)),
    appendEvent: overrides.appendEvent ?? ((...args) => base.appendEvent(...args)),
    settleInvocation: overrides.settleInvocation ?? ((...args) => base.settleInvocation(...args)),
    settleRun: overrides.settleRun ?? ((...args) => base.settleRun(...args)),
    cancel: overrides.cancel ?? ((...args) => base.cancel(...args)),
    get: overrides.get ?? ((...args) => base.get(...args)),
    listEvents: overrides.listEvents ?? ((...args) => base.listEvents(...args)),
  };
}

function failingRepository(method) {
  const base = new InMemoryRunRepository();
  return proxyRepository(base, {
    [method]() { throw new AgentError("repository_failed", `${method} failed`); },
  });
}
