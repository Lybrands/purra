import assert from "node:assert/strict";
import test from "node:test";

import {
  Agent,
  AgentCanceledError,
  AgentError,
  assertRunRepositoryConforms,
  InMemoryOutputPublisher,
  InMemoryRunRepository,
} from "purra";
import { testGateway } from "./support/model-gateway.mjs";

const RUN_OPTIONS = Object.freeze({
  budgets: Object.freeze({ maxRunGenerationTokens: null }),
});

test("in-memory Run repository passes the public conformance probe", async () => {
  await assertRunRepositoryConforms(new InMemoryRunRepository());
});

test("new Runs require an explicit Run output budget", () => {
  const agent = new Agent({
    model: testGateway({ async invoke() { return finalTurn("unused"); } }),
  });
  const request = { messages: [{ role: "user", content: "run" }] };

  assert.throws(() => agent.submit(request), /explicit budgets/);
  assert.throws(
    () => agent.submit(request, { budgets: {} }),
    /maxRunGenerationTokens must be a number or explicit null/,
  );
});

test("Run output batches are atomic, replayable, and budgeted before append", async () => {
  const repository = new InMemoryRunRepository();
  const begun = await repository.begin({
    preset: {
      schemaVersion: 5,
      presetId: "batch",
      presetRevision: "1",
      promptFingerprint: "prompt",
      toolFingerprint: "tools",
      capabilityProfileId: null,
      compositionFingerprint: "composition",
      runtimeLimits: {
        runTimeoutMs: 900_000,
        activityIdleTimeoutMs: 30_000,
        progressIdleTimeoutMs: 60_000,
        invocationTimeoutMs: 300_000,
        maxChunks: 100_000,
        maxContentChars: 1_000_000,
        maxReasoningChars: 1_000_000,
        maxToolArgumentChars: 1_000_000,
      },
      agentTree: { protocolVersion: 1, enabled: false },
    },
    deadlineAt: null,
    budgets: {
      maxModelAttempts: 1,
      maxInputTokens: null,
      maxRunGenerationTokens: null,
      maxReasoningTokens: null,
      maxOutputBytes: 10_000,
      maxOutputEvents: 1,
    },
    metadata: {},
  });
  const drafts = ["a", "b"].map((value) => ({
    sourceKey: `batch:${value}`,
    kind: "provider.delta_batch",
    channel: "model",
    visibility: "private",
    payload: { value },
  }));
  assert.equal(begun.snapshot.budgets.maxOutputBytes, 10_000);
  await rejectsCode(
    repository.appendBatch(begun.snapshot.runId, drafts),
    "runtime_budget_exceeded",
  );
  assert.equal((await repository.listEvents(begun.snapshot.runId, 0)).length, 1);
  const committed = await repository.appendBatch(begun.snapshot.runId, drafts.slice(0, 1));
  assert.equal(committed[0].sequence, 2);
  assert.deepEqual(
    await repository.appendBatch(begun.snapshot.runId, drafts.slice(0, 1)),
    committed,
  );
  await rejectsCode(
    repository.appendEvent(begun.snapshot.runId, { ...drafts[0], payload: { value: "drift" } }),
    "output_source_key_conflict",
  );
});

test("Child Runs share Root attempts, tokens, output budgets, and journal order", async () => {
  const repository = new InMemoryRunRepository();
  const preset = {
    schemaVersion: 5,
    presetId: "tree-budget",
    presetRevision: "1",
    promptFingerprint: "prompt",
    toolFingerprint: "tools",
    capabilityProfileId: null,
    compositionFingerprint: "composition",
    runtimeLimits: {
      runTimeoutMs: 900_000,
      activityIdleTimeoutMs: 30_000,
      progressIdleTimeoutMs: 60_000,
      invocationTimeoutMs: 300_000,
      maxChunks: 100_000,
      maxContentChars: 1_000_000,
      maxReasoningChars: 1_000_000,
      maxToolArgumentChars: 1_000_000,
    },
    agentTree: { protocolVersion: 1, enabled: false },
  };
  const budgets = {
    maxModelAttempts: 2,
    maxInputTokens: 3,
    maxRunGenerationTokens: null,
    maxReasoningTokens: null,
    maxOutputBytes: 10_000,
    maxOutputEvents: 1,
  };
  const root = await repository.begin({
    requestedRunId: "root-run",
    agentId: "root-agent",
    preset,
    deadlineAt: null,
    budgets,
    metadata: {},
  });
  const children = await Promise.all([1, 2, 3].map((index) => repository.begin({
    requestedRunId: `child-run-${index}`,
    rootRunId: root.snapshot.runId,
    agentId: `child-agent-${index}`,
    parentRunId: root.snapshot.runId,
    preset,
    deadlineAt: null,
    budgets,
    metadata: {},
  })));

  const attempts = await Promise.allSettled(children.map(({ snapshot }, index) => (
    repository.openInvocation(snapshot.runId, invocationInput(snapshot.runId, index + 1))
  )));
  assert.equal(attempts.filter(({ status }) => status === "fulfilled").length, 2);
  assert.equal(attempts.filter(({ status }) => status === "rejected").length, 1);
  assert.equal(attempts.find(({ status }) => status === "rejected").reason.code, "runtime_budget_exceeded");

  const settlements = await Promise.all(attempts.flatMap((attempt, index) => (
    attempt.status === "fulfilled"
      ? [repository.settleInvocation(children[index].snapshot.runId, {
          invocationId: `invocation-${index + 1}`,
          status: "completed",
          usage: { inputTokens: 2, generationTokens: 0, totalTokens: 2 },
        })]
      : []
  )));
  assert.deepEqual(
    settlements.map(({ budgetError }) => budgetError).sort(),
    ["runtime_budget_exceeded", undefined].sort(),
  );

  const outputs = await Promise.allSettled(children.map(({ snapshot }, index) => (
    repository.appendEvent(snapshot.runId, {
      sourceKey: `provider:child:${index + 1}`,
      kind: "provider.delta_batch",
      channel: "model",
      visibility: "private",
      payload: { value: index + 1 },
    })
  )));
  assert.equal(outputs.filter(({ status }) => status === "fulfilled").length, 1);
  assert.equal(outputs.filter(({ status }) => status === "rejected").length, 2);
  assert.equal((await repository.get(root.snapshot.runId)).usage.modelAttempts, 2);
  assert.equal((await repository.get(root.snapshot.runId)).usage.inputTokens, 4);

  const journal = await repository.listRootEvents(root.snapshot.runId, 0);
  assert.deepEqual(
    journal.map(({ rootSequence }) => rootSequence),
    journal.map((_event, index) => index + 1),
  );
  assert.deepEqual(new Set(journal.map(({ rootRunId }) => rootRunId)), new Set([root.snapshot.runId]));
  assert.equal(journal.every(({ agentId, sourceKey }) => agentId && sourceKey), true);
  const firstChildEvents = await repository.listEvents(children[0].snapshot.runId, 0);
  assert.equal(firstChildEvents.every(({ runId }) => runId === children[0].snapshot.runId), true);
  assert.equal(firstChildEvents.every((event) => journal.includes(event)), true);
});

test("ten thousand one-character chunks coalesce deterministically", async () => {
  const candidate = 8 * 1024 * 1024;
  const agent = new Agent({
    outputBatchLimits: {
      maxPayloadBytes: 1_000_000,
      maxFragments: 64,
      maxLatencyMs: 60_000,
      maxBackgroundLatencyMs: 60_000,
    },
    model: testGateway({
      async invoke() { throw new Error("stream expected"); },
      async *stream() {
        for (let index = 0; index < 10_000; index += 1) {
          yield {
            contentDelta: "x",
            ...(index === 9_999 ? { finishReason: "stop" } : {}),
          };
        }
      },
    }),
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "many" }] },
    { budgets: { maxRunGenerationTokens: null, maxOutputBytes: candidate } },
  );
  assert.equal((await handle.result).output.length, 10_000);
  assert.ok((await handle.snapshot()).usage.outputBytes * 2 <= candidate);
  const events = await collect(handle.events({ visibility: "all" }));
  const batches = events.filter((event) => event.kind === "provider.delta_batch");
  assert.equal(batches.length, Math.ceil(10_000 / 64));
  assert.equal(
    batches.flatMap((event) => event.payload.entries)
      .map((entry) => entry.payload.delta)
      .join("").length,
    10_000,
  );
});

test("private Provider batches use the background latency ceiling", async () => {
  const agent = new Agent({
    outputBatchLimits: {
      maxPayloadBytes: 1_000_000,
      maxFragments: 64,
      maxLatencyMs: 1_000,
      maxBackgroundLatencyMs: 1,
    },
    model: testGateway({
      async invoke() { throw new Error("stream expected"); },
      async *stream() {
        yield { contentDelta: "a" };
        await new Promise((resolve) => setTimeout(resolve, 20));
        yield { contentDelta: "b", finishReason: "stop" };
      },
    }),
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "background" }] },
    { budgets: { maxRunGenerationTokens: null, maxOutputBytes: null } },
  );

  assert.equal((await handle.result).output, "ab");
  const events = await collect(handle.events({ visibility: "all" }));
  const batches = events.filter((event) => event.kind === "provider.delta_batch");
  assert.equal(batches.length, 2);
});

test("Provider output budget failure stays coded, terminal, and non-retryable", async () => {
  let modelCalls = 0;
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream expected"); },
      async *stream() {
        modelCalls += 1;
        yield {
          reasoningDelta: "private reasoning",
          contentDelta: "answer",
          finishReason: "stop",
        };
      },
    }),
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "budget" }] },
    { budgets: { maxRunGenerationTokens: null, maxOutputBytes: 1 } },
  );

  await rejectsCode(handle.result, "runtime_budget_exceeded");
  const events = await collect(handle.events({ visibility: "all" }));
  assert.equal(modelCalls, 1);
  assert.equal(events.filter((event) => event.kind === "provider.delta_batch").length, 0);
  assert.equal(events.filter((event) => event.kind === "invocation.aborted").length, 1);
  assert.equal(events.filter((event) => event.kind === "run.failed").length, 1);
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
    model: testGateway({
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
            usage: { inputTokens: 10, generationTokens: 2, totalTokens: 12 },
          };
          return;
        }
        yield {
          contentDelta: "Found it",
          reasoningDelta: "another-secret",
          finishReason: "stop",
          usage: { inputTokens: 12, generationTokens: 2, totalTokens: 14 },
        };
      },
    }),
    tools: [readTool("lookup", (input) => readResult({ value: input.key }))],
  });

  const handle = await agent.submit({
    messages: [{ role: "user", content: "find" }],
    contextEvidence: [{ evidenceId: "ev-1", source: "fixture", itemId: "item-1" }],
  }, RUN_OPTIONS);
  const result = await handle.result;
  const all = await collect(handle.events({ visibility: "all" }));
  const publicEvents = await collect(handle.events());

  assert.equal(result.output, "Found it");
  assert.equal(JSON.stringify(result.messages).includes("private-prompt"), false);
  assert.equal(JSON.stringify(result.messages).includes("private-reasoning"), false);
  assert.deepEqual(all.map((event) => event.sequence), all.map((_event, index) => index + 1));
  assert.equal(all[0].kind, "run.started");
  assert.deepEqual(all.slice(-2).map((event) => event.kind), ["final", "run.completed"]);
  assert.equal(all.filter((event) => event.kind === "invocation.started").length, 3);
  assert.equal(all.filter((event) => event.kind === "invocation.completed").length, 3);
  assert.equal(all.some((event) => (
    event.kind === "provider.delta_batch"
    && event.payload.entries.some((entry) => entry.kind === "provider.reasoning_delta")
  )), true);
  assert.equal(all.some((event) => event.kind === "commentary"), false);
  assert.equal(all.some((event) => event.kind === "tool.started"), true);
  assert.equal(all.some((event) => event.kind === "tool.completed"), true);
  assert.equal(publicEvents.some((event) => event.channel === "reasoning"), false);
  assert.equal(publicEvents.some((event) => event.channel === "model"), false);
  assert.deepEqual(publicEvents.map((event) => event.kind), [
    "run.started",
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
  assert.equal((await handle.snapshot()).usage.inputTokens, 34);
  assert.equal((await handle.snapshot()).usage.generationTokens, 6);
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
    model: testGateway({ async invoke() { return finalTurn("uncommitted"); } }),
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "run" }] },
    RUN_OPTIONS,
  );

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
    model: testGateway({ async invoke() { modelCalls += 1; return finalTurn("no"); } }),
    tools: [readTool("read", () => { toolCalls += 1; return readResult(null); })],
  });

  await assert.rejects(agent.submit(
    { messages: [{ role: "user", content: "run" }] },
    RUN_OPTIONS,
  ));
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
    model: testGateway({ async invoke() { modelCalls += 1; return finalTurn("no"); } }),
  });

  const handle = await agent.submit(
    { messages: [{ role: "user", content: "run" }] },
    RUN_OPTIONS,
  );
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
    model: testGateway({
      async invoke() {
        providerCalls += 1;
        const events = await repository.listEvents(runId, 0);
        assert.equal(events.at(-1).kind, "invocation.started");
        return finalTurn("done");
      },
    }),
  });

  const handle = await agent.submit(
    { messages: [{ role: "user", content: "run" }] },
    RUN_OPTIONS,
  );
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
    model: testGateway(calls([{ id: "call-1", name: "read", arguments: {} }])),
    tools: [readTool("read", () => { executions += 1; return readResult(null); })],
  });

  const handle = await agent.submit(
    { messages: [{ role: "user", content: "run" }] },
    RUN_OPTIONS,
  );
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
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream() { return { [Symbol.asyncIterator]: () => iterator }; },
    }),
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "wait" }] },
    RUN_OPTIONS,
  );
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

test("cancellation flushes a pending private batch and clears its timer", async () => {
  let nextCalls = 0;
  let returned = 0;
  let secondStarted;
  const waiting = new Promise((resolve) => { secondStarted = resolve; });
  const iterator = {
    next() {
      nextCalls += 1;
      if (nextCalls === 1) {
        return Promise.resolve({ value: { reasoningDelta: "pending" }, done: false });
      }
      secondStarted();
      return new Promise(() => {});
    },
    return() {
      returned += 1;
      return Promise.resolve({ done: true });
    },
  };
  const agent = new Agent({
    outputBatchLimits: {
      maxPayloadBytes: 1_000_000,
      maxFragments: 64,
      maxLatencyMs: 1_000,
      maxBackgroundLatencyMs: 1_000,
    },
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream() { return { [Symbol.asyncIterator]: () => iterator }; },
    }),
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "wait" }] },
    RUN_OPTIONS,
  );
  await waiting;

  assert.equal((await handle.cancel()).accepted, true);
  await assert.rejects(handle.result, AgentCanceledError);
  const events = await collect(handle.events({ visibility: "all" }));
  assert.equal(events.filter((event) => event.kind === "provider.delta_batch").length, 1);
  assert.equal(events.filter((event) => event.kind === "invocation.aborted").length, 1);
  assert.equal(events.filter((event) => event.kind === "run.canceled").length, 1);
  assert.equal(returned, 1);
});

test("attempt budget prevents a second Provider call", async () => {
  let modelCalls = 0;
  let toolCalls = 0;
  const agent = new Agent({
    model: testGateway({
      async invoke() {
        modelCalls += 1;
        return callsTurn([{ id: "call-1", name: "read", arguments: {} }]);
      },
    }),
    tools: [readTool("read", () => { toolCalls += 1; return readResult(null); })],
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "loop" }] },
    { budgets: { maxRunGenerationTokens: null, maxModelAttempts: 1 } },
  );

  await rejectsCode(handle.result, "runtime_budget_exceeded");
  assert.equal(modelCalls, 1);
  assert.equal(toolCalls, 1);
  assert.equal((await handle.snapshot()).status, "failed");
});

test("reported token budget and final-output budget fail before completion commit", async () => {
  const tokenAgent = new Agent({
    model: testGateway({
      async invoke() {
        return {
          message: { role: "assistant", content: "answer" },
          finishReason: "stop",
          usage: { inputTokens: 8, generationTokens: 2, totalTokens: 10 },
        };
      },
    }),
  });
  const tokenRun = await tokenAgent.submit(
    { messages: [{ role: "user", content: "run" }] },
    { budgets: { maxRunGenerationTokens: null, maxInputTokens: 5 } },
  );
  await rejectsCode(tokenRun.result, "runtime_budget_exceeded");
  assert.equal((await tokenRun.snapshot()).usage.inputTokens, 8);
  assert.equal((await tokenRun.snapshot()).status, "failed");

  const reasoningRun = await tokenAgent.submit(
    { messages: [{ role: "user", content: "run" }] },
    { budgets: { maxRunGenerationTokens: null, maxReasoningTokens: 5 } },
  );
  await rejectsCode(reasoningRun.result, "runtime_budget_exceeded");
  const reasoningSnapshot = await reasoningRun.snapshot();
  assert.equal(reasoningSnapshot.usage.reasoningTokens, 0);
  assert.equal(reasoningSnapshot.usage.unreportedReasoningAttempts, 1);
  assert.equal(reasoningSnapshot.status, "failed");

  const outputAgent = new Agent({ model: testGateway({ async invoke() { return finalTurn("answer"); } }) });
  const outputRun = await outputAgent.submit(
    { messages: [{ role: "user", content: "run" }] },
    { budgets: { maxRunGenerationTokens: null, maxOutputBytes: 1 } },
  );
  await rejectsCode(outputRun.result, "runtime_budget_exceeded");
  const outputEvents = await collect(outputRun.events({ visibility: "all" }));
  assert.equal(outputEvents.some((event) => event.kind === "final"), false);
  assert.equal(outputEvents.some((event) => event.kind === "run.completed"), false);
  assert.equal(outputEvents.at(-1).kind, "run.failed");
});

test("non-stream terminal failures retain validated Provider usage", async () => {
  const agent = new Agent({
    model: testGateway({
      async invoke() {
        return {
          message: { role: "assistant", content: "partial" },
          finishReason: "length",
          usage: { inputTokens: 3, generationTokens: 4 },
        };
      },
    }),
  });
  const run = await agent.submit(
    { messages: [{ role: "user", content: "write" }] },
    { budgets: { maxRunGenerationTokens: 10 } },
  );
  await rejectsCode(run.result, "model_output_truncated");
  const snapshot = await run.snapshot();
  assert.equal(snapshot.usage.inputTokens, 3);
  assert.equal(snapshot.usage.generationTokens, 4);
  assert.equal(snapshot.usage.unreportedUsageAttempts, 0);
});

test("Provider budget contract failures retain reported usage", async () => {
  const agent = new Agent({
    model: {
      capabilities: generationCapabilities(10),
      async invoke(request) {
        return {
          message: { role: "assistant", content: "invalid" },
          finishReason: "stop",
          appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
          usage: { inputTokens: 2, generationTokens: 11 },
        };
      },
    },
  });
  const run = await agent.submit(
    { messages: [{ role: "user", content: "write" }] },
    { budgets: { maxRunGenerationTokens: null } },
  );
  await rejectsCode(run.result, "model_gateway_contract_violation");
  const snapshot = await run.snapshot();
  assert.equal(snapshot.usage.inputTokens, 2);
  assert.equal(snapshot.usage.generationTokens, 11);
  assert.equal(snapshot.usage.unreportedUsageAttempts, 0);
});

test("absolute deadline aborts Provider work and commits failed exactly once", async () => {
  let returned = 0;
  const iterator = {
    next() { return new Promise(() => {}); },
    return() { returned += 1; return Promise.resolve({ done: true }); },
  };
  const agent = new Agent({
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      stream() { return { [Symbol.asyncIterator]: () => iterator }; },
    }),
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "wait" }] },
    {
      deadlineAt: new Date(Date.now() + 10).toISOString(),
      budgets: { maxRunGenerationTokens: null },
    },
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
    model: testGateway({
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        yield { reasoningDelta: "secret", contentDelta: "answer", finishReason: "stop" };
      },
    }),
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "run" }] },
    RUN_OPTIONS,
  );

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
    model: testGateway({ async invoke() { return finalTurn("private answer"); } }),
  });
  const handle = await agent.submit(
    { messages: [{ role: "user", content: "run" }] },
    RUN_OPTIONS,
  );

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

function generationCapabilities(maxGenerationTokens) {
  return {
    schemaVersion: 2,
    profileId: "run-output-budget",
    providerProtocol: "custom",
    contextWindowTokens: 16_000,
    maxGenerationTokens,
    thinkingTokenAccounting: "included",
    protocol: {
      reasoningControl: "unavailable",
      reasoningReplay: "ignored",
      toolCalling: "supported",
      requiredToolChoice: "supported",
      parallelToolCalls: "supported",
      streaming: "unavailable",
      cancellation: "supported",
      assistantContentWithToolCalls: "optional",
      jsonSchemaLevel: "unknown",
      streamFinishSemantics: "normalized",
      usageSemantics: "normalized",
    },
  };
}

function invocationInput(runId, index) {
  return {
    schemaVersion: 2,
    runId,
    invocationId: `invocation-${index}`,
    messageFingerprint: `message-${index}`,
    toolFingerprint: "tools",
    requestFingerprint: `request-${index}`,
    evidenceFingerprint: "evidence",
    contextEvidence: [],
    capabilityProfileId: null,
    outputBudget: null,
  };
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
    appendBatch: overrides.appendBatch ?? ((...args) => base.appendBatch(...args)),
    settleInvocation: overrides.settleInvocation ?? ((...args) => base.settleInvocation(...args)),
    settleRun: overrides.settleRun ?? ((...args) => base.settleRun(...args)),
    cancel: overrides.cancel ?? ((...args) => base.cancel(...args)),
    get: overrides.get ?? ((...args) => base.get(...args)),
    listEvents: overrides.listEvents ?? ((...args) => base.listEvents(...args)),
    listRootEvents: overrides.listRootEvents ?? ((...args) => base.listRootEvents(...args)),
  };
}

function failingRepository(method) {
  const base = new InMemoryRunRepository();
  return proxyRepository(base, {
    [method]() { throw new AgentError("repository_failed", `${method} failed`); },
  });
}
