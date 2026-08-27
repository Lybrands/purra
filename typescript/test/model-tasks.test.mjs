import assert from "node:assert/strict";
import test from "node:test";

import {
  Agent,
  AgentError,
  AgentOperationController,
  ModelTaskRunner,
} from "purra";

const RUN_OPTIONS = Object.freeze({
  budgets: Object.freeze({ maxRunOutputTokens: null }),
});

test("standalone model tasks resolve an exact output limit and Operation", async () => {
  const requests = [];
  const operations = [];
  const runner = new ModelTaskRunner({
    runId: "standalone-run",
    model: {
      capabilities: capabilities(),
      async invoke(request) {
        requests.push(request);
        return finalTurn("done", request);
      },
    },
    operations: new AgentOperationController({
      acceptOperationEvent(event) { operations.push(event); },
    }),
  });

  const result = await runner.complete(
    [{ role: "user", content: "private task" }],
    { maxCallOutputTokens: 256 },
  );

  assert.equal(result.turn.message.content, "done");
  assert.deepEqual(result.outputLimit, {
    maxTokens: 256,
    source: "user_override",
    profileMaxTokens: 512,
  });
  assert.equal(requests[0].tools.length, 0);
  assert.equal(requests[0].outputLimit.maxTokens, 256);
  assert.deepEqual(operations.map((event) => [event.type, event.runId]), [
    ["operation.started", "standalone-run"],
    ["operation.finished", "standalone-run"],
  ]);
  assert.equal(operations[1].status, "succeeded");
});

test("managed task receipt failure prevents the Provider call", async () => {
  let calls = 0;
  const runner = new ModelTaskRunner({
    runId: "managed-run",
    model: {
      capabilities: capabilities("unavailable"),
      async invoke(request) { calls += 1; return finalTurn("unsafe", request); },
    },
    authority: {
      runId: "managed-run",
      async openInvocation() { throw new Error("receipt unavailable"); },
      async persistChunk() {},
      async persistCompletion() {},
      async settleInvocation() {},
      async publishRecoveryDecision() {},
    },
  });

  await assert.rejects(
    runner.complete([{ role: "user", content: "private task" }]),
    /receipt unavailable/,
  );
  assert.equal(calls, 0);
});

test("streamText replays private reasoning and retries empty official output within policy", async () => {
  const requests = [];
  const chunks = [];
  const runner = new ModelTaskRunner({
    runId: "stream-run",
    model: {
      capabilities: capabilities(),
      async invoke() { throw new Error("stream should be used"); },
      stream(request) {
        return acknowledgedStream(request, (async function* () {
          requests.push(request);
          if (requests.length === 1) {
            yield { reasoningDelta: "private reasoning", finishReason: "stop" };
            return;
          }
          yield { contentDelta: "final ", reasoningDelta: "continued" };
          yield { contentDelta: "answer", finishReason: "stop" };
        })());
      },
    },
  });

  const result = await runner.streamText(
    [{ role: "user", content: "private task" }],
    { onChunk(chunk) { chunks.push(chunk); } },
  );

  assert.equal(result.content, "final answer");
  assert.equal(result.reasoning, "continued");
  assert.equal(result.attempts, 2);
  assert.equal(requests.length, 2);
  assert.equal(requests[1].messages.at(-2).role, "assistant");
  assert.equal(requests[1].messages.at(-2).reasoning, "private reasoning");
  assert.equal(requests[1].messages.at(-1).role, "developer");
  assert.match(requests[1].messages.at(-1).content, /Do not return reasoning alone/);
  assert.equal(chunks.length, 3);
});

test("streamText never retries an interrupted stream after content was observed", async () => {
  let attempts = 0;
  const runner = new ModelTaskRunner({
    runId: "interrupted-run",
    model: {
      capabilities: capabilities(),
      async invoke() { throw new Error("stream should be used"); },
      stream(request) {
        return acknowledgedStream(request, (async function* () {
          attempts += 1;
          yield { contentDelta: "visible" };
        })());
      },
    },
  });

  await assert.rejects(
    runner.streamText([{ role: "user", content: "private task" }]),
    (error) => error instanceof AgentError && error.code === "upstream_stream_interrupted",
  );
  assert.equal(attempts, 1);
});

test("canceling a model task records a canceled Operation", async () => {
  const controller = new AbortController();
  const events = [];
  const runner = new ModelTaskRunner({
    runId: "canceled-run",
    operations: new AgentOperationController({
      acceptOperationEvent(event) { events.push(event); },
    }),
    model: {
      capabilities: capabilities(),
      async invoke() { throw new Error("stream should be used"); },
      stream(request, signal) {
        return acknowledgedStream(request, (async function* () {
          yield { reasoningDelta: "working" };
          await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
        })());
      },
    },
  });

  await assert.rejects(
    runner.streamText([{ role: "user", content: "private task" }], {
      signal: controller.signal,
      onChunk() { controller.abort(); },
    }),
    (error) => error instanceof AgentError && error.code === "agent_canceled",
  );
  assert.equal(events.at(-1).status, "canceled");
});

test("submitted context factory receives a runner bound to the same durable Run", async () => {
  let factoryRunId;
  let calls = 0;
  const agent = new Agent({
    model: {
      capabilities: capabilities("unavailable"),
      async invoke(request) {
        calls += 1;
        return calls === 1
          ? finalTurn("managed fact", request)
          : finalTurn("done", request);
      },
    },
    context: {
      providerFactory(modelTasks) {
        factoryRunId = modelTasks.runId;
        return {
          describeContextDemands() {
            return [{ name: "managed", desiredTokens: 64 }];
          },
          async buildContext() {
            const completion = await modelTasks.complete([
              { role: "user", content: "build context" },
            ]);
            return {
              blocks: [{ name: "managed", content: completion.turn.message.content }],
            };
          },
        };
      },
    },
  });

  const handle = await agent.submit(
    { messages: [{ role: "user", content: "run" }] },
    RUN_OPTIONS,
  );
  assert.equal((await handle.result).output, "done");
  assert.equal(factoryRunId, handle.runId);
  assert.equal(calls, 2);
  const events = await collect(handle.events({ visibility: "all" }));
  const invocations = events.filter((event) => event.kind === "invocation.started");
  assert.equal(invocations.length, 2);
  assert.equal(invocations.every((event) => event.payload.receipt.runId === handle.runId), true);
  assert.equal(events.filter((event) => event.kind === "model.completed").length, 2);
});

test("direct context ports and factories are mutually exclusive", () => {
  assert.throws(() => new Agent({
    model: {
      capabilities: capabilities("unavailable"),
      async invoke(request) { return finalTurn("done", request); },
    },
    context: {
      provider: { buildContext() { return { blocks: [] }; } },
      providerFactory() { return { buildContext() { return { blocks: [] }; } }; },
    },
  }), /mutually exclusive/);
});

function capabilities(streaming = "supported") {
  return {
    schemaVersion: 1,
    profileId: "model-task-fixture",
    providerProtocol: "custom",
    contextWindowTokens: 16_000,
    maxCallOutputTokens: 512,
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

function finalTurn(content, request) {
  return {
    message: { role: "assistant", content },
    finishReason: "stop",
    appliedOutputLimit: request.outputLimit?.maxTokens,
  };
}

function acknowledgedStream(request, stream) {
  return Object.assign(stream, {
    appliedOutputLimit: request.outputLimit?.maxTokens,
  });
}

async function collect(iterable) {
  const values = [];
  for await (const value of iterable) values.push(value);
  return values;
}
