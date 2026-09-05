import assert from "node:assert/strict";
import test from "node:test";

import {
  Agent,
  AgentError,
  AgentOperationController,
  ModelTaskRunner,
} from "purra";

const RUN_OPTIONS = Object.freeze({
  budgets: Object.freeze({ maxRunGenerationTokens: null }),
});

test("standalone model tasks resolve an exact output limit and Operation", async () => {
  const requests = [];
  const operations = [];
  const runner = new ModelTaskRunner({
    runId: "standalone-run",
    maxGenerationTokens: 256,
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

  const result = await runner.complete([{ role: "user", content: "private task" }]);

  assert.equal(result.turn.message.content, "done");
  assert.deepEqual(result.outputBudget, {
    maxGenerationTokens: 256,
    generationSource: "user",
    profileMaxGenerationTokens: 512,
    requestedUserMaxGenerationTokens: 256,
    resultCapacityTargetTokens: null,
    resultCapacitySource: null,
    nonResultHeadroomTokens: null,
  });
  assert.equal(requests[0].tools.length, 0);
  assert.equal(requests[0].outputBudget.maxGenerationTokens, 256);
  assert.deepEqual(operations.map((event) => [event.type, event.runId]), [
    ["operation.started", "standalone-run"],
    ["operation.finished", "standalone-run"],
  ]);
  assert.equal(operations[1].status, "succeeded");
});

test("per-call model tasks cannot claim user generation authority", async () => {
  let calls = 0;
  const runner = new ModelTaskRunner({
    runId: "call-generation-authority",
    model: {
      capabilities: capabilities("unavailable"),
      async invoke(request) {
        calls += 1;
        return finalTurn("unsafe", request);
      },
    },
  });

  await assert.rejects(
    runner.complete(
      [{ role: "user", content: "private task" }],
      { maxGenerationTokens: 64 },
    ),
    { code: "model_output_budget_invalid" },
  );
  assert.equal(calls, 0);
});

test("private model tasks reject input plus output reserve beyond the model window", async () => {
  let calls = 0;
  const runner = new ModelTaskRunner({
    runId: "input-budget",
    model: {
      capabilities: capabilities(),
      async invoke(request) { calls++; return finalTurn("unsafe", request); },
      async *stream() { calls++; yield { contentDelta: "unsafe", finishReason: "stop" }; },
    },
  });
  const messages = [{ role: "user", content: "x".repeat(40_000) }];
  await assert.rejects(runner.complete(messages), { code: "model_task_input_exceeds_budget" });
  await assert.rejects(runner.streamText(messages), { code: "model_task_input_exceeds_budget" });
  assert.equal(calls, 0);
  const small = [{ role: "user", content: "x".repeat(1_000) }];
  const constrainedRequests = [];
  const bounded = new ModelTaskRunner({
    runId: "reserve-boundary",
    model: {
      capabilities: { ...capabilities(), contextWindowTokens: 8_000, maxGenerationTokens: 1_000 },
      async invoke(request) {
        calls++;
        constrainedRequests.push(request);
        return finalTurn("safe", request);
      },
    },
  });
  assert.equal((await bounded.complete(small)).turn.message.content, "safe");
  assert.equal(calls, 1);
  assert.equal(constrainedRequests[0].outputBudget.maxGenerationTokens, 832);
  assert.equal(constrainedRequests[0].outputBudget.generationSource, "context_capacity");
});

test("workflow result capacity never shrinks the Provider generation allowance", async () => {
  let captured;
  const runner = new ModelTaskRunner({
    runId: "result-capacity",
    model: {
      capabilities: capabilities("unavailable"),
      async invoke(request) {
        captured = request.outputBudget;
        return finalTurn("done", request);
      },
    },
  });
  await runner.complete([{ role: "user", content: "size this result" }], {
    resultCapacityTargetTokens: 128,
    resultCapacitySource: "workflow_policy",
  });
  assert.deepEqual(captured, {
    maxGenerationTokens: 512,
    generationSource: "model_profile",
    profileMaxGenerationTokens: 512,
    requestedUserMaxGenerationTokens: null,
    resultCapacityTargetTokens: 128,
    resultCapacitySource: "workflow_policy",
    nonResultHeadroomTokens: 384,
  });
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

test("non-stream model tasks fail closed on LENGTH without continuation", async () => {
  const runner = new ModelTaskRunner({
    runId: "length-fail-closed",
    model: {
      capabilities: capabilities("unavailable"),
      async invoke(request) {
        return {
          message: { role: "assistant", content: "partial" },
          finishReason: "length",
          appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
        };
      },
    },
  });
  await assert.rejects(
    runner.complete([{ role: "user", content: "write" }]),
    (error) => error instanceof AgentError && error.code === "model_output_truncated",
  );
});

test("model tasks revalidate bound evidence before invoking the Provider", async () => {
  let calls = 0;
  const evidence = [{
    evidenceId: "mem0:store:item-1:2",
    contextBlock: "memory",
    source: "mem0/scope",
    itemId: "item-1",
    version: "2",
  }];
  const validations = [];
  const runner = new ModelTaskRunner({
    runId: "evidence-task",
    model: {
      capabilities: capabilities("unavailable"),
      async invoke(request) { calls += 1; return finalTurn("unsafe", request); },
    },
    evidenceValidator: {
      validateEvidence(receipts) {
        validations.push(receipts);
        throw new AgentError("external_evidence_stale", "stale evidence");
      },
    },
  });
  runner.bindEvidence(evidence);

  await assert.rejects(
    runner.complete([{ role: "user", content: "compress" }]),
    { code: "external_evidence_stale" },
  );
  assert.equal(calls, 0);
  assert.deepEqual(validations, [evidence]);
});

test("context compression model tasks inherit the Run evidence set", async () => {
  let calls = 0;
  const validations = [];
  const evidence = {
    evidenceId: "mem0:store:item-1:2",
    source: "mem0/scope",
    itemId: "item-1",
    version: "2",
  };
  const agent = new Agent({
    model: {
      capabilities: capabilities("unavailable"),
      async invoke(request) { calls += 1; return finalTurn("unsafe", request); },
    },
    evidenceValidator: {
      validateEvidence(receipts) {
        validations.push(receipts);
        throw new AgentError("external_evidence_stale", "stale evidence");
      },
    },
    context: {
      provider: {
        describeContextDemands() { return [{ name: "memory", desiredTokens: 64 }]; },
        buildContext() {
          return { blocks: [{ name: "memory", content: "remembered", evidence: [evidence] }] };
        },
      },
      triggerRatio: 0.001,
      compressionFactory(modelTasks) {
        return {
          async compress(request) {
            await modelTasks.complete([{ role: "user", content: "compress" }]);
            return { messages: request.messages };
          },
        };
      },
    },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "answer" }] }),
    { code: "external_evidence_stale" },
  );
  assert.equal(calls, 0);
  assert.deepEqual(validations, [[{ ...evidence, contextBlock: "memory" }]]);
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
    schemaVersion: 2,
    profileId: "model-task-fixture",
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
    appliedGenerationLimit: request.outputBudget?.maxGenerationTokens,
  };
}

function acknowledgedStream(request, stream) {
  return Object.assign(stream, {
    appliedGenerationLimit: request.outputBudget?.maxGenerationTokens,
  });
}

async function collect(iterable) {
  const values = [];
  for await (const value of iterable) values.push(value);
  return values;
}
