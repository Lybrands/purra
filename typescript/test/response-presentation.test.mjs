import assert from "node:assert/strict";
import test from "node:test";
import { Agent, InMemoryRunRepository } from "purra";
import { testGateway } from "./support/model-gateway.mjs";

const request = { messages: [{ role: "user", content: "review" }], planningMode: "reactive" };
const options = { budgets: { maxRunGenerationTokens: null } };
const candidate = "HOST_ONLY_RESULT";
function host(presentation, extra = {}) {
  let calls = 0, writes = 0;
  const model = testGateway({ async invoke(input) {
    calls++;
    if (!input.messages.some(m => m.role === "tool") && input.tools.length) {
      return { message: { role: "assistant", content: "", toolCalls: [{ id: "read-1", name: "lookup", arguments: {} }] }, finishReason: "tool_calls" };
    }
    return { message: { role: "assistant", content: candidate, reasoning: "PRIVATE_REASONING" }, finishReason: "stop" };
  } });
  const agent = new Agent({ model,
    ...(presentation === undefined ? {} : { responsePresentation: presentation }),
    tools: [{ name: "lookup", description: "Read", inputSchema: { type: "object", properties: {} },
      policy: { mode: "read", title: "Read" }, run() { writes++; return { content: "evidence" }; } }],
    ...extra,
  });
  return { agent, counts: () => ({ calls, writes }) };
}

for (const presentation of [undefined, "verbatim", "model_live", "none"]) {
  test(`presentation ${presentation} preserves tool execution and chooses publication`, async () => {
    const repository = new InMemoryRunRepository();
    const { agent, counts } = host(presentation, { runRepository: repository });
    const handle = await agent.submit(request, options);
    assert.equal((await handle.result).output, candidate);
    assert.deepEqual(counts(), { calls: presentation === "model_live" ? 3 : 2, writes: 1 });
    const events = [];
    for await (const e of handle.events()) events.push(e);
    const all = [];
    for await (const e of handle.events({ visibility: "all" })) all.push(e);
    assert.equal(all.find(e => e.kind === "final").visibility, presentation === "none" ? "private" : "public");
    assert.equal(events.some(e => e.kind === "final"), presentation !== "none");
    assert.ok(events.some(e => e.kind === "run.completed"));
    if (presentation === "none") assert.ok(!JSON.stringify(events).includes(candidate));
    assert.ok(!JSON.stringify(events).includes("PRIVATE_REASONING"));
    assert.equal((await handle.snapshot()).finalOutput, candidate); // host storage, not public output
  });
}

test("no presentation suppresses transient text deltas but returns the host result", async () => {
  let streamCalls = 0;
  const { agent } = host("none", { tools: [], model: testGateway({
    async invoke() { assert.fail("stream expected"); },
    async stream() {
      streamCalls++;
      return { async *[Symbol.asyncIterator]() {
        yield { contentDelta: candidate, reasoningDelta: "PRIVATE_REASONING" };
        yield { finishReason: "stop" };
      } };
    },
  }) });
  const events = [];
  for await (const e of agent.stream(request)) events.push(e);
  assert.deepEqual(events.map(e => e.type), ["final"]);
  assert.equal(events[0].result.output, candidate);
  assert.ok(!JSON.stringify(events[0].result.messages).includes("PRIVATE_REASONING"));
  assert.equal(events[0].visibility, "private");
  assert.equal(streamCalls, 1);
});

test("no presentation still runs result validators and bounded repair", async () => {
  const { agent, counts } = host("none", { tools: [], responseValidation: {
    validators: [{ validate() { return { violationCode: "rejected", repairGuidance: "Fix result" }; } }],
    maxAttempts: 1,
  } });
  const handle = await agent.submit(request, options);
  await assert.rejects(handle.result, { code: "response_validation_failed" });
  assert.equal(counts().calls, 1);
  const events = [];
  for await (const e of handle.events()) events.push(e);
  assert.ok(!events.some(e => e.kind === "final"));
});

test("output policy cannot promote the host-only final result", async () => {
  const { agent } = host("none", { tools: [], outputPolicy: {
    authorize(event) { return event.kind === "final" ? { ...event, visibility: "public" } : event; },
  } });
  const handle = await agent.submit(request, options);
  await assert.rejects(handle.result, { code: "output_policy_violation" });
});

test("default and explicit verbatim presentation retain the same saved identity", async () => {
  const fingerprints = [];
  for (const presentation of [undefined, "verbatim", "model_live", "none"]) {
    const { agent } = host(presentation, { tools: [] });
    const handle = await agent.submit(request, options);
    await handle.result;
    fingerprints.push((await handle.snapshot()).preset.compositionFingerprint);
  }
  assert.equal(fingerprints[0], fingerprints[1]);
  assert.notEqual(fingerprints[0], fingerprints[2]);
  assert.notEqual(fingerprints[0], fingerprints[3]);
});

test("invalid presentation mode fails at composition time", () => {
  assert.throws(() => host("raw"), /responsePresentation/);
});


test("host-only result still settles actual usage against the Run budget", async () => {
  const { agent } = host("none", { tools: [], model: testGateway({ async invoke() {
    return { message: { role: "assistant", content: candidate }, finishReason: "stop",
      usage: { inputTokens: 1, generationTokens: 3, totalTokens: 4 } };
  } }) });
  const handle = await agent.submit(request, { budgets: { maxRunGenerationTokens: 1 } });
  await assert.rejects(handle.result, { code: "runtime_budget_exceeded" });
  assert.equal((await handle.snapshot()).usage.generationTokens, 3);
});

for (const presentation of [undefined, "verbatim"]) {
  test(`long validated Unicode is unchanged with presentation ${presentation}`, async () => {
    const answer = "合成大纲🙂\n".repeat(20000);
    let calls = 0, checked = 0;
    const { agent } = host(presentation, { tools: [],
      responseValidation: { validators: [{ validate({ content }) {
        assert.equal(content, answer);
        checked++;
        return {};
      } }], maxAttempts: 1 },
      model: testGateway({ async invoke() { calls++;
        return { message: { role: "assistant", content: answer, reasoning: "PRIVATE_REASONING" }, finishReason: "stop" };
      } }),
    });
    const handle = await agent.submit(request, options);
    assert.equal((await handle.result).output, answer);
    assert.equal(calls, 1);
    assert.equal(checked, 1);
    const events = [];
    for await (const e of handle.events()) events.push(e);
    const final = events.find(e => e.kind === "final");
    assert.equal(final.payload.output, answer);
    assert.ok(final.payload.invocationId);
    assert.match(final.payload.contentDigest, /^[a-f0-9]{64}$/);
    assert.ok(!JSON.stringify(events).includes("PRIVATE_REASONING"));
  });
}

test("verbatim publication cannot rewrite an accepted candidate through output policy", async () => {
  const { agent } = host("verbatim", { tools: [], outputPolicy: {
    authorize(event) { return event.kind === "final"
      ? { ...event, payload: { ...event.payload, output: "rewritten" } } : event; },
  } });
  const handle = await agent.submit(request, options);
  await assert.rejects(handle.result, { code: "final_output_mismatch" });
  const events = [];
  for await (const e of handle.events()) events.push(e);
  assert.ok(!events.some(e => e.kind === "final"));
});
