import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { ModelTaskRunner, StructuredOutputContract } from "purra";
const cases = JSON.parse(readFileSync(new URL("../../conformance/fixtures/structured_model_task.json", import.meta.url))).cases;
const schema = { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"], additionalProperties: false };
const output = await StructuredOutputContract.create({ schemaId: "test", schemaVersion: "1", schema });
function setup(row, failure) {
  const calls = [], receipts = [], settlements = [], decisions = [];
  const authority = {
    runId: "run",
    async openInvocation(input) {
      if (failure === "open") throw new Error("storage failed");
      const receipt = { ...input, schemaVersion: 3, invocationId: crypto.randomUUID() };
      receipts.push(receipt); return receipt;
    },
    async persistChunk() {},
    async persistCompletion() { if (failure === "completion") throw new Error("storage failed"); },
    async settleInvocation(...args) { if (failure === "settle") throw new Error("storage failed"); settlements.push(args); },
    async publishRecoveryDecision(decision) { if (failure === "recovery") throw new Error("storage failed"); decisions.push(decision); },
  };
  const model = { capabilities: capabilities(), async invoke(request) {
    calls.push(request);
    return { ...finalTurn(row.outputs[calls.length - 1], request), finishReason: row.finish ?? "stop",
      ...(row.usage === false ? {} : { usage: { inputTokens: 10, generationTokens: 5, totalTokens: 15 } }) };
  } };
  return { calls, receipts, settlements, decisions, authority, model,
    runner: new ModelTaskRunner({ runId: "run", model, authority }) };
}
for (const row of cases) test(`shared structured task: ${row.name}`, async () => {
  const fixture = setup(row);
  let refs;
  try {
    const result = await fixture.runner.completeStructured([], { output, repairAttempts: row.repairs ?? 0 });
    assert.equal(row.code, undefined);
    assert.deepEqual(result.value, { ok: true });
    assert.equal(result.receipt.attempts, row.attempts);
    assert.equal(result.receipt.rootBudget, "bound");
    assert.equal(result.receipt.usageState, row.usage === false ? "unknown" : "reported");
    if (row.usage !== false) assert.equal(result.receipt.usage.generationTokens, 5 * row.attempts);
    refs = result.receipt.invocationRefs;
  } catch (error) {
    assert.equal(error.code, row.code);
    refs = error.invocationRefs;
  }
  assert.equal(fixture.calls.length, row.attempts);
  assert.equal(refs.length, row.attempts);
  assert.equal(new Set(refs.map(ref => ref.invocationId)).size, row.attempts);
  assert(refs.every(ref => ref.settled));
  assert.equal(fixture.settlements.length, row.attempts);
  for (const request of fixture.calls) {
    assert.equal(request.outputContract, output);
    assert(request.messages.at(-1).content.includes('"additionalProperties":false'));
  }
  for (const receipt of fixture.receipts) assert.equal(receipt.outputContract.contractDigest, output.contractDigest);
});
for (const failure of ["open", "completion", "settle", "recovery"]) test(`structured persistence failure: ${failure}`, async () => {
  const fixture = setup({ outputs: failure === "completion" ? ['{"ok":true}'] : ["invalid"] }, failure);
  await assert.rejects(fixture.runner.completeStructured([], { output, repairAttempts: 3 }), error => {
    assert.equal(fixture.calls.length, failure === "open" ? 0 : 1);
    assert(Array.isArray(error.invocationRefs));
    if (failure === "settle") assert.equal(error.invocationRefs.at(-1).settled, false);
    return true;
  });
});
test("native unknown is zero call; low-level result exposes missing bindings", async () => {
  const fixture = setup({ outputs: ['{"ok":true}'] });
  const native = await StructuredOutputContract.create({ schemaId: "test", schemaVersion: "1", schema, mode: "native_required" });
  await assert.rejects(fixture.runner.completeStructured([], { output: native }), { code: "structured_output_mode_unsupported" });
  assert.equal(fixture.calls.length, 0);
  const low = new ModelTaskRunner({ runId: "run", model: fixture.model });
  const result = await low.completeStructured([], { output });
  assert.equal(result.receipt.persistence, "none");
  assert.equal(result.receipt.rootBudget, "not_bound");
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


test("real Run context binds repair lineage, usage and private invocation results", async () => {
  const { Agent } = await import("purra");
  let structured;
  let calls = 0;
  const agent = new Agent({ model: {
    capabilities: capabilities("unavailable"),
    async invoke(request) {
      calls++;
      return { ...finalTurn(calls === 1 ? "invalid" : calls === 2 ? '{"ok":true}' : "done", request),
        usage: { inputTokens: 10, generationTokens: 5, totalTokens: 15 } };
    },
  }, context: { providerFactory(tasks) { return {
    describeContextDemands() { return [{ name: "managed", desiredTokens: 64 }]; },
    async buildContext() {
      structured = await tasks.completeStructured([{ role: "user", content: "build context" }], { output, repairAttempts: 1 });
      return { blocks: [] };
    },
  }; } } });
  const handle = await agent.submit({ messages: [{ role: "user", content: "run" }] }, { budgets: { maxRunGenerationTokens: 100 } });
  assert.equal((await handle.result).output, "done");
  assert.equal(structured.receipt.runId, handle.runId);
  assert.equal(structured.receipt.persistence, "bound");
  assert.equal(structured.receipt.rootBudget, "bound");
  const snapshot = await handle.snapshot();
  assert.equal(snapshot.usage.modelAttempts, 3);
  assert.equal(snapshot.usage.generationTokens, 15);
  const events = [];
  for await (const event of handle.events({ visibility: "all" })) events.push(event);
  const receipts = events.filter(e => e.kind === "invocation.started" && e.payload.receipt.outputContract).map(e => e.payload.receipt);
  assert.equal(receipts.length, 2);
  assert.equal(receipts[0].structuredTask.taskId, receipts[1].structuredTask.taskId);
  assert.equal(receipts[1].structuredTask.previousInvocationId, receipts[0].invocationId);
  assert.deepEqual(receipts.map(r => r.structuredTask.attempt), [1, 2]);
  assert.equal(events.filter(e => e.kind === "model.completed").length, 2);
  assert(!events.some(e => e.visibility === "public" && JSON.stringify(e.payload).includes('"ok"')));
});

test("repair revalidates evidence before another dispatch", async () => {
  const { AgentError } = await import("purra");
  const fixture = setup({ outputs: ["private-invalid-candidate"] });
  let validations = 0;
  const runner = new ModelTaskRunner({ runId: "run", model: fixture.model, authority: fixture.authority,
    evidenceValidator: { validateEvidence() { if (++validations === 2) throw new AgentError("external_evidence_stale", "stale"); } } });
  runner.bindEvidence([{ evidenceId: "evidence", source: "host" }]);
  await assert.rejects(runner.completeStructured([], { output, repairAttempts: 2 }), { code: "external_evidence_stale" });
  assert.equal(validations, 2);
  assert.equal(fixture.calls.length, 1);
});

test("bound Run signal remains effective when extension omits the call signal", async () => {
  const root = new AbortController();
  let started;
  const ready = new Promise(resolve => { started = resolve; });
  let calls = 0;
  const runner = new ModelTaskRunner({ runId: "run", signal: root.signal, model: {
    capabilities: capabilities(),
    async invoke(_request, signal) { calls++; started(); await new Promise(resolve => signal.addEventListener("abort", resolve, { once: true })); throw new Error("stopped"); },
  } });
  const job = runner.completeStructured([], { output, repairAttempts: 2 });
  await ready;
  root.abort();
  await assert.rejects(job, { code: "agent_canceled" });
  assert.equal(calls, 1);
});
