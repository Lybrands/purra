import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { Agent } from "purra";
import { Mem0Memory, runModel, MemoryWorkflow } from "../dist/index.js";
import { ManagedMem0Client, currentExecution } from "../dist/providers.js";
import { Journal } from "../dist/journal.js";

const source = { id: "conversation", revision: "1" };
const messages = [{ role: "user", content: "说中文" }];
const request = { query: "中文", limit: 2, scope: {} };
const budget = changes => ({ key: "job", maxLlmCalls: 2, maxEmbeddingCalls: 8, maxInputChars: 50_000,
  maxOutputTokens: 64, resultCapacityTargetTokens: 32, ...changes });
const complete = async (_, cap) => ({ message: { role: "assistant", content: JSON.stringify({ memory: [{ text: "中文", entities: [] }] }) },
  finishReason: "stop", appliedGenerationLimit: cap, usage: { inputTokens: 10, generationTokens: 5 } });
const embed = async texts => ({ vectors: texts.map(() => [1, 0]), inputTokens: texts.reduce((n, t) => n + [...t].length, 0) });
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };

class ProviderSdk {
  rows = new Map();
  async add(input, options) {
    const execution = currentExecution();
    let texts;
    if (!options.infer) { await execution.invoke("embedding", [input]); texts = [input]; }
    else {
      await execution.invoke("embedding", [input[0].content]);
      const extracted = JSON.parse(await execution.invoke("llm", input)).memory;
      texts = [];
      for (const { text } of extracted) {
        try { await execution.invoke("embedding", [text]); texts.push(text); }
        catch { try { await execution.invoke("embedding", [text]); } catch { /* swallowed fallback */ } }
      }
    }
    return { results: texts.map(text => {
      const id = String(this.rows.size + 1);
      this.rows.set(id, { id, memory: text, metadata: { ...options.metadata }, user_id: options.userId });
      return { id };
    }) };
  }
  async search(query, options) { await currentExecution().invoke("embedding", [query]); return this.getAll(options); }
  async getAll({ filters, topK }) { return { results: [...this.rows.values()].filter(r => Object.entries(filters).every(([k, v]) => (r[k] ?? r.metadata[k]) === v)).slice(0, topK) }; }
  async get(id) { return this.rows.get(id) ?? null; }
  async delete(id) { this.rows.delete(id); }
  async update(id, { text, metadata }) { this.rows.set(id, { ...this.rows.get(id), memory: text, metadata: { ...metadata } }); }
  async history() { return []; }
}
function fixture(t) {
  const path = mkdtempSync(join(tmpdir(), "purra-managed-"));
  const client = new ManagedMem0Client(new ProviderSdk(), 2);
  const memories = [];
  const create = (options = {}) => {
    const memory = new Mem0Memory({ client, scope: { user: "u", project: "p" }, journalPath: join(path, "journal.db"), allowInference: true,
      ...options, providers: { budget: budget(), complete, embed, ...options.providers } });
    memories.push(memory); return memory;
  };
  t.after(async () => { for (const memory of memories) { await memory.drain(); memory.close(); } rmSync(path, { recursive: true, force: true }); });
  return { create, path, client };
}

test("managed quota survives restart and replay; searches consume it", async t => {
  const { create } = fixture(t);
  const providers = { budget: budget({ maxEmbeddingCalls: 3 }) };
  const memory = create({ providers });
  const receipt = await memory.extract(messages, { source, key: "extract" });
  assert.equal(receipt.usage.llmCalls, 1); assert.equal(receipt.usage.embeddingCalls, 2);
  assert.equal(receipt.usage.reportedInputTokens, 15); assert.equal(receipt.usage.reportedOutputTokens, 5);
  assert.equal(receipt.usage.reservedOutputTokens, 32); assert.equal(receipt.usage.unreportedCalls, 0);
  memory.close();
  const restored = create({ providers });
  assert.deepEqual(await restored.extract(messages, { source, key: "extract" }), receipt);
  await restored.retrieve(request);
  await assert.rejects(restored.retrieve(request), { code: "retrieval_source_unavailable" });
  assert.equal(restored.budgetUsage().embeddingCalls, 3);
  assert.throws(() => create({ providers: { budget: budget({ maxEmbeddingCalls: 4 }) } }), { code: "memory_budget_conflict" });
});

for (const limits of [{ maxLlmCalls: 0 }, { maxOutputTokens: 31 }, { maxInputChars: 4 }]) {
  test(`managed quota denies before LLM: ${JSON.stringify(limits)}`, async t => {
    const { create } = fixture(t); let calls = 0;
    const memory = create({ providers: { budget: budget(limits), complete: async (...args) => { calls++; return complete(...args); } } });
    await assert.rejects(memory.extract(messages, { source, key: "denied" }), { code: "memory_budget_exceeded" });
    assert.equal(calls, 0); assert.equal(memory.operation("denied").state, "unknown");
  });
}

test("swallowed provider denial cannot commit or reconcile a partial extraction", async t => {
  const { create } = fixture(t);
  const memory = create({ providers: { budget: budget({ maxEmbeddingCalls: 1 }) } });
  await assert.rejects(memory.extract(messages, { source, key: "partial" }), { code: "memory_budget_exceeded" });
  assert.equal(memory.operation("partial").usage.embeddingCalls, 1);
  assert.deepEqual((await memory.list({ state: "pending" })).items, []);
  await assert.rejects(memory.reconcile("partial", { writerStopped: true }), { code: "memory_reconciliation_required" });
  assert.equal((await memory.discardExtraction("partial", { writerStopped: true })).state, "discarded");
});

test("source withdrawal during an LLM call denies subsequent embedding before admission", async t => {
  const { create } = fixture(t);
  const memory = create({ providers: { complete: async (...args) => {
    await memory.revokeSource(source.id, { key: "withdraw" });
    return complete(...args);
  } } });
  await assert.rejects(memory.extract(messages, { source, key: "interrupted" }), { code: "memory_source_revoked" });
  const usage = memory.operation("interrupted").usage;
  assert.equal(usage.llmCalls, 1); assert.equal(usage.embeddingCalls, 1);
  assert.equal(memory.operation("withdraw").usage.llmCalls, 0);
  assert.deepEqual((await memory.list({ state: "pending" })).items, []);
  await assert.rejects(memory.reconcile("interrupted", { writerStopped: true }), { code: "memory_reconciliation_required" });
  await memory.discardExtraction("interrupted", { writerStopped: true });
});

test("recovery requires a durable provider verdict, not just SDK IDs", async t => {
  const { create } = fixture(t);
  const memory = create();
  t.mock.method(Journal.prototype, "verifyProviders", () => { throw Error("journal unavailable after SDK IDs"); });
  await assert.rejects(memory.extract(messages, { source, key: "verdict" }), { code: "memory_sdk_error" });
  assert.ok(memory.operation("verdict").ids.length);
  await assert.rejects(memory.reconcile("verdict", { writerStopped: true }), { code: "memory_reconciliation_required" });
  assert.equal((await memory.discardExtraction("verdict", { writerStopped: true })).state, "discarded");
});

for (const abort of [false, true]) test(`managed ${abort ? "abort" : "timeout"} stops later calls and records late usage`, async t => {
  const { create } = fixture(t);
  const started = deferred(), release = deferred(), controller = new AbortController();
  const memory = create({ timeoutMs: abort ? 2000 : 30, providers: { embed: async (texts, signal) => {
    started.resolve(); await release.promise; assert.equal(signal.aborted, true); return embed(texts);
  } } });
  const task = memory.extract(messages, { source, key: "late", signal: controller.signal });
  const rejected = assert.rejects(task, { code: abort ? "memory_cancelled" : "memory_timeout" });
  await started.promise;
  if (abort) controller.abort();
  await rejected;
  assert.equal(memory.operation("late").usage.unsettledCalls, 1);
  release.resolve(); await memory.drain();
  const usage = memory.operation("late").usage;
  assert.equal(usage.unsettledCalls, 0); assert.equal(usage.llmCalls, 0); assert.equal(usage.reportedInputTokens, 3);
});

test("concurrent readers share durable quota and unknown usage is explicit", async t => {
  const { create } = fixture(t);
  const providers = { budget: budget({ maxEmbeddingCalls: 1 }), embed: async texts => ({ vectors: texts.map(() => [1, 0]) }) };
  const first = create({ providers }), second = create({ providers });
  const results = await Promise.allSettled([first.retrieve(request), second.retrieve(request)]);
  assert.equal(results.filter(x => x.status === "rejected").length, 1);
  assert.equal(first.budgetUsage().embeddingCalls, 1); assert.equal(second.budgetUsage().unreportedCalls, 1);
});

test("malformed model output does not become a successful empty extraction", async t => {
  const { create } = fixture(t);
  const memory = create({ providers: { complete: async (...args) => ({ ...await complete(...args), message: { role: "assistant", content: "not json" } }) } });
  await assert.rejects(memory.extract(messages, { source, key: "invalid" }), { code: "memory_invalid_extraction" });
  assert.equal(memory.operation("invalid").usage.reportedOutputTokens, 5);
});

test("missing output cap and invalid embeddings fail closed", async t => {
  const { create } = fixture(t);
  const first = create({ providers: { complete: async (...args) => ({ ...await complete(...args), appliedGenerationLimit: undefined }) } });
  await assert.rejects(first.extract(messages, { source, key: "cap" }), { code: "memory_provider_contract" });
  assert.equal(first.operation("cap").usage.reportedOutputTokens, 5);
  await first.discardExtraction("cap", { writerStopped: true });
  const second = create({ providers: { embed: async () => ({ vectors: [[NaN, 0]], inputTokens: 2 }) } });
  await assert.rejects(second.add("中文", { source, key: "shape" }), { code: "memory_provider_contract" });
  assert.equal(second.operation("shape").usage.embeddingCalls, 1);
  assert.deepEqual((await second.list()).items, []);
});

test("result capacity does not require the Provider generation allowance to equal the target", async t => {
  const { create } = fixture(t);
  const memory = create({ providers: {
    budget: budget({ resultCapacityTargetTokens: 16 }),
    complete: async (...args) => ({ ...await complete(...args), appliedGenerationLimit: 32 }),
  } });
  const receipt = await memory.extract(messages, { source, key: "headroom" });
  assert.equal(receipt.usage.reservedOutputTokens, 16);
  assert.equal(receipt.usage.reportedOutputTokens, 5);
});

test("input envelope counts Unicode codepoints", async t => {
  const { create } = fixture(t);
  const memory = create({ providers: { budget: budget({ maxInputChars: 2 }) } });
  const receipt = await memory.add("猫😺", { source, key: "unicode" });
  assert.equal(receipt.usage.inputChars, 2);
  await assert.rejects(memory.retrieve(request), { code: "retrieval_source_unavailable" });
});

test("runModel uses the Agent-injected runner and the canonical invocation journal", async t => {
  const { create } = fixture(t);
  let memory, modelCalls = 0;
  const agent = new Agent({
    model: {
      capabilities: { schemaVersion: 2, profileId: "memory-test", providerProtocol: "custom", contextWindowTokens: 8192, maxGenerationTokens: 32,
        thinkingTokenAccounting: "included", protocol: { reasoningControl: "unavailable", reasoningReplay: "ignored", toolCalling: "unavailable",
          requiredToolChoice: "unavailable", parallelToolCalls: "unavailable", streaming: "unavailable", cancellation: "supported",
          assistantContentWithToolCalls: "optional", jsonSchemaLevel: "unknown", streamFinishSemantics: "normalized", usageSemantics: "normalized" } },
      async invoke(request) {
        modelCalls++;
        assert.equal(request.outputBudget.maxGenerationTokens, 32);
        if (modelCalls < 3) {
          assert.equal(request.outputBudget.resultCapacityTargetTokens, 16);
          assert.equal(request.outputBudget.resultCapacitySource, "workflow_policy");
        }
        if (modelCalls === 1) return complete(request.messages, request.outputBudget.maxGenerationTokens);
        if (modelCalls === 2) return { ...await complete(request.messages, request.outputBudget.maxGenerationTokens),
          message: { role: "assistant", content: '{"relations":[{"item":"0","kind":"duplicate"}]}' } };
        return {
          message: { role: "assistant", content: "done" }, finishReason: "stop", appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
          usage: { inputTokens: 1, generationTokens: 1 },
        };
      },
    },
    context: { providerFactory(runner) {
      memory = create({ providers: {
        budget: budget({ resultCapacityTargetTokens: 16 }),
        complete: runModel(runner),
      } });
      return { describeContextDemands() { return []; }, async buildContext() {
        await memory.add("中文", { source, key: "existing" });
        const receipt = await memory.extract(messages, { source, key: "run" });
        await memory.review({ id: receipt.ids[0], version: 1 }, { key: "run-review" });
        return { blocks: [] };
      } };
    } },
  });
  const handle = await agent.submit({ messages: [{ role: "user", content: "run" }] }, { budgets: { maxRunGenerationTokens: 96 } });
  assert.equal((await handle.result).output, "done");
  const events = []; for await (const event of handle.events({ visibility: "all" })) events.push(event);
  const invocations = events.filter(e => e.kind === "invocation.started");
  assert.equal(invocations.length, 3); assert.ok(invocations.every(e => e.payload.receipt.runId === handle.runId));
  assert.equal(memory.operation("run").usage.reportedOutputTokens, 5);
  assert.equal(memory.operation("run-review").usage.reportedOutputTokens, 5);
  assert.equal(memory.operation("run-review").review.proposal.kind, "duplicate");
});

test("a crashed reservation remains charged and visibly unsettled", t => {
  const { path } = fixture(t), journalPath = join(path, "crash.db");
  const script = `import { Journal } from ${JSON.stringify(new URL("../dist/journal.js", import.meta.url).href)};
    const j = new Journal(process.argv[1], 'scope');
    j.budget('job', { max_llm_calls:0, max_embedding_calls:1, max_input_chars:20, max_output_tokens:0, result_capacity_target_tokens:1 });
    j.admit('job', undefined, 'embedding', 2, 0); process.exit(17);`;
  assert.equal(spawnSync(process.execPath, ["--input-type=module", "-e", script, journalPath]).status, 17);
  const journal = new Journal(journalPath, "scope");
  try {
    assert.equal(journal.usage("budget", "job").unsettledCalls, 1);
    assert.throws(() => journal.admit("job", undefined, "embedding", 2, 0), { code: "memory_budget_exceeded" });
  } finally { journal.close(); }
});

const reviewFixture = JSON.parse(readFileSync(new URL("../../fixtures/review.json", import.meta.url), "utf8"));
const reviewBudget = budget({ maxLlmCalls: 8, maxEmbeddingCalls: 32, maxOutputTokens: 4096, resultCapacityTargetTokens: 512 });
test("memory workflow requires host authorization and resumes from the journal", async t => {
  const { create } = fixture(t);
  const providers = { budget: reviewBudget, complete: classifier(["independent"], []) };
  const memory = create({ providers });
  await memory.add("existing", { source: { id: "old", revision: "1" }, key: "old" });
  const options = { source, key: "capture" };
  const pending = await new MemoryWorkflow(memory).capture(messages, options);
  assert.equal(pending.pendingIds.length, 1);
  assert.equal(await memory.get(pending.pendingIds[0]), undefined);
  const config = { policy: (_candidate, review) => review.proposal, policyRevision: "preferences-v1" };
  const activated = await new MemoryWorkflow(memory, config).capture(messages, options);
  assert.equal(activated.pendingIds.length, 0);
  assert.equal(activated.resolutions.length, 1);
  const usage = memory.budgetUsage();
  memory.close();
  const restored = create({ providers });
  assert.deepEqual(await new MemoryWorkflow(restored, config).capture(messages, options), activated);
  assert.deepEqual(restored.budgetUsage(), usage);
  await assert.rejects(new MemoryWorkflow(restored, config).capture([{ role: "user", content: "changed" }], options), { code: "memory_idempotency_conflict" });
});
function classifier(kinds, calls, raw) {
  return async (messages, cap, signal) => {
    const base = await complete(messages, cap, signal);
    if (messages[0].content.startsWith("Review a pending memory")) {
      calls.push(messages);
      return { ...base, message: { role: "assistant", content: raw ?? JSON.stringify({ relations: kinds.map((kind, i) => ({ item: String(i), kind })) }) } };
    }
    return base;
  };
}
async function seedReview(memory, peers = 1) {
  const old = [];
  for (let i = 0; i < peers; i++) {
    const op = await memory.add(`既有资料 ${i}`, { source: { id: `old-${i}`, revision: "1" }, key: `old-${i}` });
    old.push({ id: op.ids[0], version: 1 });
  }
  const op = await memory.extract([{ role: "user", content: "这次请详细解释。" }], { source: { id: "candidate", revision: "1" }, key: "candidate" });
  return { candidate: { id: op.ids[0], version: 1 }, old };
}

for (const scenario of reviewFixture.cases) test(`semantic advice: ${scenario.name}`, async t => {
  const { create, path } = fixture(t), calls = [];
  const providers = { budget: reviewBudget, complete: classifier(scenario.kinds, calls) };
  let memory = create({ providers });
  const { candidate, old } = await seedReview(memory, scenario.kinds.length), epoch = memory.epoch;
  const result = await memory.review(candidate, { key: "review", instructions: "临时例外不能覆盖长期偏好。" });
  assert.equal(result.state, "complete"); assert.deepEqual(result.ids, []); assert.equal(memory.epoch, epoch);
  assert.equal(result.usage.llmCalls, Number(old.length > 0)); assert.equal(result.usage.embeddingCalls, 1);
  assert.deepEqual(result.review.candidate, candidate); assert.deepEqual(result.review.matches.map(m => m.item), old);
  assert.deepEqual(result.review.matches.map(m => m.kind), scenario.kinds);
  assert.equal((await memory.get(candidate.id, { includeInactive: true })).state, "pending");
  const proposal = result.review.proposal;
  assert.equal(proposal?.kind ?? null, scenario.proposal);
  if (old.length) {
    assert.ok(calls[0][0].content.includes("untrusted data")); assert.ok(calls[0][0].content.includes("临时例外"));
    const payload = JSON.parse(calls[0][1].content);
    assert.equal(payload.candidate.source.id, "candidate"); assert.equal(payload.related[0].item, "0");
  }
  memory.close(); memory = create({ providers });
  const before = memory.budgetUsage();
  assert.deepEqual(await memory.review(candidate, { key: "review", instructions: "临时例外不能覆盖长期偏好。" }), result);
  assert.deepEqual(memory.budgetUsage(), before); assert.deepEqual(memory.operation("review"), result);
  await assert.rejects(memory.review(candidate, { key: "review", instructions: "changed policy" }), { code: "memory_idempotency_conflict" });
  const db = new DatabaseSync(join(path, "journal.db"));
  try {
    const plan = db.prepare("SELECT plan FROM purra_mem0_ops WHERE key='review'").get().plan;
    for (const text of ["临时例外", "既有资料", "中文"]) assert.ok(!plan.includes(text));
  } finally { db.close(); }
  if (proposal) {
    const applied = await memory.resolve(proposal, { key: "apply" });
    assert.equal(applied.resolution.reviewKey, "review"); assert.deepEqual(applied.resolution, proposal); assert.equal(memory.epoch, epoch + 1);
    if (proposal.kind === "conflict") assert.equal(await memory.get(candidate.id), undefined);
    else assert.ok(await memory.get(proposal.keep));
  }
});

for (const raw of reviewFixture.invalid) test(`reject unsafe review output: ${raw}`, async t => {
  const { create } = fixture(t), memory = create({ providers: { budget: reviewBudget, complete: classifier([], [], raw) } });
  const { candidate } = await seedReview(memory), epoch = memory.epoch;
  await assert.rejects(memory.review(candidate, { key: "invalid" }), { code: "memory_invalid_review" });
  const op = memory.operation("invalid");
  assert.equal(op.state, "failed"); assert.equal(op.review, undefined); assert.equal(op.usage.llmCalls, 1);
  assert.equal(op.usage.reportedOutputTokens, 5); assert.equal(memory.epoch, epoch);
  await memory.add("still writable", { source: { id: "next", revision: "1" }, key: "next" });
});

test("review quota denies before model dispatch and releases its writer fence", async t => {
  const { create } = fixture(t), calls = [];
  const memory = create({ providers: { budget: { ...reviewBudget, maxLlmCalls: 1 }, complete: classifier(["duplicate"], calls) } });
  const { candidate } = await seedReview(memory);
  await assert.rejects(memory.review(candidate, { key: "denied" }), { code: "memory_budget_exceeded" });
  assert.equal(calls.length, 0); assert.equal(memory.operation("denied").state, "failed");
  assert.equal(memory.operation("denied").usage.llmCalls, 0);
  await memory.add("next", { source: { id: "next", revision: "1" }, key: "next" });
});

for (const event of ["withdraw", "tamper", "cancel", "timeout"]) test(`${event} never publishes late review advice`, async t => {
  const { create, client } = fixture(t), started = deferred(), release = deferred(), cancel = new AbortController();
  const regular = classifier(["duplicate"], []);
  const model = async (messages, cap, signal) => {
    if (messages[0].content.startsWith("Review a pending memory")) { started.resolve(); await release.promise; }
    return regular(messages, cap, signal);
  };
  const memory = create({ providers: { budget: reviewBudget, complete: model }, timeoutMs: 30_000 });
  const { candidate, old } = await seedReview(memory);
  if (event === "timeout") t.mock.timers.enable({ apis: ["setTimeout"] });
  const task = memory.review(candidate, { key: "slow", signal: cancel.signal }), rejected = assert.rejects(task);
  try {
    await started.promise;
    if (event === "withdraw") await create({ providers: { budget: reviewBudget } }).revokeSource("old-0", { key: "withdraw" });
    else if (event === "tamper") client.sdk.rows.get(old[0].id).memory = "changed outside adapter";
    else if (event === "cancel") cancel.abort();
    else if (event === "timeout") t.mock.timers.tick(30_000);
    if (event !== "timeout" && event !== "cancel") release.resolve();
    await rejected;
  } finally { release.resolve(); await memory.drain(); }
  const op = memory.operation("slow");
  assert.equal(op.state, "failed"); assert.equal(op.review, undefined);
  assert.equal(op.usage.unsettledCalls, 0); assert.equal(op.usage.reportedOutputTokens, 5);
  assert.equal((await memory.get(candidate.id, { includeInactive: true })).state, "pending");
});

test("bound execution rechecks independent comparisons and rejects foreign references", async t => {
  const { create } = fixture(t), memory = create({ providers: { budget: reviewBudget, complete: classifier(["duplicate", "independent"], []) } });
  const { candidate, old } = await seedReview(memory, 2), review = (await memory.review(candidate, { key: "review" })).review;
  assert.ok(!review.proposal.items.some(r => r.id === old[1].id));
  await assert.rejects(memory.resolve({ kind: "independent", items: [old[0]], keep: old[0].id, reviewKey: "review" }, { key: "foreign" }), { code: "memory_review_mismatch" });
  await memory.update(old[1].id, "new fact", { source: { id: "old-1", revision: "2" }, version: 1, key: "change" });
  await assert.rejects(memory.resolve(review.proposal, { key: "apply" }), { code: "memory_context_stale" });
  assert.equal(memory.operation("apply"), undefined);
});

test("bound review execution checks expiry without requiring an epoch mutation", async t => {
  const { create } = fixture(t), memory = create({ providers: { budget: reviewBudget, complete: classifier(["independent"], []) } });
  const { candidate, old } = await seedReview(memory);
  await memory.update(old[0].id, "short lived", { source: { id: "old-0", revision: "1" }, version: 1, key: "expires", expiresAt: new Date(Date.now() + 60_000).toISOString() });
  const proposal = (await memory.review(candidate, { key: "review" })).review.proposal;
  const now = Date.now(); t.mock.method(Date, "now", () => now + 120_000);
  await assert.rejects(memory.resolve(proposal, { key: "apply" }), { code: "memory_context_stale" });
});

test("review requires managed pending sources and an untruncated bounded input", async t => {
  const { create, path } = fixture(t);
  const raw = new Mem0Memory({ client: new ProviderSdk(), scope: { user: "raw", project: "p" }, journalPath: join(path, "raw.db") });
  try { await assert.rejects(raw.review({ id: "x", version: 1 }, { key: "raw" }), { code: "memory_review_requires_managed" }); }
  finally { raw.close(); }
  const memory = create({ maxInputChars: 100, providers: { budget: reviewBudget, complete: classifier(["independent"], []) } });
  const { candidate, old } = await seedReview(memory);
  await assert.rejects(memory.review(old[0], { key: "active" }), { code: "memory_review_candidate_state" });
  await assert.rejects(memory.review(candidate, { key: "oversize" }), { code: "memory_review_input_too_large" });
  assert.equal(memory.operation("oversize").usage.llmCalls, 0);
});

test("crashed review is abandoned without replaying its charged provider call", async t => {
  const { create, path, client } = fixture(t), memory = create({ providers: { budget: reviewBudget } });
  const { candidate } = await seedReview(memory), scope = client.sdk.rows.get(candidate.id).user_id;
  const script = `
    import {Journal} from ${JSON.stringify(new URL("../dist/journal.js", import.meta.url).href)};
    const j=new Journal(process.argv[1],process.argv[2]);
    j.begin('crashed','opaque',{kind:'review',target:null,meta:null,budget:'job',review_epoch:j.epoch,review_refs:[{id:process.argv[3],version:1}]});
    j.admit('job','crashed','llm',10,512);
    process.exit(17);`;
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", script, join(path, "journal.db"), scope, candidate.id], { encoding: "utf8", timeout: 10_000 });
  assert.equal(result.status, 17, result.stderr);
  const receipt = await memory.reconcile("crashed", { writerStopped: true });
  assert.equal(receipt.state, "failed"); assert.equal(receipt.review, undefined); assert.equal(receipt.usage.unsettledCalls, 1);
  assert.equal((await memory.get(candidate.id, { includeInactive: true })).state, "pending");
  await memory.add("recovered", { source: { id: "next", revision: "1" }, key: "next" });
});

test("withdrawal during search prevents dispatching the semantic review prompt", async t => {
  const { create } = fixture(t), calls = [];
  let phase = false;
  const memory = create({ providers: { budget: reviewBudget, complete: classifier(["duplicate"], calls),
    async embed(texts, signal) {
      if (phase) await create({ providers: { budget: reviewBudget } }).revokeSource("old-0", { key: "withdraw" });
      return embed(texts, signal);
    },
  } });
  const { candidate } = await seedReview(memory); phase = true;
  await assert.rejects(memory.review(candidate, { key: "review" }), { code: "memory_context_stale" });
  assert.equal(calls.length, 0); assert.equal(memory.operation("review").usage.llmCalls, 0);
});
