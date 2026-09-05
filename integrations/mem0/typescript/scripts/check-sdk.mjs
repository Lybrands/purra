/** Private SDK extension + SQLite, deterministic providers. This is not a quality eval. */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Mem0Memory, createManagedClient } from "purra-mem0";
import { evaluateCase } from "./evaluate.mjs";

const root = mkdtempSync(join(tmpdir(), "purra-mem0-sdk-"));
process.env.MEM0_TELEMETRY = "false";
process.env.MEM0_DIR = join(root, "sdk");
const calls = { llm: 0, embedding: 0 };
const captured = [];
function embed(text) { calls.embedding++; return [Number(text.includes("简短")), Number(text.includes("详细")), Number(text.includes("中文")), 1]; }
const providers = {
  budget: { key: "sdk-test", maxLlmCalls: 4, maxEmbeddingCalls: 64, maxInputChars: 100_000, maxOutputTokens: 8192, resultCapacityTargetTokens: 2048 },
  async complete(messages, cap, signal) {
    calls.llm++; captured.push(messages);
    const payload = messages[0].content.startsWith("Review a pending memory")
      ? { relations: JSON.parse(messages[1].content).related.map(r => ({ item: r.item, kind: "independent" })) }
      : { memory: [{ text: "用户希望使用中文回复。", entities: [] }] };
    return { message: { role: "assistant", content: JSON.stringify(payload) },
      finishReason: "stop", appliedGenerationLimit: cap, usage: { inputTokens: 100, generationTokens: 20 } };
  },
  async embed(texts, signal) { return { vectors: texts.map(embed), inputTokens: texts.reduce((n, t) => n + [...t].length, 0) }; },
};
function sdk() {
  return createManagedClient({ embeddingDims: 4, config: {
    vectorStore: { provider: "memory", config: { dimension: 4, dbPath: join(root, "vectors.db"), collectionName: "memory" } },
    historyDbPath: join(root, "history.db"),
  } });
}
// Upstream has no public aggregate close(). Test-only teardown of its SQLite handles.
function closeSdk(client) {
  client = client.sdk;
  client.db.close(); client.vectorStore.db.close(); client._entityStore?.db.close();
}
const source = { id: "message:1", revision: "1" };
const scope = { user: "user", project: "project" };
const request = { query: "简短", limit: 8, scope: {} };
let direct;
try {
  const client = await sdk();
  const memory = new Mem0Memory({ client, scope, journalPath: join(root, "journal.db"), allowInference: true, providers });
  try {
    const [adminId] = (await memory.add("管理员待审记录。", { source, key: "admin", state: "pending", metadata: { kind: "note", pinned: false } })).ids;
    assert.equal(await memory.get(adminId), undefined);
    const beforeAdmin = { ...calls };
    await memory.annotate(adminId, { kind: "note", pinned: true }, { version: 1, key: "admin-pin" });
    await memory.setState(adminId, "active", { version: 2, key: "admin-accept" });
    assert.deepEqual(calls, beforeAdmin);
    assert.equal((await memory.get(adminId)).version, 3);
    assert.equal((await memory.list({ filters: { pinned: true } })).items[0].id, adminId);
    assert.equal((await client.sdk.get(adminId)).metadata.purra_metadata.pinned, false);
    await memory.delete(adminId, { version: 3, key: "admin-delete" });
    direct = await memory.add("用户喜欢简短回复。", { source, key: "direct" });
    assert.equal(calls.llm, 0);
    const candidate = await memory.extract([{ role: "user", content: "请使用中文回复。" }], { source, key: "extract" });
    assert.equal(calls.llm, 1); assert.equal(candidate.ids.length, 1);
    assert.equal(candidate.usage.llmCalls, 1); assert.equal(candidate.usage.reportedOutputTokens, 20);
    assert.ok(!captured[0][1].content.includes("用户喜欢简短回复。"));
    assert.deepEqual((await memory.retrieve(request)).map(hit => hit.id), direct.ids);
    const reviewed = await memory.review({ id: candidate.ids[0], version: 1 }, { key: "semantic-review" });
    assert.equal(reviewed.review.proposal.kind, "independent"); assert.equal(reviewed.review.proposal.reviewKey, "semantic-review");
    assert.equal(reviewed.usage.llmCalls, 1); assert.equal(reviewed.usage.embeddingCalls, 1);
    assert.equal(await memory.get(candidate.ids[0]), undefined);
    const beforeReplay = { ...calls };
    assert.deepEqual(await memory.review({ id: candidate.ids[0], version: 1 }, { key: "semantic-review" }), reviewed);
    assert.deepEqual(calls, beforeReplay);
    const applied = await memory.resolve(reviewed.review.proposal, { key: "apply-review" });
    assert.equal(applied.resolution.reviewKey, "semantic-review");
    assert.equal((await memory.get(candidate.ids[0])).state, "active");
    assert.equal((await memory.retrieve(request)).length, 2);
    await memory.update(direct.ids[0], "复杂问题需要详细解释。", { source: { id: "correction:1", revision: "2" }, version: 1, key: "update" });
    assert.equal((await memory.get(direct.ids[0])).version, 2);
    assert.ok((await memory.history(direct.ids[0])).length >= 2);
  } finally { await memory.drain(); memory.close(); closeSdk(client); }
  const client2 = await sdk();
  const restored = new Mem0Memory({ client: client2, scope, journalPath: join(root, "journal.db"), providers, allowInference: true });
  try {
    assert.equal((await restored.get(direct.ids[0])).text, "复杂问题需要详细解释。");
    assert.deepEqual(await restored.add("用户喜欢简短回复。", { source, key: "direct" }), direct);
    await restored.delete(direct.ids[0], { version: 2, key: "delete" });
    assert.equal(await restored.get(direct.ids[0], { includeInactive: true }), undefined);
    assert.ok((await restored.history(direct.ids[0])).length);
    assert.equal((await restored.retrieve(request)).length, 1);
    const denied = new Mem0Memory({ client: client2, scope, journalPath: join(root, "journal.db"), allowInference: true,
      providers: { ...providers, budget: { ...providers.budget, key: "denied", maxEmbeddingCalls: 1 } } });
    try {
      await assert.rejects(denied.extract([{ role: "user", content: "我使用中文。" }], { source, key: "denied" }), { code: "memory_budget_exceeded" });
      assert.equal(denied.operation("denied").state, "unknown");
      assert.equal(denied.budgetUsage().embeddingCalls, 1);
      assert.equal((await denied.discardExtraction("denied", { writerStopped: true })).state, "discarded");
    } finally { await denied.drain(); denied.close(); }
    const [hit] = await restored.retrieve(request);
    const evidence = [{ evidenceId: hit.metadata.evidenceId, source: hit.source, itemId: hit.id, version: String(hit.version) }];
    await restored.validateEvidence(evidence);
    const beforeWithdrawal = { ...calls };
    const withdrawn = await restored.revokeSource(source.id, { revision: source.revision, key: "withdraw" });
    assert.equal(withdrawn.usage.llmCalls, 0); assert.equal(withdrawn.usage.embeddingCalls, 0);
    assert.deepEqual(calls, beforeWithdrawal); assert.equal(restored.isSourceRevoked(source), true);
    assert.equal(await restored.get(hit.id, { includeInactive: true }), undefined);
    assert.deepEqual(await restored.retrieve(request), []);
    await assert.rejects(restored.validateEvidence(evidence), { code: "memory_context_stale" });
    await restored.update(hit.id, "用户希望中文回复，引用保留原文。", { source: { id: source.id, revision: "2" }, version: 2, key: "correct" });
    const [corrected] = await restored.retrieve(request);
    assert.equal(corrected.version, 3);
    await restored.validateEvidence([{ evidenceId: corrected.metadata.evidenceId, source: corrected.source, itemId: corrected.id, version: String(corrected.version) }]);
    await restored.revokeSource(source.id, { key: "withdraw-all" });
    const beforeReingest = { ...calls };
    await assert.rejects(restored.add("不得重新写入。", { source: { id: source.id, revision: "future" }, key: "reingest" }), { code: "memory_source_revoked" });
    assert.deepEqual(calls, beforeReingest); assert.deepEqual((await restored.list()).items, []);
    await restored.delete(hit.id, { version: 3, key: "cleanup" });
    assert.ok((await restored.history(hit.id)).length); // withdrawal is not audit erasure
    const old = await restored.add("用户希望默认英文回复。", { source: { id: "old", revision: "1" }, key: "old" });
    const fresh = await restored.extract([{ role: "user", content: "今后默认中文回复。" }], { source: { id: "new", revision: "1" }, key: "new" });
    const items = [{ id: old.ids[0], version: 1 }, { id: fresh.ids[0], version: 1 }];
    const beforeResolve = { ...calls };
    const resolved = await restored.resolve({ kind: "supersede", items, keep: fresh.ids[0] }, { key: "resolve" });
    assert.deepEqual(calls, beforeResolve); assert.equal(resolved.usage.llmCalls, 0); assert.equal(resolved.usage.embeddingCalls, 0);
    assert.equal((await client2.sdk.get(fresh.ids[0])).metadata.purra_state, "pending");
    const reopened = new Mem0Memory({ client: client2, scope, journalPath: join(root, "journal.db"), providers });
    try {
      assert.deepEqual(reopened.operation("resolve"), resolved);
      assert.deepEqual((await reopened.retrieve(request)).map(h => h.id), fresh.ids);
      assert.equal((await reopened.get(fresh.ids[0])).resolutionKey, "resolve");
      await reopened.resolve({ kind: "conflict", items: items.map(r => ({ ...r, version: 2 })) }, { key: "conflict" });
      assert.deepEqual(await reopened.retrieve(request), []);
      await reopened.resolve({ kind: "duplicate", items: items.map(r => ({ ...r, version: 3 })), keep: old.ids[0] }, { key: "dedupe" });
      await reopened.update(old.ids[0], "用户允许中英文回复。", { source: { id: "accepted", revision: "1" }, version: 4, key: "after-review" });
      assert.equal((await reopened.get(old.ids[0])).version, 5); assert.equal((await reopened.get(old.ids[0])).resolutionKey, undefined);
      await reopened.delete(fresh.ids[0], { version: 4, key: "retired-cleanup" });
    } finally { await reopened.drain(); reopened.close(); }
  } finally { await restored.drain(); restored.close(); closeSdk(client2); }
  // Full evaluator mechanics with substituted answers/vectors; not a quality score.
  const fixture = JSON.parse(readFileSync(new URL("../../fixtures/evaluation.json", import.meta.url), "utf8"));
  const transport = {
    config: { embedding: { dimensions: 2 } },
    async complete(messages, cap) {
      let payload;
      if (messages[0].content.startsWith("Review a pending memory")) {
        payload = { relations: JSON.parse(messages[1].content).related.map(row => ({ item: row.item, kind: row.text === this.test.seed ? this.test.review_kinds[0] : "independent" })) };
      } else if (this.phase === "ingestion") payload = { memory: [{ text: this.test.incoming, entities: [] }] };
      else {
        const body = JSON.parse(messages[1].content);
        const supporting = this.test.answers.flatMap(answer => body.memories.filter(row => row.text.includes(answer)).map(row => [answer, row.id]))[0];
        payload = { answer: supporting?.[0] ?? null, evidence: supporting ? [supporting[1]] : [] };
      }
      return { message: { role: "assistant", content: JSON.stringify(payload) }, finishReason: "stop", appliedGenerationLimit: cap, usage: { inputTokens: 10, generationTokens: 20 } };
    },
    async embed(texts) { return { vectors: texts.map(() => [1, 0]), inputTokens: texts.reduce((n, t) => n + [...t].length, 0) }; },
  };
  for (const [index, test] of fixture.cases.entries()) {
    transport.test = test;
    const result = await evaluateCase(test, fixture, join(root, test.id), transport, index);
    assert.ok(result.passed, JSON.stringify({ case: test.id, checks: result.checks }));
  }
  console.log(JSON.stringify({ sdk: "purra-mem0 private extension of mem0ai@3.1.7", checks: "CRUD/inference/restart/idempotency/managed-budget/swallowed-denial/source-withdrawal/correction/evidence/atomic-resolution/semantic-review/evaluation-harness", provider: "deterministic fixture via native PurrA adapters", calls, offline_evaluation_cases: fixture.cases.length }));
} finally { rmSync(root, { recursive: true, force: true }); }
