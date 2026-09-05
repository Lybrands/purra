import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { DirectLlm, DirectEmbedder } from "../dist/direct-providers.js";
import { ProviderExecution, providerLimits } from "../dist/providers.js";
import { Journal } from "../dist/journal.js";

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "purra-direct-"));
  const journal = new Journal(join(root, "journal.db"), "direct");
  const calls = [];
  const providers = {
    budget: { key: "direct", maxLlmCalls: 3, maxEmbeddingCalls: 3, maxInputChars: 10000, maxOutputTokens: 100, resultCapacityTargetTokens: 32 },
    async complete(messages, target) {
      calls.push(["llm", messages, target]);
      return { message: { role: "assistant", content: '{"memory":[]}' }, finishReason: "stop", appliedGenerationLimit: target, usage: { inputTokens: 10, generationTokens: 2 } };
    },
    async embed(texts) { calls.push(["embedding", texts]); return { vectors: texts.map((_, i) => [i, 1]), inputTokens: 4 }; },
  };
  journal.budget(providers.budget.key, providerLimits(providers));
  t.after(() => { journal.close(); rmSync(root, { recursive: true, force: true }); });
  return { bound: new ProviderExecution(providers, journal, 2, 10000, 10, 10000), calls, journal };
}
test("native text and batch preserve one call per admission", async t => {
  const { bound, calls } = fixture(t);
  const messages = [{ role: "system", content: "policy" }, { role: "user", content: "中文" }];
  await bound.run(async () => {
    assert.deepEqual(JSON.parse(await new DirectLlm().generateResponse(messages, { type: "json_object" })), { memory: [] });
    assert.deepEqual(await new DirectEmbedder().embedBatch(["a", "b"], "search"), [[0, 1], [1, 1]]);
  });
  assert.deepEqual(calls, [["llm", messages, 32], ["embedding", ["a", "b"]]]);
});
for (const invalid of [
  () => new DirectLlm().generateResponse(Array(1)),
  () => new DirectEmbedder().embedBatch(Array(1)),
  () => new DirectLlm().generateResponse([{ role: "user", content: [] }]),
  () => new DirectLlm().generateResponse([{ role: "user", content: "x" }], undefined, [{}]),
  () => new DirectLlm().generateResponse([{ role: "user", content: "x" }], undefined, undefined, true),
  () => new DirectLlm().generateResponse([{ role: "tool", content: "x" }]),
  () => new DirectLlm().generateResponse([{ role: "user", content: "x" }], { type: "json_schema" }),
  () => new DirectLlm().generateChat(),
  () => new DirectEmbedder().embedBatch(["x", null]),
  () => new DirectEmbedder().embed("x", "unknown"),
]) test("protocol denial survives SDK swallowing and fallback", async t => {
  const { bound, calls } = fixture(t);
  await assert.rejects(bound.run(async () => {
    try { await invalid(); } catch {}
    await assert.rejects(new DirectEmbedder().embed("fallback"), { code: "memory_provider_contract" });
    return "SDK claimed success";
  }), { code: "memory_provider_contract" });
  assert.deepEqual(calls, []);
});
test("unbound and cancelled calls never reach callbacks", async t => {
  await assert.rejects(new DirectEmbedder().embed("unbound"), { code: "memory_provider_unbound" });
  const { bound, calls } = fixture(t);
  await assert.rejects(bound.run(async () => {
    bound.stop("memory_cancelled");
    await new DirectEmbedder().embed("cancelled");
  }), { code: "memory_cancelled" });
  assert.deepEqual(calls, []);
});
