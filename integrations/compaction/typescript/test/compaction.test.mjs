import assert from "node:assert/strict";
import test from "node:test";
import { SemanticCompaction } from "../dist/index.js";
const summary = Object.fromEntries(["goals", "constraints", "decisions", "completed", "open_questions", "evidence"].map(k => [k, []]));
const messages = [{ role: "system", content: "instructions" }, { role: "user", content: "old".repeat(1000) },
  { role: "assistant", content: "old answer", reasoning: "private-secret" }, { role: "user", content: "latest question" }];
const request = { messages, previousSummary: null, compressionRequired: true, availableMessageTokens: 1500 };
test("semantic summary preserves instructions and latest turn; private reasoning is excluded", async () => {
  let calls = 0;
  const hook = new SemanticCompaction({ async complete(input, options) {
    calls++; assert.equal(options.resultCapacityTargetTokens, 256);
    assert.equal(options.resultCapacitySource, "workflow_policy");
    assert.equal(options.maxGenerationTokens, undefined);
    assert.ok(!JSON.stringify(input).includes("private-secret"));
    return { turn: { message: { role: "assistant", content: JSON.stringify(summary) }, finishReason: "stop" } };
  } }, { maxSummaryTokens: 256, keepRecentMessages: 1 });
  const result = await hook.compress(request);
  assert.deepEqual(result.messages, [messages[0], messages.at(-1)]);
  assert.equal(result.summary.untrusted, true);
  assert.equal(calls, 1);
  await hook.compress({ ...request, compressionRequired: false });
  assert.equal(calls, 1);
});
test("invalid/truncated summaries fail without replacing input", async () => {
  for (const [content, finishReason] of [["no json", "stop"], [JSON.stringify(summary), "length"]]) {
    const hook = new SemanticCompaction({ async complete() { return { turn: { message: { role: "assistant", content }, finishReason } }; } }, { maxSummaryTokens: 256, keepRecentMessages: 1 });
    await assert.rejects(hook.compress(request), { code: "compaction_invalid_summary" });
  }
});
