/** Offline evaluator checks are not semantic quality evidence. */
import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync, mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { grade, preflight, validateFixture, saveReport, Transport, LIMITS } from "../scripts/evaluate.mjs";

for (const [label, body, correct] of [
  ["supported value", { answer: "新值", evidence: ["fact"] }, true],
  ["old value", { answer: "旧值", evidence: ["fact"] }, false],
  ["no evidence", { answer: "新值", evidence: [] }, false],
  ["distractor citation", { answer: "新值", evidence: ["distractor"] }, false],
  ["invented citation", { answer: "新值", evidence: ["foreign"] }, false],
  ["duplicate citation", { answer: "新值", evidence: ["fact", "fact"] }, false],
  ["nested answer", { answer: { nested: "新值" }, evidence: [] }, false],
  ["extra fields", { answer: "新值", evidence: ["fact"], extra: true }, false],
]) test("evaluation rejects unsupported success: " + label, () => {
  assert.equal(grade(JSON.stringify(body), [{ id: "fact" }, { id: "distractor" }], ["新值"], ["fact"]).correct, correct);
});
test("abstention is safe but does not count as recalled personalization", () => {
  assert.deepEqual(grade('{"answer":null,"evidence":[]}', [], ["fact"]), { valid: true, correct: false, abstains: true });
  assert.equal(grade('{"answer":null,"evidence":[]}', [], []).correct, true);
  assert.equal(grade("not json", [], []).valid, false);
});
test("empty case lists and paths cannot produce successful trials", () => {
  const fixture = JSON.parse(readFileSync(new URL("../../fixtures/evaluation.json", import.meta.url), "utf8"));
  validateFixture(fixture);
  fixture.cases[0].id = "../../outside"; assert.throws(() => validateFixture(fixture), /invalid_evaluation_cases/);
  fixture.cases = []; assert.throws(() => validateFixture(fixture), /invalid_evaluation_cases/);
});
test("interrupted trial has unknown usage and remains running", t => {
  const root = mkdtempSync(join(tmpdir(), "purra-eval-report-")); t.after(() => rmSync(root, { recursive: true, force: true }));
  const path = join(root, "report.json");
  saveReport(path, { status: "running", calls: [{ phase: "review", kind: "chat", input_tokens: null, output_tokens: null }] });
  const saved = JSON.parse(readFileSync(path, "utf8"));
  assert.equal(saved.status, "running"); assert.equal(saved.usage_by_phase["review/chat"].unreported_calls, 1);
  assert.equal(existsSync(path + ".tmp"), false);
});
const config = () => ({ chat: { base_url: "https://chat.example", model: "test", key_env: "PURRA_EVAL_TEST_KEY" },
  embedding: { base_url: "https://embedding.example", model: "test", key_env: "PURRA_EVAL_TEST_KEY", dimensions: 2 } });
function key(t, value) {
  const previous = process.env.PURRA_EVAL_TEST_KEY;
  const set = v => { if (v === undefined) delete process.env.PURRA_EVAL_TEST_KEY; else process.env.PURRA_EVAL_TEST_KEY = v; };
  set(value); t.after(() => set(previous));
}
test("preflight cannot override hard limits or put credentials in URLs", t => {
  key(t, undefined);
  assert.deepEqual(preflight(config()), ["PURRA_EVAL_TEST_KEY", "chat.base_url", "embedding.base_url"]);
  const value = config(); value.chat.extra = { max_tokens: 1_000_000 };
  assert.throws(() => preflight(value), /unsupported_chat_options/);
  const bad = config(); bad.chat.base_url = "https://secret:password@chat.example";
  assert.throws(() => preflight(bad), /invalid_provider_url/);
});
test("transport applies exact cap, accounts before dispatch and records no content", async t => {
  key(t, "private-key");
  const requests = [];
  const transport = new Transport(config(), async (url, options) => {
    requests.push({ url, options });
    return Response.json({ choices: [{ finish_reason: "stop", message: { role: "assistant", content: "private model text" } }], usage: { prompt_tokens: 7, completion_tokens: 3 } });
  });
  const result = await transport.complete([{ role: "user", content: "私有😀" }], 32);
  assert.equal(result.appliedOutputLimit, 32); assert.equal(result.usage.outputTokens, 3);
  assert.equal(JSON.parse(requests[0].options.body).max_tokens, 32); assert.equal(transport.calls[0].input_chars, 3);
  for (const secret of ["private-key", "private model text", "私有"]) assert.ok(!JSON.stringify(transport.calls).includes(secret));
  transport.reserved = LIMITS.reserved_output_tokens;
  await assert.rejects(transport.complete([{ role: "user", content: "x" }], 32), /trial_budget_exceeded/);
  assert.equal(requests.length, 1);
});
test("HTTP failures are redacted without retry or refund", async t => {
  key(t, "private-key");
  let calls = 0;
  const transport = new Transport(config(), async () => { calls++; return new Response("private failure", { status: 429 }); });
  await assert.rejects(transport.post("chat", {}, 1, 32), /provider_http_429/);
  assert.equal(calls, 1); assert.equal(transport.reserved, 32); assert.equal(transport.calls[0].input_tokens, null);
});
test("abort stops the request but does not refund its reservation", async t => {
  key(t, "private-key");
  const controller = new AbortController();
  let notify;
  const started = new Promise(resolve => { notify = resolve; });
  const transport = new Transport(config(), async (_, options) => {
    notify();
    return new Promise((_, reject) => options.signal.addEventListener("abort", () => reject(Error("private failure")), { once: true }));
  });
  const pending = transport.post("chat", {}, 1, 32, controller.signal);
  await started; controller.abort(); await assert.rejects(pending, /cancelled/);
  assert.equal(transport.reserved, 32); assert.equal(transport.calls[0].input_tokens, null);
});
