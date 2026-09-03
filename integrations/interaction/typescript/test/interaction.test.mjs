import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { ClarificationStore, ClarificationWorkflow } from "../dist/index.js";
const input = { key: "draft-1", questions: [{ id: "length", prompt: "选择篇幅", choices: ["短篇", "长篇"], allowFreeform: false }],
  checkpoint: { messages: [{ role: "user", content: "写故事" }] } };
function setup(t) {
  const path = mkdtempSync(join(tmpdir(), "purra-interaction-")), stores = [];
  t.after(() => { stores.forEach(s => { try { s.close(); } catch {} }); rmSync(path, { force: true, recursive: true }); });
  return (scope = "owner") => { const store = new ClarificationStore(join(path, "state.db"), { scope }); stores.push(store); return store; };
}
test("persistent clarification resumes once with validated answers", async t => {
  const create = setup(t), first = create(), saved = first.ask(input); first.close();
  const store = create(), competitor = create();
  const ready = store.answer(saved.id, { revision: 1, key: "answer", answers: { length: "短篇" } });
  assert.deepEqual(competitor.answer(saved.id, { revision: 1, key: "answer", answers: { length: "短篇" } }), ready);
  const resumed = await new ClarificationWorkflow(store).resume(saved.id, { revision: ready.revision, submit: async (snapshot, key) => {
    assert.deepEqual(snapshot.answers, { length: "短篇" }); assert.equal(key, "clarification:" + saved.id);
    assert.throws(() => competitor.claim(saved.id, { revision: ready.revision }), /state_conflict/);
    return "continued-run";
  } });
  assert.equal(resumed.state, "resumed"); assert.equal(resumed.runId, "continued-run");
  assert.equal("checkpoint" in ClarificationStore.publicView(resumed), false);
  assert.equal("resumeToken" in ClarificationStore.publicView(resumed), false);
  assert.equal(store.ask(input).id, saved.id);
});
test("ambiguous submission stays claimed across restart until reconciled", async t => {
  const create = setup(t), store = create(), saved = store.ask(input);
  store.answer(saved.id, { revision: 1, key: "a", answers: { length: "长篇" } });
  await assert.rejects(new ClarificationWorkflow(store).resume(saved.id, { revision: 2, submit: async () => { throw Error("lost receipt"); } }));
  store.close(); const restored = create(), snapshot = restored.get(saved.id);
  assert.equal(snapshot.state, "resuming");
  assert.throws(() => restored.claim(saved.id, { revision: snapshot.revision }), /state_conflict/);
  assert.equal(restored.reconcile(saved.id, { token: snapshot.resumeToken, runId: "existing-run" }).state, "resumed");
});
test("scope, answer shape, expiry and cancel are enforced", t => {
  const create = setup(t), store = create(), other = create("other"), saved = store.ask(input);
  assert.throws(() => other.get(saved.id), /not_found/);
  for (const answers of [{ length: "invalid" }, { extra: "短篇" }, {}]) assert.throws(() => store.answer(saved.id, { revision: 1, key: "a", answers }));
  store.cancel(saved.id, { revision: 1 });
  assert.throws(() => store.answer(saved.id, { revision: 2, key: "a", answers: { length: "短篇" } }), /state_conflict/);
  const expired = other.ask({ ...input, expiresAtMs: 1 });
  assert.throws(() => other.answer(expired.id, { revision: 1, key: "a", answers: { length: "短篇" } }), /expired/);
});
