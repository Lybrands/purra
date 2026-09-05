import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { InMemoryRunRepository, assertRunRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";
import { OutputJournal } from "../dist/journal.js";

async function seed(runs) {
  const reference = new InMemoryRunRepository();
  await assertRunRepositoryConforms(reference);
  const { preset, budgets } = await reference.get("conformance-run-1");
  const base = { preset, budgets: { ...budgets, maxModelAttempts: 1 }, deadlineAt: null, metadata: {} };
  await runs.begin({ ...base, requestedRunId: "root" });
  for (const id of ["child-1", "child-2"]) await runs.begin({ ...base, requestedRunId: id, rootRunId: "root", parentRunId: "root" });
  await runs.begin({ ...base, requestedRunId: "other" });
}
const draft = (key) => ({ sourceKey: key, kind: "model.diagnostics", channel: "model", visibility: "private", payload: { text: key } });
const invocation = (id) => ({ schemaVersion: 2, runId: id, invocationId: `invocation:${id}`,
  messageFingerprint: "messages", toolFingerprint: "tools", requestFingerprint: "request",
  evidenceFingerprint: "evidence", contextEvidence: [], capabilityProfileId: null, outputBudget: null });

test("partial journal import blocks unloaded Roots and preserves their checkpoints", async () => {
  const all = new InMemoryRunRepository();
  await seed(all);
  const before = await all.get("other");
  const checkpoint = all.exportJournalState();
  const rootEvents = await all.listRootEvents("root", 0);
  const otherEvents = await all.listRootEvents("other", 0);
  const partial = new InMemoryRunRepository();
  partial.importState(checkpoint.state, rootEvents, { rootRunId: "root" });
  await assert.rejects(partial.get("other"), { code: "run_scope_not_loaded" });
  await assert.rejects(partial.appendEvent("other", draft("forbidden")), { code: "run_scope_not_loaded" });
  assert.throws(() => partial.exportState(), /unloaded output journals/);
  await partial.appendEvent("child-1", draft("new-child-event"));
  const next = partial.exportJournalState();
  assert.ok(next.journals.every((entry) => entry.rootRunId === "root"));
  const events = [...otherEvents, ...await partial.listRootEvents("root", 0)].sort((a, b) => a.rootRunId.localeCompare(b.rootRunId) || a.rootSequence - b.rootSequence);
  const restored = new InMemoryRunRepository();
  restored.importState(next.state, events);
  assert.deepEqual(await restored.get("other"), before);
  assert.deepEqual(await restored.listRootEvents("other", 0), otherEvents);
  assert.equal((await restored.listRootEvents("root", 0)).length, rootEvents.length + 1);
});

test("Run writes defer history while retaining sibling budgets and Root isolation", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-root-scope-"));
  const path = join(dir, "agent.db");
  const storage = new SqliteAgentAdapters(path, { scope: "roots" });
  const original = OutputJournal.prototype.restore;
  try {
    await seed(storage.runs);
    const otherEvents = await storage.runs.listEvents("other", 0);
    const seen = new Set();
    OutputJournal.prototype.restore = function(root) {
      assert.equal(root, "root", "Run write hydrated unrelated Root history");
      const events = original.call(this, root);
      for (const event of events) seen.add(event.runId);
      return events;
    };
    const added = await storage.runs.appendEvent("child-1", draft("added"));
    assert.deepEqual(await storage.runs.appendEvent("child-1", draft("added")), added);
    await storage.runs.openInvocation("child-1", invocation("child-1"));
    await assert.rejects(storage.runs.openInvocation("child-2", invocation("child-2")), { code: "runtime_budget_exceeded" });
    assert.deepEqual(seen, new Set(), "ordinary writes should not restore historical bodies");
    OutputJournal.prototype.restore = original;
    assert.deepEqual(await storage.runs.listEvents("other", 0), otherEvents);
    await storage.transaction(async ({ runs }) => {
      assert.deepEqual(await runs.listEvents("other", 0), otherEvents);
    });
    const db = new DatabaseSync(path);
    try { db.prepare("DELETE FROM purra_output_events WHERE run_id=?").run("other"); }
    finally { db.close(); }
    await storage.runs.appendEvent("root", draft("after-unrelated-corruption"));
    await assert.rejects(storage.runs.get("other"), /incomplete output journal/);
    await assert.rejects(storage.transaction(async () => {}), /incomplete output journal/);
  } finally {
    OutputJournal.prototype.restore = original;
    storage.close(); rmSync(dir, { recursive: true });
  }
});
