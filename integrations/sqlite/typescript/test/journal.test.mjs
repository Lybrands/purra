import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { InMemoryRunRepository, assertRunRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";

async function params() {
  const reference = new InMemoryRunRepository();
  await assertRunRepositoryConforms(reference);
  const { preset, budgets } = await reference.get("conformance-run-1");
  return { preset, budgets, deadlineAt: null, metadata: {} };
}

const draft = (key) => ({ sourceKey: key, kind: "model.diagnostics", channel: "model", visibility: "private", payload: { text: key } });

test("detached execution state requires its complete journal", async () => {
  const runs = new InMemoryRunRepository();
  await runs.begin({ ...await params(), requestedRunId: "root" });
  const checkpoint = runs.exportJournalState();
  assert.throws(() => checkpoint.journals[0].events.pop(), TypeError);
  const restored = new InMemoryRunRepository();
  assert.throws(() => restored.importState(checkpoint.state), /output journal is required/);
  assert.throws(() => restored.importState(checkpoint.state, []), /incomplete output journal/);
  restored.importState(checkpoint.state, checkpoint.journals[0].events);
  assert.deepEqual(await restored.listEvents("root", 0), await runs.listEvents("root", 0));
});

test("indexed pagination preserves child order and scopes without loading state", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-journal-"));
  const path = join(dir, "agent.db");
  let storage = new SqliteAgentAdapters(path, { scope: "a" });
  const other = new SqliteAgentAdapters(path, { scope: "b" });
  const originalImport = InMemoryRunRepository.prototype.importState;
  try {
    const base = await params();
    await storage.runs.begin({ ...base, requestedRunId: "root" });
    await storage.runs.begin({ ...base, requestedRunId: "child", rootRunId: "root", parentRunId: "root" });
    await other.runs.begin({ ...base, requestedRunId: "root" });
    const a = await storage.runs.appendEvent("root", draft("a"));
    const b = await storage.runs.appendEvent("child", draft("b"));
    const c = await storage.runs.appendEvent("root", draft("c"));
    storage.close(); storage = new SqliteAgentAdapters(path, { scope: "a" });
    InMemoryRunRepository.prototype.importState = () => { throw new Error("event query must not load execution state"); };
    assert.deepEqual(await storage.runs.listEvents("root", a.sequence, 1), [c]);
    assert.deepEqual(await storage.runs.listRootEvents("root", a.rootSequence, 2), [b, c]);
    assert.deepEqual(await storage.runs.listEvents("child", 1), [b]);
    assert.deepEqual(await other.runs.listEvents("root", 1), []);
    assert.deepEqual(await storage.runs.listEvents("root", 99), []);
    await storage.publisher.publishCommitted(c);
    await assert.rejects(storage.runs.listRootEvents("child", 0), { code: "run_scope_conflict" });
    await assert.rejects(storage.runs.listEvents("missing", 0), { code: "run_not_found" });
    await assert.rejects(storage.runs.listEvents("root", -1), /non-negative/);
    await assert.rejects(storage.runs.listEvents("root", 0, 0), /positive/);
    const db = new DatabaseSync(path);
    try {
      const saved = JSON.parse(db.prepare("SELECT body FROM purra_state WHERE scope='a'").get().body);
      assert.ok(!saved.stores.runs.includes(c.eventId));
      const plan = db.prepare("EXPLAIN QUERY PLAN SELECT body FROM purra_output_events WHERE scope=? AND sdk='typescript' AND root_run_id=? AND root_sequence>? ORDER BY root_sequence LIMIT ?").all("a", "root", 2, 2);
      assert.ok(plan.some((row) => row.detail.includes("SEARCH")));
      assert.ok(!plan.some((row) => row.detail.includes("TEMP B-TREE")));
    } finally { db.close(); }
  } finally {
    InMemoryRunRepository.prototype.importState = originalImport;
    storage.close(); other.close(); rmSync(dir, { recursive: true });
  }
});

test("journal insertion failure rolls back state, replay never rewrites history", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-journal-atomic-"));
  const path = join(dir, "agent.db");
  let storage = new SqliteAgentAdapters(path, { scope: "atomic" });
  const db = new DatabaseSync(path);
  try {
    await storage.runs.begin({ ...await params(), requestedRunId: "root" });
    const first = await storage.runs.appendEvent("root", draft("first"));
    const originalState = db.prepare("SELECT body FROM purra_state").get().body;
    db.exec(`CREATE TRIGGER reject_fourth BEFORE INSERT ON purra_output_events WHEN NEW.sequence=4 BEGIN SELECT RAISE(ABORT, 'injected journal failure'); END;
      CREATE TRIGGER no_update BEFORE UPDATE ON purra_output_events BEGIN SELECT RAISE(ABORT, 'journal rewrite'); END;
      CREATE TRIGGER no_delete BEFORE DELETE ON purra_output_events BEGIN SELECT RAISE(ABORT, 'journal rewrite'); END;`);
    await assert.rejects(storage.runs.appendBatch("root", [draft("second"), draft("third")]), /injected journal failure/);
    assert.equal(db.prepare("SELECT body FROM purra_state").get().body, originalState);
    assert.deepEqual(await storage.runs.listEvents("root", 1), [first]);
    assert.deepEqual(await storage.runs.appendEvent("root", draft("first")), first);
    storage.close(); storage = new SqliteAgentAdapters(path, { scope: "atomic" });
    const second = await storage.runs.appendEvent("root", draft("second"));
    assert.equal(second.sequence, 3);
    assert.equal(db.prepare("SELECT count(*) AS count FROM purra_output_events").get().count, 3);
    db.exec("DROP TRIGGER no_delete; DELETE FROM purra_output_events WHERE sequence=3");
    await assert.rejects(storage.runs.get("root"), /incomplete output journal/);
  } finally { db.close(); storage.close(); rmSync(dir, { recursive: true }); }
});
