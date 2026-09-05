import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { InMemoryRunRepository, assertRunRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";

test("existing v3 adds covering indexes and bounds the actual child join without rewriting data", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "purra-cover-"));
  const path = join(dir, "agent.db");
  let storage = new SqliteAgentAdapters(path, { scope: "cover" });
  const db = new DatabaseSync(path);
  try {
    const reference = new InMemoryRunRepository();
    await assertRunRepositoryConforms(reference);
    const { preset, budgets } = await reference.get("conformance-run-1");
    const base = { preset, budgets, deadlineAt: null, metadata: {} };
    await storage.runs.begin({ ...base, requestedRunId: "root" });
    await storage.runs.begin({ ...base, requestedRunId: "child", rootRunId: "root", parentRunId: "root" });
    await storage.runs.begin({ ...base, requestedRunId: "other" });
    storage.close();
    db.exec("DROP INDEX purra_output_sequence_cover; DROP INDEX purra_journal_roots");
    const snapshot = db.prepare("SELECT * FROM purra_state").all();
    const events = db.prepare("SELECT * FROM purra_output_events ORDER BY run_id,sequence").all();
    storage = new SqliteAgentAdapters(path, { scope: "cover" });
    assert.deepEqual(db.prepare("SELECT * FROM purra_state").all(), snapshot);
    assert.deepEqual(db.prepare("SELECT * FROM purra_output_events ORDER BY run_id,sequence").all(), events);
    const queries = [];
    const original = DatabaseSync.prototype.prepare;
    const capture = t.mock.method(DatabaseSync.prototype, "prepare", function(query) {
      queries.push(query);
      return original.call(this, query);
    });
    const added = await storage.runs.appendEvent("child", { sourceKey: "next", kind: "model.diagnostics", channel: "model", visibility: "private", payload: {} });
    capture.mock.restore();
    const query = queries.find((query) => query.includes("COUNT(e.sequence)"));
    assert.ok(query);
    const plan = db.prepare("EXPLAIN QUERY PLAN " + query).all("cover", "root").map((row) => row.detail);
    assert.ok(plan.some((row) => row.includes("USING COVERING INDEX purra_output_sequence_cover") && row.includes("root_run_id=? AND run_id=?")));
    assert.ok(plan.some((row) => row.includes("USING COVERING INDEX purra_journal_roots") && row.includes("root_run_id=?")));
    assert.ok(!plan.some((row) => row.includes("TEMP B-TREE")));
    assert.equal(added.sequence, 2);
    assert.equal(added.rootSequence, 3);
    assert.equal((await storage.runs.listEvents("other", 0)).length, 1);
  } finally { db.close(); storage.close(); rmSync(dir, { recursive: true }); }
});
