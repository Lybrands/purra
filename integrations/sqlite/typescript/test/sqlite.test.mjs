import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { Agent, UserInputRequired, InMemoryRunRepository, assertRunRepositoryConforms, assertArtifactRepositoryConforms, assertLongTaskRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";

test("reads use committed snapshots without writer locks or state export", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-sqlite-readers-"));
  const path = join(dir, "agent.db");
  const storage = new SqliteAgentAdapters(path, { scope: "reader" });
  const writer = new DatabaseSync(path);
  const originalExport = InMemoryRunRepository.prototype.exportState;
  let writing = false;
  try {
    await assertRunRepositoryConforms(storage.runs);
    const id = "conformance-run-1";
    const expected = await storage.runs.get(id);
    const events = await storage.runs.listEvents(id, 0);
    const rootEvents = await storage.runs.listRootEvents(id, 0);
    InMemoryRunRepository.prototype.exportState = () => { throw new Error("read queries must not export storage"); };
    writer.exec("BEGIN IMMEDIATE"); writing = true;
    writer.exec("DELETE FROM purra_state WHERE scope='reader'");
    assert.deepEqual(await storage.runs.get(id), expected);
    assert.deepEqual(await storage.runs.listEvents(id, 0), events);
    assert.deepEqual(await storage.runs.listRootEvents(id, 0), rootEvents);
    await storage.publisher.publishCommitted(events[0]);
    writer.exec("COMMIT"); writing = false;
    await assert.rejects(storage.runs.get(id));
    assert.equal(writer.prepare("SELECT count(*) AS count FROM purra_state").get().count, 0);
  } finally {
    InMemoryRunRepository.prototype.exportState = originalExport;
    if (writing) writer.exec("ROLLBACK");
    writer.close(); storage.close(); rmSync(dir, { recursive: true });
  }
});

test("canonical storage conformance survives reopen and rejects terminal writes", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-sqlite-"));
  let storage = new SqliteAgentAdapters(join(dir, "agent.db"), { scope: "conformance" });
  try {
    await assertRunRepositoryConforms(storage.runs);
    await assertArtifactRepositoryConforms(storage.artifacts);
    await assertLongTaskRepositoryConforms(storage.longTasks);
    const events = await storage.runs.listEvents("conformance-run-1", 0);
    storage.close(); storage = new SqliteAgentAdapters(join(dir, "agent.db"), { scope: "conformance" });
    assert.deepEqual(await storage.runs.listEvents("conformance-run-1", 0), events);
    assert.equal((await storage.runs.get("conformance-run-1")).status, "completed");
    await assert.rejects(storage.runs.appendEvent("conformance-run-1", { sourceKey: "late", kind: "commentary", channel: "commentary", visibility: "public", payload: { text: "late" } }));
  } finally { storage.close(); rmSync(dir, { recursive: true }); }
});

for (const version of [1, 2]) test(`storage v3 rejects v${version} snapshots`, async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-sqlite-v1-"));
  const path = join(dir, "agent.db");
  const db = new DatabaseSync(path);
  try {
    db.exec("CREATE TABLE purra_state (scope TEXT NOT NULL, sdk TEXT NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(scope,sdk))");
    db.prepare("INSERT INTO purra_state VALUES(?, 'typescript', ?, ?)").run("legacy", version, JSON.stringify({ extra: { tools: {} } }));
  } finally {
    db.close();
  }
  const storage = new SqliteAgentAdapters(path, { scope: "legacy" });
  try {
    await assert.rejects(storage.runs.get("missing"), /unsupported SQLite storage version/);
    await assert.rejects(storage.runs.listEvents("missing", 0), /unsupported SQLite storage version/);
  } finally {
    storage.close();
    rmSync(dir, { recursive: true });
  }
});
