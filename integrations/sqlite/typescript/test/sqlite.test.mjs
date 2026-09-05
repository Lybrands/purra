import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { Agent, AgentCapabilityGrant, UserInputRequired, InMemoryRunRepository, assertRunRepositoryConforms, assertArtifactRepositoryConforms, assertLongTaskRepositoryConforms } from "purra";
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

for (const version of [1, 2, 3]) test(`storage v4 rejects v${version} before any database write`, () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-sqlite-v1-"));
  const path = join(dir, "agent.db");
  const db = new DatabaseSync(path);
  try {
    db.exec("CREATE TABLE purra_state (scope TEXT NOT NULL, sdk TEXT NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(scope,sdk))");
    db.prepare("INSERT INTO purra_state VALUES(?, 'typescript', ?, ?)").run("legacy", version, JSON.stringify({ extra: { tools: {} } }));
  } finally {
    db.close();
  }
  const before = readFileSync(path);
  try {
    assert.throws(() => new SqliteAgentAdapters(path, { scope: "new" }), /unsupported SQLite storage version/);
    assert.deepEqual(readFileSync(path), before);
    assert.equal(existsSync(path + "-wal"), false);
  } finally {
    rmSync(dir, { recursive: true });
  }
});

for (const group of ["runs", "runTree", "artifacts", "longTasks"]) test(`invalid ${group} state aborts transaction without changing rows`, async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-invalid-state-"));
  const path = join(dir,"agent.db");
  const storage = new SqliteAgentAdapters(path,{scope:"invalid"});
  const db = new DatabaseSync(path);
  try {
    await storage.transaction(async (_stores,extra) => { extra.committed=true; });
    const state=JSON.parse(db.prepare("SELECT body FROM purra_state").get().body);
    const inner=JSON.parse(state.stores[group]);
    inner.value[1].push(["unexpectedPrivateField",["map",[]]]);
    state.stores[group]=JSON.stringify(inner);
    const body=JSON.stringify(state);
    db.prepare("UPDATE purra_state SET body=?").run(body);
    await assert.rejects(storage.transaction(async () => assert.fail("invalid state reached callback")),/storage fields/);
    assert.equal(db.prepare("SELECT body FROM purra_state").get().body,body);
    assert.equal(db.prepare("SELECT count(*) AS n FROM purra_output_events").get().n,0);
  } finally { db.close(); storage.close(); rmSync(dir,{recursive:true}); }
});


test("explicit tree ports preserve persisted lease fencing after reopen", async () => {
  const dir=mkdtempSync(join(tmpdir(),"purra-tree-ports-"));
  const path=join(dir,"agent.db");
  let storage=new SqliteAgentAdapters(path,{scope:"tree"});
  try {
    await storage.runTree.beginRoot({runId:"root",agentId:"agent",name:"root",title:"Root",instruction:"Own task",objective:"Finish",capabilityGrant:new AgentCapabilityGrant({canSpawnAgents:true}),idempotencyKey:"begin"});
    const spawned=await storage.runTree.spawnAgents({parentRunId:"root",idempotencyKey:"spawn",children:[{name:"child",title:"Child",instruction:"Read",objective:"Read"}]});
    const childId=spawned.items[0].run.runId;
    const claimed=await storage.runTree.claimRun(childId,{ownerId:"worker",leaseDurationMs:30000});
    assert.ok(claimed);
    storage.close(); storage=new SqliteAgentAdapters(path,{scope:"tree"});
    await storage.runTree.requireRunClaim(childId,{leaseOwnerId:claimed.leaseOwnerId,leaseEpoch:claimed.leaseEpoch});
    await assert.rejects(storage.runTree.requireRunClaim(childId,{leaseOwnerId:"other",leaseEpoch:claimed.leaseEpoch}),{code:"agent_run_lease_lost"});
    assert.equal(storage.runTree.importState,undefined);
    assert.equal(storage.runs.exportJournalState,undefined);
  } finally { storage.close();rmSync(dir,{recursive:true}); }
});
