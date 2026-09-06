import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { InMemoryRunRepository, assertRunRepositoryConforms, PlanningStreamParser } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";
import { OutputJournal } from "../dist/journal.js";

async function params() {
  const reference = new InMemoryRunRepository();
  await assertRunRepositoryConforms(reference);
  const { preset, budgets, executionCheckpoint } = await reference.get("conformance-run-1");
  return { begin: { preset, budgets, deadlineAt: null, metadata: {} }, checkpoint: { ...executionCheckpoint, runId: "root" } };
}
const draft = (key) => ({ sourceKey: key, kind: "model.diagnostics", channel: "model", visibility: "private", payload: { text: key } });
const invocation = (id = "invoke") => ({ schemaVersion: 3, runId: "root", invocationId: id,
  messageFingerprint: "messages", toolFingerprint: "tools", requestFingerprint: "request",
  evidenceFingerprint: "evidence", contextEvidence: [], capabilityProfileId: null, outputBudget: null });

test("same-Root writes use indexed replay, preserve checkpoints and reopen without decoding history", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "purra-deferred-"));
  const path = join(dir, "agent.db");
  let storage = new SqliteAgentAdapters(path, { scope: "deferred" });
  try {
    const base = await params();
    await storage.runs.begin({ ...base.begin, requestedRunId: "root" });
    await storage.runs.begin({ ...base.begin, requestedRunId: "other" });
    await storage.runs.appendEvent("other", draft("shared"));
    const first = await storage.runs.appendEvent("root", draft("shared"));
    const forbid = () => { throw new Error("ordinary write decoded historical bodies"); };
    const restores = [t.mock.method(OutputJournal.prototype, "restore", forbid), t.mock.method(OutputJournal.prototype, "restoreRun", forbid)];
    const opened = await storage.runs.openInvocation("root", invocation());
    assert.deepEqual(await storage.runs.openInvocation("root", invocation()), opened);
    const saved = await storage.runs.saveExecutionCheckpoint("root", base.checkpoint);
    assert.deepEqual(await storage.runs.saveExecutionCheckpoint("root", base.checkpoint), saved);
    const settled = await storage.runs.settleInvocation("root", { invocationId: "invoke", status: "completed", usage: { inputTokens: 3, generationTokens: 5 } });
    assert.deepEqual(await storage.runs.settleInvocation("root", { invocationId: "invoke", status: "completed", usage: { inputTokens: 3, generationTokens: 5 } }), settled);
    const batch = await storage.runs.appendBatch("root", [draft("shared"), draft("new:1"), draft("new:2")]);
    assert.deepEqual(batch[0], first);
    assert.deepEqual(batch.slice(1).map((event) => event.sequence), [6, 7]);
    await assert.rejects(storage.runs.appendBatch("root", [draft("rollback"), { ...draft("shared"), payload: { text: "conflict" } }]), { code: "output_source_key_conflict" });
    for (const mocked of restores) mocked.mock.restore();
    storage.close(); storage = new SqliteAgentAdapters(path, { scope: "deferred" });
    const snapshot = await storage.runs.get("root");
    assert.deepEqual(snapshot.executionCheckpoint, saved.snapshot.executionCheckpoint);
    assert.equal(snapshot.usage.modelAttempts, 1);
    assert.equal(snapshot.usage.generationTokens, 5);
    assert.deepEqual((await storage.runs.listEvents("root", 0)).map((event) => event.sequence), [1, 2, 3, 4, 5, 6, 7]);
    assert.equal((await storage.runs.listEvents("other", 0)).length, 2);
    const db = new DatabaseSync(path);
    try {
      const plan = db.prepare("EXPLAIN QUERY PLAN SELECT body FROM purra_output_events WHERE scope=? AND sdk='typescript' AND root_run_id=? AND json_extract(body,'$.sourceKey')=?").all("deferred", "root", "shared");
      assert.ok(plan.some((row) => row.detail.includes("purra_typescript_output_source")));
    } finally { db.close(); }
  } finally { storage.close(); rmSync(dir, { recursive: true }); }
});

test("deferred Core state exports complete journals, includes pending evidence and supports new children", async () => {
  const base = await params();
  const original = new InMemoryRunRepository();
  await original.begin({ ...base.begin, requestedRunId: "root" });
  const old = await original.appendEvent("root", draft("old"));
  const events = await original.listRootEvents("root", 0);
  let reads = 0;
  const journal = { counts: new Map([["root", 2]]),
    readRun: (id) => { reads++; return events.filter((event) => event.runId === id); },
    readRoot: () => { reads++; return events; },
    findSource: (key) => events.find((event) => event.sourceKey === key) };
  const run = new InMemoryRunRepository();
  run.importState(original.exportJournalState().state, undefined, { rootRunId: "root", deferredJournal: journal });
  assert.deepEqual(await run.appendEvent("root", draft("old")), old);
  const child = await run.begin({ ...base.begin, requestedRunId: "child", rootRunId: "root", parentRunId: "root" });
  const added = await run.appendEvent("root", draft("added"));
  assert.equal(child.event.sequence, 1);
  assert.equal(child.event.rootSequence, 3);
  assert.equal(added.sequence, 3);
  assert.equal(added.rootSequence, 4);
  const incremental = run.exportJournalState({ incremental: true });
  assert.equal(reads, 0);
  assert.equal(incremental.journals.find((row) => row.runId === "root").afterSequence, 2);
  assert.deepEqual(incremental.journals.find((row) => row.runId === "root").events, [added]);
  const expected = [...events, child.event, added];
  assert.deepEqual(await run.listRootEvents("root", 0), expected);
  const restored = new InMemoryRunRepository();
  restored.importState(run.exportState());
  assert.deepEqual(await restored.listRootEvents("root", 0), expected);
  assert.deepEqual(await restored.listEvents("child", 0), [child.event]);
  assert.equal(run.exportJournalState().journals.find((row) => row.runId === "root").events.length, 3);
  const broken = new InMemoryRunRepository();
  assert.throws(() => broken.importState(original.exportJournalState().state, undefined,
    { rootRunId: "root", deferredJournal: { ...journal, counts: new Map([["root", 1]]) } }), /incomplete output journal/);
  const forged = new InMemoryRunRepository();
  forged.importState(original.exportJournalState().state, undefined,
    { rootRunId: "root", deferredJournal: { ...journal, findSource: () => ({ ...old, rootRunId: "other" }) } });
  await assert.rejects(forged.appendEvent("root", draft("old")), /invalid output journal identity/);
});

test("unloaded sibling history retains shared output budgets and source-key conflicts", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-deferred-budget-"));
  const storage = new SqliteAgentAdapters(join(dir, "agent.db"), { scope: "deferred" });
  try {
    const base = (await params()).begin;
    await storage.runs.begin({ ...base, budgets: { ...base.budgets, maxOutputEvents: 1 }, requestedRunId: "root" });
    for (const id of ["a", "b"]) await storage.runs.begin({ ...base, requestedRunId: id, rootRunId: "root", parentRunId: "root" });
    const event = { ...draft("shared"), kind: "provider.delta_batch" };
    const first = await storage.runs.appendEvent("a", event);
    assert.deepEqual(await storage.runs.appendEvent("a", event), first);
    await assert.rejects(storage.runs.appendEvent("b", event), { code: "output_source_key_conflict" });
    await assert.rejects(storage.runs.appendEvent("b", { ...event, sourceKey: "new" }), { code: "runtime_budget_exceeded" });
    assert.equal((await storage.runs.get("root")).usage.outputEvents, 1);
    assert.equal((await storage.runs.listEvents("b", 0)).length, 1);
  } finally { storage.close(); rmSync(dir, { recursive: true }); }
});

for (const corruption of ["DELETE FROM purra_output_events WHERE sequence=2", "DELETE FROM purra_output_events WHERE sequence=3",
  "UPDATE purra_output_events SET root_sequence=9 WHERE sequence=2"]) {
  test(`deferred writes reject corrupt sequence columns: ${corruption}`, async () => {
    const dir = mkdtempSync(join(tmpdir(), "purra-deferred-corrupt-"));
    const path = join(dir, "agent.db");
    const storage = new SqliteAgentAdapters(path, { scope: "deferred" });
    const db = new DatabaseSync(path);
    try {
      await storage.runs.begin({ ...((await params()).begin), requestedRunId: "root" });
      await storage.runs.appendBatch("root", [draft("a"), draft("b")]);
      const before = db.prepare("SELECT body FROM purra_state").get().body;
      db.exec(corruption);
      await assert.rejects(storage.runs.appendEvent("root", draft("rejected")), /output journal/);
      assert.equal(db.prepare("SELECT body FROM purra_state").get().body, before);
    } finally { db.close(); storage.close(); rmSync(dir, { recursive: true }); }
  });
}

test("planning and terminal settlement read persisted evidence and reject forged progress", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "purra-deferred-planning-"));
  const storage = new SqliteAgentAdapters(join(dir, "agent.db"), { scope: "deferred" });
  try {
    await storage.runs.begin({ ...((await params()).begin), requestedRunId: "root" });
    await storage.runs.appendEvent("root", { ...draft("operation"), kind: "operation.started", payload: { operationId: "plan", kind: "planning" } });
    const reader = t.mock.method(OutputJournal.prototype, "restoreRun");
    await storage.runs.openInvocation("root", { ...invocation(), outputProtocol: "purra.planning-stream/v1", planningScope: { runId: "root", operationId: "plan", revision: 0 }, planningAttempt: 1 });
    const wire = JSON.stringify({ v: 1, type: "progress", text: "核对资料。" }) + "\n";
    const progress = new PlanningStreamParser().feed(wire)[0];
    await storage.runs.appendEvent("root", { ...draft("raw"), kind: "provider.delta_batch", payload: { invocationId: "invoke", entries: [{ kind: "provider.content_delta", payload: { delta: wire } }] } });
    const projection = { sourceKey: `planning:invoke:${progress.recordIndex}`, kind: "planning.progress", channel: "commentary", visibility: "public",
      payload: { schemaVersion: "purra.planning-stream/v1", source: "provider", invocationId: "invoke", operationId: "plan", revision: 0, attempt: 1, ...progress } };
    const before = await storage.runs.listEvents("root", 0);
    await assert.rejects(storage.runs.appendEvent("root", { ...projection, payload: { ...projection.payload, text: "forged" } }), { code: "planning_projection_invalid" });
    assert.deepEqual(await storage.runs.listEvents("root", 0), before);
    const accepted = await storage.runs.appendEvent("root", projection);
    assert.deepEqual(await storage.runs.appendEvent("root", projection), accepted);
    await storage.runs.appendEvent("root", { ...draft("usage"), kind: "model.usage", payload: { invocationId: "invoke", usage: { inputTokens: 3, generationTokens: 5 } } });
    const canceled = await storage.runs.cancel("root");
    assert.equal(canceled.accepted, true);
    assert.equal((await storage.runs.get("root")).usage.generationTokens, 5);
    const events = await storage.runs.listEvents("root", 0);
    assert.equal(events.filter((event) => event.kind === "operation.finished").length, 1);
    assert.equal(events.filter((event) => event.kind === "invocation.aborted").length, 1);
    assert.ok(reader.mock.callCount() >= 3);
    assert.ok(reader.mock.calls.every((call) => call.arguments[0] === "root"));
  } finally { storage.close(); rmSync(dir, { recursive: true }); }
});

test("deferred evidence and replay reject bodies that disagree with indexed sequence columns", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-deferred-body-"));
  const path = join(dir, "agent.db");
  const storage = new SqliteAgentAdapters(path, { scope: "deferred" });
  const db = new DatabaseSync(path);
  try {
    await storage.runs.begin({ ...((await params()).begin), requestedRunId: "root" });
    await storage.runs.appendEvent("root", draft("old"));
    db.exec("UPDATE purra_output_events SET body=json_set(body,'$.rootSequence',99) WHERE sequence=2");
    const before = db.prepare("SELECT body FROM purra_state").get().body;
    await assert.rejects(storage.runs.appendEvent("root", draft("old")), /invalid output journal/);
    await assert.rejects(storage.runs.cancel("root"), /invalid output journal/);
    assert.equal(db.prepare("SELECT body FROM purra_state").get().body, before);
  } finally { db.close(); storage.close(); rmSync(dir, { recursive: true }); }
});
