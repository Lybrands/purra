import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import test from "node:test";
import { InMemoryRunRepository, assertRunRepositoryConforms, assertArtifactRepositoryConforms, assertLongTaskRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";
import { OutputJournal } from "../dist/journal.js";

async function begin(storage) {
  const reference = new InMemoryRunRepository();
  await assertRunRepositoryConforms(reference);
  const { preset, budgets } = await reference.get("conformance-run-1");
  await storage.runs.begin({ preset, budgets, requestedRunId: "live", deadlineAt: null, metadata: {} });
}

test("metadata writes and lease heartbeat preserve unloaded Run state and journal", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-metadata-"));
  const storage = new SqliteAgentAdapters(join(dir, "agent.db"), { scope: "metadata" });
  const restore = OutputJournal.prototype.restore;
  const append = OutputJournal.prototype.append;
  const importState = InMemoryRunRepository.prototype.importState;
  try {
    await begin(storage);
    const events = await storage.runs.listEvents("live", 0);
    await storage.runs.executeOwned("live", async () => {
      const forbidden = () => { throw new Error("metadata operation loaded or flushed output history"); };
      OutputJournal.prototype.restore = forbidden;
      OutputJournal.prototype.append = forbidden;
      InMemoryRunRepository.prototype.importState = forbidden;
      let effects = 0;
      const effect = async () => { effects++; return { content: "receipt" }; };
      await storage.idempotency.executeOnce("receipt", effect);
      assert.equal((await storage.idempotency.executeOnce("receipt", effect)).content, "receipt");
      assert.equal(effects, 1);
      await assert.rejects(storage.idempotency.executeOnce("unknown", async () => { throw new Error("effect failed"); }), /effect failed/);
      await assert.rejects(storage.idempotency.executeOnce("unknown", effect), /tool_effect_unknown/);
      await storage.reconcileTool("unknown", { result: { content: "reconciled" } });
      assert.equal((await storage.idempotency.executeOnce("unknown", effect)).content, "reconciled");
      await assertArtifactRepositoryConforms(storage.artifacts);
      await assertLongTaskRepositoryConforms(storage.longTasks);
      await sleep(1100);
    });
    OutputJournal.prototype.restore = restore;
    OutputJournal.prototype.append = append;
    InMemoryRunRepository.prototype.importState = importState;
    assert.deepEqual(await storage.runs.listEvents("live", 0), events);
    assert.equal((await storage.runs.get("live")).status, "running");
    await storage.runs.executeOwned("live", async () => {});
  } finally {
    OutputJournal.prototype.restore = restore;
    OutputJournal.prototype.append = append;
    InMemoryRunRepository.prototype.importState = importState;
    storage.close(); rmSync(dir, { recursive: true });
  }
});

test("concurrent claims and output writes survive later receipt commit", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-metadata-concurrent-"));
  const path = join(dir, "agent.db");
  const first = new SqliteAgentAdapters(path, { scope: "shared" });
  const second = new SqliteAgentAdapters(path, { scope: "shared" });
  let markStarted, finish;
  const started = new Promise((resolve) => { markStarted = resolve; });
  const ready = new Promise((resolve) => { finish = resolve; });
  let task, effects = 0;
  try {
    await begin(first);
    const effect = async () => { effects++; markStarted(); await ready; return { content: "committed" }; };
    task = first.idempotency.executeOnce("once", effect);
    await started;
    await assert.rejects(second.idempotency.executeOnce("once", effect), /tool_effect_unknown/);
    const event = await second.runs.appendEvent("live", { sourceKey: "concurrent:event", kind: "model.diagnostics", channel: "model", visibility: "private", payload: { text: "preserve" } });
    finish();
    await task;
    assert.equal((await second.idempotency.executeOnce("once", effect)).content, "committed");
    assert.equal(effects, 1);
    assert.deepEqual(await first.runs.listEvents("live", 1), [event]);
    assert.equal((await first.runs.get("live")).runId, "live");
  } finally {
    finish(); if (task) await task;
    first.close(); second.close(); rmSync(dir, { recursive: true });
  }
});
