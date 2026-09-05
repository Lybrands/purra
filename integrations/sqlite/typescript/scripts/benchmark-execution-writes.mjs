// Build Core and SQLite first, then run this script with Node.
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { InMemoryRunRepository, assertRunRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";
import { OutputJournal } from "../dist/journal.js";

const profile = process.argv.includes("--profile");
const timings = { journal_ms: 0, run_state_codec_ms: 0 };
function instrument(owner, name, phase) {
  const original = owner[name];
  owner[name] = function(...args) {
    const start = performance.now();
    try { return original.apply(this, args); }
    finally { timings[phase] += performance.now() - start; }
  };
}
if (profile) {
  instrument(OutputJournal.prototype, "deferred", "journal_ms");
  instrument(InMemoryRunRepository.prototype, "importState", "run_state_codec_ms");
  instrument(InMemoryRunRepository.prototype, "exportJournalState", "run_state_codec_ms");
}
function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return Number(((sorted[4] + sorted[5]) / 2).toFixed(3));
}

const reference = new InMemoryRunRepository();
await assertRunRepositoryConforms(reference);
const { preset, budgets, executionCheckpoint } = await reference.get("conformance-run-1");
for (const count of [100, 1000, 5000]) {
  const dir = mkdtempSync(join(tmpdir(), "purra-execution-bench-"));
  const storage = new SqliteAgentAdapters(join(dir, "agent.db"), { scope: "benchmark" });
  const runId = "benchmark";
  try {
    await storage.transaction(async ({ runs }) => {
      await runs.begin({ preset, budgets: { ...budgets, maxModelAttempts: 100 }, requestedRunId: runId, deadlineAt: null, metadata: {} });
      for (let index = 0; index < count; index++) {
        await runs.appendEvent(runId, { sourceKey: `event:${index}`, kind: "model.diagnostics",
          channel: "model", visibility: "private", payload: { text: "x".repeat(128) } });
      }
    });
    const samples = { append: [], invocation: [], checkpoint: [] };
    const phases = { append: [], invocation: [], checkpoint: [] };
    for (let attempt = 0; attempt < 12; attempt++) {
      const operations = {
        append: () => storage.runs.appendEvent(runId, { sourceKey: `new:${attempt}`, kind: "model.diagnostics",
          channel: "model", visibility: "private", payload: { text: "x".repeat(128) } }),
        invocation: () => storage.runs.openInvocation(runId, { schemaVersion: 2, runId, invocationId: `invoke:${attempt}`,
          messageFingerprint: "messages", toolFingerprint: "tools", requestFingerprint: "request",
          evidenceFingerprint: "evidence", contextEvidence: [], capabilityProfileId: null, outputBudget: null }),
        checkpoint: () => storage.runs.saveExecutionCheckpoint(runId, { ...executionCheckpoint, runId, nextRound: attempt + 1 }),
      };
      for (const [name, operation] of Object.entries(operations)) {
        const before = { ...timings };
        const start = performance.now();
        await operation();
        const elapsed = performance.now() - start;
        samples[name].push(elapsed);
        const costs = Object.fromEntries(Object.keys(timings).map((phase) => [phase, timings[phase] - before[phase]]));
        phases[name].push({ ...costs, remainder_ms: elapsed - Object.values(costs).reduce((sum, value) => sum + value, 0) });
      }
    }
    assert.equal((await storage.runs.listEvents(runId, count + 1)).length, 36);
    assert.equal((await storage.runs.get(runId)).executionCheckpoint.nextRound, 12);
    const medians = Object.fromEntries(Object.entries(samples).map(([name, values]) => [`median_${name}_ms`, median(values.slice(2))]));
    const costs = Object.fromEntries(Object.entries(phases).map(([name, values]) => [name,
      Object.fromEntries([...Object.keys(timings), "remainder_ms"].map((phase) => [phase, median(values.slice(2).map((row) => row[phase]))]))]));
    console.log(JSON.stringify({ history_events: count, measured_writes_per_operation: 10, ...medians, ...(profile ? { profile: costs } : {}) }));
  } finally { storage.close(); rmSync(dir, { recursive: true }); }
}
