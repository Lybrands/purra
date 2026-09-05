// Run after building Core and SQLite: node scripts/benchmark-run-writes.mjs
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { InMemoryRunRepository, assertRunRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";

const reference = new InMemoryRunRepository();
await assertRunRepositoryConforms(reference);
const { preset, budgets } = await reference.get("conformance-run-1");
for (const count of [100, 1000, 5000]) {
  const dir = mkdtempSync(join(tmpdir(), "purra-write-bench-"));
  const storage = new SqliteAgentAdapters(join(dir, "agent.db"), { scope: "benchmark" });
  try {
    await storage.transaction(async ({ runs }) => {
      await runs.begin({ preset, budgets, requestedRunId: "benchmark", deadlineAt: null, metadata: {} });
      for (let index = 0; index < count; index++) {
        await runs.appendEvent("benchmark", { sourceKey: `event:${index}`, kind: "model.diagnostics",
          channel: "model", visibility: "private", payload: { text: "x".repeat(128) } });
      }
    });
    await storage.runs.begin({ preset, budgets, requestedRunId: "active", deadlineAt: null, metadata: {} });
    const elapsed = [];
    for (let attempt = 0; attempt < 12; attempt++) {
      const start = performance.now();
      const result = await storage.runs.appendEvent("active", { sourceKey: `active:${attempt}`, kind: "model.diagnostics", channel: "model", visibility: "private", payload: { text: "active" } });
      elapsed.push(performance.now() - start);
      if (result.sequence !== attempt + 2) throw new Error("missing output");
    }
    const sorted = elapsed.slice(2).sort((a, b) => a - b);
    console.log(JSON.stringify({ history_events: count, median_run_write_ms: Number(((sorted[4] + sorted[5]) / 2).toFixed(3)), measured_writes: 10 }));
  } finally { storage.close(); rmSync(dir, { recursive: true }); }
}
