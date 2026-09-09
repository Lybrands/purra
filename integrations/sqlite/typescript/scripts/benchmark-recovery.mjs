// Synthetic recovery costs; build Core/SQLite first. No Provider or business data.
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { InMemoryRunRepository, assertRunRepositoryConforms } from 'purra';
import { SqliteAgentAdapters } from '../dist/index.js';
const reference = new InMemoryRunRepository(); await assertRunRepositoryConforms(reference);
const { preset, budgets } = await reference.get('conformance-run-1');
for (const count of [20, 100]) {
  const directory = mkdtempSync(join(tmpdir(), 'purra-recovery-bench-'));
  const storage = new SqliteAgentAdapters(join(directory, 'db'), { scope: 'benchmark' });
  try {
    await storage.transaction(async ({ runs }) => {
      for (let i = 0; i < count; i++) {
        const id = `run-${String(i).padStart(4, '0')}`;
        await runs.begin({ preset, budgets, requestedRunId: id, deadlineAt: null, metadata: {} });
        if (i % 5) await runs.cancel(id);
      }
    });
    const candidates = () => storage.listRunCandidates({ limit: 20 });
    const { runIds } = await candidates();
    const individual = async () => { const reports = []; for (const id of runIds) reports.push(await storage.inspectRecovery(id)); return reports; };
    const operations = { candidate_page: candidates, individual_inspection: individual };
    if (storage.inspectRecoveryMany) operations.batch_inspection = () => storage.inspectRecoveryMany(runIds);
    for (const [operation, run] of Object.entries(operations)) {
      const samples = [];
      for (let i = 0; i < 7; i++) { const start = performance.now(); await run(); samples.push(performance.now() - start); }
      const sorted = samples.slice(2).sort((a,b) => a-b);
      console.log(JSON.stringify({ sdk:'typescript', runs: count, terminal_runs: count*4/5, page_size:runIds.length, operation, measured_reads:5, median_ms:Number(sorted[2].toFixed(3)) }));
    }
  } finally { storage.close(); rmSync(directory, { recursive:true }); }
}
