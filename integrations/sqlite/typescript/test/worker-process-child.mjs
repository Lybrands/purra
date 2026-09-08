import { appendFileSync, openSync, fsyncSync, closeSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { RecoveryWorker } from 'purra';
import { createApprovalHost, request } from './approval-host.mjs';

const [path, effect, expiry, mode] = process.argv.slice(2);
const lines = createInterface({ input: process.stdin })[Symbol.asyncIterator]();
const emit = value => process.stdout.write(`${JSON.stringify(value)}\n`);
const barrier = async () => {
  if ((await lines.next()).value !== 'continue') throw new Error('missing release');
};
const service = mode.startsWith('service-');
let activeRunId;
const host = createApprovalHost(path, { expiry: Number(expiry), run: async () => {
  // No fixture deduplication: the public Core gate must prevent a second append.
  const startNs = process.hrtime.bigint().toString();
  if (service) await new Promise(resolve => setTimeout(resolve, 50));
  appendFileSync(effect, service ? `${JSON.stringify({ runId: activeRunId, worker: mode, startNs, endNs: process.hrtime.bigint().toString() })}\n` : 'write\n');
  const fd = openSync(effect, 'r+'); try { fsyncSync(fd); } finally { closeSync(fd); }
  if (mode === 'exit_after_effect') {
    await new Promise(resolve => process.stdout.write(`${JSON.stringify({ stage: 'effect' })}\n`, resolve));
    process.exit(74);
  }
  if (mode === 'owner') { emit({ stage: 'effect' }); await barrier(); }
  return { content: 'written', effectState: 'committed' };
} });
const cursor = host.storage.recoveryCursor(service ? mode : (mode === 'competitor' ? 'competitor' : 'worker'), { pageSize: 5 });
const errors = [];
try {
  const worker = new RecoveryWorker({
    ...(service ? { maxRunsPerScan: 3, schedule: host.storage.recoverySchedule({ intervalMs: 50, maxBackoffMs: 200 }) } : {}),
    discover: async () => { const ids = await cursor.discover(); if (!service) emit({ stage: 'discovered', ids }); return ids; },
    inspect: async id => {
      const report = await host.storage.inspectRecovery(id);
      if (mode === 'force_resume') return { blockers: [] };
      if (mode === 'competitor') { emit({ stage: 'inspected', blockers: report.blockers }); await barrier(); }
      return report;
    },
    resume: async id => {
      activeRunId = id;
      try { await (await host.agent.resume(id, request)).result; }
      catch (error) { errors.push(error.code ?? error.name); throw error; }
    },
    acknowledge: async ids => {
      if (mode === 'exit_before_ack') {
        // Flush the marker before abrupt exit, without closing the storage or acknowledging.
        await new Promise(resolve => process.stdout.write(`${JSON.stringify({ stage: 'before_ack', ids, tools: host.counts.tool })}\n`, resolve));
        process.exit(73);
      }
      await cursor.acknowledge(ids);
    },
  });
  if (service) {
    emit({ stage: 'ready' }); await barrier();
    const stop = new AbortController();
    const monitor = lines.next().then(line => {
      if (line.value !== 'stop') throw new Error('missing stop');
      stop.abort();
    });
    const counts = { scans: 0, blocked: 0, settled: 0, failed: 0 };
    await worker.run({ signal: stop.signal, pollIntervalMs: 20, maxBackoffMs: 200,
      onScan: async results => { counts.scans++; for (const row of results) counts[row.action]++; },
    });
    await monitor;
    emit({ stage: 'result', ...counts, errors, ...host.counts, diagnostics: worker.diagnostics() });
  } else {
  const results = await worker.runOnce();
  emit({ stage: 'result', actions: results.map(r => r.action), errors, reasons: results.map(r => r.reasons), ...host.counts });
  }
} finally {
  host.storage.close();
  process.stdin.destroy();
}
