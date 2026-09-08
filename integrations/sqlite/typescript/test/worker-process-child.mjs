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
const host = createApprovalHost(path, { expiry: Number(expiry), run: async () => {
  // No fixture deduplication: the public Core gate must prevent a second append.
  appendFileSync(effect, 'write\n');
  const fd = openSync(effect, 'r+'); try { fsyncSync(fd); } finally { closeSync(fd); }
  if (mode === 'owner') { emit({ stage: 'effect' }); await barrier(); }
  return { content: 'written', effectState: 'committed' };
} });
const cursor = host.storage.recoveryCursor(mode === 'competitor' ? 'competitor' : 'worker');
const errors = [];
try {
  const results = await new RecoveryWorker({
    discover: async () => { const ids = await cursor.discover(); emit({ stage: 'discovered', ids }); return ids; },
    inspect: async id => {
      const report = await host.storage.inspectRecovery(id);
      if (mode === 'competitor') { emit({ stage: 'inspected', blockers: report.blockers }); await barrier(); }
      return report;
    },
    resume: async id => {
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
  }).runOnce();
  emit({ stage: 'result', actions: results.map(r => r.action), errors, ...host.counts });
} finally {
  host.storage.close();
  process.stdin.destroy();
}
