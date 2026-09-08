import assert from 'node:assert/strict';
import test from 'node:test';
import { RecoveryWorker, buildRecoveryInspection } from '../dist/index.js';

test('scan isolates failure, skips unknown effects and revalidates through the host', async () => {
  const calls = [];
  const worker = new RecoveryWorker({
    discover: async () => ['corrupt', 'claimed', 'raced', 'good', 'good'],
    inspect: async id => {
      if (id === 'corrupt') throw new Error('secret');
      return buildRecoveryInspection({ unknownToolReceipts: id === 'claimed' ? 1 : 0, receiptScope: 'run' });
    },
    resume: async id => { calls.push(id); if (id === 'raced') throw new Error('private lease conflict'); },
  });
  const report = await worker.runOnce();
  assert.deepEqual(calls, ['raced', 'good']);
  assert.deepEqual(report.map(r => r.action), ['failed', 'blocked', 'failed', 'settled']);
  assert.deepEqual(report[0].reasons, ['inspection_failed']);
  assert.deepEqual(report[2].reasons, ['resume_failed']);
  assert.doesNotMatch(JSON.stringify(report), /secret|private/);
});

test('overlap is rejected and discovery failure releases the guard', async () => {
  let release;
  const gate = new Promise(resolve => release = resolve);
  const worker = new RecoveryWorker({ discover: async () => { await gate; throw new Error('discovery'); },
    inspect: async () => buildRecoveryInspection({}), resume: async () => {} });
  const first = worker.runOnce();
  await assert.rejects(worker.runOnce(), /recovery_worker_scan_active/);
  release();
  await assert.rejects(first, /discovery/);
  await assert.rejects(worker.runOnce(), /discovery/);
});
