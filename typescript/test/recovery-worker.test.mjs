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

test('wake during scan is retained; stop drains current Run and prevents the next', async () => {
  const stop = new AbortController();
  let enter, release;
  const entered = new Promise(resolve => enter = resolve), gate = new Promise(resolve => release = resolve);
  const calls = [];
  let scans = 0;
  const worker = new RecoveryWorker({ discover: async () => ['one', 'two'],
    inspect: async () => buildRecoveryInspection({}),
    resume: async id => {
      calls.push(id);
      if (calls.length === 1) worker.wake();
      if (calls.length === 3) { enter(); await gate; }
    },
  });
  let finished = false;
  const task = worker.run({ signal: stop.signal, pollIntervalMs: 60000, maxBackoffMs: 60000,
    onScan: async () => { scans++; } }).then(() => { finished = true; });
  try {
    await entered;
    await assert.rejects(worker.runOnce(), /recovery_worker_scan_active/);
    stop.abort();
    await Promise.resolve();
    assert.equal(finished, false);
    release(); await task;
    assert.deepEqual(calls, ['one', 'two', 'one']);
    assert.equal(scans, 2);
  } finally { stop.abort(); release(); await task; }
});

test('stop during inspection prevents resume', async () => {
  const stop = new AbortController(), reports = [];
  let calls = 0;
  const worker = new RecoveryWorker({ discover: async () => ['one'],
    inspect: async () => { stop.abort(); return buildRecoveryInspection({}); },
    resume: async () => { calls++; },
  });
  await worker.run({ signal: stop.signal, onScan: async report => { reports.push(report); } });
  assert.deepEqual(reports, [[]]); assert.equal(calls, 0);
});

test('idle stop clears timer and observer failure releases lifecycle', async () => {
  const stop = new AbortController();
  let scanned;
  const observed = new Promise(resolve => scanned = resolve);
  const worker = new RecoveryWorker({ discover: async () => [], inspect: async () => buildRecoveryInspection({}), resume: async () => {} });
  const task = worker.run({ signal: stop.signal, pollIntervalMs: 60000, maxBackoffMs: 60000,
    onScan: async () => { scanned(); } });
  await observed;
  await new Promise(resolve => setImmediate(resolve));
  stop.abort(); await task;
  assert.deepEqual(await worker.runOnce(), []);
  await assert.rejects(worker.run({ signal: new AbortController().signal, onScan: async () => { throw new Error('observer'); } }), /observer/);
  assert.deepEqual(await worker.runOnce(), []);
});

test('failed scans back off to cap and success resets', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const stop = new AbortController();
  let scans = 0;
  const worker = new RecoveryWorker({ discover: async () => ['one'], inspect: async () => buildRecoveryInspection({}),
    resume: async () => { if (scans < 3) throw new Error('temporary'); } });
  const task = worker.run({ signal: stop.signal, pollIntervalMs: 10, maxBackoffMs: 40,
    onScan: async () => { scans++; } });
  const flush = () => new Promise(resolve => setImmediate(resolve));
  try {
    await flush(); assert.equal(scans, 1);
    for (const [delay, expected] of [[20, 2], [40, 3], [40, 4], [10, 5]]) {
      t.mock.timers.tick(delay - 1); await flush(); assert.equal(scans, expected - 1);
      t.mock.timers.tick(1); await flush(); assert.equal(scans, expected);
    }
  } finally { stop.abort(); await task; }
});
