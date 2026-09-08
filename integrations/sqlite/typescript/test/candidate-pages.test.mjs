import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { InMemoryRunRepository, assertRunRepositoryConforms } from 'purra';
import { SqliteAgentAdapters } from '../dist/index.js';

test('index pages avoid state and journal reads under another writer', async t => {
  const dir = mkdtempSync(join(tmpdir(), 'purra-page-')), path = join(dir, 'db');
  let storage = new SqliteAgentAdapters(path, { scope: 'pages' });
  const other = new SqliteAgentAdapters(path, { scope: 'other' }), writer = new DatabaseSync(path);
  t.after(() => { writer.close(); storage.close(); other.close(); rmSync(dir, { recursive: true }); });
  const reference = new InMemoryRunRepository(); await assertRunRepositoryConforms(reference);
  const { preset, budgets } = await reference.get('conformance-run-1');
  for (const requestedRunId of ['a', 'b', 'c']) await storage.runs.begin({ preset, budgets, requestedRunId, deadlineAt: null, metadata: {} });
  await other.runs.begin({ preset, budgets, requestedRunId: 'other', deadlineAt: null, metadata: {} });
  storage.close(); storage = new SqliteAgentAdapters(path, { scope: 'pages' });
  const original = DatabaseSync.prototype.prepare;
  const queries = [];
  t.mock.method(DatabaseSync.prototype, 'prepare', function(sql) {
    queries.push(sql);
    assert.doesNotMatch(sql, /purra_output_events/i);
    if (!sql.includes("sdk='purra.approvals'")) assert.doesNotMatch(sql, /\bbody\b/i);
    return original.call(this, sql);
  });
  writer.exec('BEGIN IMMEDIATE');
  writer.exec("DELETE FROM purra_journal_runs WHERE scope='pages' AND run_id='a'");
  try {
    assert.deepEqual(await storage.listRunCandidates({ limit: 2 }), { authority: 'candidate_only', runIds: ['a', 'b'], nextAfterRunId: 'b' });
    assert.deepEqual(await storage.listRunCandidates({ afterRunId: 'b', limit: 2 }), { authority: 'candidate_only', runIds: ['c'], nextAfterRunId: null });
    assert.deepEqual((await storage.listRunCandidates({ afterRunId: 'c' })).runIds, []);
  } finally { writer.exec('ROLLBACK'); }
  const plan = writer.prepare("EXPLAIN QUERY PLAN SELECT run_id FROM purra_journal_runs WHERE scope=? AND sdk='typescript' AND run_id>? ORDER BY run_id LIMIT ?").all('pages', 'a', 3);
  assert.ok(plan.some(row => row.detail.includes('COVERING INDEX')));
  assert.ok(plan.every(row => !row.detail.includes('TEMP B-TREE')));
  assert.ok(queries.some(sql => sql.includes('LIMIT ?')));
  for (const limit of [true, 0, -1, 1.5, 1001]) await assert.rejects(storage.listRunCandidates({ limit }));
});
