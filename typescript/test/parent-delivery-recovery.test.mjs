import assert from 'node:assert/strict';
import test from 'node:test';
import { completedParentDeliveries } from '../dist/run/parent-delivery.js';

for (const state of ['completed', 'started', 'aborted']) {
  test(`delivery recovery checks latest ${state} marker across short pages`, async () => {
    const events = Array.from({length: 9}, (_, i) => ({sequence:i+1, payload:{}}));
    const marker = state => ({schemaVersion:'purra.parent-delivery/v1', deliveryId:'delivery', state, childRunIds:['child']});
    events[0].payload = marker('started');
    events[8].payload = marker(state);
    const cursors = [];
    const repository = {async listEvents(runId, cursor) {
      cursors.push(cursor);
      return events.filter(event => event.sequence > cursor).slice(0,2);
    }};
    if (state === 'completed') assert.deepEqual(await completedParentDeliveries(repository, 'root'), new Set(['child']));
    else await assert.rejects(completedParentDeliveries(repository, 'root'), {code:'parent_delivery_reconciliation_required'});
    assert.deepEqual(cursors,[0,2,4,6,8,9]);
  });
}

test('delivery recovery rejects a journal that repeats its cursor', async () => {
  await assert.rejects(completedParentDeliveries({async listEvents() {
    return [{sequence:1,payload:{}}];
  }}, 'root'), {code:'run_repository_nonconforming'});
});
