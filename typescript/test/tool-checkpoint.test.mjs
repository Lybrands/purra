import assert from 'node:assert/strict';
import test from 'node:test';
import { testGateway } from './support/model-gateway.mjs';
import { InMemoryRunRepository, assertRunRepositoryConforms, copyToolExecutionCheckpoint } from 'purra';
const source = new InMemoryRunRepository();
await assertRunRepositoryConforms(source);
const { preset, budgets } = await source.get('conformance-run-1');
function checkpoint() {
  return { schemaVersion: 3, runId: 'run', phase: 'tool_ready', executionProfile: 'reactive', roundLimit: 5,
    nextRound: 1, initialPlanningOpen: false, messages: [{ role: 'user', content: 'write' }], context: null,
    contextEvidence: [], responseAttempts: 0, recoveryAttempts: [], invocationId: 'actual', appliedGenerationLimit: 100,
    allowedToolNames: ['write'], assistant: { role: 'assistant', content: '', reasoning: 'private', providerData: { signed: 'opaque' },
      toolCalls: [{ id: 'call', name: 'write', arguments: { value: 42 } }] } };
}
async function repository({ settle = true } = {}) {
  const runs = new InMemoryRunRepository();
  await runs.begin({ requestedRunId: 'run', preset, budgets, deadlineAt: null, metadata: {} });
  await runs.openInvocation('run', { schemaVersion: 3, runId: 'run', invocationId: 'actual', messageFingerprint: 'messages',
    toolFingerprint: 'tools', requestFingerprint: 'request', evidenceFingerprint: 'evidence', contextEvidence: [], capabilityProfileId: null, outputBudget: null });
  if (settle) await runs.settleInvocation('run', { invocationId: 'actual', status: 'completed' });
  return runs;
}
test('tool continuation is immutable and round-trips without changing the v2 contract', async () => {
  const input = checkpoint(), copied = copyToolExecutionCheckpoint(input);
  input.assistant.toolCalls[0].arguments.value = 7;
  assert.equal(copied.assistant.toolCalls[0].arguments.value, 42);
  const runs = await repository();
  await runs.saveToolExecutionCheckpoint('run', copied);
  const restored = new InMemoryRunRepository(); restored.importState(runs.exportState());
  assert.deepEqual((await restored.get('run')).toolExecutionCheckpoint, copied);
  assert.equal((await restored.get('run')).executionCheckpoint, undefined);
  const { assistant, invocationId, appliedGenerationLimit, allowedToolNames, ...base } = copied;
  const next = { ...base, schemaVersion: 2, phase: 'model_ready', nextRound: 2,
    messages: [...base.messages, assistant, { role: 'tool', toolCallId: 'call', content: 'written' }] };
  await restored.saveExecutionCheckpoint('run', next);
  const snapshot = await restored.get('run');
  assert.equal(snapshot.toolExecutionCheckpoint, undefined); assert.deepEqual(snapshot.executionCheckpoint, next);
});
for (const [name, mutate] of [
  ['wrong phase', value => value.phase = 'model_ready'],
  ['fractional round', value => value.nextRound = 1.2],
  ['no remaining round', value => value.nextRound = 5],
  ['batch of writes', value => value.assistant.toolCalls.push({ id: 'other', name: 'write', arguments: {} })],
  ['duplicate call identity', value => value.messages.push({ role: 'assistant', content: '', toolCalls: value.assistant.toolCalls })],
  ['missing authority', value => value.allowedToolNames = []],
  ['invalid generation limit', value => value.appliedGenerationLimit = 0],
  ['negative recovery count', value => value.recoveryAttempts = [{ cause: 'tool_input_invalid', scope: 'tool', attempts: -1 }]],
]) test(`rejects ${name} in a tool continuation`, () => {
  const input = checkpoint(); mutate(input); assert.throws(() => copyToolExecutionCheckpoint(input));
});
test('unsettled or unrelated invocations cannot mint a continuation', async () => {
  const runs = await repository({ settle: false });
  await assert.rejects(runs.saveToolExecutionCheckpoint('run', checkpoint()), { code: 'agent_execution_checkpoint_conflict' });
  await runs.settleInvocation('run', { invocationId: 'actual', status: 'completed' });
  await assert.rejects(runs.saveToolExecutionCheckpoint('run', { ...checkpoint(), invocationId: 'invented' }), { code: 'agent_execution_checkpoint_conflict' });
  assert.equal((await runs.get('run')).toolExecutionCheckpoint, undefined);
});
test('tool continuation replay is immutable and clarification cannot rewrite the pending round', async () => {
  const runs = await repository(), input = checkpoint();
  await runs.saveToolExecutionCheckpoint('run', input);
  const events = await runs.listEvents('run', 0);
  await runs.saveToolExecutionCheckpoint('run', input);
  assert.deepEqual(await runs.listEvents('run', 0), events);
  await assert.rejects(runs.saveToolExecutionCheckpoint('run', { ...input, responseAttempts: 1 }), { code: 'agent_execution_checkpoint_conflict' });
  const { assistant, invocationId, appliedGenerationLimit, allowedToolNames, ...base } = input;
  await assert.rejects(runs.saveExecutionCheckpoint('run', { ...base, schemaVersion: 2, phase: 'model_ready', inputRevision: 1,
    messages: [...base.messages, { role: 'user', content: 'answer', attributes: { inputRequestId: 'question' } }] }), { code: 'agent_execution_checkpoint_conflict' });
});

test('durable gateway cannot bind a different receipt store or host-managed write bypass', async () => {
  const { Agent } = await import('purra');
  const model = testGateway({ invoke: async () => assert.fail('model') });
  const bound = { executeOnce: async (_, operation) => operation() };
  const approval = { requiresDurableIdempotency: true, idempotencyGateway: bound, request: () => 'approved' };
  for (const idempotency of [undefined, { executeOnce: bound.executeOnce }]) {
    assert.throws(() => new Agent({ model, approval, ...(idempotency ? { idempotency } : {}) }), { code: 'approval_idempotency_required' });
  }
  assert.throws(() => new Agent({ model, tools: [{ name: 'write', description: 'write', inputSchema: { type: 'object' },
    policy: { mode: 'confirm', title: 'write' }, hostManagedDurability: true, run: async () => ({ content: 'done', effectState: 'committed' })
  }], approval, idempotency: bound }), { code: 'approval_idempotency_required' });
  assert.throws(() => new Agent({ model: { invoke: async () => assert.fail('model') }, toolCheckpointHandler: async () => {},
    approval: { request: () => 'approved' }, idempotency: bound }), { code: 'approval_runtime_required' });
});
