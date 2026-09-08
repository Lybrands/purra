import test from 'node:test';
import assert from 'node:assert/strict';
import { selectModelRoute, resolveModelRoute } from 'purra';
import { ModelRouteRegistry } from 'purra';

function candidate(bindingId) {
  return { bindingId, revision: '1', configIdentity: 'cfg', capabilities: {
    schemaVersion: 2, profileId: 'fixture', providerProtocol: 'custom',
    contextWindowTokens: 1000, maxGenerationTokens: 128, thinkingTokenAccounting: 'unknown',
    protocol: { reasoningControl: 'unavailable', reasoningReplay: 'ignored',
      toolCalling: 'unknown', requiredToolChoice: 'unknown', parallelToolCalls: 'unknown',
      streaming: 'unknown', cancellation: 'unknown', assistantContentWithToolCalls: 'optional',
      jsonSchemaLevel: 'unknown', streamFinishSemantics: 'normalized', usageSemantics: 'normalized' },
  } };
}
const req = { reasoningMode: 'default' };
test('authorization, registration order and duplicate/unknown rejection', () => {
  const a = candidate('a'), b = candidate('b');
  assert.equal(selectModelRoute([a,b], ['b'], req).bindingId, 'b');
  assert.equal(selectModelRoute([a,b], ['b','a'], req).bindingId, 'a');
  assert.throws(() => selectModelRoute([a,a], ['a'], req));
  assert.throws(() => selectModelRoute([a], ['missing'], req));
  assert.throws(() => selectModelRoute([a], [], req), {code: 'model_route_unavailable'});
});
test('required capabilities reject unknown and skip ineligible bindings', () => {
  const a = candidate('a');
  for (const requirement of [{toolCalling:'required'}, {streamingRequired:true},
    {cancellationRequired:true}, {structuredOutputLevel:'json_schema'}, {reasoningMode:'enabled'}]) {
    assert.throws(() => selectModelRoute([a], ['a'], {...req,...requirement}), {code:'model_route_unavailable'});
  }
  a.capabilities.actionable = false;
  assert.equal(selectModelRoute([a,candidate('b')], ['a','b'], req).bindingId, 'b');
});
test('selected data is detached and immutable', () => {
  const a = candidate('a'), selected = selectModelRoute([a], ['a'], req);
  a.revision = '2'; a.capabilities.protocol.toolCalling = 'supported';
  assert.equal(selected.revision, '1');
  assert.equal(selected.capabilities.protocol.toolCalling, 'unknown');
  assert.throws(() => { selected.capabilities.protocol.toolCalling = 'supported'; });
});

test('saved route ignores current ordering/policy and rejects missing, revoked or changed binding', async () => {
  const a = candidate('a'), b = candidate('b');
  const saved = selectModelRoute([a,b], ['a','b'], req, {id:'ordered', revision:'1'});
  const restored = await resolveModelRoute([b, {...a, policyId:'new-policy', policyRevision:'2'}], saved, ['a','b']);
  assert.equal(restored.bindingId, 'a'); assert.equal(restored.policyRevision, '1');
  for (const [rows, allowed] of [[[b], ['b']], [[a], []], [[{...a,revision:'2'}], ['a']],
    [[{...a, capabilities:{...a.capabilities, contextWindowTokens:2048}}], ['a']]]) {
    await assert.rejects(resolveModelRoute(rows, saved, allowed), {code:'model_route_mismatch'});
  }
  await assert.rejects(resolveModelRoute([a,a], saved, ['a']));
  assert.throws(() => selectModelRoute([a], ['a'], req, {id:'partial'}));
});

test('registry isolates concurrent factories, snapshots registration and never falls back', async () => {
  const entered = [];
  let release;
  const both = new Promise(resolve => {release = resolve;});
  const create = async route => {
    entered.push(route.bindingId);
    if (entered.length === 2) release();
    await both;
    return {route};
  };
  const bindings = ['a','b'].map(id => ({candidate:candidate(id), create}));
  const registry = new ModelRouteRegistry(bindings);
  bindings[0].candidate.revision = 'changed'; bindings[0].create = () => {throw Error('mutated');}; bindings.length = 0;
  const [a,b] = await Promise.all(['a','b'].map(id => registry.createNew([id], req, {id:'ordered', revision:'1'})));
  assert.notEqual(a,b); assert.deepEqual([a.route.bindingId,b.route.bindingId], ['a','b']);
  assert.equal(a.route.revision,'1');
  assert.deepEqual((await registry.createRecovery(a.route, ['a'])).route, a.route);
  const count = entered.length;
  await assert.rejects(registry.createRecovery(a.route, []), {code:'model_route_mismatch'});
  await assert.rejects(registry.createNew([], req), {code:'model_route_unavailable'});
  const failing = new ModelRouteRegistry([{candidate:candidate('a'), create:async () => {throw Error('factory failed');}}, {candidate:candidate('b'), create}]);
  await assert.rejects(failing.createNew(['a','b'], req), /factory failed/);
  assert.equal(entered.length,count);
});
