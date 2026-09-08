import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createApprovalHost, paused, approve, request } from './approval-host.mjs';
import { testGateway } from '../../../../typescript/test/support/model-gateway.mjs';
import { resolveModelRoute, ModelRouteRegistry } from 'purra';

test('route survives reopen, rejects missing/changed binding, and resumes once', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'purra-route-')), path = join(dir, 'db');
  const route = {bindingId:'host-model', revision:'1', configIdentity:'config-1', capabilities:testGateway({}).capabilities,
    policyId:'ordered', policyRevision:'1'};
  let host;
  const create = async selected => createApprovalHost(path, {modelRoute:selected});
  const registry = new ModelRouteRegistry([{candidate:route, create}]);
  try {
    host = await registry.createNew(['host-model'], {reasoningMode:'default'});
    const id = await paused(host);
    assert.equal((await host.storage.runs.get(id)).preset.modelRoute.bindingId, 'host-model');
    host.storage.close(); host = undefined;
    for (const current of [undefined, {...route, revision:'2'}, {...route, configIdentity:'config-2'}, {...route, policyRevision:'2'}]) {
      host = createApprovalHost(path, {modelRoute:current});
      await assert.rejects(host.agent.resume(id, request), {code:'agent_preset_mismatch'});
      assert.deepEqual(host.counts, {model:0, tool:0});
      host.storage.close(); host = undefined;
    }
    host = createApprovalHost(path);
    const savedRoute = (await host.storage.runs.get(id)).preset.modelRoute;
    const resolved = await resolveModelRoute([{...route, policyRevision:'2'}], savedRoute, ['host-model']);
    assert.equal(resolved.policyRevision, '1');
    host.storage.close(); host = undefined;
    const rebuiltRegistry = new ModelRouteRegistry([{candidate:{...route,policyRevision:'2'}, create}]);
    host = await rebuiltRegistry.createRecovery(savedRoute, ['host-model']);
    const restored = await host.storage.runs.get(id);
    assert.throws(() => {restored.preset.modelRoute.capabilities.protocol.toolCalling = 'unavailable';});
    await assert.rejects(host.agent.resume(id, {...request, maxGenerationTokens:128}), {code:'agent_preset_mismatch'});
    assert.deepEqual(host.counts, {model:0, tool:0});
    await approve(host, id);
    await (await host.agent.resume(id, request)).result;
    assert.equal(host.counts.tool, 1);
    assert.equal((await host.storage.runs.get(id)).status, 'completed');
  } finally { host?.storage.close(); rmSync(dir, {recursive:true, force:true}); }
});

test('two routed Runs overlap without sharing binding state', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'purra-route-concurrent-')), path = join(dir, 'db');
  const hosts = [], entered = [];
  let release;
  const both = new Promise(resolve => {release = resolve;});
  const create = async route => {
    const host = createApprovalHost(path, {modelRoute:route, script:async () => {
      entered.push(route.bindingId);
      if (entered.length === 2) release();
      await both;
      return {message:{role:'assistant', content:route.bindingId}, finishReason:'stop'};
    }});
    hosts.push(host); return host;
  };
  const registry = new ModelRouteRegistry(['model-a','model-b'].map(bindingId => ({
    candidate:{bindingId, revision:'1', configIdentity:bindingId, capabilities:testGateway({}).capabilities}, create,
  })));
  try {
    const [a,b] = await Promise.all(['model-a','model-b'].map(id => registry.createNew([id], {reasoningMode:'default'})));
    const handles = await Promise.all([a,b].map(host => host.agent.submit({...request, enabledTools:[]}, {budgets:{maxRunGenerationTokens:null}})));
    const results = await Promise.all(handles.map(handle => handle.result));
    assert.deepEqual(entered.sort(), ['model-a','model-b']);
    assert.notEqual(handles[0].runId, handles[1].runId);
    for (const [index, host] of [a,b].entries()) {
      const id = ['model-a','model-b'][index];
      const saved = await host.storage.runs.get(handles[index].runId);
      assert.equal(saved.preset.modelRoute.bindingId,id);
      assert.equal(saved.status,'completed');
      assert.equal(results[index].messages.at(-1).content,id);
      assert.deepEqual(host.counts,{model:1,tool:0});
    }
  } finally { for (const host of hosts) host.storage.close(); rmSync(dir,{recursive:true,force:true}); }
});
