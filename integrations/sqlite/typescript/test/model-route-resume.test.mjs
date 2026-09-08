import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createApprovalHost, paused, approve, request } from './approval-host.mjs';
import { testGateway } from '../../../../typescript/test/support/model-gateway.mjs';
import { resolveModelRoute } from 'purra';

test('route survives reopen, rejects missing/changed binding, and resumes once', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'purra-route-')), path = join(dir, 'db');
  const route = {bindingId:'host-model', revision:'1', configIdentity:'config-1', capabilities:testGateway({}).capabilities,
    policyId:'ordered', policyRevision:'1'};
  let host;
  try {
    host = createApprovalHost(path, {modelRoute:route});
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
    host = createApprovalHost(path, {modelRoute:resolved});
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
