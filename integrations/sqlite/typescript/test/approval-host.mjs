import assert from 'node:assert/strict';
import { Agent, ApprovalIntent, ApprovalRequired, jsonIdentityDigest } from 'purra';
import { SqliteAgentAdapters } from '../dist/index.js';
import { testGateway } from '../../../../typescript/test/support/model-gateway.mjs';

export const request = { messages: [{ role: 'user', content: 'Write the fixture' }], planningMode: 'reactive' };
const options = { budgets: { maxRunGenerationTokens: null } };
export function createApprovalHost(path, settings = {}) {
  const storage = new SqliteAgentAdapters(path, { scope: 'approval' });
  const counts = { model: 0, tool: 0 }, expiry = settings.expiry ?? Date.now() + 600000;
  const approvals = storage.approvalStore({ authorize: () => true, ...(settings.clockMs ? { clockMs: settings.clockMs } : {}) });
  const repository = new Proxy(storage.runs, { get(target, key) {
    if (key === 'saveExecutionCheckpoint' && settings.stopAfterTool) return async () => { throw new ApprovalRequired('fixture', 'stop-after-receipt'); };
    return target[key];
  } });
  const agent = new Agent({
    runRepository: repository, outputPublisher: storage.publisher, preset: { id: 'approval', revision: '1' },
    ...(settings.planner ? { planning: { planner: settings.planner } } : {}),
    ...(settings.tree ? { agentTree: { repository: storage.runTree } } : {}),
    toolCheckpointNames: ['write'],
    model: testGateway({ async invoke(input) {
      counts.model++;
      if (settings.script) return settings.script(input, counts.model);
      if (input.messages.some(message => message.role === 'tool') || input.tools.length === 0) return { message: { role: 'assistant', content: 'done' }, finishReason: 'stop' };
      return { message: { role: 'assistant', content: '', reasoning: 'private fixture reasoning', providerData: { fixture: 'opaque replay' },
        toolCalls: [{ id: 'write-1', name: 'write', arguments: { value: 42 } }] }, finishReason: 'tool_calls' };
    } }),
    tools: [{ name: 'write', description: 'Write a synthetic value', inputSchema: { type: 'object', properties: { value: { type: 'integer' } }, required: ['value'] },
      policy: { mode: 'confirm', title: 'Write', riskLevel: 'write' }, scope: settings.scope ?? (() => true),
      async run() { counts.tool++; if (settings.run) return settings.run(); return { content: 'written', effectState: 'committed' }; },
    }],
    approval: approvals.gateway(), idempotency: storage.idempotency,
    ...(settings.noHandler ? {} : { toolCheckpointHandler: async checkpoint => {
      const call = checkpoint.assistant.toolCalls[0], snapshot = await storage.runs.get(checkpoint.runId);
      const intent = await ApprovalIntent.create({ schemaVersion: 1, runId: checkpoint.runId, rootRunId: checkpoint.runId,
        toolCallId: call.id, toolName: call.name, arguments: call.arguments, presetFingerprint: await jsonIdentityDigest(snapshot.preset),
        bindingId: 'fixture', bindingRevision: '1', scopeId: 'fixture-only', scopeRevision: '1', effect: 'write' });
      const [existing] = await approvals.listPending({ runId: checkpoint.runId });
      await approvals.prepare(checkpoint, intent, { expiresAtMs: existing?.expiresAtMs ?? expiry });
    } }),
  });
  return { storage, approvals, agent, counts, path };
}
export async function paused(host) {
  await host.storage.enableApprovals();
  const handle = await host.agent.submit(request, options);
  await assert.rejects(handle.result, ApprovalRequired);
  assert.deepEqual(host.counts, { model: 1, tool: 0 });
  return handle.runId;
}
export async function approve(host, id) {
  const records = await host.approvals.listPending({ runId: id });
  const record = records.find(record => record.status === "pending") ?? records[0];
  await host.approvals.decide({ approvalId: record.approvalId, expectedRevision: record.revision, intentDigest: record.intentDigest,
    commandKey: 'approve', decision: 'approve' }, { principalId: 'host' });
  return record;
}

