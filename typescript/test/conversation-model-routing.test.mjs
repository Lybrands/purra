import test from 'node:test';
import assert from 'node:assert/strict';
import { Agent, InMemoryRunRepository, ModelRouteRegistry } from 'purra';
import { testGateway } from './support/model-gateway.mjs';

test('host transfers conversation across models while completed Run bindings stay unchanged', async () => {
  const repository = new InMemoryRunRepository();
  const calls = [];
  const capabilities = testGateway({}).capabilities;
  const registry = new ModelRouteRegistry(['model-a', 'model-b'].map(bindingId => ({
    candidate: { bindingId, revision: '1', configIdentity: bindingId, capabilities },
    async create(route) {
      return new Agent({
        preset: { id: 'conversation', revision: '1', modelRoute: route },
        runRepository: repository, responsePresentation: 'none',
        model: testGateway({ capabilities, async invoke(request) {
          calls.push({ bindingId: route.bindingId, messages: request.messages });
          return { message: { role: 'assistant', content: `Answer from ${route.bindingId}` }, finishReason: 'stop' };
        } }),
      });
    },
  })));
  const history = [{ role: 'user', content: 'My project is called PURRA.' }];
  const request = messages => ({ messages, planningMode: 'reactive', metadata: { conversationId: 'conversation-1' } });
  const options = { budgets: { maxRunGenerationTokens: null } };
  const first = await registry.createNew(['model-a'], { reasoningMode: 'default' });
  const handleA = await first.submit(request(history), options);
  const answerA = await handleA.result;
  const savedA = await handleA.snapshot();
  const next = [...history, { role: 'assistant', content: answerA.output }, { role: 'user', content: 'Continue with that project.' }];
  const second = await registry.createNew(['model-b'], { reasoningMode: 'default' });
  const handleB = await second.submit(request(next), options);
  const answerB = await handleB.result;
  const savedB = await handleB.snapshot();
  assert.equal(answerB.output, 'Answer from model-b');
  assert.notEqual(savedA.runId, savedB.runId);
  assert.deepEqual(await handleA.snapshot(), savedA);
  assert.equal(savedA.preset.modelRoute.bindingId, 'model-a');
  assert.equal(savedB.preset.modelRoute.bindingId, 'model-b');
  assert.deepEqual(calls.map(c => c.bindingId), ['model-a', 'model-b']);
  assert.deepEqual(calls[1].messages.filter(m => ['user', 'assistant'].includes(m.role)), next);
});
