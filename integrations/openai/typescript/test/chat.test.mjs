import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import OpenAI from 'openai';
import { staticImageContent } from 'purra';
import { OpenAIChatCompletionsGateway } from '../dist/index.js';

const fixture=JSON.parse(readFileSync(new URL('../../fixtures/chat.json',import.meta.url),'utf8'));
const request={messages:[{role:'developer',content:'instructions'},{role:'user',content:'check'}],tools:[{name:'lookup',description:'lookup',inputSchema:{type:'object',properties:{query:{type:'string'}}}}],outputBudget:{maxGenerationTokens:128,generationSource:'user',profileMaxGenerationTokens:256,requestedUserMaxGenerationTokens:128,resultCapacityTargetTokens:null,resultCapacitySource:null,nonResultHeadroomTokens:null}};
function gateway(fetch){return new OpenAIChatCompletionsGateway({model:'fixture-model',capabilities:{},client:new OpenAI({apiKey:'fixture-not-a-key',fetch,maxRetries:5})});}
function sse(events){return events.map(e=>`data: ${JSON.stringify(e)}\n\n`).join('')+'data: [DONE]\n\n';}

test('omitted interim finish reasons remain nonterminal; a real finish is still required', async () => {
  for (const omitTerminal of [false, true]) {
    const events = structuredClone(fixture.events);
    for (const event of events) for (const choice of event.choices ?? []) {
      if (omitTerminal || choice.finish_reason == null) delete choice.finish_reason;
    }
    const model = gateway(async () => new Response(sse(events), { headers: { 'content-type': 'text/event-stream' } }));
    const consume = async () => { const chunks = []; for await (const chunk of await model.stream(request)) chunks.push(chunk); return chunks; };
    if (omitTerminal) await assert.rejects(consume(), { code: 'upstream_stream_interrupted' });
    else assert.equal((await consume()).at(-1).finishReason, 'tool_calls');
  }
});

test('inline images reach the SDK in complete and stream; default rejects before network', async () => {
  const dataBase64 = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aE9sAAAAASUVORK5CYII=';
  const input = { ...request, capabilitySnapshot: { protocol: { imageInput: 'supported' } }, messages: [{ role: 'user', content: staticImageContent('Describe', [{ mediaType: 'image/png', dataBase64, inputTokens: 2000 }]) }] };
  const bodies = [];
  const fetch = async (_url, options) => {
    const body = JSON.parse(options.body); bodies.push(body);
    assert.deepEqual(body.messages[0].content, [{ type: 'text', text: 'Describe' }, { type: 'image_url', image_url: { url: `data:image/png;base64,${dataBase64}` } }]);
    assert.ok(!options.body.includes('inputTokens'));
    return body.stream ? new Response(sse(fixture.events), { headers: { 'content-type': 'text/event-stream' } }) : Response.json(fixture.response);
  };
  const disabled = new OpenAIChatCompletionsGateway({ model: "fixture-model", capabilities: { protocol: { imageInput: "supported" } }, client: new OpenAI({ apiKey: "fixture-not-a-key", fetch }) });
  await assert.rejects(disabled.invoke(input), /host-enabled/);
  await assert.rejects(disabled.stream(input), /host-enabled/);
  assert.equal(bodies.length, 0);
  const model = new OpenAIChatCompletionsGateway({ model: 'fixture-model', capabilities: { protocol: { imageInput: 'supported' } }, imageInput: true, client: new OpenAI({ apiKey: 'fixture-not-a-key', fetch }) });
  await model.invoke(input);
  for await (const _ of await model.stream(input)) {}
  assert.equal(bodies.length, 2);
  await assert.rejects(model.invoke({ ...input, messages: [{ ...input.messages[0], role: 'system' }] }), /user role/);
  assert.equal(bodies.length, 2);
});

test('official Chat SDK maps tool continuations and usage after the finish marker',async()=>{
  const requests=[];const model=gateway(async(url,options)=>{
    assert.equal(new URL(url).pathname,'/v1/chat/completions');const body=JSON.parse(options.body);requests.push(body);
    return body.stream?new Response(sse(fixture.events),{headers:{'content-type':'text/event-stream'}}):Response.json(fixture.response);
  });
  const result=await model.invoke(request);assert.equal(result.finishReason,'tool_calls');assert.equal(result.usage.reasoningTokens,4);
  const chunks=[];for await(const chunk of await model.stream(request))chunks.push(chunk);
  assert.equal(chunks.filter(c=>c.finishReason).length,1);assert.deepEqual(chunks.at(-1).usage,result.usage);
  assert.equal(chunks.flatMap(c=>c.toolCallDeltas??[]).map(c=>c.argumentsFragment??'').join(''),'{"query":"中文"}');
  await model.invoke({...request,messages:[...request.messages,result.message,{role:'tool',toolCallId:'call_lookup',content:'done'}]});
  assert.equal(requests.at(-1).max_completion_tokens,128);assert.equal(requests.at(-1).max_tokens,undefined);assert.equal(requests.at(-1).store,false);
  assert.equal(requests.at(-1).messages[0].role,'developer');assert.equal(requests.at(-1).messages.at(-1).tool_call_id,'call_lookup');
});
for(const [reason,expected] of [['stop','stop'],['length','length'],['content_filter','filtered']]){
 test(`Chat stop reason ${reason}`,async()=>{
  const model=gateway(async()=>Response.json({...fixture.response,usage:null,choices:[{index:0,message:{role:'assistant',content:'answer'},finish_reason:reason}]}));
  const result=await model.invoke(request);assert.equal(result.finishReason,expected);assert.equal(result.usage,undefined);
 });
}
test('Chat errors, truncated streams, prior cancellation and unused streams',async()=>{
 let attempts=0;const model=gateway(async(_url,options)=>{attempts++;return JSON.parse(options.body).stream?new Response(sse(fixture.events.slice(0,2)),{headers:{'content-type':'text/event-stream'}}):Response.json({error:{message:'private-provider-error'}},{status:429});});
 await assert.rejects(model.invoke(request),{code:'openai_http_429',message:'OpenAI request failed'});assert.equal(attempts,1);
 await assert.rejects(async()=>{for await(const _ of await model.stream(request)){}},{code:'upstream_stream_interrupted'});
 const signal=AbortSignal.abort();await assert.rejects(model.invoke(request,signal),{code:'agent_canceled'});
 const stream=await model.stream(request);await stream[Symbol.asyncIterator]().return();assert.equal(attempts,2);
});
