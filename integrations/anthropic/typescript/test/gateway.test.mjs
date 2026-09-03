import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import Anthropic from '@anthropic-ai/sdk';
import { AnthropicMessagesGateway } from '../dist/index.js';

const fixture = JSON.parse(readFileSync(new URL('../../fixtures/message.json', import.meta.url), 'utf8'));
const request = { messages:[{role:'system',content:'system'},{role:'developer',content:'developer'},{role:'user',content:'check'},{role:'developer',content:'runtime instruction'}],
  tools:[{name:'lookup',description:'lookup',inputSchema:{type:'object',properties:{query:{type:'string'}}}},{name:'ready',description:'ready',inputSchema:{type:'object'}}],
  outputLimit:{maxTokens:4096,source:'user_override',profileMaxTokens:8192} };
function gateway(fetch, options={}) { return new AnthropicMessagesGateway({ model:'fixture-model',capabilities:{},client:new Anthropic({apiKey:'fixture-not-a-key',fetch,maxRetries:5}),...options }); }
function sse(events) { return events.map(e=>`event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`).join(''); }
function streamResponse(events) { return new Response(sse(events),{headers:{'content-type':'text/event-stream'}}); }

test('official SDK preserves signed thinking, parallel tools and cache usage through a tool round',async()=>{
  const requests=[];
  const model=gateway(async(url,options)=>{
    assert.equal(new URL(url).pathname,'/v1/messages');
    const body=JSON.parse(options.body);requests.push(body);
    return body.stream ? streamResponse(fixture.events) : Response.json(fixture.response);
  },{thinking:{type:'adaptive'}});
  const result=await model.invoke(request);
  assert.equal(result.finishReason,'tool_calls');assert.equal(result.message.content,'先查资料。');assert.equal(result.message.reasoning,undefined);
  assert.deepEqual(result.usage,{inputTokens:20,outputTokens:20,totalTokens:40,cachedInputTokens:7});
  const chunks=[];for await(const chunk of await model.stream(request)) chunks.push(chunk);
  assert.equal(JSON.stringify(chunks.slice(0,-1)).includes('private-thought'),false);
  assert.equal(JSON.stringify(chunks.slice(0,-1)).includes('opaque-signature'),false);
  assert.deepEqual(chunks.at(-1).providerData,result.message.providerData);
  assert.deepEqual(chunks.at(-1).usage,result.usage);
  const calls=new Map();
  for(const chunk of chunks) for(const delta of chunk.toolCallDeltas??[]){
    const c=calls.get(delta.index)??{arguments:''};
    Object.assign(c,{...(delta.id?{id:delta.id}:{}),...(delta.name?{name:delta.name}:{})});c.arguments+=delta.argumentsFragment??'';calls.set(delta.index,c);
  }
  assert.deepEqual([...calls.values()].map(c=>({...c,arguments:JSON.parse(c.arguments)})),result.message.toolCalls);
  const restored=JSON.parse(JSON.stringify(result.message));
  await model.invoke({...request,messages:[...request.messages,restored,...restored.toolCalls.map(c=>({role:'tool',toolCallId:c.id,content:'done'}))]});
  assert.deepEqual(requests.at(-1).messages.at(-2).content,fixture.response.content);
  assert.deepEqual(requests.at(-1).messages.at(-1).content.map(b=>b.tool_use_id),['call_lookup','call_ready']);
  assert.deepEqual(requests.at(-1).system,[{type:'text',text:'system'},{type:'text',text:'developer'},{type:'text',text:'runtime instruction'}]);
  assert.equal(requests.at(-1).max_tokens,4096);
  await assert.rejects(model.invoke({...request,messages:[...request.messages,{...restored,content:'changed'}]}),/does not match/);
});

for(const [reason,expected] of [['end_turn','stop'],['stop_sequence','stop'],['max_tokens','length'],['refusal','filtered'],['model_context_window_exceeded','length']]){
  test(`stop reason ${reason} and missing usage`,async()=>{
    const model=gateway(async()=>Response.json({...fixture.response,content:[{type:'text',text:'answer'}],stop_reason:reason,usage:null}));
    const result=await model.invoke(request);assert.equal(result.finishReason,expected);assert.equal(result.usage,undefined);
  });
}
test('SDK retries are disabled and provider errors stay private',async()=>{
  let attempts=0;const model=gateway(async()=>{attempts++;return Response.json({type:'error',error:{type:'rate_limit_error',message:'private-provider-error'}},{status:429});});
  await assert.rejects(model.invoke(request),{code:'anthropic_http_429',message:'Anthropic request failed'});assert.equal(attempts,1);
});
test('truncated SSE is not a successful completion',async()=>{
  const model=gateway(async()=>streamResponse(fixture.events.slice(0,-1)));
  await assert.rejects(async()=>{for await(const _ of await model.stream(request)){}},{code:'upstream_stream_interrupted'});
});
test('invalid budget, unused stream and prior cancellation do not dispatch',async()=>{
  let attempts=0;const fetch=async()=>{attempts++;throw Error('must not dispatch');};
  await assert.rejects(gateway(fetch,{thinking:{type:'enabled',budget_tokens:4096}}).invoke(request),/budget/);
  const model=gateway(fetch);const stream=await model.stream(request);await stream[Symbol.asyncIterator]().return();
  const controller=new AbortController();controller.abort();
  await assert.rejects(model.invoke(request,controller.signal),{code:'agent_canceled'});
  await assert.rejects(async()=>{for await(const _ of await model.stream(request,controller.signal)){}},{code:'agent_canceled'});
  assert.equal(attempts,0);
});
test('canceling a stalled stream closes the HTTP body',async()=>{
  const controller=new AbortController();let closed=false;
  const model=gateway(async(_url,options)=>new Response(new ReadableStream({
    start(body){
      body.enqueue(new TextEncoder().encode(sse(fixture.events.slice(0,4))));
      options.signal.addEventListener('abort',()=>{closed=true;body.error(new Error('aborted'));},{once:true});
    },cancel(){closed=true;}
  }),{headers:{'content-type':'text/event-stream'}}));
  const iterator=(await model.stream(request,controller.signal))[Symbol.asyncIterator]();
  await iterator.next();controller.abort();
  await assert.rejects(async()=>{while(!(await iterator.next()).done){}},{code:'agent_canceled'});
  assert.equal(closed,true);
});

test('Core executes the tool round and removes private thinking from public messages',async()=>{
  const {Agent}=await import('purra');
  const requests=[];
  const model=gateway(async(_url,options)=>{
    const body=JSON.parse(options.body);requests.push(body);
    if(body.messages.some(m=>m.content.some(b=>b.type==='tool_result'))){
      return streamResponse([
        {type:'message_start',message:{...fixture.response,content:[],stop_reason:null}},
        {type:'content_block_start',index:0,content_block:{type:'text',text:''}},
        {type:'content_block_delta',index:0,delta:{type:'text_delta',text:'Found it.'}},
        {type:'content_block_stop',index:0},
        {type:'message_delta',delta:{stop_reason:'end_turn',stop_sequence:null},usage:{output_tokens:3}},
        {type:'message_stop'},
      ]);
    }
    return streamResponse(fixture.events.filter(e=>e.index!==4));
  },{capabilities:{schemaVersion:1,profileId:'fixture',providerProtocol:'anthropic',contextWindowTokens:65536,maxCallOutputTokens:8192,thinkingTokenAccounting:'included',protocol:{reasoningControl:'selectable',reasoningReplay:'ignored',toolCalling:'supported',requiredToolChoice:'unavailable',parallelToolCalls:'supported',streaming:'supported',cancellation:'supported',assistantContentWithToolCalls:'optional',jsonSchemaLevel:'unknown',streamFinishSemantics:'normalized',usageSemantics:'normalized'}}});
  const agent=new Agent({model,tools:[{name:'lookup',description:'lookup',inputSchema:{type:'object',properties:{query:{type:'string'}},required:['query']},policy:{mode:'read',title:'lookup'},run:()=>({content:'found',effectState:'not_started'})}]});
  const result=await agent.invoke({messages:[{role:'user',content:'lookup'}],maxCallOutputTokens:4096});
  assert.equal(result.output,'Found it.');
  assert.ok(requests[1].messages.some(m=>m.content.some(b=>b.signature==='opaque-signature')));
  for(const secret of ['private-thought','opaque-signature','opaque-redacted'])assert.equal(JSON.stringify(result.messages).includes(secret),false);
});
