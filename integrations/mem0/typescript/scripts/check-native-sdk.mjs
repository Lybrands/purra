import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createManagedClient, Mem0Memory } from 'purra-mem0';
process.env.MEM0_TELEMETRY='false';
const root=mkdtempSync(join(tmpdir(),'purra-direct-boundary-'));
process.env.MEM0_DIR=join(root,'sdk');
const entry = import.meta.resolve('purra-mem0');
const { DirectLlm, DirectEmbedder } = await import(new URL('./direct-providers.js', entry));
const { LLMFactory, EmbedderFactory, VectorStoreFactory, Memory } = await import(new URL('./sdk-memory.js', entry));
let factoryCalls=0;
const originalLlm=LLMFactory.create, originalEmbedder=EmbedderFactory.create;
// Test-only tripwires; the shipped implementation never replaces factories.
LLMFactory.create=EmbedderFactory.create=()=>{factoryCalls++;throw new Error('native factory should not run')};
const clients=[],memories=[];
try {
 assert.throws(() => new Memory(), /native factory should not run/);
 assert.equal(factoryCalls, 1); factoryCalls = 0;
 assert.throws(() => new Memory({}, {llm:{}, embedder:new DirectEmbedder()}), /invalid injected/);
 assert.equal(factoryCalls, 0);
 const originalVector = VectorStoreFactory.create;
 let closed = 0;
 const failure = new Error('vector initialization sentinel');
 VectorStoreFactory.create = () => ({ initialize: async () => {throw failure}, close: async () => {closed++} });
 let failed;
 try {
  failed = new Memory({vectorStore:{provider:'memory',config:{dimension:2,dbPath:join(root,'failed-vector.db')}},historyDbPath:join(root,'failed-history.db')}, {llm:new DirectLlm(), embedder:new DirectEmbedder()});
  // The memory store owns a db handle; other native stores expose close().
  await failed._initPromise;
  failed.vectorStore.db = {close: async () => {closed++}};
  await assert.rejects(failed.ready(), error => error === failure);
  assert.equal(closed, 1);
  assert.equal(failed.db.db.open, false);
 } finally { VectorStoreFactory.create = originalVector; }
 for (const name of ['a','b']) {
  const client=await createManagedClient({embeddingDims:2,config:{vectorStore:{provider:'memory',config:{dimension:2,collectionName:'memory',dbPath:join(root,`${name}.db`)}},historyDbPath:join(root,`${name}-history.db`)}});
  clients.push(client);
  assert.deepEqual(client.sdk.providerBinding, {llm:"injected", embedder:"injected"});
  await client.sdk.getAll({filters:{user_id:'construction'}});
  await client.sdk.reset();
  const providers={budget:{key:name,maxLlmCalls:1,maxEmbeddingCalls:20,maxInputChars:100000,maxOutputTokens:64,resultCapacityTargetTokens:32},
   async complete(messages,cap,signal){await new Promise(r=>setTimeout(r,name==='a'?5:1));assert.ok(messages.at(-1).content.includes(`question-${name}`));return {message:{role:'assistant',content:JSON.stringify({memory:[{text:`result-${name}`,entities:[]}]})},finishReason:'stop',appliedGenerationLimit:cap,usage:{inputTokens:10,generationTokens:5}}},
   async embed(texts){return {vectors:texts.map(()=>[1,0]),inputTokens:texts.join('').length}}};
  memories.push(new Mem0Memory({client,scope:{user:name,project:'design'},journalPath:join(root,`${name}-journal.db`),allowInference:true,providers}));
 }
 const results=await Promise.all(memories.map((m,i)=>m.extract([{role:'user',content:`question-${['a','b'][i]}`}],{source:{id:'source',revision:'1'},key:'extract'})));
 for(let i=0;i<2;i++){assert.equal(results[i].usage.llmCalls,1);assert.equal((await clients[i].sdk.get(results[i].ids[0])).memory,`result-${['a','b'][i]}`)}
 await assert.rejects(clients[0].sdk.add('unbound',{userId:'unbound',infer:false}),/memory_provider_unbound/);
 assert.equal(factoryCalls,0);
 console.log(JSON.stringify({nativeAdapter:true,failedInitializationCleanup:true,constructorAndResetNativeFactoryCalls:factoryCalls,concurrentClients:2,contextIsolation:true,unboundRejected:true}));
}finally{
 for(const m of memories){await m.drain();m.close()}
 for(const c of clients){c.sdk.db.close();c.sdk.vectorStore.db.close();c.sdk._entityStore?.db.close()}
 LLMFactory.create=originalLlm;EmbedderFactory.create=originalEmbedder;
 rmSync(root,{recursive:true,force:true});
}
