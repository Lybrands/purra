import assert from 'node:assert/strict';
import test from 'node:test';
import { StructuredOutputContract } from 'purra';
import { ToolCatalog } from '../dist/tools/catalog.js';

test('object contract admits code points, rejects whole batch before scope, and passes authority context', async () => {
  const schema = {type:'object',properties:{q:{type:'string',maxLength:1},tags:{type:'array',items:{type:'string'}}},required:['q'],additionalProperties:false};
  const contract = await StructuredOutputContract.create({schemaId:'tool',schemaVersion:'1',schema});
  const scopes = [], handlers = [];
  const catalog = new ToolCatalog([{name:'search',description:'Search',inputSchema:schema,argumentContract:contract,concurrencySafe:true,
    policy:{mode:'read',title:'Search'},scope:(args,context) => {scopes.push(context);},run:(args,context) => {handlers.push(context);return {content:args,effectState:'not_started'};}}]);
  const options = {executionKey:'execution',runId:'run',rootRunId:'root',agentId:'agent',parentRunId:'parent',leaseOwnerId:'worker',leaseEpoch:2};
  await assert.rejects(catalog.executeBatch([{id:'1',name:'search',arguments:{q:'😀'}},{id:'2',name:'search',arguments:{q:'a',tags:'["x"]'}}],options),e=>e.code === 'invalid_tool_arguments_schema');
  assert.equal(scopes.length,0);
  assert.equal(handlers.length,0);
  await catalog.executeBatch([{id:'3',name:'search',arguments:{q:'😀'}}],options);
  assert.equal(handlers.length,1);
  for (const context of [...scopes,...handlers]) for (const [key,value] of Object.entries(options)) {
    if (key !== 'executionKey') assert.equal(context[key],value);
  }
  assert.throws(() => new ToolCatalog([{name:'mismatch',description:'Mismatch',policy:{mode:'read',title:'Read'},inputSchema:{type:'object'},argumentContract:contract,run:()=>({content:null,effectState:'not_started'})}]),TypeError);
});
