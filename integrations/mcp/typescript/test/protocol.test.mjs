import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { ListToolsRequestSchema, CallToolRequestSchema, ToolListChangedNotificationSchema } from '@modelcontextprotocol/sdk/types.js';
import { AgentCanceledError } from 'purra';
import { discoverMcpTools, McpCatalogMonitor } from '../dist/index.js';

const fixture = JSON.parse(readFileSync(new URL('../../fixtures/tools.json', import.meta.url)));
const tool = fixture.tool;
const binding = { localName:'search', policy:{mode:'read', title:'Search'}, scope:null, concurrencySafe:true };
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const errorCode = code => error => error.code === code;
async function connect(t, options = {}) {
  const server = new Server({name:'fixture',version:'1'}, {capabilities:{tools:{listChanged:true}}});
  const client = new Client({name:'fixture-client',version:'1'});
  const monitor = new McpCatalogMonitor();
  const [ct, st] = InMemoryTransport.createLinkedPair();
  ct.setProtocolVersion = version => monitor.acceptProtocolVersion(version);
  // Exercise a JSON serialization boundary in addition to SDK request dispatch.
  for (const transport of [ct,st]) {
    const send = transport.send.bind(transport);
    transport.send = (message, ...rest) => {
      const wire = JSON.parse(JSON.stringify(message));
      if (transport === st && options.malformed && wire.result?.content) wire.result = options.result;
      return send(wire, ...rest);
    };
  }
  const state = { lists:[], calls:[], started:deferred(), finished:deferred(), gate:undefined, changed:false, rpcError:false };
  client.setNotificationHandler(ToolListChangedNotificationSchema, notification => monitor.onNotification(notification));
  client.onclose = () => monitor.close();
  server.setRequestHandler(ListToolsRequestSchema, async request => {
    state.lists.push(request.params?.cursor ?? null);
    if (state.changed) await server.sendToolListChanged();
    const pages = options.pages ?? [{tools:[tool]}];
    return pages[Math.min(state.lists.length - 1,pages.length - 1)];
  });
  server.setRequestHandler(CallToolRequestSchema, async (request, extra) => {
    state.calls.push(request.params);
    state.started.resolve();
    try {
      if (state.gate) await Promise.race([state.gate.promise, new Promise(resolve => extra.signal.addEventListener('abort',resolve,{once:true}))]);
      if (state.rpcError) throw new Error('private remote exception');
      return !options.malformed && options.result || {content:[],structuredContent:{count:1}};
    } finally { state.finished.resolve(); }
  });
  await server.connect(st);
  await client.connect(ct);
  t.after(async () => { state.gate?.resolve(); await client.close(); await server.close(); });
  return { ...state, state, client, monitor, server };
}
const discover = (f, extra = {}) => discoverMcpTools(f.client,'fixture',{'remote.search':binding},{monitor:f.monitor,...extra});
const run = (catalog, args = {q:'a'}, context = {}) => catalog.registrations[0].run(args,{toolCallId:'call',toolName:'search',...context});

for (const row of fixture.cases) test(`shared boundary: ${row.id}`, async t => {
  const f = await connect(t, {result:row.result,malformed:row.malformed});
  const catalog = await discover(f,{limits:row.limits});
  const result = await run(catalog,row.arguments);
  assert.equal(result.errorCode ?? null,row.errorCode);
  assert.equal(f.lists.length,1);
  assert.equal(f.calls.length,row.errorCode === 'mcp_invalid_arguments' ? 0 : 1);
  if (row.errorCode === null) {
    assert.deepEqual(result.content,{text:row.result.content.map(c => c.text),structured:row.result.structuredContent});
    assert.deepEqual(f.calls[0].arguments,row.arguments);
  } else assert.deepEqual(result.content,{text:[],structured:null});
  await f.client.ping();
});

test('allowlist, pagination, stable identity and immutable snapshot', async t => {
  const other = {name:'unbound',inputSchema:{type:'object',$ref:'https://example.invalid/private'}};
  const f = await connect(t,{pages:[{tools:[other],nextCursor:'next'},{tools:[tool]}]});
  const bindings = {'remote.search':{...binding}};
  const catalog = await discoverMcpTools(f.client,'fixture',bindings,{monitor:f.monitor});
  bindings['remote.search'].localName = 'mutated';
  delete bindings['remote.search'];
  assert.deepEqual(f.lists,[null,'next']);
  assert.equal(catalog.registrations.length,1);
  assert.equal(catalog.registrations[0].concurrencySafe,true);
  assert.equal(catalog.snapshot.entries[0].policy.mode,'read');
  assert.throws(() => { catalog.snapshot.entries[0].inputSchema.required[0] = 'other'; },TypeError);
  assert.equal(catalog.snapshot.revisionDigest,fixture.revisionDigest);
  await f.server.sendToolListChanged();
  assert.equal(catalog.stale,true);
  assert.equal((await run(catalog)).errorCode,'mcp_catalog_stale');
  assert.equal(f.calls.length,0);
  const fresh = await discover(f);
  assert.equal(fresh.stale,false);
  assert.equal(fresh.snapshot.revisionDigest,catalog.snapshot.revisionDigest);
  f.monitor.close();
  assert.equal(fresh.stale,true);
  await f.client.ping();
});

for (const [name,pages,code] of [
  ['duplicate',[{tools:[tool,tool]}],'mcp_name_conflict'],
  ['cursor loop',[{tools:[],nextCursor:'again'}],'mcp_pagination_invalid'],
  ['missing',[{tools:[]}],'mcp_binding_missing'],
  ['schema',[{tools:[{...tool,inputSchema:{type:'object',$ref:'#/$defs/x'}}]}],'mcp_schema_unsupported'],
  ['required task',[{tools:[{...tool,execution:{taskSupport:'required'}}]}],'mcp_tool_unsupported'],
]) test(`catalog rejects ${name}`, async t => {
  const f = await connect(t,{pages});
  await assert.rejects(discover(f),errorCode(code));
  assert.equal(f.calls.length,0);
});

test('policy and names preflight before IO, host scope, stale discovery', async t => {
  const f = await connect(t);
  await assert.rejects(discoverMcpTools(f.client,'fixture',{'remote.search':{...binding,policy:{mode:'write',title:'Write'}}},{monitor:f.monitor}),errorCode('mcp_binding_invalid'));
  await assert.rejects(discoverMcpTools(f.client,'fixture',{'remote.search':binding,other:binding},{monitor:f.monitor}),errorCode('mcp_name_conflict'));
  assert.equal(f.lists.length,0);
  const catalog = await discoverMcpTools(f.client,'fixture',{'remote.search':{...binding,scope:(args) => {assert.equal(args.q,'a');return 'no access';}}},{monitor:f.monitor});
  assert.equal((await run(catalog)).errorCode,'mcp_scope_denied');
  assert.equal(f.calls.length,0);
  f.state.changed = true;
  await assert.rejects(discover(f),errorCode('mcp_catalog_stale'));
});

for (const mode of ['cancel','timeout','stale','rpc','disconnect']) test(`inflight ${mode}`, {timeout:5000}, async t => {
  const f = await connect(t);
  f.state.gate = deferred();
  f.state.rpcError = mode === 'rpc';
  const catalog = await discover(f,{limits:{timeoutMs:mode === 'timeout' ? 100 : 2000}});
  const controller = new AbortController();
  const running = run(catalog,{q:'a'},{signal:controller.signal});
  await f.started.promise;
  if (mode === 'cancel') controller.abort();
  else if (mode === 'stale') { await f.server.sendToolListChanged(); f.state.gate.resolve(); }
  else if (mode === 'rpc') f.state.gate.resolve();
  else if (mode === 'disconnect') await f.client.close();
  if (mode === 'cancel') await assert.rejects(running,AgentCanceledError);
  else {
    const result = await running;
    assert.equal(result.errorCode,{timeout:'mcp_timeout',stale:'mcp_catalog_stale',rpc:'mcp_protocol_error',disconnect:'mcp_transport_error'}[mode]);
    assert.deepEqual(result.content,{text:[],structured:null});
  }
  f.state.gate.resolve();
  await f.finished.promise;
  assert.equal(f.calls.length,1);
  if (mode !== 'disconnect') await f.client.ping();
});

for (const limits of [{maxPages:1},{maxTools:1},{maxCatalogBytes:64},{maxDescriptionBytes:1}]) test(`bounded catalog ${JSON.stringify(limits)}`, async t => {
  const f = await connect(t,{pages:[{tools:[tool],nextCursor:'next'},{tools:[{name:'other',inputSchema:{type:'object'}}]}]});
  await assert.rejects(discover(f,{limits}),errorCode('mcp_catalog_limit_exceeded'));
  assert.ok(f.lists.length <= 2);
  assert.equal(f.calls.length,0);
});

test('canceled or closed operations issue no requests', async t => {
  const f = await connect(t);
  const controller = new AbortController(); controller.abort();
  await assert.rejects(discover(f,{signal:controller.signal}),AgentCanceledError);
  assert.equal(f.lists.length,0);
  const catalog = await discover(f);
  await assert.rejects(run(catalog,{q:'a'},{signal:controller.signal}),AgentCanceledError);
  assert.equal(f.calls.length,0);
  f.monitor.close();
  await assert.rejects(discover(f),errorCode('mcp_connection_closed'));
  assert.equal(f.lists.length,1);
});

test('unknown negotiated protocol fails closed', () => {
  const monitor = new McpCatalogMonitor();
  assert.throws(() => monitor.acceptProtocolVersion('2099-01-01'),errorCode('mcp_protocol_unsupported'));
});

test('snapshot identity tracks selected contract, independent of discovery order', async t => {
  const other = {...tool,name:'remote.other'};
  const bindings = {'remote.search':binding,'remote.other':{localName:'another',policy:{mode:'read',title:'Other'},scope:null}};
  const digests = [];
  for (const tools of [[tool,other],[other,tool],[other,{...tool,description:'Changed description'}]]) {
    const f = await connect(t,{pages:[{tools}]});
    const catalog = await discoverMcpTools(f.client,'fixture',bindings,{monitor:f.monitor});
    assert.deepEqual(catalog.snapshot.entries.map(e => e.localName),['another','search']);
    digests.push(catalog.snapshot.revisionDigest);
  }
  assert.equal(digests[0],digests[1]);
  assert.notEqual(digests[1],digests[2]);
});

test('text-only tool does not guess structured JSON', async t => {
  const {outputSchema,...textTool} = tool;
  const f = await connect(t,{pages:[{tools:[textTool]}],result:{content:[{type:'text',text:'{"count":3}'}]}});
  const result = await run(await discover(f));
  assert.equal(result.errorCode,undefined);
  assert.deepEqual(result.content,{text:['{"count":3}'],structured:null});
});

test('Agent parallel dispatch shares one host MCP session', {timeout:3000}, async t => {
  const { Agent } = await import('purra');
  const f = await connect(t);
  f.state.gate = deferred();
  const catalog = await discover(f);let round=0;const toolIds=[];
  const agent = new Agent({tools:catalog.registrations,toolLimits:{maxConcurrency:2},model:{
    capabilities:fixture.testModelCapabilities,
    async invoke(request) {
      round++;
      toolIds.push(request.messages.filter(m=>m.role==='tool').map(m=>m.toolCallId));
      return {appliedGenerationLimit:request.outputBudget.maxGenerationTokens,
        message:round===1 ? {role:'assistant',content:'',toolCalls:[0,1,2].map(i=>({id:String(i),name:'search',arguments:{q:String(i)}}))} : {role:'assistant',content:'done'},
        finishReason:round===1?'tool_calls':'stop'};
    },
  }});
  const running = agent.invoke({messages:[{role:'user',content:'Read three documents'}]});
  while(f.calls.length<2)await new Promise(resolve=>setImmediate(resolve));
  assert.equal(f.calls.length,2);f.state.gate.resolve();await running;
  assert.deepEqual(toolIds,[[],['0','1','2'],['0','1','2']]);assert.equal(round,3);assert.equal(f.lists.length,1);assert.equal(f.calls.length,3);
});

for (const row of fixture.schemaCases) test(`selected schema metadata: ${row.id}`, async t => {
  const f = await connect(t, {pages:[{tools:[row.tool]}]});
  await assert.rejects(discover(f), errorCode(row.errorCode));
  assert.deepEqual(f.calls, []);
});

test('declared dialect keeps snapshot identity and argument validation', async t => {
  const f = await connect(t,{pages:[{tools:[fixture.declaredDialectTool]}]});
  const catalog = await discover(f);
  for (const key of ['inputSchema','outputSchema']) assert.equal(catalog.snapshot.entries[0][key].$schema,fixture.declaredDialectTool[key].$schema);
  assert.equal(Object.hasOwn(catalog.registrations[0].argumentContract.schema,'$schema'),false);
  assert.equal((await run(catalog,{q:'too long'})).errorCode,'mcp_invalid_arguments');
  assert.equal(f.calls.length,0);
  const result = await run(catalog);
  assert.equal(result.errorCode,undefined);assert.deepEqual(result.content.structured,{count:1});assert.equal(f.calls.length,1);
  const baseline = await discover(await connect(t));
  assert.notEqual(catalog.snapshot.revisionDigest,baseline.snapshot.revisionDigest);
});

test('declared output dialect still rejects wrong result', async t => {
  const f = await connect(t,{pages:[{tools:[fixture.declaredDialectTool]}],result:{content:[],structuredContent:{count:'wrong'}}});
  assert.equal((await run(await discover(f))).errorCode,'mcp_result_invalid');assert.equal(f.calls.length,1);
});

test('schema byte limit includes dialect declaration', async t => {
  const f = await connect(t,{pages:[{tools:[{name:'remote.search',inputSchema:{type:'object',$schema:'https://json-schema.org/draft/2020-12/schema'}}]}]});
  await assert.rejects(discover(f,{limits:{maxSchemaBytes:60}}),errorCode('mcp_schema_unsupported'));
  assert.equal(f.calls.length,0);
});
