// Installed-package smoke against a host-launched local stdio MCP server.
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { ListToolsRequestSchema, CallToolRequestSchema, ToolListChangedNotificationSchema } from '@modelcontextprotocol/sdk/types.js';
import { discoverMcpTools, McpCatalogMonitor } from 'purra-mcp';

if (process.argv[2] === 'serve') {
  const server = new Server({name:'installed-local-fixture',version:'1'},{capabilities:{tools:{}}});
  server.setRequestHandler(ListToolsRequestSchema,async () => ({tools:[{name:'count',inputSchema:{type:'object',properties:{},additionalProperties:false},
    outputSchema:{type:'object',properties:{count:{type:'integer'}},required:['count'],additionalProperties:false}}]}));
  server.setRequestHandler(CallToolRequestSchema,async request => {
    assert.equal(request.params.name,'count');assert.deepEqual(request.params.arguments,{});
    return {content:[],structuredContent:{count:3}};
  });
  await server.connect(new StdioServerTransport());
} else {
  const monitor = new McpCatalogMonitor();
  const transport = new StdioClientTransport({command:process.execPath,args:[fileURLToPath(import.meta.url),'serve']});
  transport.setProtocolVersion = version => monitor.acceptProtocolVersion(version);
  const client = new Client({name:'consumer',version:'1'});
  client.setNotificationHandler(ToolListChangedNotificationSchema,n => monitor.onNotification(n));
  client.onclose = () => monitor.close();
  try {
    await client.connect(transport);
    const catalog = await discoverMcpTools(client,'local',{count:{localName:'count',policy:{mode:'read',title:'Count'},scope:null}},{monitor});
    const result = await catalog.registrations[0].run({}, {call:{id:'1',name:'count',arguments:{}}});
    assert.equal(result.errorCode,undefined);
    assert.deepEqual(result.content.structured,{count:3});
    await client.ping();
    console.log('installed TypeScript MCP stdio smoke passed');
  } finally { await client.close(); monitor.close(); }
}
