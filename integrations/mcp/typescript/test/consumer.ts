import type { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { Agent, StructuredOutputContract, jsonIdentityDigest, type ToolDefinition } from 'purra';
import { McpCatalogMonitor, discoverMcpTools, type McpToolBinding, type McpToolLimits, type McpToolSnapshot } from 'purra-mcp';
export async function consumer(client: Client, monitor: McpCatalogMonitor) {
  const binding: McpToolBinding = {localName:'search',policy:{mode:'read',title:'Search'},scope:(args,context) => context.runId ? undefined : 'Missing Run',concurrencySafe:true};
  const limits: Partial<McpToolLimits> = {maxTools:8};
  const catalog = await discoverMcpTools(client,'docs',{search:binding},{monitor,limits});
  const snapshot: McpToolSnapshot = catalog.snapshot;
  const tools: readonly ToolDefinition[] = catalog.registrations;
  const agentOptions: Pick<ConstructorParameters<typeof Agent>[0], "tools" | "toolLimits"> = {tools,toolLimits:{maxConcurrency:2}};
  const output = await StructuredOutputContract.create({schemaId:'out',schemaVersion:'1',schema:{type:'object'}});
  return {snapshot,tools,agentOptions,digest:await jsonIdentityDigest(output.validateValue({ok:true}))};
}
void Agent;
