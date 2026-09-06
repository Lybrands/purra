"""Installed-package smoke against a host-launched local stdio MCP server."""
import asyncio
import json
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client
from mcp.server import Server
from mcp.server.stdio import stdio_server
from purra.contracts import ExecutionState, ToolPolicy
from purra_mcp import McpCatalogMonitor, McpToolBinding, discover_mcp_tools


async def serve():
    server = Server('installed-local-fixture')
    @server.list_tools()
    async def tools():
        return [types.Tool(name='count', inputSchema={'type':'object','properties':{},'additionalProperties':False},
            outputSchema={'type':'object','properties':{'count':{'type':'integer'}},'required':['count'],'additionalProperties':False})]
    @server.call_tool()
    async def call(name, arguments):
        assert name == 'count' and arguments == {}
        return {'count':3}
    async with stdio_server() as streams:
        await server.run(*streams,server.create_initialization_options())


async def check():
    monitor = McpCatalogMonitor()
    params = StdioServerParameters(command=sys.executable,args=['-I',str(Path(__file__).resolve()),'serve'])
    try:
        async with stdio_client(params) as streams:
            async with ClientSession(*streams,message_handler=monitor.on_message) as client:
                initialized = await client.initialize()
                monitor.accept_protocol_version(initialized.protocolVersion)
                catalog = await discover_mcp_tools(client,'local',{'count':McpToolBinding('count',ToolPolicy(mode='read',title='Count'),None)},monitor=monitor)
                result = await catalog.registrations[0].handler(ExecutionState(),{})
                assert result.error_code is None
                assert json.loads(result.content)['structured'] == {'count':3}
                await client.send_ping()
        print('installed Python MCP stdio smoke passed')
    finally:
        monitor.close()


if __name__ == '__main__':
    asyncio.run(serve() if sys.argv[1:] == ['serve'] else check())
