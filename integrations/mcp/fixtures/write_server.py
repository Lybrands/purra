"""Independent synthetic MCP writer for installed-consumer fault checks."""
import asyncio
import json
import os
from pathlib import Path
import sys

from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server


async def main():
    directory, mode = Path(sys.argv[1]), sys.argv[2]
    assert mode in {'success', 'response-loss', 'exit-before', 'exit-after', 'error-after'}
    assert directory.is_dir()
    ledger = directory / 'remote.jsonl'
    def record(event):
        with ledger.open('a', encoding='utf-8') as file:
            file.write(json.dumps({'event': event, 'pid': os.getpid()})+'\n')
            file.flush(); os.fsync(file.fileno())
    record('started')
    server = Server('purra-synthetic-writer')
    @server.list_tools()
    async def tools():
        return [types.Tool(name='write', inputSchema={'type':'object','properties':{'value':{'type':'integer'}},'required':['value'],'additionalProperties':False},
            outputSchema={'type':'object','properties':{'written':{'type':'boolean'}},'required':['written'],'additionalProperties':False})]
    async def call(request):
        assert request.params.name == 'write' and request.params.arguments == {'value':42}
        record('received')
        if mode == 'exit-before': os._exit(42)
        # This fixture alone owns this temporary resource. No business paths are accepted.
        with (directory/'value.txt').open('x', encoding='utf-8') as file:
            file.write('42'); file.flush(); os.fsync(file.fileno())
        record('written')
        if mode == 'exit-after': os._exit(43)
        if mode == 'response-loss': await asyncio.Event().wait()
        record('responded')
        return types.ServerResult(types.CallToolResult(content=[], structuredContent={'written':True}, isError=mode=='error-after'))
    server.request_handlers[types.CallToolRequest] = call
    async with stdio_server() as streams:
        await server.run(*streams, server.create_initialization_options())


if __name__ == '__main__': asyncio.run(main())
