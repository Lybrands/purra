import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path

import pytest
from mcp import ClientSession, types
from mcp.server import Server
from mcp.shared.memory import create_client_server_memory_streams
from purra.cancellation import OperationCanceled
from purra.contracts import ExecutionState, ToolPolicy
from purra_mcp import McpAdapterError, McpCatalogMonitor, McpToolBinding, McpToolLimits, discover_mcp_tools

FIXTURE = json.loads((Path(__file__).parents[2] / 'fixtures/tools.json').read_text())
TOOL = FIXTURE['tool']
BINDING = McpToolBinding('search', ToolPolicy(mode='read', title='Search'), None, True)


class ProtocolServer:
    def __init__(self, pages=None, result=None, malformed=False):
        self.server = Server('fixture')
        self.pages = pages or [{'tools': [TOOL]}]
        self.result = result or {'content': [], 'structuredContent': {'count': 1}}
        self.lists = []
        self.calls = []
        self.started = asyncio.Event()
        self.finished = asyncio.Event()
        self.gate = None
        self.changed = False
        self.rpc_error = False
        @self.server.list_tools()
        async def list_tools(request: types.ListToolsRequest):
            self.session = self.server.request_context.session
            self.lists.append(request.params.cursor if request.params else None)
            if self.changed:
                await self.session.send_tool_list_changed()
            return types.ListToolsResult(**self.pages[min(len(self.lists)-1, len(self.pages)-1)])
        async def call_tool(request):
            self.calls.append(request.params)
            self.started.set()
            try:
                if self.gate is not None:
                    await self.gate.wait()
                if self.rpc_error:
                    raise ValueError('private remote exception')
                if malformed:
                    return types.ServerResult.model_construct(root=types.CallToolResult.model_construct(**self.result))
                return types.ServerResult(types.CallToolResult(**self.result))
            finally:
                self.finished.set()
        self.server.request_handlers[types.CallToolRequest] = call_tool

    @asynccontextmanager
    async def connect(self):
        monitor = McpCatalogMonitor()
        async with create_client_server_memory_streams() as (client_streams, server_streams):
            self.server_write = server_streams[1]
            running = asyncio.create_task(self.server.run(*server_streams, self.server.create_initialization_options()))
            try:
                async with ClientSession(*client_streams, message_handler=monitor.on_message) as client:
                    initialized = await client.initialize()
                    monitor.accept_protocol_version(initialized.protocolVersion)
                    yield client, monitor
            finally:
                monitor.close()
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)


async def discover(client, monitor, **kwargs):
    return await discover_mcp_tools(client, 'fixture', {'remote.search': BINDING}, monitor=monitor, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize('row', FIXTURE['cases'], ids=lambda row: row['id'])
async def test_shared_result_boundary(row):
    fixture = ProtocolServer(result=row['result'], malformed=row.get('malformed',False))
    async with fixture.connect() as (client, monitor):
        limits = McpToolLimits(**{'max_result_bytes' if k == 'maxResultBytes' else 'max_content_blocks': v for k,v in row.get('limits', {}).items()})
        catalog = await discover(client, monitor, limits=limits)
        result = await catalog.registrations[0].handler(ExecutionState(), row['arguments'])
        assert result.error_code == row['errorCode']
        assert len(fixture.lists) == 1  # No hidden call_tool cache refresh.
        assert len(fixture.calls) == (0 if row['errorCode'] == 'mcp_invalid_arguments' else 1)
        if row['errorCode'] is None:
            assert json.loads(result.content) == {'text':[c['text'] for c in row['result']['content']], 'structured':row['result']['structuredContent']}
            assert fixture.calls[0].arguments == row['arguments']
        else:
            assert json.loads(result.content) == {'text':[], 'structured':None}
        await client.send_ping()  # Adapter leaves host session open.


@pytest.mark.asyncio
async def test_snapshot_allowlist_pagination_and_immutability():
    other = {'name':'unbound','inputSchema':{'type':'object','$ref':'https://example.invalid/private'}}
    fixture = ProtocolServer(pages=[{'tools':[other], 'nextCursor':'next'}, {'tools':[TOOL]}])
    async with fixture.connect() as (client, monitor):
        bindings = {'remote.search': BINDING}
        catalog = await discover_mcp_tools(client, 'fixture', bindings, monitor=monitor)
        bindings.clear()
        assert fixture.lists == [None, 'next']
        assert len(catalog.registrations) == 1
        assert catalog.snapshot.entries[0]['policy']['mode'] == 'read'
        assert catalog.registrations[0].concurrency_safe is True
        with pytest.raises(TypeError):
            catalog.snapshot.entries[0]['inputSchema']['required'][0] = 'other'
        assert catalog.snapshot.revision_digest == FIXTURE['revisionDigest']
        await fixture.session.send_tool_list_changed()
        for _ in range(100):
            if catalog.stale: break
            await asyncio.sleep(0)
        assert catalog.stale
        result = await catalog.registrations[0].handler(ExecutionState(), {'q':'a'})
        assert result.error_code == 'mcp_catalog_stale'
        assert not fixture.calls
        fresh = await discover(client, monitor)
        assert not fresh.stale
        assert fresh.snapshot.revision_digest == catalog.snapshot.revision_digest
        monitor.close()
        assert fresh.stale
        await client.send_ping()


@pytest.mark.asyncio
@pytest.mark.parametrize('pages,code', [
    ([{'tools':[TOOL, TOOL]}], 'mcp_name_conflict'),
    ([{'tools':[], 'nextCursor':'again'}], 'mcp_pagination_invalid'),
    ([{'tools':[]}], 'mcp_binding_missing'),
    ([{'tools':[{**TOOL, 'inputSchema':{'type':'object','$ref':'#/$defs/x'}}]}], 'mcp_schema_unsupported'),
    ([{'tools':[{**TOOL, 'execution':{'taskSupport':'required'}}]}], 'mcp_tool_unsupported'),
])
async def test_catalog_rejections(pages, code):
    fixture = ProtocolServer(pages=pages)
    async with fixture.connect() as (client, monitor):
        with pytest.raises(McpAdapterError) as error:
            await discover(client, monitor)
        assert error.value.code == code
        assert not fixture.calls


@pytest.mark.asyncio
async def test_preflight_policy_scope_and_stale_discovery():
    fixture = ProtocolServer()
    async with fixture.connect() as (client, monitor):
        with pytest.raises(McpAdapterError):
            McpToolBinding('write', ToolPolicy(mode='propose', title='Write'), None)
        with pytest.raises(McpAdapterError) as error:
            await discover_mcp_tools(client, 'fixture', {'remote.search': BINDING, 'other': BINDING}, monitor=monitor)
        assert error.value.code == 'mcp_name_conflict'
        assert not fixture.lists
        async def deny(state, arguments, signal):
            assert arguments['q'] == 'a'
            return 'no access'
        binding = McpToolBinding('search', ToolPolicy(mode='read', title='Search'), deny)
        catalog = await discover_mcp_tools(client,'fixture',{'remote.search':binding},monitor=monitor)
        assert (await catalog.registrations[0].handler(ExecutionState(), {'q':'a'})).error_code == 'mcp_scope_denied'
        assert not fixture.calls
        fixture.changed = True
        with pytest.raises(McpAdapterError) as error:
            await discover(client, monitor)
        assert error.value.code == 'mcp_catalog_stale'


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['cancel','timeout','stale','rpc','disconnect'])
async def test_inflight_boundaries(mode):
    fixture = ProtocolServer()
    fixture.gate = asyncio.Event()
    fixture.rpc_error = mode == 'rpc'
    async with fixture.connect() as (client, monitor):
        catalog = await discover(client, monitor, limits=McpToolLimits(timeout_ms=100 if mode == 'timeout' else 2000))
        signal = asyncio.Event()
        running = asyncio.create_task(catalog.registrations[0].handler(ExecutionState(), {'q':'a'}, signal))
        await asyncio.wait_for(fixture.started.wait(), 1)
        if mode == 'cancel': signal.set()
        elif mode == 'stale':
            await fixture.session.send_tool_list_changed()
            while not catalog.stale: await asyncio.sleep(0)
            fixture.gate.set()
        elif mode == 'rpc': fixture.gate.set()
        elif mode == 'disconnect': await fixture.server_write.aclose()
        if mode == 'cancel':
            with pytest.raises(OperationCanceled): await asyncio.wait_for(running, 1)
        else:
            result = await asyncio.wait_for(running, 2)
            assert result.error_code == {'timeout':'mcp_timeout','stale':'mcp_catalog_stale','rpc':'mcp_protocol_error','disconnect':'mcp_transport_error'}[mode]
            assert 'private' not in result.content
        fixture.gate.set()
        await asyncio.wait_for(fixture.finished.wait(), 1)
        assert len(fixture.calls) == 1
        if mode != 'disconnect': await client.send_ping()


@pytest.mark.asyncio
async def test_core_executor_strict_admission_before_any_remote_call():
    from purra.contracts import ToolBatchRequest, ToolCall
    from purra.tools import CoreToolExecutor, InMemoryToolCatalog
    fixture = ProtocolServer()
    async with fixture.connect() as (client, monitor):
        catalog = await discover(client, monitor)
        executor = CoreToolExecutor(InMemoryToolCatalog(catalog.registrations))
        class Sink:
            async def emit(self, event): pass
        sink = Sink()
        def batch(calls):
            return ToolBatchRequest('run',tuple(ToolCall(str(i),'search',args) for i,args in enumerate(calls)),frozenset({'search'}),ExecutionState())
        for invalid in ['{"q":"a","tags":"[\\"x\\"]"}', '{"q":"a","q":"b"}']:
            result = await executor.execute_batch(batch(['{"q":"a"}',invalid]),sink)
            assert result.outcome.value == 'failed'
            assert all(row.error == 'invalid_tool_arguments_schema' for row in result.results)
            assert not fixture.calls
        result = await executor.execute_batch(batch(['{"q":"😀","n":1.0}']),sink)
        assert result.outcome.value == 'completed'
        assert len(fixture.calls) == 1
        assert fixture.calls[0].arguments == {'q':'😀','n':1}


@pytest.mark.asyncio
@pytest.mark.parametrize('limit', [McpToolLimits(max_pages=1), McpToolLimits(max_tools=1), McpToolLimits(max_catalog_bytes=64), McpToolLimits(max_description_bytes=1)])
async def test_catalog_capacity_is_bounded(limit):
    pages = [{'tools':[TOOL], 'nextCursor':'next'}, {'tools':[{'name':'other','inputSchema':{'type':'object'}}]}]
    fixture = ProtocolServer(pages=pages)
    async with fixture.connect() as (client, monitor):
        with pytest.raises(McpAdapterError) as error:
            await discover(client, monitor, limits=limit)
        assert error.value.code == 'mcp_catalog_limit_exceeded'
        assert len(fixture.lists) <= 2
        assert not fixture.calls


@pytest.mark.asyncio
async def test_cancel_before_discovery_and_call_does_no_io():
    fixture = ProtocolServer()
    async with fixture.connect() as (client, monitor):
        signal = asyncio.Event(); signal.set()
        with pytest.raises(OperationCanceled):
            await discover(client, monitor, signal=signal)
        assert not fixture.lists
        catalog = await discover(client, monitor)
        with pytest.raises(OperationCanceled):
            await catalog.registrations[0].handler(ExecutionState(), {'q':'a'}, signal)
        assert not fixture.calls
        monitor.close()
        with pytest.raises(McpAdapterError) as error:
            await discover(client, monitor)
        assert error.value.code == 'mcp_connection_closed'
        assert len(fixture.lists) == 1


def test_unknown_negotiated_protocol_rejected():
    monitor = McpCatalogMonitor()
    with pytest.raises(McpAdapterError) as error:
        monitor.accept_protocol_version('2099-01-01')
    assert error.value.code == 'mcp_protocol_unsupported'


@pytest.mark.asyncio
async def test_snapshot_identity_tracks_selected_contract_not_discovery_order():
    other = {**TOOL, 'name':'remote.other'}
    bindings = {'remote.search':BINDING,'remote.other':McpToolBinding('another',ToolPolicy(mode='read',title='Other'),None)}
    digests = []
    for tools in ([TOOL,other],[other,TOOL],[other,{**TOOL,'description':'Changed description'}]):
        fixture = ProtocolServer(pages=[{'tools':tools}])
        async with fixture.connect() as (client, monitor):
            catalog = await discover_mcp_tools(client,'fixture',bindings,monitor=monitor)
            assert [entry['localName'] for entry in catalog.snapshot.entries] == ['another','search']
            digests.append(catalog.snapshot.revision_digest)
    assert digests[0] == digests[1]
    assert digests[1] != digests[2]


@pytest.mark.asyncio
async def test_text_only_tool_does_not_guess_structured_json():
    fixture = ProtocolServer(pages=[{'tools':[{key:value for key,value in TOOL.items() if key != 'outputSchema'}]}],
        result={'content':[{'type':'text','text':'{"count":3}'}]})
    async with fixture.connect() as (client, monitor):
        result = await (await discover(client,monitor)).registrations[0].handler(ExecutionState(),{'q':'a'})
        assert result.error_code is None
        assert json.loads(result.content) == {'text':['{"count":3}'],'structured':None}


@pytest.mark.asyncio
async def test_core_parallel_calls_share_one_host_session():
    from purra.contracts import ToolBatchRequest, ToolCall, ToolExecutionLimits
    from purra.tools import CoreToolExecutor, InMemoryToolCatalog
    fixture = ProtocolServer(); fixture.gate = asyncio.Event()
    async with fixture.connect() as (client,monitor):
        catalog = await discover(client,monitor)
        executor = CoreToolExecutor(InMemoryToolCatalog(catalog.registrations),limits=ToolExecutionLimits(max_concurrency=2))
        class Sink:
            async def emit(self,event): pass
        request = ToolBatchRequest('parallel',tuple(ToolCall(str(i),'search',json.dumps({'q':str(i)})) for i in range(3)),frozenset({'search'}),ExecutionState())
        running = asyncio.create_task(executor.execute_batch(request,Sink()))
        async def two_calls():
            while len(fixture.calls)<2: await asyncio.sleep(0)
        await asyncio.wait_for(two_calls(),1)
        assert len(fixture.calls)==2
        fixture.gate.set()
        result = await asyncio.wait_for(running,1)
        assert result.outcome.value=='completed'
        assert [r.tool_call_id for r in result.results]==['0','1','2']
        assert len(fixture.lists)==1 and len(fixture.calls)==3

@pytest.mark.asyncio
@pytest.mark.parametrize('row', FIXTURE['schemaCases'], ids=lambda row: row['id'])
async def test_selected_schema_metadata_is_not_silently_discarded(row):
    fixture = ProtocolServer(pages=[{'tools': [row['tool']]}])
    async with fixture.connect() as (client, monitor):
        with pytest.raises(McpAdapterError) as caught:
            await discover(client, monitor)
        assert caught.value.code == row['errorCode']
        assert fixture.calls == []
