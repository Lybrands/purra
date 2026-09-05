import asyncio
import json
from dataclasses import replace
from pathlib import Path

import httpx2
import pytest
from anthropic import AsyncAnthropic
from purra.cancellation import OperationCanceled
from purra.contracts import AgentMessage, ModelRequest, ModelInvocation, ToolSchema, ToolCallResult
from purra.model_protocol import generic_capability_snapshot, resolve_invocation_output_budget
from purra.runtime.model_round import ModelRoundAccumulator
from purra.runtime.tool_round import continuation_messages
from purra_anthropic import AnthropicMessagesGateway

FIXTURE = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/message.json').read_text())
MESSAGES = (AgentMessage('system', 'system'), AgentMessage('developer', 'developer'), AgentMessage('user', 'check'), AgentMessage('developer', 'runtime instruction'))


def invocation(options=None, **kwargs):
    model = ModelRequest(
        'anthropic',
        'fixture-model',
        replace(generic_capability_snapshot(), max_generation_tokens=8192),
        max_generation_tokens=4096,
        options=options or {},
    )
    return ModelInvocation(model, tools=(ToolSchema('lookup', 'lookup', {'type': 'object', 'properties': {'query': {'type': 'string'}}}), ToolSchema('ready', 'ready', {'type': 'object'})),
                           output_budget=resolve_invocation_output_budget(
                               model.capability_snapshot,
                               max_generation_tokens=model.max_generation_tokens,
                           ), **kwargs)


def sse(events):
    return ''.join('event: '+e['type']+'\ndata: '+json.dumps(e)+'\n\n' for e in events)


def gateway(http):
    return AnthropicMessagesGateway(AsyncAnthropic(api_key='fixture-not-a-key', http_client=http, max_retries=5))


@pytest.mark.asyncio
async def test_sdk_tool_round_checkpoint_signed_thinking_and_cache_accounting():
    bodies = []
    def respond(request):
        body = json.loads(request.content); bodies.append(body)
        assert request.url.path == '/v1/messages'
        return httpx2.Response(200, headers={'content-type': 'text/event-stream'}, text=sse(FIXTURE['events'])) if body.get('stream') else httpx2.Response(200, json=FIXTURE['response'])
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        model = gateway(http); call = invocation({'thinking': {'type': 'adaptive'}}, reasoning_mode='enabled')
        complete = await model.complete(MESSAGES, call)
        assert complete.message.content == '先查资料。' and complete.message.reasoning is None
        assert complete.finish_reason.value == 'tool_calls' and complete.applied_generation_limit == 4096
        assert complete.usage.input_tokens == 20 and complete.usage.generation_tokens == 20
        assert complete.usage.cached_input_tokens == 7 and complete.usage.total_tokens == 40
        stream = await model.stream(MESSAGES, call)
        acc = ModelRoundAccumulator(); chunks = []
        async for chunk in stream.chunks:
            chunks.append(chunk)
            if hasattr(chunk, 'finish_reason'): acc.add(chunk)
        assert 'private-thought' not in str(chunks[:-1]) and 'opaque-signature' not in str(chunks[:-1])
        calls, error = acc.tool_calls()
        assert not error
        assert [(c.id, c.name, json.loads(c.arguments_json)) for c in calls] == [(c.id, c.name, json.loads(c.arguments_json)) for c in complete.message.tool_calls]
        assert acc.provider_data == complete.message.provider_data
        from purra.api import AgentExecutionCheckpoint
        checkpoint = AgentExecutionCheckpoint(run_id='fixture', next_round=2, messages=(complete.message,), round_limit=4)
        saved = AgentExecutionCheckpoint.from_mapping(checkpoint.to_mapping()).messages[0]
        assert saved.provider_data == complete.message.provider_data
        followup = continuation_messages(calls, tuple(ToolCallResult(c.id, c.name, 'done') for c in calls),
                                         content=complete.message.content, reasoning='', provider_data=acc.provider_data)
        await model.complete((*MESSAGES, *followup), call)
        assert bodies[-1]['messages'][-2]['content'] == FIXTURE['response']['content']
        assert [b['tool_use_id'] for b in bodies[-1]['messages'][-1]['content']] == ['call_lookup', 'call_ready']
        assert bodies[-1]['system'] == [{'type':'text','text':'system'}, {'type':'text','text':'developer'}, {'type':'text','text':'runtime instruction'}]
        assert bodies[-1]['max_tokens'] == 4096 and bodies[-1]['tool_choice'] == {'type':'auto'}
        with pytest.raises(ValueError, match='does not match'):
            await model.complete((*MESSAGES, replace(saved, content='changed')), call)
        await model.close()
        assert not http.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize('reason,expected', [('end_turn','stop'), ('stop_sequence','stop'), ('max_tokens','length'), ('refusal','filtered'), ('model_context_window_exceeded','length')])
async def test_stop_reasons_and_missing_usage(reason, expected):
    response = {**FIXTURE['response'], 'content':[{'type':'text','text':'answer'}], 'stop_reason':reason, 'usage':None}
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(lambda _: httpx2.Response(200, json=response))) as http:
        result = await gateway(http).complete(MESSAGES, invocation())
        assert result.finish_reason.value == expected and result.usage is None


@pytest.mark.asyncio
async def test_sdk_errors_are_sanitized_without_hidden_retries():
    requests = []
    def respond(request):
        requests.append(request)
        return httpx2.Response(429, json={'type':'error','error':{'type':'rate_limit_error','message':'private-provider-error'}})
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        with pytest.raises(Exception) as error:
            await gateway(http).complete(MESSAGES, invocation())
        assert error.value.code == 'anthropic_http_429' and error.value.retryable
        assert 'private-provider-error' not in str(error.value) and len(requests) == 1


@pytest.mark.asyncio
async def test_truncated_stream_is_not_a_completion():
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(lambda _: httpx2.Response(200, headers={'content-type':'text/event-stream'}, text=sse(FIXTURE['events'][:-1])))) as http:
        with pytest.raises(Exception) as error:
            async for _ in (await gateway(http).stream(MESSAGES, invocation())).chunks: pass
        assert error.value.code == 'upstream_stream_interrupted'


@pytest.mark.asyncio
async def test_invalid_options_and_unused_or_precanceled_stream_do_not_dispatch():
    def respond(_): raise AssertionError('must not dispatch')
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        model = gateway(http)
        for call in [invocation({'thinking':{'type':'enabled','budget_tokens':4096}}), invocation({'thinking':{'type':'adaptive'}}, reasoning_mode='disabled'), invocation({'thinking':{'type':'adaptive'}}, tool_choice='required'), invocation({'vendor_option':True})]:
            with pytest.raises(Exception): await model.complete(MESSAGES, call)
        stream = await model.stream(MESSAGES, invocation())
        await stream.chunks.aclose()
        signal = asyncio.Event(); signal.set()
        with pytest.raises(OperationCanceled): await model.complete(MESSAGES, invocation(), signal)
        with pytest.raises(OperationCanceled):
            async for _ in (await model.stream(MESSAGES, invocation(), signal)).chunks: pass


@pytest.mark.asyncio
async def test_cancel_stalled_stream_closes_transport():
    class Body(httpx2.AsyncByteStream):
        closed = False
        stalled = asyncio.Event()
        async def __aiter__(self):
            yield sse(FIXTURE['events'][:4]).encode()
            self.stalled.set()
            await asyncio.Event().wait()
        async def aclose(self): self.closed = True
    body = Body()
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(lambda _: httpx2.Response(200, headers={'content-type':'text/event-stream'}, stream=body))) as http:
        signal = asyncio.Event()
        stream = await gateway(http).stream(MESSAGES, invocation(), signal)
        async def consume():
            async for _ in stream.chunks: pass
        task = asyncio.create_task(consume())
        await asyncio.wait_for(body.stalled.wait(), 2)
        signal.set()
        with pytest.raises(OperationCanceled): await asyncio.wait_for(task, 2)
        assert body.closed


@pytest.mark.asyncio
async def test_core_tool_round_keeps_thinking_private():
    from purra.api import AgentCore, AgentPreset, InMemoryAgentAdapters
    from purra.contracts import AgentRunRequest, DomainContext, RuntimeLimits
    from purra.retrieval import RetrieverTool, RetrievalHit
    from purra.tools import InMemoryToolCatalog
    class Retriever:
        async def retrieve(self, request, signal=None):
            return (RetrievalHit(id='one', content='found', source='fixture'),)
    requests = []
    def respond(request):
        body = json.loads(request.content); requests.append(body)
        if any(b.get('type') == 'tool_result' for m in body['messages'] for b in m['content']):
            events = [{'type':'message_start','message':{**FIXTURE['response'],'content':[],'stop_reason':None}},
                      {'type':'content_block_start','index':0,'content_block':{'type':'text','text':''}},
                      {'type':'content_block_delta','index':0,'delta':{'type':'text_delta','text':'Found it.'}},
                      {'type':'content_block_stop','index':0},
                      {'type':'message_delta','delta':{'stop_reason':'end_turn','stop_sequence':None},'usage':{'output_tokens':3}},
                      {'type':'message_stop'}]
        else:
            events = [e for e in FIXTURE['events'] if e.get('index') != 4]
        return httpx2.Response(200, headers={'content-type':'text/event-stream'}, text=sse(events))
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        adapters = InMemoryAgentAdapters()
        lookup = RetrieverTool(retriever=Retriever(), name='lookup', description='lookup', scope={})
        core = AgentCore(model_gateway=gateway(http), run_repository=adapters.runs,
                         output_repository=adapters.outputs, output_publisher=adapters.publisher,
                         preset=AgentPreset(id='anthropic-fixture', revision='1', tool_catalog=InMemoryToolCatalog((lookup.registration,)),
                                            runtime_limits=RuntimeLimits(max_run_generation_tokens=10000)))
        try:
            handle = await core.submit(AgentRunRequest(messages=(AgentMessage('user','lookup'),), model=invocation().request,
                                                      domain_context=DomainContext('fixture'), context_window=65536, tools_enabled=True))
            result = await handle.wait()
            assert result.final_response == 'Found it.', (result.status, result.error)
            assert any(b.get('signature') == 'opaque-signature' for m in requests[1]['messages'] for b in m['content'])
            events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
            public = str([e.payload for e in events if e.visibility.value == 'public'])
            assert all(secret not in public for secret in ('private-thought','opaque-signature','opaque-redacted'))
        finally:
            await core.close()
