import asyncio
import json
from pathlib import Path

import httpx2
import pytest
from openai import AsyncOpenAI
from purra.cancellation import OperationCanceled
from purra.contracts import AgentMessage, ToolCallResult
from purra.runtime.model_round import ModelRoundAccumulator
from purra.runtime.tool_round import continuation_messages
from purra_openai import OpenAIChatCompletionsGateway
from test_gateway import invocation

FIXTURE = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/chat.json').read_text())
MESSAGES = (AgentMessage('developer', 'instructions'), AgentMessage('user','check'))


def sse(events): return ''.join('data: '+json.dumps(e)+'\n\n' for e in events)+'data: [DONE]\n\n'
def gateway(http): return OpenAIChatCompletionsGateway(AsyncOpenAI(api_key='fixture-not-a-key', http_client=http, max_retries=5))


@pytest.mark.asyncio
async def test_completion_stream_tool_round_and_trailing_usage():
    requests = []
    def respond(request):
        body=json.loads(request.content); requests.append(body)
        assert request.url.path == '/v1/chat/completions'
        return httpx2.Response(200, headers={'content-type':'text/event-stream'}, text=sse(FIXTURE['events'])) if body.get('stream') else httpx2.Response(200, json=FIXTURE['response'])
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        model=gateway(http); call=invocation()
        result=await model.complete(MESSAGES,call)
        assert result.usage.reasoning_tokens == 4 and result.message.reasoning is None
        stream=await model.stream(MESSAGES,call); acc=ModelRoundAccumulator(); terminal=[]
        async for chunk in stream.chunks:
            if hasattr(chunk,'finish_reason'):
                acc.add(chunk)
                if chunk.finish_reason: terminal.append(chunk)
        assert len(terminal)==1 and terminal[0].usage == result.usage
        calls,error=acc.tool_calls(); assert not error and calls==result.message.tool_calls
        followup=continuation_messages(calls,(ToolCallResult(calls[0].id,'lookup','done'),),content=result.message.content,reasoning='')
        await model.complete((*MESSAGES,*followup),call)
        assert requests[-1]['messages'][0]['role']=='developer'
        assert requests[-1]['messages'][-1]['tool_call_id']=='call_lookup'
        assert requests[-1]['max_completion_tokens']==128 and 'max_tokens' not in requests[-1]
        assert requests[-1]['store'] is False and requests[-1]['tools'][0]['function']['strict'] is False
        await model.close(); assert not http.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize('reason,expected',[('stop','stop'),('length','length'),('content_filter','filtered')])
async def test_stop_reasons_and_missing_usage(reason,expected):
    response={**FIXTURE['response'],'usage':None,'choices':[{'index':0,'message':{'role':'assistant','content':'answer'},'finish_reason':reason}]}
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(lambda _:httpx2.Response(200,json=response))) as http:
        result=await gateway(http).complete(MESSAGES,invocation())
        assert result.finish_reason.value==expected and result.usage is None


@pytest.mark.asyncio
async def test_stream_truncation_error_privacy_and_cancellation():
    attempts=[]
    def respond(request):
        attempts.append(request)
        if json.loads(request.content).get('stream'):
            return httpx2.Response(200,headers={'content-type':'text/event-stream'},text=sse(FIXTURE['events'][:2]))
        return httpx2.Response(429,json={'error':{'message':'private-provider-error'}})
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        model=gateway(http)
        with pytest.raises(Exception) as error: await model.complete(MESSAGES,invocation())
        assert error.value.code=='openai_http_429' and 'private-provider-error' not in str(error.value) and len(attempts)==1
        with pytest.raises(Exception) as error:
            async for _ in (await model.stream(MESSAGES,invocation())).chunks: pass
        assert error.value.code=='upstream_stream_interrupted'
        signal=asyncio.Event(); signal.set()
        with pytest.raises(OperationCanceled): await model.complete(MESSAGES,invocation(),signal)
        stream=await model.stream(MESSAGES,invocation()); await stream.chunks.aclose()
        assert len(attempts)==2
