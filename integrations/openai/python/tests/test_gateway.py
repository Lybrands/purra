import asyncio
import json
from dataclasses import replace
from pathlib import Path

import httpx2
import pytest
from openai import AsyncOpenAI
from purra.contracts import AgentMessage, ModelRequest, ModelInvocation, ToolSchema, ToolHandlerResult, ToolCallResult
from purra.model_protocol import generic_capability_snapshot, resolve_invocation_output_limit
from purra.runtime.model_round import ModelRoundAccumulator
from purra.runtime.tool_round import continuation_messages
from purra_openai import OpenAIResponsesGateway

RESPONSE = json.loads((Path(__file__).resolve().parents[2] / "fixtures/response.json").read_text())


def invocation():
    model = ModelRequest("openai", "fixture-model", replace(generic_capability_snapshot(), max_call_output_tokens=256))
    return ModelInvocation(model, tools=(ToolSchema("lookup", "look up", {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}),),
                           output_limit=resolve_invocation_output_limit(model.capability_snapshot, 128))


def sse():
    events = [
        {"type": "response.reasoning_summary_text.delta", "delta": "private-working-text"},
        {"type": "response.output_item.added", "output_index": 1, "item": {**RESPONSE["output"][1], "arguments": ""}},
        {"type": "response.function_call_arguments.delta", "output_index": 1, "delta": '{"query":"中文"}'},
        {"type": "response.completed", "response": RESPONSE},
    ]
    return "".join("data: " + json.dumps(e) + "\n\n" for e in events)


@pytest.mark.asyncio
async def test_sdk_completion_stream_and_tool_continuation_preserve_private_state():
    requests = []
    def respond(request):
        body = json.loads(request.content)
        requests.append(body)
        return httpx2.Response(200, headers={"content-type": "text/event-stream"}, text=sse()) if body.get("stream") else httpx2.Response(200, json=RESPONSE)
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        gateway = OpenAIResponsesGateway(AsyncOpenAI(api_key="fixture-not-a-key", http_client=http))
        call = invocation()
        messages = (AgentMessage("user", "check"),)
        completion = await gateway.complete(messages, call)
        assert completion.finish_reason.value == "tool_calls"
        assert completion.applied_output_limit == 128 and completion.usage.reasoning_output_tokens == 6
        assert completion.message.reasoning is None
        stream = await gateway.stream(messages, call)
        accumulator = ModelRoundAccumulator()
        activity = []
        async for chunk in stream.chunks:
            if hasattr(chunk, "finish_reason"):
                accumulator.add(chunk)
            else:
                activity.append(chunk.kind.value)
            assert "private-working-text" not in str(chunk)
        assert activity == ["working"]
        from purra.api import AgentExecutionCheckpoint
        checkpoint = AgentExecutionCheckpoint(run_id="fixture", next_round=2, messages=(completion.message,), round_limit=4)
        assert AgentExecutionCheckpoint.from_mapping(checkpoint.to_mapping()).messages[0].provider_data == completion.message.provider_data
        calls, error = accumulator.tool_calls()
        assert not error and calls == completion.message.tool_calls
        followup = continuation_messages(calls, (ToolCallResult(calls[0].id, "lookup", "found"),),
                                         content="", reasoning="", provider_data=accumulator.provider_data)
        await gateway.complete((*messages, *followup), call)
        assert requests[-1]["input"][1]["encrypted_content"] == "opaque-encrypted-state"
        assert requests[-1]["input"][-1]["type"] == "function_call_output"
        assert requests[-1]["store"] is False and requests[-1]["max_output_tokens"] == 128
        assert requests[-1]["tools"][0]["strict"] is False


@pytest.mark.asyncio
async def test_sdk_does_not_hide_retries_or_provider_error_payloads():
    count = 0
    def respond(request):
        nonlocal count
        count += 1
        return httpx2.Response(429, json={"error": {"message": "private-provider-error", "type": "rate_limit_error"}})
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        gateway = OpenAIResponsesGateway(AsyncOpenAI(api_key="fixture-not-a-key", http_client=http, max_retries=5))
        with pytest.raises(Exception, match="OpenAI request failed") as error:
            await gateway.complete((AgentMessage("user", "check"),), invocation())
        assert count == 1 and "private-provider-error" not in str(error.value)
        assert error.value.code == "openai_http_429"


@pytest.mark.asyncio
async def test_cancel_before_call_does_not_dispatch():
    def respond(request):
        raise AssertionError("request must not be sent")
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        signal = asyncio.Event(); signal.set()
        gateway = OpenAIResponsesGateway(AsyncOpenAI(api_key="fixture-not-a-key", http_client=http))
        unused = await gateway.stream((AgentMessage("user", "check"),), invocation())
        await unused.chunks.aclose()
        with pytest.raises(Exception, match="canceled"):
            await gateway.complete((AgentMessage("user", "check"),), invocation(), signal)


@pytest.mark.asyncio
async def test_core_tool_round_keeps_encrypted_state_out_of_public_output():
    from purra.api import AgentCore, AgentPreset, InMemoryAgentAdapters
    from purra.contracts import AgentRunRequest, DomainContext
    from purra.retrieval import RetrieverTool, RetrievalHit
    from purra.tools import InMemoryToolCatalog
    from purra.contracts import RuntimeLimits
    class Retriever:
        async def retrieve(self, request, signal=None):
            return (RetrievalHit(id="one", content="found", source="fixture"),)
    requests = []
    def respond(request):
        body = json.loads(request.content); requests.append(body)
        if any(item.get("type") == "function_call_output" for item in body["input"]):
            final = {**RESPONSE, "output": [{"type": "message", "id": "msg_fixture", "role": "assistant", "status": "completed", "content": [{"type": "output_text", "text": "Found it.", "annotations": []}]}]}
            data = [{"type": "response.output_text.delta", "delta": "Found it."}, {"type": "response.completed", "response": final}]
            payload = "".join("data: " + json.dumps(e) + "\n\n" for e in data)
        else:
            payload = sse()
        return httpx2.Response(200, headers={"content-type": "text/event-stream"}, text=payload)
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        adapters = InMemoryAgentAdapters()
        lookup = RetrieverTool(retriever=Retriever(), name="lookup", description="lookup", scope={})
        core = AgentCore(model_gateway=OpenAIResponsesGateway(AsyncOpenAI(api_key="fixture-not-a-key", http_client=http)),
                         run_repository=adapters.runs, output_repository=adapters.outputs, output_publisher=adapters.publisher,
                         preset=AgentPreset(id="openai-fixture", revision="1", tool_catalog=InMemoryToolCatalog((lookup.registration,)),
                                            runtime_limits=RuntimeLimits(max_run_output_tokens=1000)))
        try:
            handle = await core.submit(AgentRunRequest(messages=(AgentMessage("user", "lookup"),), model=invocation().request,
                                                      domain_context=DomainContext("fixture"), context_window=65536, tools_enabled=True))
            result = await handle.wait()
            assert result.final_response == "Found it.", (result.status, result.error)
            assert any(item.get("encrypted_content") == "opaque-encrypted-state" for item in requests[1]["input"])
            events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
            assert all("opaque-encrypted-state" not in str(e.payload) for e in events if e.visibility.value == "public")
        finally:
            await core.close()
