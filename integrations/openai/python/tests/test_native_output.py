import json
from dataclasses import replace
from pathlib import Path
import httpx2
import pytest
from openai import AsyncOpenAI
from purra.contracts import AgentMessage, ModelRequest
from purra.model_protocol import generic_capability_snapshot
from purra.model_execution import AgentModelTaskRunner, AgentModelTask
from purra.model_invocation import AgentModelInvocationManager, ModelInvocationContext
from purra.structured import StructuredOutputContract
from purra_openai import OpenAIResponsesGateway, OpenAIChatCompletionsGateway
SCHEMA = {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"], "additionalProperties": False}

@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["responses", "chat"])
async def test_native_sdk_wire_and_non_weakening_preflight(kind):
    requests = []
    def respond(request):
        body = json.loads(request.content); requests.append(body)
        if kind == "responses":
            response = json.loads((Path(__file__).resolve().parents[2] / "fixtures/response.json").read_text())
            response["output"] = [{"type": "message", "id": "msg", "role": "assistant", "status": "completed", "content": [{"type": "output_text", "text": ('{"ok":true}' if len(requests) == 1 else '{"ok":"wrong"}'), "annotations": []}]}]
        else:
            response = json.loads((Path(__file__).resolve().parents[2] / "fixtures/chat.json").read_text())["response"]
            response["choices"] = [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": ('{"ok":true}' if len(requests) == 1 else '{"ok":"wrong"}')}}]
        return httpx2.Response(200, json=response)
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(respond)) as http:
        gateway = (OpenAIResponsesGateway if kind == "responses" else OpenAIChatCompletionsGateway)( AsyncOpenAI(api_key="fixture-not-a-key", http_client=http))
        caps = generic_capability_snapshot()
        caps = replace(caps, max_generation_tokens=256, protocol=replace(caps.protocol, json_schema_level="json_schema"))
        model = ModelRequest("openai", "fixture-model", caps)
        runner = AgentModelTaskRunner(AgentModelInvocationManager(gateway), ModelInvocationContext("run"), model)
        output = StructuredOutputContract("test", "1", SCHEMA, mode="native_required")
        result = await runner.complete_structured((AgentMessage("user", "check"),), AgentModelTask(model), output=output)
        assert result.value == {"ok": True}
        assert result.receipt.output_contract["nativeDialect"] == "purra.openai-json-schema/v1"
        fmt = requests[0]["text"]["format"] if kind == "responses" else requests[0]["response_format"]["json_schema"]
        assert fmt["strict"] is True
        assert fmt["schema"] == SCHEMA
        with pytest.raises(Exception) as mismatch:
            await runner.complete_structured((AgentMessage("user", "check"),), AgentModelTask(model), output=output)
        assert mismatch.value.code == "structured_output_schema_mismatch"
        bad = StructuredOutputContract("test", "2", {**SCHEMA, "properties": {"ok": {"type": "string", "minLength": 1}}}, mode="native_required")
        with pytest.raises(Exception) as caught:
            await runner.complete_structured((), AgentModelTask(model), output=bad, repair_attempts=3)
        assert caught.value.code == "structured_output_mode_unsupported"
        assert len(requests) == 2


@pytest.mark.asyncio
async def test_shared_native_dialect_admission():
    from purra.contracts import ModelInvocation
    from purra.model_protocol import resolve_invocation_output_budget
    cases = json.loads((Path(__file__).resolve().parents[4] / "conformance/fixtures/native_output_schema.json").read_text())["cases"]
    def no_io(request):
        raise AssertionError("preflight cannot perform I/O")
    async with httpx2.AsyncClient(transport=httpx2.MockTransport(no_io)) as http:
        gateways = [OpenAIResponsesGateway(AsyncOpenAI(api_key="fixture-not-a-key", http_client=http)), OpenAIChatCompletionsGateway(AsyncOpenAI(api_key="fixture-not-a-key", http_client=http))]
        caps = generic_capability_snapshot()
        caps = replace(caps, max_generation_tokens=256, protocol=replace(caps.protocol, json_schema_level="json_schema"))
        model = ModelRequest("openai", "fixture-model", caps)
        for case in cases:
            output = StructuredOutputContract("fixture", "1", case["schema"], mode="native_required")
            call = ModelInvocation(model, output_budget=resolve_invocation_output_budget(caps, max_generation_tokens=None), output_contract=output)
            for gateway in gateways:
                if case["supported"]:
                    assert gateway.validate_output_contract(call) == "purra.openai-json-schema/v1", case["name"]
                else:
                    with pytest.raises(Exception) as caught:
                        gateway.validate_output_contract(call)
                    assert caught.value.code == "structured_output_mode_unsupported", case["name"]
