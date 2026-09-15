import asyncio
import json

import pytest

from purra.api import AgentCore, AgentPreset, AgentTreePolicy, InMemoryAgentAdapters
from purra.contracts import ModelStream, ModelStreamChunk, ModelFinishReason, ToolCallDelta, RuntimeLimits
from purra.tools import InMemoryToolCatalog
from test_second_host_conformance import _request


@pytest.mark.asyncio
async def test_model_reuses_agent_and_receives_prior_turns_without_new_identity():
    adapters = InMemoryAgentAdapters()
    seen = []
    class Gateway:
        async def complete(self, *args, **kwargs):
            raise AssertionError("stream required")
        async def stream(self, messages, invocation, signal=None):
            async def chunks():
                texts = [m.content for m in messages]
                if "SPECIALIST" in texts:
                    seen.append(texts)
                    if "Follow up" in texts:
                        assert any(text.startswith("First task") for text in texts)
                        assert "Remember cobalt" in texts
                        assert any('"source":"fixture"' in text for text in texts)
                    yield ModelStreamChunk(content_delta="Remember cobalt", finish_reason=ModelFinishReason.STOP)
                    return
                receipts = [json.loads(m.content) for m in messages if m.role.value == "tool"]
                if not receipts:
                    name, args = "delegateToAgents", {"children": [{"name": "specialist", "title": "Specialist", "instruction": "SPECIALIST", "objective": "First task", "input": {"source": "fixture"}}]}
                elif len(receipts) == 1:
                    name, args = "listAgents", {}
                elif len(receipts) == 2:
                    child = receipts[-1]["agents"][0]
                    assert child["contextVersion"] == 1
                    name, args = "continueAgent", {"agentId": child["agentId"], "expectedContextVersion": 1, "message": "Follow up"}
                else:
                    assert receipts[-1]["agents"][0]["contextVersion"] == 2
                    yield ModelStreamChunk(content_delta="Combined answer", finish_reason=ModelFinishReason.STOP)
                    return
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=0, id=f"call-{len(receipts)}", type="function", name=name, arguments_fragment=json.dumps(args)),), finish_reason=ModelFinishReason.TOOL_CALLS)
            return ModelStream(chunks=chunks(), model="operations-model", applied_generation_limit=invocation.output_budget.max_generation_tokens)
    core = AgentCore(model_gateway=Gateway(), run_repository=adapters.runs,
        output_repository=adapters.outputs, output_publisher=adapters.publisher, run_tree_repository=adapters.run_tree,
        preset=AgentPreset(id="reuse", revision="1", tool_catalog=InMemoryToolCatalog(()),
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None), agent_tree_policy=AgentTreePolicy()))
    try:
        handle = await core.submit(_request())
        result = await asyncio.wait_for(handle.wait(), 5)
        assert result.status.value == "done", result.error
        descendants = await adapters.run_tree.list_descendants(handle.run_id)
        assert len(descendants) == 2
        assert len({run.agent_id for run in descendants}) == 1
        assert len(seen) == 2
        assert result.final_response == "Combined answer"
    finally:
        await core.close()
