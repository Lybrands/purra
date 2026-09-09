"""A host-owned conversation can span completed Runs with different models."""
from dataclasses import replace

import pytest

from purra.api import (
    AgentCore, AgentPreset, InMemoryAgentAdapters, ModelRouteBinding,
    ModelRouteCandidate, ModelRouteRegistry,
)
from purra.contracts import AgentMessage, PlanningMode, RunStatus, RuntimeLimits
from purra.model_protocol import TaskCapabilityRequirements
from purra.tools import InMemoryToolCatalog
from test_standalone_agent_conformance import _Gateway, _request


@pytest.mark.asyncio
async def test_host_transfers_conversation_to_new_model_without_rebinding_old_run():
    adapters = InMemoryAgentAdapters()
    base = replace(_request(), session_id="conversation-1", planning_mode=PlanningMode.REACTIVE)
    hosts = []
    gateways = {}

    async def create(route):
        gateway = _Gateway(f"Answer from {route.binding_id}")
        gateways[route.binding_id] = gateway
        core = AgentCore(
            model_gateway=gateway, run_repository=adapters.runs,
            output_repository=adapters.outputs, output_publisher=adapters.publisher,
            preset=AgentPreset(
                id="conversation", revision="1", model_route=route,
                tool_catalog=InMemoryToolCatalog(()),
                runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            ),
        )
        hosts.append(core)
        return core, replace(base, model=replace(base.model, model=route.binding_id))

    registry = ModelRouteRegistry([
        ModelRouteBinding(ModelRouteCandidate(name, "1", name, base.model.capability_snapshot), create)
        for name in ("model-a", "model-b")
    ])
    history = (AgentMessage("user", "My project is called PURRA."),)
    try:
        first, request = await registry.create_new(["model-a"], TaskCapabilityRequirements("default"))
        handle_a = await first.submit(replace(request, messages=history))
        answer_a = await handle_a.wait()
        assert answer_a.status is RunStatus.DONE
        saved_a = await adapters.runs.get(handle_a.run_id)

        history += (AgentMessage("assistant", answer_a.final_response), AgentMessage("user", "Continue with that project."))
        second, request = await registry.create_new(["model-b"], TaskCapabilityRequirements("default"))
        handle_b = await second.submit(replace(request, messages=history))
        answer_b = await handle_b.wait()
        assert answer_b.status is RunStatus.DONE
        assert answer_b.final_response == "Answer from model-b"
        assert handle_a.run_id != handle_b.run_id
        assert await adapters.runs.get(handle_a.run_id) == saved_a
        saved_b = await adapters.runs.get(handle_b.run_id)
        assert request.session_id == "conversation-1"
        for saved, name in ((saved_a, "model-a"), (saved_b, "model-b")):
            assert saved.agent_preset_snapshot["composition"]["modelRoute"]["bindingId"] == name
            assert all(call.request.model == name for call in gateways[name].invocations)
        received = tuple(m for m in gateways["model-b"].messages[0] if m.role in ("user", "assistant"))
        assert received == history
    finally:
        for host in hosts:
            await host.close()
