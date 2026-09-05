"""Deterministic public-API example: managed planning, subscription, cancel, replay.

The fixture Gateway replaces an external Provider; no network or paid calls.
Production Gateways must stream actual Provider records, never invent progress.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import replace

from purra.api import AgentCore, AgentPlanner, InMemoryAgentAdapters
from purra.contracts import (
    AgentMessage, AgentRunRequest, DomainContext, ModelRequest, ModelStream,
    ModelStreamChunk, PlanningConstraints, PlanningMode, RuntimeLimits,
)
from purra.model_protocol import generic_capability_snapshot


class Policy:
    def planning_constraints(self, request, capabilities):
        return PlanningConstraints(allow_model_only_fallback=False)


class FixtureGateway:
    def __init__(self):
        self.release_plan = asyncio.Event()

    async def complete(self, *args, **kwargs):
        raise AssertionError("This example uses managed streams")

    async def stream(self, messages, invocation, signal=None):
        planning = any("planning component" in message.content for message in messages)

        async def chunks():
            if planning:
                yield ModelStreamChunk(content_delta=json.dumps({
                    "v": 1, "type": "progress", "text": "I will check the request's scope."
                }) + "\n")
                await self.release_plan.wait()
                yield ModelStreamChunk(content_delta=json.dumps({
                    "v": 1, "type": "plan", "plan": {
                        "needsTodos": True, "title": "Answer", "todos": [
                            {"id": "answer", "title": "Answer", "type": "review", "executor": "model"}
                        ]
                    }
                }) + "\n")
            else:
                yield ModelStreamChunk(content_delta="The answer is ready.")
            yield ModelStreamChunk(finish_reason="stop")

        return ModelStream(chunks=chunks(), model="fixture",
                           applied_generation_limit=invocation.output_budget.max_generation_tokens)


async def exercise() -> None:
    storage = InMemoryAgentAdapters()  # Replace BOTH repositories for durable storage.
    gateway = FixtureGateway()
    core = AgentCore(model_gateway=gateway, planner=AgentPlanner(gateway),
                     planning_policy=Policy(), run_repository=storage.runs,
                     output_repository=storage.outputs, output_publisher=storage.publisher,
                     runtime_limits=RuntimeLimits(max_run_generation_tokens=None))
    request = AgentRunRequest(
        messages=(AgentMessage(role="user", content="Give a concise answer."),),
        model=ModelRequest(provider="fixture", model="fixture",
            capability_snapshot=replace(generic_capability_snapshot(), profile_id="example:planner",
                                        max_generation_tokens=512)),
        domain_context=DomainContext(namespace="example"), context_window=32_768,
        planning_mode=PlanningMode.PLANNED,
    )
    try:
        handle = await core.submit(request)
        live = []
        async for event in handle.subscribe():  # Public canonical stream by default.
            live.append(event)
            if event.kind.value == "planning.progress":
                persisted = await storage.outputs.list_events(handle.run_id, after_sequence=0)
                assert event in persisted  # Durable receipt before notification.
                assert not gateway.release_plan.is_set()
                gateway.release_plan.set()
        assert (await handle.wait()).status.value == "done"
        assert live == [event async for event in handle.subscribe()]  # Same IDs/sequence on replay.
        snapshots = await storage.runs.get(handle.run_id)
        assert snapshots.steps[0].id == "answer"  # Complete, validated plan only.

        gateway.release_plan.clear()
        canceled = await core.submit(request)
        async for event in canceled.subscribe():
            if event.kind.value == "planning.progress":
                await canceled.cancel("example cancellation")
                break
        assert (await canceled.wait()).status.value == "canceled"
        history = [event async for event in canceled.subscribe()]
        gateway.release_plan.set()
        assert history == [event async for event in canceled.subscribe()]
    finally:
        await core.close()


if __name__ == "__main__":
    asyncio.run(exercise())
