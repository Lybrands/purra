"""Run the Python SDK as an installed black-box consumer."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace
from importlib.metadata import version
from pathlib import Path

import purra
from purra.api import AgentCore, AgentPreset, InMemoryAgentAdapters
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    DomainContext,
    MessageRole,
    ModelCompletion,
    ModelFinishReason,
    ModelRequest,
    ModelStream,
    ModelStreamChunk,
    RuntimeLimits,
    RunStatus,
    ToolCallDelta,
    ToolEffectState,
    ToolHandlerResult,
    ToolPolicy,
    ToolSchema,
)
from purra.model_protocol import generic_capability_snapshot
from purra.ports import ToolRegistration
from purra.tools import InMemoryToolCatalog


SCENARIO = "local-tool-roundtrip"


async def main() -> None:
    expected_site = Path(os.environ["PURRA_EXPECTED_SITE"]).resolve()
    package_path = Path(purra.__file__).resolve()
    if expected_site not in package_path.parents:
        raise RuntimeError(f"purra was not imported from {expected_site}: {package_path}")

    model_calls = 0
    tool_calls: list[dict[str, object]] = []

    class LocalModelGateway:
        async def stream(self, messages, invocation, signal=None):
            nonlocal model_calls
            del signal
            model_calls += 1

            async def chunks():
                if model_calls == 1:
                    yield ModelStreamChunk(
                        tool_call_deltas=(ToolCallDelta(
                            index=0,
                            id="lookup-1",
                            name="lookup",
                            arguments_fragment='{"key":"status"}',
                        ),),
                        finish_reason=ModelFinishReason.TOOL_CALLS,
                    )
                    return
                if not any(message.role is MessageRole.TOOL for message in messages):
                    raise RuntimeError("model did not receive the tool result")
                yield ModelStreamChunk(
                    content_delta="PurrA is ready.",
                    finish_reason=ModelFinishReason.STOP,
                )

            return ModelStream(
                chunks=chunks(),
                model="local-parity-smoke",
                applied_generation_limit=invocation.output_budget.max_generation_tokens,
            )

        async def complete(self, messages, invocation, signal=None):
            del messages, signal
            return ModelCompletion(
                message=AgentMessage(
                    role=MessageRole.ASSISTANT,
                    content="PurrA is ready.",
                ),
                model="local-parity-smoke",
                applied_generation_limit=invocation.output_budget.max_generation_tokens,
                finish_reason=ModelFinishReason.STOP,
            )

    async def lookup(state, arguments, signal=None):
        del state, signal
        result = {"key": arguments["key"], "value": "ready"}
        tool_calls.append({
            "name": "lookup",
            "arguments": dict(arguments),
            "result": result,
        })
        return ToolHandlerResult(
            json.dumps(result, separators=(",", ":")),
            effect_state=ToolEffectState.NOT_STARTED,
        )

    catalog = InMemoryToolCatalog((ToolRegistration(
        schema=ToolSchema(
            name="lookup",
            description="Look up one local value.",
            parameters={
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
                "additionalProperties": False,
            },
        ),
        handler=lookup,
        policy=ToolPolicy(mode="read", title="Look up"),
    ),))
    adapters = InMemoryAgentAdapters()
    agent = AgentCore(
        model_gateway=LocalModelGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=AgentPreset(
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            id="sdk-parity-smoke",
            revision="1",
            tool_catalog=catalog,
        ),
    )
    request = AgentRunRequest(
        messages=(AgentMessage(
            role=MessageRole.USER,
            content="Check the local status.",
        ),),
        model=ModelRequest(
            provider="local",
            model="local-parity-smoke",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="local:parity-smoke",
                max_generation_tokens=256,
            ),
            max_generation_tokens=128,
        ),
        domain_context=DomainContext(namespace="sdk.parity.smoke"),
        context_window=65_536,
        tools_enabled=True,
    )

    try:
        handle = await agent.submit(request)
        result = await handle.wait()
    finally:
        await agent.close()

    if result.status is not RunStatus.DONE or result.final_response != "PurrA is ready.":
        raise RuntimeError(f"unexpected Python Agent result: {result}")
    print(json.dumps({
        "runtime": "python",
        "install": "wheel",
        "version": version("purra"),
        "scenario": SCENARIO,
        "status": "completed",
        "output": result.final_response,
        "modelCalls": model_calls,
        "toolCalls": tool_calls,
    }, separators=(",", ":")))


if __name__ == "__main__":
    asyncio.run(main())
