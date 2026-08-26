"""Run a canonical Child Agent through the installed Python SDK."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace
from importlib.metadata import version
from pathlib import Path

import purra
from purra.api import (
    AgentCore,
    AgentPreset,
    DelegationPolicy,
    InMemoryAgentAdapters,
)
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


SCENARIO = "canonical-child-agent"
PARENT_SECRET = "parent-private-secret"
CHILD_INSTRUCTION = "Inspect child evidence."


async def main() -> None:
    expected_site = Path(os.environ["PURRA_EXPECTED_SITE"]).resolve()
    package_path = Path(purra.__file__).resolve()
    if expected_site not in package_path.parents:
        raise RuntimeError(
            f"purra was not imported from {expected_site}: {package_path}"
        )

    root_model_calls = 0
    child_model_calls = 0
    tool_calls: list[dict[str, object]] = []

    class LocalChildAgentGateway:
        async def stream(self, messages, invocation, signal=None):
            nonlocal root_model_calls, child_model_calls
            del signal
            messages = tuple(messages)
            child = any(
                message.content == CHILD_INSTRUCTION for message in messages
            )
            message_text = "\n".join(str(message.content) for message in messages)
            has_tool_result = any(
                message.role is MessageRole.TOOL for message in messages
            )
            if child:
                child_model_calls += 1
                if PARENT_SECRET in message_text:
                    raise RuntimeError(
                        "child Agent received the parent's private prompt"
                    )
            else:
                root_model_calls += 1

            async def chunks():
                if not invocation.tools:
                    candidate = (
                        "child evidence ready"
                        if child
                        else "Parent received: child evidence ready."
                    )
                    if (
                        not any(message.content == candidate for message in messages)
                        or messages[-1].role is not MessageRole.DEVELOPER
                    ):
                        raise RuntimeError(
                            "child Agent public presentation context is incomplete"
                        )
                    yield ModelStreamChunk(
                        content_delta=(
                            "child evidence ready"
                            if child
                            else "Parent received: child evidence ready."
                        ),
                        finish_reason=ModelFinishReason.STOP,
                    )
                    return

                if child:
                    if tuple(tool.name for tool in invocation.tools) != ("lookup",):
                        raise RuntimeError(
                            "child Agent did not receive exactly the allowed read tool"
                        )
                    if not has_tool_result:
                        yield ModelStreamChunk(
                            tool_call_deltas=(ToolCallDelta(
                                index=0,
                                id="child-lookup",
                                name="lookup",
                                arguments_fragment='{"key":"child-status"}',
                            ),),
                            finish_reason=ModelFinishReason.TOOL_CALLS,
                        )
                        return
                    yield ModelStreamChunk(
                        content_delta="child evidence ready",
                        finish_reason=ModelFinishReason.STOP,
                    )
                    return

                if not has_tool_result:
                    yield ModelStreamChunk(
                        tool_call_deltas=(ToolCallDelta(
                            index=0,
                            id="delegate-child",
                            name="delegateToAgents",
                            arguments_fragment=json.dumps({
                                "delegations": [{
                                    "agentName": "evidence-reader",
                                    "title": "Evidence reader",
                                    "instruction": CHILD_INSTRUCTION,
                                    "objective": "Read and report the child status.",
                                }],
                            }, separators=(",", ":")),
                        ),),
                        finish_reason=ModelFinishReason.TOOL_CALLS,
                    )
                    return
                if "child evidence ready" not in message_text:
                    raise RuntimeError(
                        "parent Agent did not receive the child Agent result"
                    )
                yield ModelStreamChunk(
                    content_delta="Parent received: child evidence ready.",
                    finish_reason=ModelFinishReason.STOP,
                )

            return ModelStream(chunks=chunks(), model="local-child-agent-smoke")

        async def complete(self, messages, invocation, signal=None):
            del messages, invocation, signal
            return ModelCompletion(
                message=AgentMessage(
                    role=MessageRole.ASSISTANT,
                    content="unused",
                ),
                model="local-child-agent-smoke",
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
            description="Read one local child value.",
            parameters={
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
                "additionalProperties": False,
            },
        ),
        handler=lookup,
        policy=ToolPolicy(mode="read", title="Lookup"),
    ),))
    adapters = InMemoryAgentAdapters()
    agent = AgentCore(
        model_gateway=LocalChildAgentGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        run_tree_repository=adapters.run_tree,
        root_agent_id="sdk-parity-root-agent",
        preset=AgentPreset(
            id="sdk-parity-child-agent-smoke",
            revision="1",
            tool_catalog=catalog,
            delegation_policy=DelegationPolicy(
                max_agents_per_call=1,
                max_parallel=1,
            ),
        ),
    )
    request = AgentRunRequest(
        messages=(AgentMessage(
            role=MessageRole.USER,
            content=PARENT_SECRET,
        ),),
        model=ModelRequest(
            provider="local",
            model="local-child-agent-smoke",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="local:child-agent-smoke",
                max_output_tokens=256,
            ),
            options={"max_tokens": 128},
        ),
        domain_context=DomainContext(namespace="sdk.parity.child-agent-smoke"),
        context_window=65_536,
        tools_enabled=True,
    )

    try:
        handle = await agent.submit(request)
        result = await handle.wait()
        descendants = await adapters.run_tree.list_descendants(handle.run_id)
        journal = await adapters.outputs.list_root_events(
            handle.run_id,
            after_root_sequence=0,
        )
    finally:
        await agent.close()

    if (
        result.status is not RunStatus.DONE
        or result.final_response != "Parent received: child evidence ready."
    ):
        raise RuntimeError(f"unexpected Python parent Agent result: {result}")
    if (
        len(descendants) != 1
        or descendants[0].status.value != "done"
        or descendants[0].parent_run_id != handle.run_id
        or descendants[0].run_id == handle.run_id
    ):
        raise RuntimeError(
            "Python child Agent did not complete as a canonical Child Run"
        )
    if (
        any(
            event.root_sequence != index
            for index, event in enumerate(journal, start=1)
        )
        or not any(event.run_id == descendants[0].run_id for event in journal)
    ):
        raise RuntimeError("Python child Agent journal attribution is invalid")
    if root_model_calls != 2 or child_model_calls != 2 or len(tool_calls) != 1:
        raise RuntimeError("unexpected Python child Agent execution counts")

    print(json.dumps({
        "runtime": "python",
        "install": "wheel",
        "version": version("purra"),
        "scenario": SCENARIO,
        "status": "completed",
        "output": result.final_response,
        "modelCalls": root_model_calls + child_model_calls,
        "toolCalls": tool_calls,
        "childRuns": len(descendants),
    }, separators=(",", ":")))


if __name__ == "__main__":
    asyncio.run(main())
