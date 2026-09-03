"""Run a deterministic PurrA Agent with one host-provided Retriever."""

from __future__ import annotations

import asyncio
from dataclasses import replace

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
    ToolCallDelta,
)
from purra.model_protocol import generic_capability_snapshot
from purra.retrieval import RetrievalHit, RetrieverTool
from purra.tools import InMemoryToolCatalog


class LocalModelGateway:
    async def stream(self, messages, invocation, signal=None):
        del signal

        async def chunks():
            if not any(message.role is MessageRole.TOOL for message in messages):
                yield ModelStreamChunk(
                    tool_call_deltas=(ToolCallDelta(
                        index=0,
                        id="lookup-1",
                        name="searchKnowledge",
                        arguments_fragment='{"query":"status"}',
                    ),),
                    finish_reason=ModelFinishReason.TOOL_CALLS,
                )
                return
            yield ModelStreamChunk(
                content_delta="PurrA is ready.",
                finish_reason=ModelFinishReason.STOP,
            )

        return ModelStream(
            chunks=chunks(),
            model="local-example",
            applied_output_limit=invocation.output_limit.max_tokens,
        )

    async def complete(self, messages, invocation, signal=None):
        del messages, signal
        return ModelCompletion(
            message=AgentMessage(
                role=MessageRole.ASSISTANT,
                content="PurrA is ready.",
            ),
            model="local-example",
            applied_output_limit=invocation.output_limit.max_tokens,
            finish_reason=ModelFinishReason.STOP,
        )


class LocalRetriever:
    async def retrieve(self, request, signal=None):
        del request, signal
        return (RetrievalHit(
            id="status",
            content="PurrA is ready.",
            source="local-example",
        ),)


async def main() -> None:
    retriever_tool = RetrieverTool(
        retriever=LocalRetriever(),
        name="searchKnowledge",
        description="Search the configured local knowledge source.",
        scope={"namespace": "example.quickstart"},
    )
    catalog = InMemoryToolCatalog((retriever_tool.registration,))
    adapters = InMemoryAgentAdapters()
    agent = AgentCore(
        model_gateway=LocalModelGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=AgentPreset(
            runtime_limits=RuntimeLimits(max_run_output_tokens=None),
            id="quickstart",
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
            model="local-example",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="local:example",
                max_call_output_tokens=256,
            ),
            options={"max_tokens": 128},
        ),
        domain_context=DomainContext(namespace="example.quickstart"),
        context_window=65_536,
        tools_enabled=True,
    )

    try:
        handle = await agent.submit(request)
        result = await handle.wait()
        events = await adapters.outputs.list_events(
            handle.run_id,
            after_sequence=0,
        )
    finally:
        await agent.close()

    if result.final_response != "PurrA is ready.":
        raise RuntimeError(
            f"unexpected example result: status={result.status.value} "
            f"error={result.error!r} response={result.final_response!r}"
        )
    print("events:", ", ".join(
        event.kind.value
        for event in events
        if event.visibility.value == "public"
    ))
    print("result:", result.final_response)


if __name__ == "__main__":
    asyncio.run(main())
