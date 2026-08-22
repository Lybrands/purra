"""Run one Agent exclusively from an installed PurrA distribution."""

from __future__ import annotations

import asyncio
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
    RunStatus,
)
from purra.model_protocol import generic_capability_snapshot
from purra.tools import InMemoryToolCatalog


async def _chunks():
    yield ModelStreamChunk(
        content_delta="installed PurrA is runnable",
        finish_reason=ModelFinishReason.STOP,
    )


class _Gateway:
    async def stream(self, messages, invocation, signal=None):
        del messages, invocation, signal
        return ModelStream(chunks=_chunks(), model="smoke-model")

    async def complete(self, messages, invocation, signal=None):
        del messages, invocation, signal
        return ModelCompletion(
            message=AgentMessage(
                role=MessageRole.ASSISTANT,
                content="installed PurrA is runnable",
            ),
            model="smoke-model",
            finish_reason=ModelFinishReason.STOP,
        )


async def _run() -> None:
    package_path = Path(purra.__file__).resolve()
    assert "site-packages" in package_path.parts, package_path
    assert version("purra") == "0.1.1"

    adapters = InMemoryAgentAdapters()
    core = AgentCore(
        model_gateway=_Gateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=AgentPreset(
            id="installed-smoke",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
        ),
    )
    request = AgentRunRequest(
        messages=(AgentMessage(
            role=MessageRole.USER,
            content="Prove the installed Agent can run.",
        ),),
        model=ModelRequest(
            provider="smoke",
            model="smoke-model",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="smoke:model",
                max_output_tokens=1_024,
            ),
            options={"max_tokens": 256},
        ),
        domain_context=DomainContext(namespace="smoke"),
        context_window=8_192,
    )
    try:
        result = await (await core.submit(request)).wait()
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "installed PurrA is runnable"


if __name__ == "__main__":
    asyncio.run(_run())
