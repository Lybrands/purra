"""Offline response admission checks, also runnable from a clean installed wheel."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from importlib.metadata import version
from pathlib import Path

import purra
from purra.contracts import (
    AgentMessage, AgentRunResult, ModelFinishReason, ModelRequest, ModelStream,
    ModelStreamChunk, ReasoningMode, RunStatus,
)
from purra.model_invocation import AgentModelInvocationManager, ModelInvocationContext
from purra.model_protocol import generic_capability_snapshot
from purra.output import (
    AgentResponseTransaction, PublicFact, PublicFactBundle, PublicPresentationMode,
    ResponseTransactionMode, ResponseTransactionPolicy,
)


class _Gateway:
    def __init__(self):
        self.invocations = []

    async def complete(self, messages, invocation, signal=None):
        raise AssertionError("response must use streaming")

    async def stream(self, messages, invocation, signal=None):
        self.invocations.append(invocation)

        async def chunks():
            yield ModelStreamChunk(
                content_delta="Response ready", finish_reason=ModelFinishReason.STOP,
            )

        return ModelStream(
            chunks=chunks(), model=invocation.request.model,
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
        )


class _RecordingManager(AgentModelInvocationManager):
    def __init__(self, gateway):
        super().__init__(gateway)
        self.calls = []

    async def stream(self, messages, call, context, signal=None):
        self.calls.append((call, context))
        return await super().stream(messages, call, context, signal)


class _Facts:
    async def facts_for(self, run_id, result):
        return PublicFactBundle(facts=(PublicFact("summary", "Analysis committed"),))


async def verify_response_reasoning_mode(path: str, mode: ReasoningMode) -> None:
    gateway = _Gateway()
    manager = _RecordingManager(gateway)
    request = ModelRequest(
        provider="offline", model="fixture",
        capability_snapshot=replace(generic_capability_snapshot(), max_generation_tokens=256),
    )
    context = ModelInvocationContext("response-smoke", requested_reasoning_mode=mode)
    if path == "direct":
        transaction = AgentResponseTransaction(manager, policy=ResponseTransactionPolicy(
            mode=ResponseTransactionMode.DIRECT_LIVE,
        ))
        result = await transaction.execute_direct(
            (AgentMessage("user", "Reply"),), request=request, context=context,
        )
        response = result.final_response
    else:
        assert path == "presentation"
        transaction = AgentResponseTransaction(
            manager, facts_provider=_Facts(), policy=ResponseTransactionPolicy(
                mode=ResponseTransactionMode.VALIDATED_RESULT,
                public_presentation=PublicPresentationMode.MODEL_LIVE,
            ),
        )
        response = await transaction.present(
            AgentRunResult(run_id=context.run_id, status=RunStatus.DONE),
            request=request, context=context,
        )
    assert response == "Response ready"
    assert len(manager.calls) == len(gateway.invocations) == 1
    call, received_context = manager.calls[0]
    assert received_context is context
    assert call.reasoning_mode is context.requested_reasoning_mode is mode
    assert gateway.invocations[0].reasoning_mode is mode


async def _main() -> None:
    module_path = Path(purra.__file__).resolve()
    assert "site-packages" in module_path.parts, module_path
    print(f"Installed purra {version('purra')}: {module_path}")
    for path in ("direct", "presentation"):
        for mode in ReasoningMode:
            await verify_response_reasoning_mode(path, mode)
            print(f"PASS {path}/{mode.value} (offline Gateway)")


if __name__ == "__main__":
    asyncio.run(_main())
