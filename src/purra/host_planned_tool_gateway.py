"""Bypass model mediation for explicitly registered host-owned tool calls."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from uuid import uuid4

from purra.contracts import (
    AgentMessage,
    ModelCompletion,
    ModelFinishReason,
    ModelInvocation,
    ModelStream,
    ModelStreamChunk,
    ToolCallDelta,
    ToolChoiceMode,
)
from purra.model_call_parameters import (
    build_model_call_parameters,
    describe_model_call,
)
from purra.ports import CancellationSignal, ModelGateway


HOST_PLANNED_EXECUTION_ROUTE = "host_planned_tool"


class HostPlannedToolGateway:
    """Synthesize a native tool call only for an explicit host registration.

    This adapter is intentionally narrow: the current invocation must expose
    exactly one matching tool. All other model traffic is delegated unchanged.
    """

    def __init__(
        self,
        delegate: ModelGateway,
        arguments_by_tool: Mapping[str, Mapping[str, object]],
    ) -> None:
        self._delegate = delegate
        self._arguments_by_tool = {
            str(name): dict(arguments)
            for name, arguments in arguments_by_tool.items()
            if str(name).strip()
        }

    def _direct_tool(
        self,
        invocation: ModelInvocation,
    ) -> tuple[str, Mapping[str, object]] | None:
        if (
            invocation.tool_choice is not ToolChoiceMode.REQUIRED
            or len(invocation.tools) != 1
        ):
            return None
        name = invocation.tools[0].name
        arguments = self._arguments_by_tool.get(name)
        return (name, arguments) if arguments is not None else None

    def describe_invocation(
        self,
        messages: Sequence[AgentMessage],
        invocation: ModelInvocation,
    ) -> Mapping[str, object]:
        direct = self._direct_tool(invocation)
        if direct is None:
            return describe_model_call(self._delegate, messages, invocation)
        name, _arguments = direct
        return {
            **build_model_call_parameters(messages, invocation),
            "provider": "host",
            "upstreamProvider": invocation.request.provider,
            "executionRoute": HOST_PLANNED_EXECUTION_ROUTE,
            "toolNames": [name],
        }

    async def stream(
        self,
        messages: Sequence[AgentMessage],
        invocation: ModelInvocation,
        signal: CancellationSignal | None = None,
    ) -> ModelStream:
        direct = self._direct_tool(invocation)
        if direct is None:
            return await self._delegate.stream(messages, invocation, signal)
        name, arguments = direct
        arguments_json = json.dumps(
            arguments,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )

        async def _chunks():
            yield ModelStreamChunk(
                tool_call_deltas=(ToolCallDelta(
                    index=0,
                    id=f"host-plan-{uuid4().hex}",
                    type="function",
                    name=name,
                    arguments_fragment=arguments_json,
                ),),
                finish_reason=ModelFinishReason.TOOL_CALLS,
            )

        return ModelStream(
            chunks=_chunks(),
            model=invocation.request.model,
            metadata={"executionRoute": HOST_PLANNED_EXECUTION_ROUTE},
        )

    async def complete(
        self,
        messages: Sequence[AgentMessage],
        invocation: ModelInvocation,
        signal: CancellationSignal | None = None,
    ) -> ModelCompletion:
        return await self._delegate.complete(messages, invocation, signal)


__all__ = [
    "HOST_PLANNED_EXECUTION_ROUTE",
    "HostPlannedToolGateway",
]
