"""Model provider ports."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from purra.contracts import (
    AgentMessage,
    ModelCompletion,
    ModelInvocation,
    ModelStream,
)


@runtime_checkable
class CancellationSignal(Protocol):
    def is_set(self) -> bool: ...

    async def wait(self) -> bool: ...


@runtime_checkable
class ModelGateway(Protocol):
    async def stream(
        self,
        messages: Sequence[AgentMessage],
        invocation: ModelInvocation,
        signal: CancellationSignal | None = None,
    ) -> ModelStream: ...

    async def complete(
        self,
        messages: Sequence[AgentMessage],
        invocation: ModelInvocation,
        signal: CancellationSignal | None = None,
    ) -> ModelCompletion: ...


__all__ = ["CancellationSignal", "ModelGateway"]
