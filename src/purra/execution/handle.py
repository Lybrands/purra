"""Stable handle contract for a server-owned Agent Run."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from purra.contracts import AgentRunResult, RunId
from purra.output.contracts import AgentOutputEvent


@runtime_checkable
class AgentRunHandle(Protocol):
    @property
    def run_id(self) -> RunId: ...

    def subscribe(
        self,
        after_sequence: int = 0,
    ) -> AsyncIterator[AgentOutputEvent]: ...

    async def wait(self) -> AgentRunResult: ...

    async def cancel(self, reason: str) -> None: ...


__all__ = ["AgentRunHandle"]
