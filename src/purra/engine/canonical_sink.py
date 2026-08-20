"""Canonical projection for Core-owned structured runtime events."""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from uuid import uuid4

from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.output.contracts import RuntimeOutputEvent
from purra.output.processor import AgentOutputProcessor


class BufferedEventSink:
    def __init__(
        self,
        output_processor: AgentOutputProcessor | None = None,
    ) -> None:
        self._events: deque[AgentEvent] = deque()
        self._output = output_processor

    async def emit(self, event: AgentEvent) -> None:
        if self._output is not None:
            run_id = str(event.run_id or "").strip()
            if not run_id:
                raise ContractViolationError(
                    "canonical runtime output requires a run id"
                )
            await self._output.accept_runtime_event(
                runtime_output_event(event, run_id)
            )
        self._events.append(event)

    def drain(self) -> tuple[AgentEvent, ...]:
        events = tuple(self._events)
        self._events.clear()
        return events


def runtime_output_event(event: AgentEvent, run_id: str) -> RuntimeOutputEvent:
    return RuntimeOutputEvent(
        event_id=f"core-{uuid4().hex}",
        run_id=run_id,
        event_type=str(event.type),
        payload=event.payload,
        occurred_at=datetime.now(timezone.utc),
    )


__all__ = ["BufferedEventSink", "runtime_output_event"]
