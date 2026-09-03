"""The sole owner of canonical Agent operation lifecycle timing."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from time import monotonic
from typing import Protocol
from uuid import uuid4

from purra.errors import ContractViolationError
from purra.normalization import required_text
from purra.operations.contracts import (
    OperationDisplay,
    OperationFinished,
    OperationKind,
    OperationReceipt,
    OperationScope,
    OperationStarted,
    OperationStatus,
)


class OperationEventProcessor(Protocol):
    async def accept_operation_event(
        self,
        event: OperationStarted | OperationFinished,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class _RunningOperation:
    receipt: OperationReceipt
    monotonic_started: float
    display: OperationDisplay


class AgentOperationController:
    """Persist one start and exactly one monotonic terminal per operation."""

    def __init__(
        self,
        output: OperationEventProcessor,
        *,
        wall_clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
    ) -> None:
        if not callable(getattr(output, "accept_operation_event", None)):
            raise TypeError("operation controller requires an output processor")
        self._output = output
        self._wall_clock = wall_clock or _utc_now
        self._monotonic_clock = monotonic_clock or monotonic
        self._running: dict[str, _RunningOperation] = {}
        self._terminal: set[str] = set()
        self._lock = asyncio.Lock()

    @property
    def running_operation_ids(self) -> tuple[str, ...]:
        return tuple(self._running)

    def with_output(self, output: OperationEventProcessor) -> AgentOperationController:
        """Bind Core persistence first while preserving a Host operation observer."""
        if self._output is output:
            return self
        observer = self._output

        class CombinedOutput:
            async def accept_operation_event(self, event):
                receipt = await output.accept_operation_event(event)
                await observer.accept_operation_event(event)
                return receipt

        return AgentOperationController(CombinedOutput(), wall_clock=self._wall_clock,
                                        monotonic_clock=self._monotonic_clock)

    async def start(
        self,
        kind: OperationKind,
        scope: OperationScope,
    ) -> OperationReceipt:
        if not isinstance(scope, OperationScope):
            raise TypeError("operation start requires an OperationScope")
        normalized_kind = OperationKind(kind)
        operation_id = f"operation-{uuid4().hex}"
        started_at = self._aware_wall_time()
        started = OperationStarted(
            operation_id=operation_id,
            run_id=scope.run_id,
            invocation_id=scope.invocation_id,
            kind=normalized_kind,
            started_at=started_at,
            parent_operation_id=scope.parent_operation_id,
            display=scope.display.as_mapping(),
        )
        receipt = OperationReceipt(
            operation_id=operation_id,
            kind=normalized_kind,
            run_id=scope.run_id,
            invocation_id=scope.invocation_id,
            started_at=started_at,
            started_event=started,
        )
        monotonic_started = self._monotonic_clock()
        async with self._lock:
            await self._output.accept_operation_event(started)
            self._running[operation_id] = _RunningOperation(
                receipt=receipt,
                monotonic_started=monotonic_started,
                display=scope.display,
            )
        return receipt

    async def succeed(
        self,
        operation_id: str,
        display: OperationDisplay | None = None,
    ) -> OperationFinished:
        return await self._finish(
            operation_id,
            OperationStatus.SUCCEEDED,
            display=display,
        )

    async def fail(
        self,
        operation_id: str,
        error_code: str,
        display: OperationDisplay | None = None,
    ) -> OperationFinished:
        return await self._finish(
            operation_id,
            OperationStatus.FAILED,
            error_code=required_text(error_code, "operation error code"),
            display=display,
        )

    async def cancel(
        self,
        operation_id: str,
        error_code: str = "operation_canceled",
        display: OperationDisplay | None = None,
    ) -> OperationFinished:
        return await self._finish(
            operation_id,
            OperationStatus.CANCELED,
            error_code=required_text(error_code, "operation error code"),
            display=display,
        )

    async def _finish(
        self,
        operation_id: str,
        status: OperationStatus,
        *,
        error_code: str | None = None,
        display: OperationDisplay | None = None,
    ) -> OperationFinished:
        normalized_id = required_text(operation_id, "operation id")
        if display is not None and not isinstance(display, OperationDisplay):
            raise TypeError("operation terminal display requires OperationDisplay")
        async with self._lock:
            if normalized_id in self._terminal:
                raise ContractViolationError(
                    f"operation {normalized_id!r} is already terminal"
                )
            running = self._running.get(normalized_id)
            if running is None:
                raise ContractViolationError(
                    f"operation {normalized_id!r} was not started"
                )
            duration_ms = max(
                0,
                round(
                    (self._monotonic_clock() - running.monotonic_started)
                    * 1_000
                ),
            )
            finished = OperationFinished(
                operation_id=normalized_id,
                run_id=running.receipt.run_id,
                invocation_id=running.receipt.invocation_id,
                parent_operation_id=running.receipt.started_event.parent_operation_id,
                status=status,
                finished_at=self._aware_wall_time(),
                duration_ms=duration_ms,
                error_code=error_code,
                display=(display or running.display).as_mapping(),
            )
            await self._output.accept_operation_event(finished)
            del self._running[normalized_id]
            self._terminal.add(normalized_id)
            return finished

    async def release_terminal_run(self, run_id: str) -> None:
        """Discard local handles after canonical Run terminalization fenced them.

        This emits no events and does not manufacture successful operations.
        The caller must already hold the repository's terminal Run receipt.
        """
        async with self._lock:
            for operation_id, running in tuple(self._running.items()):
                if running.receipt.run_id == run_id:
                    del self._running[operation_id]
                    self._terminal.add(operation_id)

    def _aware_wall_time(self) -> datetime:
        value = self._wall_clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ContractViolationError(
                "operation wall clock must return a timezone-aware datetime"
            )
        if value.utcoffset() is None:
            raise ContractViolationError(
                "operation wall clock must return a timezone-aware datetime"
            )
        return value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


__all__ = ["AgentOperationController", "OperationEventProcessor"]
