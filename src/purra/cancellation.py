"""Core-owned cancellation primitives for adapter operations."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import time
from types import MappingProxyType
from typing import Awaitable, TypeVar

from purra.errors import AgentCoreError, CodedAgentCoreError
from purra.ports import CancellationSignal


T = TypeVar("T")


class OperationCanceled(AgentCoreError):
    """An in-flight Core operation was canceled by its run signal."""


class ExecutionDeadlineExceeded(CodedAgentCoreError):
    """A Core-owned absolute execution deadline elapsed."""

    default_code = "execution_deadline_exceeded"


class ExecutionStopSignal:
    """Reason-aware signal that clips one absolute deadline to its parent."""

    def __init__(
        self,
        parent: CancellationSignal | None = None,
        *,
        deadline_at_ms: int | None = None,
        deadline_code: str = "execution_deadline_exceeded",
    ) -> None:
        self._parent = parent
        self._event = asyncio.Event()
        self._reason_code: str | None = None
        self._reason_details: Mapping[str, object] = MappingProxyType({})
        self._timer: asyncio.TimerHandle | None = None
        if deadline_at_ms is not None:
            remaining = max(0.0, (int(deadline_at_ms) - time.time() * 1000) / 1000)
            self._timer = asyncio.get_running_loop().call_later(
                remaining,
                self.set,
                deadline_code,
            )

    @property
    def reason_code(self) -> str | None:
        if self._parent is not None and self._parent.is_set():
            return str(
                getattr(self._parent, "reason_code", None) or "request_canceled"
            )
        return self._reason_code

    @property
    def reason_details(self) -> Mapping[str, object]:
        if self._parent is not None and self._parent.is_set():
            return MappingProxyType(dict(
                getattr(self._parent, "reason_details", {}) or {}
            ))
        return self._reason_details

    def is_set(self) -> bool:
        return self._event.is_set() or bool(
            self._parent is not None and self._parent.is_set()
        )

    def set(
        self,
        reason_code: str = "request_canceled",
        details: Mapping[str, object] | None = None,
    ) -> None:
        if self._event.is_set():
            return
        self._reason_code = str(reason_code or "request_canceled")
        self._reason_details = MappingProxyType(dict(details or {}))
        self._event.set()

    async def wait(self) -> bool:
        if self.is_set():
            return True
        local_waiter = asyncio.create_task(self._event.wait())
        if self._parent is None:
            return await local_waiter
        parent_waiter = asyncio.create_task(self._parent.wait())
        try:
            await asyncio.wait(
                {local_waiter, parent_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
            return True
        finally:
            for waiter in (local_waiter, parent_waiter):
                if not waiter.done():
                    waiter.cancel()
            await asyncio.gather(
                local_waiter,
                parent_waiter,
                return_exceptions=True,
            )

    def close(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None


def is_canceled(signal: CancellationSignal | None) -> bool:
    return bool(signal is not None and signal.is_set())


def stop_reason(signal: CancellationSignal | None) -> str:
    return str(getattr(signal, "reason_code", None) or "request_canceled")


def raise_if_stopped(signal: CancellationSignal | None) -> None:
    if is_canceled(signal):
        raise _stop_exception(signal)


def _stop_exception(signal: CancellationSignal | None) -> AgentCoreError:
    reason = stop_reason(signal)
    if reason.endswith("_deadline_exceeded"):
        return ExecutionDeadlineExceeded(
            "execution deadline was exceeded",
            code=reason,
            details=getattr(signal, "reason_details", None),
        )
    return OperationCanceled("agent run was canceled")


async def cancel_and_wait(task: asyncio.Future) -> None:
    if not task.done():
        task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _await_linearizable_completion(
    operation: asyncio.Future[T],
) -> T:
    """Forward repeated cancellation until an adapter gives its outcome."""

    while True:
        try:
            return await asyncio.shield(operation)
        except asyncio.CancelledError:
            if operation.cancelled():
                raise
            if operation.done():
                return operation.result()
            operation.cancel()


async def await_with_cancellation(
    awaitable: Awaitable[T],
    signal: CancellationSignal | None,
    *,
    completion_wins_after_cancel: bool = False,
) -> T:
    """Await an adapter operation while Core owns liveness and cleanup.

    ``completion_wins_after_cancel`` is an explicit persistence capability,
    not a generic tolerance for swallowed cancellation.  It is only valid when
    the adapter guarantees that cancellation either rolls back or returns an
    authoritative durable receipt once COMMIT has begun.
    """

    operation = asyncio.ensure_future(awaitable)
    cancel_waiter: asyncio.Task[bool] | None = None
    try:
        if signal is None:
            return await operation
        if signal.is_set():
            await cancel_and_wait(operation)
            raise _stop_exception(signal)

        cancel_waiter = asyncio.create_task(signal.wait())
        done, _ = await asyncio.wait(
            {operation, cancel_waiter},
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Once an operation has completed, its result wins: a tool side effect
        # may already have committed and must not be reported as canceled.
        if operation in done:
            return await operation
        if signal.is_set():
            if not completion_wins_after_cancel:
                await cancel_and_wait(operation)
                raise _stop_exception(signal)
            operation.cancel()
            try:
                # Receipt-returning persistence adapters may suppress task
                # cancellation once their durable commit has begun.  In that
                # case the completed receipt wins; reporting cancellation
                # would invite a duplicate retry for an already-applied write.
                return await operation
            except asyncio.CancelledError:
                raise _stop_exception(signal) from None
        return await operation
    except asyncio.CancelledError:
        if completion_wins_after_cancel:
            if not operation.done():
                operation.cancel()
            return await _await_linearizable_completion(operation)
        await cancel_and_wait(operation)
        raise
    finally:
        if cancel_waiter is not None:
            await cancel_and_wait(cancel_waiter)


__all__ = [
    "ExecutionDeadlineExceeded",
    "ExecutionStopSignal",
    "OperationCanceled",
    "await_with_cancellation",
    "is_canceled",
    "raise_if_stopped",
    "stop_reason",
]
