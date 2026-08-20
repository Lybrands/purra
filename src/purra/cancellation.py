"""Core-owned cancellation primitives for adapter operations."""

from __future__ import annotations

import asyncio
from typing import Awaitable, TypeVar

from purra.errors import AgentCoreError
from purra.ports import CancellationSignal


T = TypeVar("T")


class OperationCanceled(AgentCoreError):
    """An in-flight Core operation was canceled by its run signal."""


def is_canceled(signal: CancellationSignal | None) -> bool:
    return bool(signal is not None and signal.is_set())


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
            raise OperationCanceled("agent run was canceled")

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
                raise OperationCanceled("agent run was canceled")
            operation.cancel()
            try:
                # Receipt-returning persistence adapters may suppress task
                # cancellation once their durable commit has begun.  In that
                # case the completed receipt wins; reporting cancellation
                # would invite a duplicate retry for an already-applied write.
                return await operation
            except asyncio.CancelledError:
                raise OperationCanceled("agent run was canceled") from None
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
