"""Ownership-safe async stream wrappers for PurrA and its adapters.

Python async generators do not execute their body or ``finally`` block when
``aclose()`` is called before the first ``anext()``. Provider resources are
already open at that point, so every transforming layer must explicitly own
and close the resource below it.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable
from typing import Any, Generic, TypeVar


T = TypeVar("T")
_CLOSE_TIMEOUT_SECONDS = 1.0


class OwnedAsyncIterator(AsyncIterator[T], Generic[T]):
    """Wrap a transforming iterator and own its underlying resources."""

    def __init__(
        self,
        iterator: AsyncIterator[T],
        *resources: Any,
        terminal_predicate: Callable[[T], bool] | None = None,
    ):
        self._iterator = iterator
        self._resources = tuple(resources)
        self._terminal_predicate = terminal_predicate
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def __aiter__(self) -> "OwnedAsyncIterator[T]":
        return self

    async def __anext__(self) -> T:
        if self._closed:
            raise StopAsyncIteration
        try:
            item = await anext(self._iterator)
            if (
                self._terminal_predicate is not None
                and self._terminal_predicate(item)
            ):
                await self.aclose()
            return item
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close_all())
            self._close_task.add_done_callback(_consume_task_result)

        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                await asyncio.shield(self._close_task)
                break
            except asyncio.CancelledError as error:
                if self._close_task.done():
                    raise
                if cancellation is None:
                    cancellation = error
        if cancellation is not None:
            raise cancellation

    async def _close_all(self) -> None:
        await close_async_resource(self._iterator)
        for resource in self._resources:
            if resource is self or resource is self._iterator:
                continue
            await close_async_resource(resource)


async def close_async_resource(resource: Any) -> None:
    """Close an async iterator or SDK stream without replacing its outcome."""

    close = getattr(resource, "aclose", None)
    if close is None:
        close = getattr(resource, "close", None)
    if close is None:
        return
    try:
        result = close()
        if inspect.isawaitable(result):
            close_task = asyncio.ensure_future(result)
            try:
                done, _ = await asyncio.wait(
                    {close_task},
                    timeout=_CLOSE_TIMEOUT_SECONDS,
                )
            except BaseException:
                close_task.cancel()
                close_task.add_done_callback(_consume_task_result)
                raise
            if close_task not in done:
                close_task.cancel()
                close_task.add_done_callback(_consume_task_result)
                return
            if close_task.cancelled():
                return
            await close_task
    except (GeneratorExit, StopAsyncIteration):
        pass
    except Exception:
        # Cleanup is private once the consumer selected a public outcome.
        pass


def _consume_task_result(task: asyncio.Future[Any]) -> None:
    if task.cancelled():
        return
    try:
        task.exception()
    except BaseException:
        pass


def openai_chunk_is_terminal(chunk: Any) -> bool:
    """Return whether a provider-shaped chunk carries an explicit finish."""

    if not isinstance(chunk, dict):
        return False
    choices = chunk.get("choices")
    if not isinstance(choices, list):
        return False
    return any(
        isinstance(choice, dict) and choice.get("finish_reason") is not None
        for choice in choices
    )


__all__ = [
    "OwnedAsyncIterator",
    "close_async_resource",
    "openai_chunk_is_terminal",
]
