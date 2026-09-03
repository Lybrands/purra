"""Cancellation-linearizable execution of one Runtime tool batch."""

from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping, Sequence
from typing import Any, AsyncIterator

from purra.cancellation import OperationCanceled, is_canceled as _is_canceled
from purra.contracts import (
    AgentMessage,
    MessageOrigin,
    MessageRole,
    ToolBatchOutcome,
    ToolBatchRequest,
    ToolBatchResult,
    ToolCall,
    ToolCallResult,
)
from purra.events import AgentEvent
from purra.ports import CancellationSignal, ToolExecutionGateway


class _QueueEventSink:
    def __init__(self, queue: asyncio.Queue[AgentEvent]):
        self._queue = queue

    async def emit(self, event: AgentEvent) -> None:
        await self._queue.put(event)


async def stream_tool_batch(
    gateway: ToolExecutionGateway,
    request: ToolBatchRequest,
    signal: CancellationSignal | None,
) -> AsyncIterator[AgentEvent | ToolBatchResult]:
    queue: asyncio.Queue[AgentEvent] = asyncio.Queue()
    gateway_task = asyncio.create_task(
        gateway.execute_batch(request, _QueueEventSink(queue), signal)
    )
    progress_task: asyncio.Task[AgentEvent] | None = asyncio.create_task(queue.get())
    signal_task: asyncio.Task[bool] | None = (
        asyncio.create_task(signal.wait()) if signal is not None else None
    )
    gateway_terminal_selected = False
    try:
        while True:
            # A committed result/exception wins over same-tick progress and
            # cancellation. Progress already emitted by that gateway is still
            # drained before its terminal update.
            if gateway_task.done():
                gateway_terminal_selected = True
                terminal_result: ToolBatchResult | None = None
                terminal_error: BaseException | None = None
                try:
                    terminal_result = gateway_task.result()
                except BaseException as error:
                    terminal_error = error

                if progress_task is not None and progress_task.done():
                    try:
                        yield progress_task.result()
                    except BaseException:
                        pass
                    progress_task = None
                while not queue.empty():
                    yield queue.get_nowait()
                if terminal_error is not None:
                    raise terminal_error
                if terminal_result is None:
                    raise RuntimeError("tool gateway returned no result")
                yield terminal_result
                return

            # Progress wins over cancellation when both become observable in
            # the same event-loop turn.
            if progress_task is not None and progress_task.done():
                yield progress_task.result()
                progress_task = asyncio.create_task(queue.get())
                continue

            if _is_canceled(signal):
                # The gateway owns the authoritative tool outcome. A
                # cancellation-linearizable handler may already be committing;
                # after cancellation is forwarded, preserve a durable receipt
                # instead of unconditionally rewriting it as canceled.
                terminal_result = await _cancel_gateway_for_result(gateway_task)
                if terminal_result is None:
                    raise OperationCanceled("agent run was canceled")
                gateway_terminal_selected = True
                if progress_task is not None and progress_task.done():
                    try:
                        yield progress_task.result()
                    except BaseException:
                        pass
                    progress_task = None
                while not queue.empty():
                    yield queue.get_nowait()
                yield terminal_result
                return

            waiters: set[asyncio.Future[Any]] = {gateway_task}
            if progress_task is not None:
                waiters.add(progress_task)
            if signal_task is not None:
                waiters.add(signal_task)
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        # A caller cancellation is also forwarded to the gateway. If the
        # gateway suppresses it because a durable tool commit is already in
        # flight, finish delivering that authoritative receipt instead of
        # exposing a false non-commit to the caller.
        terminal_result = await _cancel_gateway_for_result(gateway_task)
        if terminal_result is None:
            raise
        gateway_terminal_selected = True
        if progress_task is not None and progress_task.done():
            try:
                yield progress_task.result()
            except BaseException:
                pass
            progress_task = None
        while not queue.empty():
            yield queue.get_nowait()
        yield terminal_result
        return
    finally:
        await _cancel_task_safely(progress_task)
        await _cancel_task_safely(signal_task)
        if not gateway_terminal_selected:
            await _cancel_task_safely(gateway_task)


async def _cancel_task_safely(task: asyncio.Future[Any] | None) -> None:
    """Cleanup must never replace the already selected runtime outcome."""

    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await task
    except BaseException:
        pass


async def _cancel_gateway_for_result(
    task: asyncio.Task[ToolBatchResult],
) -> ToolBatchResult | None:
    """Cancel a gateway but retain a commit-wins result if it returns one."""

    if not task.done():
        task.cancel()
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.cancelled():
                return None
            if task.done():
                return task.result()
            task.cancel()
        except BaseException:
            # Once the signal branch has selected cancellation, cleanup
            # failures are private unless the gateway returns an authoritative
            # ToolBatchResult receipt.
            return None


async def close_async_iterator(iterator: AsyncIterator) -> None:
    close = getattr(iterator, "aclose", None)
    if close is None:
        return
    try:
        await close()
    except (GeneratorExit, StopAsyncIteration):
        pass
    except Exception:
        # Stream termination has already been classified by the runtime. A
        # provider cleanup failure must not replace that public outcome.
        pass


def tool_round_trace_details(
    *,
    round_number: int,
    requested_names: Collection[str],
    allowed_names: Collection[str],
    calls: Sequence[ToolCall],
    batch_result: ToolBatchResult,
    evidence_record_count: int,
    evidence_tokens: int,
) -> dict[str, object]:
    return {
        "round": round_number,
        "requestedTools": sorted(requested_names),
        "allowedTools": sorted(allowed_names),
        "callCount": len(calls),
        "approvalStatuses": sorted({
            result.approval_status.value
            for result in batch_result.results
            if result.approval_status is not None
        }),
        "evidenceRecordCount": evidence_record_count,
        "evidenceTokens": evidence_tokens,
    }


def extend_progress_round_budget(
    outcome: ToolBatchOutcome,
    *,
    progress_rounds: int,
    round_limit: int,
    max_progress_rounds: int,
    round_number: int,
) -> tuple[int, int, dict[str, int] | None]:
    if (
        outcome is not ToolBatchOutcome.PROGRESSED
        or progress_rounds >= max_progress_rounds
    ):
        return progress_rounds, round_limit, None
    next_progress_rounds = progress_rounds + 1
    next_round_limit = round_limit + 1
    return next_progress_rounds, next_round_limit, {
        "round": round_number,
        "previousRoundLimit": round_limit,
        "roundLimit": next_round_limit,
        "progressRoundsUsed": next_progress_rounds,
        "maxProgressRounds": max_progress_rounds,
    }


def tool_call_payload(
    call: ToolCall,
    *,
    display_names: Mapping[str, str] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": call.id,
        "name": call.name,
        "arguments_json": call.arguments_json,
    }
    if display_names:
        payload["display_names"] = dict(display_names)
    return payload


def tool_result_payload(result: ToolCallResult) -> dict[str, object]:
    payload: dict[str, object] = {
        "tool_call_id": result.tool_call_id,
        "tool_name": result.tool_name,
        "content": result.content,
        "from_cache": result.from_cache,
        "planning_disposition": result.planning_disposition.value,
    }
    if result.approval_status is not None:
        payload["approval_status"] = result.approval_status.value
    if result.error is not None:
        payload["error"] = result.error
    return payload


def results_match_calls(
    calls: Sequence[ToolCall],
    results: Sequence[ToolCallResult],
) -> bool:
    return [call.id for call in calls] == [result.tool_call_id for result in results]


def continuation_messages(
    calls: Sequence[ToolCall],
    results: Sequence[ToolCallResult],
    *,
    content: str,
    reasoning: str,
    provider_data: Mapping[str, Any] | None = None,
) -> list[AgentMessage]:
    messages = [AgentMessage(
        role=MessageRole.ASSISTANT,
        content=content,
        reasoning=reasoning or None,
        tool_calls=tuple(calls),
        origin=MessageOrigin.MODEL,
        provider_data=provider_data or {},
    )]
    messages.extend(
        AgentMessage(
            role=MessageRole.TOOL,
            content=result.content,
            tool_call_id=result.tool_call_id,
            origin=MessageOrigin.HOST_TOOL_RESULT,
            host_metadata={"purra_tool_name": result.tool_name},
        )
        for result in results
    )
    return messages
