"""Run-bound, one-shot in-memory approval broker for Core tools."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from uuid import uuid4

from purra.contracts import (
    ApprovalDecision,
    ApprovalRequest,
    ApprovalResult,
    ApprovalStatus,
    RunId,
)
from purra.events import AgentEvent, CoreEventType
from purra.ports import CancellationSignal, EventSink


@dataclass(slots=True)
class _PendingApproval:
    run_id: RunId
    approval_id: str
    future: asyncio.Future[ApprovalStatus]


class InMemoryApprovalGateway:
    """Keep live approvals isolated by run and consume every decision once."""

    def __init__(self) -> None:
        self._pending: dict[tuple[RunId, str], _PendingApproval] = {}
        self._closed = False

    async def request(
        self,
        run_id: RunId,
        approval: ApprovalRequest,
        event_sink: EventSink,
        signal: CancellationSignal | None = None,
    ) -> ApprovalResult:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return ApprovalResult(None, ApprovalStatus.UNAVAILABLE)
        if self._closed or (signal is not None and signal.is_set()):
            return ApprovalResult(None, ApprovalStatus.CANCELED)

        approval_id = str(uuid4())
        future: asyncio.Future[ApprovalStatus] = (
            asyncio.get_running_loop().create_future()
        )
        key = (normalized_run_id, approval_id)
        self._pending[key] = _PendingApproval(
            run_id=normalized_run_id,
            approval_id=approval_id,
            future=future,
        )

        signal_waiter: asyncio.Task[None] | None = None
        timeout_handle: asyncio.TimerHandle | None = None
        try:
            await event_sink.emit(AgentEvent(
                type=CoreEventType.APPROVAL_REQUESTED,
                run_id=normalized_run_id,
                payload={
                    "approvalId": approval_id,
                    "toolName": approval.tool_call.name,
                    "title": approval.title,
                    "riskLevel": approval.risk_level.value,
                    "summary": approval.summary,
                },
            ))
            # Every terminal source settles the same Future.  This makes the
            # first observed decision authoritative: a resolve that returns
            # APPROVED cannot later be rewritten to CANCELED by a same-tick
            # signal, and a cancellation/timeout rejects every late resolve.
            if signal is not None and not future.done():
                if signal.is_set():
                    self._settle(key, ApprovalStatus.CANCELED)
                else:
                    signal_waiter = asyncio.create_task(
                        self._settle_when_signaled(key, signal)
                    )
            if not future.done():
                timeout_handle = asyncio.get_running_loop().call_later(
                    max(0.0, float(approval.timeout_seconds)),
                    self._settle,
                    key,
                    ApprovalStatus.TIMED_OUT,
                )

            status = await asyncio.shield(future)

            await event_sink.emit(AgentEvent(
                type=CoreEventType.APPROVAL_RESOLVED,
                run_id=normalized_run_id,
                payload={
                    "approvalId": approval_id,
                    "toolName": approval.tool_call.name,
                    "status": status.value,
                },
            ))
            return ApprovalResult(approval_id, status)
        except asyncio.CancelledError:
            self._settle(key, ApprovalStatus.CANCELED)
            raise
        finally:
            if timeout_handle is not None:
                timeout_handle.cancel()
            if signal_waiter is not None:
                signal_waiter.cancel()
                with suppress(asyncio.CancelledError):
                    await signal_waiter
            self._pending.pop(key, None)
            if not future.done():
                future.cancel()

    async def _settle_when_signaled(
        self,
        key: tuple[RunId, str],
        signal: CancellationSignal,
    ) -> None:
        await signal.wait()
        self._settle(key, ApprovalStatus.CANCELED)

    def _settle(
        self,
        key: tuple[RunId, str],
        status: ApprovalStatus,
    ) -> ApprovalStatus | None:
        pending = self._pending.get(key)
        if pending is None or pending.future.done():
            return None
        pending.future.set_result(status)
        return status

    async def resolve(
        self,
        run_id: RunId,
        approval_id: str,
        decision: ApprovalDecision,
    ) -> ApprovalStatus | None:
        key = (str(run_id or "").strip(), str(approval_id or "").strip())
        normalized = ApprovalDecision(decision)
        status = (
            ApprovalStatus.APPROVED
            if normalized is ApprovalDecision.APPROVE
            else ApprovalStatus.REJECTED
        )
        return self._settle(key, status)

    async def cancel_pending(self, run_id: RunId) -> int:
        normalized_run_id = str(run_id or "").strip()
        count = 0
        for key in tuple(self._pending):
            if key[0] != normalized_run_id:
                continue
            if self._settle(key, ApprovalStatus.CANCELED) is not None:
                count += 1
        return count

    async def cancel_all(self) -> int:
        """Cancel every unresolved request owned by this gateway instance."""

        count = 0
        for key in tuple(self._pending):
            if self._settle(key, ApprovalStatus.CANCELED) is not None:
                count += 1
        return count

    async def close(self) -> int:
        """Permanently reject late requests and cancel every live approval."""

        self._closed = True
        return await self.cancel_all()

    def pending_count(self, run_id: RunId | None = None) -> int:
        if run_id is None:
            return sum(
                1 for pending in self._pending.values()
                if not pending.future.done()
            )
        normalized = str(run_id or "").strip()
        return sum(
            1
            for (pending_run_id, _), pending in self._pending.items()
            if pending_run_id == normalized and not pending.future.done()
        )
