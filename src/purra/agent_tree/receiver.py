"""Own result subscriptions and their process-local execution tasks."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from collections.abc import Awaitable, Callable
from purra.agent_tree import AgentRunAggregation, RunTreeRepository
from purra.ports import CancellationSignal
from purra.cancellation import is_canceled
from purra.errors import ContractViolationError


class AgentResultsCanceled(Exception):
    code = "child_run_join_canceled"


class AgentResultReceiver:
    def __init__(self, repository: RunTreeRepository, join: Callable[..., Awaitable[AgentRunAggregation]]):
        self._repository = repository
        self._join = join
        self._receivers: dict[tuple[str, tuple[str, ...]], asyncio.Task] = {}
        self._received: dict[str, set[str]] = {}

    async def receive(
        self, requester_run_id: str, run_ids: tuple[str, ...],
        signal: CancellationSignal | None = None, *,
        after_run_ids: tuple[str, ...] = (),
        lease_owner_id: str | None = None, lease_epoch: int | None = None,
    ) -> AgentRunAggregation:
        """Receive available results at the caller's next decision boundary.

        The repository is the inbox source of truth. The execution task never
        invokes a presentation model or writes into the caller's message list.
        """
        ids = tuple(sorted(set(run_ids)))
        await self._repository.require_run_claim(requester_run_id, lease_owner_id=lease_owner_id, lease_epoch=lease_epoch)
        # Validate ancestry before starting execution or waiting.
        await self._repository.aggregate_runs(requester_run_id, ids)
        key = (requester_run_id, ids)
        task = self._receivers.get(key)
        if task is None:
            task = asyncio.create_task(self._join(
                requester_run_id, ids, signal,
                lease_owner_id=lease_owner_id, lease_epoch=lease_epoch,
            ))
            self._receivers[key] = task
        after = set(after_run_ids)
        while True:
            if is_canceled(signal):
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise AgentResultsCanceled()
            aggregate = await self._repository.aggregate_runs(requester_run_id, ids)
            available = tuple(item for item in aggregate.results if item["runId"] not in after)
            settled = task.done() or not aggregate.pending_run_ids
            if settled:
                await task
            if settled or available:
                self._received.setdefault(requester_run_id, set()).update(item["runId"] for item in available)
                return replace(aggregate, results=available)
            await asyncio.sleep(0.01)

    async def wait(self, requester_run_id: str) -> None:
        tasks = [task for (owner, _), task in self._receivers.items() if owner == requester_run_id]
        if tasks:
            await asyncio.gather(*tasks)

    def require_received(self, requester_run_id: str) -> None:
        expected = {identity for (owner, identities) in self._receivers
                    if owner == requester_run_id for identity in identities}
        if expected - self._received.get(requester_run_id, set()):
            raise ContractViolationError("Receive delegated results before finishing", code="agent_results_pending")

    async def close(self, requester_run_id: str | None = None) -> None:
        selected = [(key, task) for key, task in self._receivers.items()
                    if requester_run_id is None or key[0] == requester_run_id]
        for _, task in selected:
            if not task.done():
                task.cancel()
        await asyncio.gather(*(task for _, task in selected), return_exceptions=True)
        for key, _ in selected:
            self._receivers.pop(key, None)
        if requester_run_id is None:
            self._received.clear()
        else:
            self._received.pop(requester_run_id, None)

