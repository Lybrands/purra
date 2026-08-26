"""Server-owned Agent execution with replayable, disposable subscriptions."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from typing import Any, Protocol
from uuid import uuid4

from purra.agent_tree import AgentRunAggregation, RunTreeRepository
from purra.agent_tree_execution import (
    AgentTreeRunExecutor,
    _AgentTreeSchedulingCapability,
)
from purra.contracts import AgentRunRequest, AgentRunResult, RunId
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.execution.handle import AgentRunHandle
from purra.normalization import non_negative_int, required_text
from purra.output.contracts import AgentOutputEvent, OutputVisibility
from purra.output.ports import AgentOutputPublisher, AgentOutputRepository
from purra.ports.persistence import ExecutionLeaseStore


class AgentExecutionFactory(Protocol):
    def __call__(
        self,
        request: AgentRunRequest,
        options: object | None,
        signal: asyncio.Event,
    ) -> AsyncIterator[AgentEvent | AgentRunResult]: ...


class AgentRunSupervisor:
    """Own execution Tasks; subscriptions only observe the canonical journal."""

    def __init__(
        self,
        *,
        output_repository: AgentOutputRepository,
        output_publisher: AgentOutputPublisher,
        execution_factory: AgentExecutionFactory,
        lease_store: ExecutionLeaseStore | None = None,
        owner_id: str | None = None,
        lease_duration_ms: int = 30_000,
        poll_interval_seconds: float = 0.25,
    ) -> None:
        if not callable(execution_factory):
            raise TypeError("run supervisor requires an execution factory")
        self._repository = output_repository
        self._publisher = output_publisher
        self._execution_factory = execution_factory
        self._lease_store = lease_store
        self._owner_id = required_text(
            owner_id or f"supervisor-{uuid4().hex}",
            "execution owner id",
        )
        self._lease_duration_ms = int(lease_duration_ms)
        if self._lease_duration_ms <= 0:
            raise ValueError("run supervisor lease duration must be positive")
        self._poll_interval_seconds = max(
            0.01,
            float(poll_interval_seconds),
        )
        self._tasks: set[asyncio.Task[None]] = set()
        self._agent_tree: _AgentTreeSchedulingCapability | None = None

    def configure_agent_tree(
        self,
        repository: RunTreeRepository,
        executor: AgentTreeRunExecutor,
    ) -> None:
        """Attach tree scheduling to this execution owner exactly once."""

        if self._agent_tree is not None:
            raise RuntimeError("Agent tree scheduling is already configured")
        self._agent_tree = _AgentTreeSchedulingCapability(
            repository=repository,
            executor=executor,
            owner_id=self._owner_id,
            lease_duration_ms=self._lease_duration_ms,
        )

    async def execute_and_join(
        self,
        requester_run_id: str,
        run_ids: tuple[str, ...],
        signal: asyncio.Event | None = None,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> AgentRunAggregation:
        tree = self._agent_tree
        if tree is None:
            raise ContractViolationError(
                "Agent tree scheduling is not configured",
                code="agent_tree_unavailable",
            )
        return await tree.execute_and_join(
            requester_run_id,
            run_ids,
            signal,
            lease_owner_id=lease_owner_id,
            lease_epoch=lease_epoch,
        )

    async def submit(
        self,
        request: AgentRunRequest,
        *,
        options: object | None = None,
    ) -> AgentRunHandle:
        if not isinstance(request, AgentRunRequest):
            raise TypeError("run supervisor requires an AgentRunRequest")
        loop = asyncio.get_running_loop()
        ready: asyncio.Future[RunId] = loop.create_future()
        result: asyncio.Future[AgentRunResult] = loop.create_future()
        signal = asyncio.Event()
        task = asyncio.create_task(
            self._execute(request, options, signal, ready, result)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        run_id = await asyncio.shield(ready)
        return _SupervisedRunHandle(
            supervisor=self,
            run_id=run_id,
            signal=signal,
            result=result,
            task=task,
        )

    async def close(self) -> None:
        """Stop owned Tasks only during explicit host shutdown."""

        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute(
        self,
        request: AgentRunRequest,
        options: object | None,
        signal: asyncio.Event,
        ready: asyncio.Future[RunId],
        result_future: asyncio.Future[AgentRunResult],
    ) -> None:
        session: _LeaseSession | None = None
        result: AgentRunResult | None = None
        stream: AsyncIterator[AgentEvent | AgentRunResult] | None = None
        try:
            stream = self._execution_factory(request, options, signal)
            async for update in stream:
                update_run_id = str(getattr(update, "run_id", "") or "").strip()
                if update_run_id and not ready.done():
                    session = await self._bind_lease(update_run_id, signal)
                    ready.set_result(update_run_id)
                if isinstance(update, AgentRunResult):
                    result = update
            if not ready.done():
                raise ContractViolationError(
                    "supervised execution returned no run identity"
                )
            if result is None:
                raise ContractViolationError(
                    "supervised execution returned no terminal result"
                )
            if not result_future.done():
                result_future.set_result(result)
        except asyncio.CancelledError:
            error = ContractViolationError(
                "supervised execution was stopped by host shutdown"
            )
            _settle_future_exception(ready, error)
            _settle_future_exception(result_future, error)
            raise
        except BaseException as error:
            _settle_future_exception(ready, error)
            _settle_future_exception(result_future, error)
        finally:
            if stream is not None:
                with suppress(Exception):
                    await stream.aclose()
            if session is not None:
                await session.close()

    async def _bind_lease(
        self,
        run_id: RunId,
        signal: asyncio.Event,
    ) -> _LeaseSession | None:
        if self._lease_store is None:
            return None
        session = _LeaseSession(
            self._lease_store,
            owner_id=self._owner_id,
            lease_duration_ms=self._lease_duration_ms,
            poll_interval_seconds=self._poll_interval_seconds,
            signal=signal,
        )
        await session.bind(run_id)
        return session

    async def _subscribe(
        self,
        run_id: RunId,
        result_future: asyncio.Future[AgentRunResult],
        *,
        after_sequence: int,
    ) -> AsyncIterator[AgentOutputEvent]:
        cursor = non_negative_int(after_sequence, "output cursor")
        while True:
            events = await self._repository.list_events(
                run_id,
                after_sequence=cursor,
            )
            if events:
                for event in events:
                    if event.sequence <= cursor:
                        raise ContractViolationError(
                            "output repository returned a stale sequence"
                        )
                    cursor = event.sequence
                    if event.visibility is OutputVisibility.PUBLIC:
                        yield event
                continue
            if result_future.done():
                return

            published = asyncio.create_task(
                self._publisher.wait_for_sequence(
                    run_id,
                    after_sequence=cursor,
                )
            )
            completed = asyncio.ensure_future(asyncio.shield(result_future))
            try:
                await asyncio.wait(
                    (published, completed),
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for waiter in (published, completed):
                    if not waiter.done():
                        waiter.cancel()
                await asyncio.gather(
                    published,
                    completed,
                    return_exceptions=True,
                )


class _SupervisedRunHandle:
    def __init__(
        self,
        *,
        supervisor: AgentRunSupervisor,
        run_id: RunId,
        signal: asyncio.Event,
        result: asyncio.Future[AgentRunResult],
        task: asyncio.Task[None],
    ) -> None:
        self._supervisor = supervisor
        self._run_id = run_id
        self._signal = signal
        self._result = result
        self._task = task
        self._cancel_lock = asyncio.Lock()
        self._cancel_requested = False

    @property
    def run_id(self) -> RunId:
        return self._run_id

    def subscribe(
        self,
        after_sequence: int = 0,
    ) -> AsyncIterator[AgentOutputEvent]:
        return self._supervisor._subscribe(
            self._run_id,
            self._result,
            after_sequence=after_sequence,
        )

    async def wait(self) -> AgentRunResult:
        result = await asyncio.shield(self._result)
        await asyncio.shield(self._task)
        return result

    async def cancel(self, reason: str) -> None:
        required_text(reason, "run cancellation reason")
        async with self._cancel_lock:
            if self._result.done() or self._cancel_requested:
                return
            self._cancel_requested = True
            if self._supervisor._lease_store is not None:
                requested = await self._supervisor._lease_store.request_cancellation(
                    self._run_id
                )
                if not requested:
                    self._cancel_requested = False
                    raise ContractViolationError(
                        "run cancellation could not be persisted"
                    )
            self._signal.set()


class _LeaseSession:
    def __init__(
        self,
        store: ExecutionLeaseStore,
        *,
        owner_id: str,
        lease_duration_ms: int,
        poll_interval_seconds: float,
        signal: asyncio.Event,
    ) -> None:
        self._store = store
        self._owner_id = owner_id
        self._lease_duration_ms = lease_duration_ms
        self._poll_interval_seconds = poll_interval_seconds
        self._signal = signal
        self._run_id: RunId | None = None
        self._monitor: asyncio.Task[None] | None = None

    async def bind(self, run_id: RunId) -> None:
        state = await self._store.get(run_id)
        if state is None or state.owner_id != self._owner_id:
            claimed = await self._store.claim(
                run_id,
                self._owner_id,
                lease_duration_ms=self._lease_duration_ms,
            )
            if not claimed:
                raise ContractViolationError(
                    "run execution lease could not be acquired"
                )
        self._run_id = run_id
        self._monitor = asyncio.create_task(self._monitor_lease())

    async def close(self) -> None:
        monitor = self._monitor
        self._monitor = None
        if monitor is not None:
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor
        if self._run_id is not None:
            await self._store.release(self._run_id, self._owner_id)

    async def _monitor_lease(self) -> None:
        assert self._run_id is not None
        loop = asyncio.get_running_loop()
        heartbeat_interval = max(0.05, self._lease_duration_ms / 3_000)
        next_heartbeat = loop.time() + heartbeat_interval
        while True:
            await asyncio.sleep(self._poll_interval_seconds)
            state = await self._store.get(self._run_id)
            if (
                state is None
                or state.cancellation_requested_at_ms is not None
                or state.owner_id != self._owner_id
            ):
                self._signal.set()
                return
            if loop.time() < next_heartbeat:
                continue
            renewed = await self._store.renew(
                self._run_id,
                self._owner_id,
                lease_duration_ms=self._lease_duration_ms,
            )
            if not renewed:
                self._signal.set()
                return
            next_heartbeat = loop.time() + heartbeat_interval


def _settle_future_exception(
    future: asyncio.Future[Any],
    error: BaseException,
) -> None:
    if not future.done():
        future.set_exception(error)


__all__ = ["AgentExecutionFactory", "AgentRunSupervisor"]
