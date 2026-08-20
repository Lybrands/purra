from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    AgentRunResult,
    DomainContext,
    ModelRequest,
    RunExecutionLease,
    RunStatus,
)
from purra.events import AgentEvent, CoreEventType
from purra.model_protocol import generic_capability_snapshot
from purra.output import (
    AgentOutputEvent,
    OutputChannel,
    OutputEventKind,
    OutputSource,
    OutputVisibility,
)


def _types():
    try:
        from purra.execution import AgentRunSupervisor
    except ImportError as error:
        pytest.fail(f"run supervisor is missing: {error}")
    return AgentRunSupervisor


def _request() -> AgentRunRequest:
    return AgentRunRequest(
        messages=(AgentMessage(role="user", content="run"),),
        model=ModelRequest(
            provider="test",
            model="model",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="test:model",
                max_output_tokens=1_024,
            ),
        ),
        domain_context=DomainContext(namespace="test"),
    )


class _Journal:
    def __init__(self) -> None:
        self.events: dict[str, list[AgentOutputEvent]] = {}

    async def list_events(self, run_id, *, after_sequence, limit=200):
        return tuple(
            event
            for event in self.events.get(run_id, ())
            if event.sequence > after_sequence
        )[:limit]

    def append(
        self,
        run_id: str,
        *,
        terminal: bool = False,
        visibility: OutputVisibility = OutputVisibility.PUBLIC,
    ):
        sequence = len(self.events.setdefault(run_id, [])) + 1
        now = datetime.now(timezone.utc)
        event = AgentOutputEvent(
            event_id=f"event-{sequence}",
            output_stream_id=None,
            run_id=run_id,
            turn_id=None,
            invocation_id=None,
            sequence=sequence,
            source=OutputSource.RUNTIME,
            kind=OutputEventKind.RUN_LIFECYCLE,
            channel=OutputChannel.LIFECYCLE,
            visibility=visibility,
            payload={"terminal": terminal},
            occurred_at=now,
            emitted_at=now,
        )
        self.events[run_id].append(event)
        return event


class _Publisher:
    def __init__(self, journal: _Journal) -> None:
        self._journal = journal
        self._condition = asyncio.Condition()

    async def publish_committed(self, event):
        del event
        async with self._condition:
            self._condition.notify_all()

    async def wait_for_sequence(self, run_id, *, after_sequence):
        async with self._condition:
            await self._condition.wait_for(
                lambda: any(
                    event.sequence > after_sequence
                    for event in self._journal.events.get(run_id, ())
                )
            )

    async def append(self, event):
        await self.publish_committed(event)


class _Execution:
    def __init__(self, journal: _Journal, publisher: _Publisher) -> None:
        self.journal = journal
        self.publisher = publisher
        self.release = asyncio.Event()
        self.signals = []
        self.terminal_count = 0

    def __call__(self, request, options, signal):
        del request, options
        self.signals.append(signal)
        return self.run(signal)

    async def run(self, signal):
        run_id = "run-1"
        first = self.journal.append(run_id)
        await self.publisher.append(first)
        yield AgentEvent(
            type=CoreEventType.RUN_STARTED,
            run_id=run_id,
        )
        while not self.release.is_set() and not signal.is_set():
            release_waiter = asyncio.create_task(self.release.wait())
            cancel_waiter = asyncio.create_task(signal.wait())
            done, pending = await asyncio.wait(
                (release_waiter, cancel_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            del done
            for waiter in pending:
                waiter.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        for _ in range(2):
            event = self.journal.append(run_id)
            await self.publisher.append(event)
        self.terminal_count += 1
        terminal = self.journal.append(run_id, terminal=True)
        await self.publisher.append(terminal)
        status = RunStatus.CANCELED if signal.is_set() else RunStatus.DONE
        yield AgentRunResult(
            run_id=run_id,
            status=status,
            final_response="done" if status is RunStatus.DONE else "",
        )


class _LeaseStore:
    def __init__(self) -> None:
        self.state = None
        self.cancel_requests = 0
        self.releases = 0

    async def claim(self, run_id, owner_id, *, lease_duration_ms):
        del lease_duration_ms
        self.state = RunExecutionLease(
            run_id=run_id,
            status=RunStatus.RUNNING,
            owner_id=owner_id,
            expires_at_ms=999_999_999_999,
            heartbeat_at_ms=1,
            attempt=1,
        )
        return True

    async def renew(self, run_id, owner_id, *, lease_duration_ms):
        del run_id, owner_id, lease_duration_ms
        return True

    async def release(self, run_id, owner_id):
        del run_id, owner_id
        self.releases += 1
        return True

    async def request_cancellation(self, run_id):
        del run_id
        self.cancel_requests += 1
        self.state = replace(
            self.state,
            cancellation_requested_at_ms=1,
        )
        return True

    async def get(self, run_id):
        del run_id
        return self.state


async def _fixture():
    journal = _Journal()
    publisher = _Publisher(journal)
    execution = _Execution(journal, publisher)
    supervisor = _types()(
        output_repository=journal,
        output_publisher=publisher,
        execution_factory=execution,
    )
    handle = await supervisor.submit(_request())
    return handle, execution


@pytest.mark.asyncio
async def test_closing_subscription_does_not_cancel_run():
    handle, execution = await _fixture()
    subscription = handle.subscribe(after_sequence=0)

    first = await anext(subscription)
    await subscription.aclose()

    assert first.sequence == 1
    assert not execution.signals[0].is_set()
    execution.release.set()
    result = await handle.wait()
    assert result.status is RunStatus.DONE


@pytest.mark.asyncio
async def test_reconnect_reads_only_events_after_cursor():
    handle, execution = await _fixture()
    first_subscription = handle.subscribe(after_sequence=0)
    first = await anext(first_subscription)
    await first_subscription.aclose()
    execution.release.set()
    await handle.wait()

    replayed = [event async for event in handle.subscribe(
        after_sequence=first.sequence
    )]

    assert [event.sequence for event in replayed] == [2, 3, 4]


@pytest.mark.asyncio
async def test_subscription_skips_private_events_without_stalling_cursor():
    handle, execution = await _fixture()
    first_subscription = handle.subscribe(after_sequence=0)
    first = await anext(first_subscription)
    await first_subscription.aclose()
    private = execution.journal.append(
        handle.run_id,
        visibility=OutputVisibility.PRIVATE,
    )
    await execution.publisher.append(private)
    execution.release.set()
    await handle.wait()

    replayed = [event async for event in handle.subscribe(
        after_sequence=first.sequence
    )]

    assert private.sequence not in [event.sequence for event in replayed]
    assert [event.sequence for event in replayed] == [3, 4, 5]


@pytest.mark.asyncio
async def test_explicit_cancel_produces_one_terminal():
    handle, execution = await _fixture()

    await handle.cancel("user_requested")
    await handle.cancel("user_requested")
    result = await handle.wait()
    replayed = [event async for event in handle.subscribe(after_sequence=0)]

    assert result.status is RunStatus.CANCELED
    assert execution.terminal_count == 1
    assert sum(bool(event.payload["terminal"]) for event in replayed) == 1


@pytest.mark.asyncio
async def test_explicit_cancel_is_persisted_before_execution_signal():
    journal = _Journal()
    publisher = _Publisher(journal)
    execution = _Execution(journal, publisher)
    leases = _LeaseStore()
    supervisor = _types()(
        output_repository=journal,
        output_publisher=publisher,
        execution_factory=execution,
        lease_store=leases,
        owner_id="owner-1",
        poll_interval_seconds=0.01,
    )
    handle = await supervisor.submit(_request())

    await handle.cancel("user_requested")
    result = await handle.wait()

    assert result.status is RunStatus.CANCELED
    assert leases.cancel_requests == 1
    assert execution.signals[0].is_set()
    assert leases.releases == 1
