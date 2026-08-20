from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from purra.contracts import (
    AgentDelegation,
    DelegationAggregation,
    DelegationContextMode,
    DelegationStatus,
    ExecutionState,
    RuntimeOutcome,
)
from purra.delegation import (
    DelegatedAgentRequest,
    DelegatedAgentResult,
    DelegationCoordinator,
    DelegationPolicy,
    build_delegation_tool_registration,
)
from purra.errors import ContractViolationError
from purra.operations import AgentOperationController
from purra.output import AgentOutputEvent
from purra.output.processor import AgentOutputProcessor


class _OutputRepository:
    def __init__(self) -> None:
        self.events: dict[str, list[AgentOutputEvent]] = {}

    async def append_event(self, draft):
        rows = self.events.setdefault(draft.run_id, [])
        now = datetime.now(timezone.utc)
        event = AgentOutputEvent(
            event_id=f"event-{len(rows) + 1}",
            output_stream_id=draft.output_stream_id,
            run_id=draft.run_id,
            turn_id=draft.turn_id,
            invocation_id=draft.invocation_id,
            sequence=len(rows) + 1,
            source=draft.source,
            kind=draft.kind,
            channel=draft.channel,
            visibility=draft.visibility,
            payload=draft.payload,
            occurred_at=draft.occurred_at,
            emitted_at=now,
        )
        rows.append(event)
        return event

    async def list_events(self, run_id, *, after_sequence, limit=200):
        return tuple(
            event
            for event in self.events.get(run_id, ())
            if event.sequence > after_sequence
        )[:limit]


class _Publisher:
    def __init__(self) -> None:
        self.events = []

    async def publish_committed(self, event):
        self.events.append(event)


class _Delegations:
    def __init__(self) -> None:
        self.rows: dict[str, AgentDelegation] = {}
        self.next_id = 1

    async def create(self, **values):
        delegation = AgentDelegation(
            id=f"delegation-{self.next_id}",
            **values,
        )
        self.next_id += 1
        self.rows[delegation.id] = delegation
        return delegation

    async def start(self, delegation_id, *, run_id, batch_id):
        row = self._owned(delegation_id, run_id, batch_id)
        if row.status is not DelegationStatus.QUEUED:
            return None
        row = replace(row, status=DelegationStatus.RUNNING)
        self.rows[row.id] = row
        return row

    async def complete(
        self,
        delegation_id,
        *,
        run_id,
        batch_id,
        result_summary,
    ):
        row = self._owned(delegation_id, run_id, batch_id)
        if row.status is not DelegationStatus.RUNNING:
            return False
        self.rows[row.id] = replace(
            row,
            status=DelegationStatus.DONE,
            result_summary=result_summary,
        )
        return True

    async def fail(self, delegation_id, *, run_id, batch_id, error):
        row = self._owned(delegation_id, run_id, batch_id)
        if row.status not in {DelegationStatus.QUEUED, DelegationStatus.RUNNING}:
            return False
        self.rows[row.id] = replace(
            row,
            status=DelegationStatus.FAILED,
            error=error,
        )
        return True

    async def cancel(self, delegation_id, *, run_id, batch_id, reason):
        row = self._owned(delegation_id, run_id, batch_id)
        if row.status not in {DelegationStatus.QUEUED, DelegationStatus.RUNNING}:
            return False
        self.rows[row.id] = replace(
            row,
            status=DelegationStatus.CANCELED,
            error=reason,
        )
        return True

    async def list_for_run(self, run_id):
        return tuple(row for row in self.rows.values() if row.run_id == run_id)

    async def aggregate_batch(self, run_id, batch_id):
        rows = tuple(
            row
            for row in self.rows.values()
            if row.run_id == run_id and row.batch_id == batch_id
        )
        counts = {
            status.value: sum(row.status is status for row in rows)
            for status in DelegationStatus
        }
        failures = tuple(
            row.id
            for row in rows
            if row.required
            and row.status in {DelegationStatus.FAILED, DelegationStatus.CANCELED}
        )
        pending = counts["queued"] + counts["running"]
        return DelegationAggregation(
            state="pending" if pending else ("blocked" if failures else "ready"),
            counts=counts,
            required_failures=failures,
            results=tuple(
                {
                    "delegationId": row.id,
                    "agentName": row.agent_name,
                    "agentTitle": row.agent_title,
                    "summary": row.result_summary or "",
                }
                for row in rows
                if row.status is DelegationStatus.DONE
            ),
        )

    async def cancel_batch(self, run_id, batch_id):
        canceled = 0
        for row in tuple(self.rows.values()):
            if row.run_id != run_id or row.batch_id != batch_id:
                continue
            canceled += await self.cancel(
                row.id,
                run_id=run_id,
                batch_id=batch_id,
                reason="delegation_canceled",
            )
        return canceled

    def _owned(self, delegation_id, run_id, batch_id):
        row = self.rows[delegation_id]
        if row.run_id != run_id or row.batch_id != batch_id:
            raise AssertionError("delegation escaped its Root Run batch")
        return row


class _Executor:
    def __init__(self) -> None:
        self.requests: list[DelegatedAgentRequest] = []

    async def execute(self, request, signal=None):
        del signal
        self.requests.append(request)
        return DelegatedAgentResult(
            outcome=RuntimeOutcome.COMPLETED,
            content=f"result:{request.objective}",
        )


class _PendingExecutor(_Executor):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.canceled = asyncio.Event()

    async def execute(self, request, signal=None):
        del signal
        self.requests.append(request)
        self.started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.canceled.set()
            raise


def _fixture(executor=None):
    output_repository = _OutputRepository()
    publisher = _Publisher()
    processor = AgentOutputProcessor(output_repository, publisher)
    repository = _Delegations()
    executor = executor or _Executor()
    coordinator = DelegationCoordinator(
        repository=repository,
        executor=executor,
        output_processor=processor,
        operation_controller=AgentOperationController(processor),
        max_parallel=2,
    )
    return coordinator, output_repository, repository, executor


@pytest.mark.asyncio
async def test_delegation_executes_inside_one_root_run_without_child_identity():
    coordinator, output, repository, executor = _fixture()
    delegation = await coordinator.create(
        run_id="run-root",
        batch_id="batch-1",
        agent_name="evidence-researcher",
        agent_title="Evidence researcher",
        agent_instruction="Collect evidence and report uncertainty.",
        objective="collect facts",
        input_payload={"topic": "PurrA"},
    )

    results = await coordinator.execute_batch((delegation,))

    assert results == (DelegatedAgentResult(
        outcome=RuntimeOutcome.COMPLETED,
        content="result:collect facts",
    ),)
    request = executor.requests[0]
    assert request.run_id == "run-root"
    assert request.delegation_id == delegation.id
    assert request.context_mode is DelegationContextMode.ISOLATED
    assert request.agent_title == "Evidence researcher"
    assert request.agent_instruction == (
        "Collect evidence and report uncertainty."
    )
    assert not hasattr(delegation, "child_run_id")
    assert not hasattr(delegation, "root_run_id")
    assert repository.rows[delegation.id].status is DelegationStatus.DONE

    statuses = [
        event.payload["status"]
        for event in await output.list_events("run-root", after_sequence=0)
        if event.payload.get("eventType") == "status"
    ]
    assert statuses == ["queued", "running", "done"]
    assert all(
        event.run_id == "run-root"
        for event in await output.list_events("run-root", after_sequence=0)
    )


@pytest.mark.asyncio
async def test_tool_results_are_scoped_to_the_current_delegation_batch():
    coordinator, _output, _repository, executor = _fixture()
    registration = build_delegation_tool_registration(
        coordinator,
    )
    assert registration.host_managed_durability is True
    state = ExecutionState(run_id="run-root")

    first = await registration.handler(state, {
        "delegations": [{
            "agentName": "first-reviewer",
            "title": "First reviewer",
            "instruction": "Review the first claim independently.",
            "objective": "first",
        }],
    })
    second = await registration.handler(state, {
        "delegations": [{
            "agentName": "second-reviewer",
            "title": "Second reviewer",
            "instruction": "Review the second claim independently.",
            "objective": "second",
        }],
    })

    first_payload = json.loads(first.content)
    second_payload = json.loads(second.content)
    assert [item["summary"] for item in first_payload["results"]] == [
        "result:first"
    ]
    assert [item["summary"] for item in second_payload["results"]] == [
        "result:second"
    ]
    assert all(
        request.run_id == "run-root"
        and request.context_mode is DelegationContextMode.ISOLATED
        for request in executor.requests
    )


@pytest.mark.asyncio
async def test_delegation_policy_bounds_model_defined_agents():
    coordinator, _output, _repository, _executor = _fixture()
    policy = DelegationPolicy(
        max_agents_per_call=1,
        max_parallel=1,
        max_agent_name_chars=8,
    )
    registration = build_delegation_tool_registration(coordinator, policy)
    schema = registration.schema.parameters
    assert schema["properties"]["delegations"]["maxItems"] == 1
    assert (
        schema["properties"]["delegations"]["items"]["properties"]
        ["agentName"]["maxLength"]
    ) == 8
    with pytest.raises(ContractViolationError, match="between one and 1"):
        await registration.handler(ExecutionState(run_id="run-root"), {
            "delegations": [{
                "agentName": "first",
                "title": "First",
                "instruction": "Review.",
                "objective": "first",
            }, {
                "agentName": "second",
                "title": "Second",
                "instruction": "Review.",
                "objective": "second",
            }],
        })


@pytest.mark.asyncio
async def test_cancel_batch_stops_only_its_active_delegated_execution():
    executor = _PendingExecutor()
    coordinator, _output, repository, _executor = _fixture(executor)
    delegation = await coordinator.create(
        run_id="run-root",
        batch_id="batch-cancel",
        agent_name="researcher",
        agent_title="Researcher",
        agent_instruction="Wait until canceled.",
        objective="wait",
    )
    execution = asyncio.create_task(coordinator.execute_batch((delegation,)))
    await executor.started.wait()

    assert await coordinator.cancel_batch("run-root", "batch-cancel") == 1
    with pytest.raises(asyncio.CancelledError):
        await execution

    assert executor.canceled.is_set()
    assert repository.rows[delegation.id].status is DelegationStatus.CANCELED
    await coordinator.close()
