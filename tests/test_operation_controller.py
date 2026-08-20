from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from purra.errors import ContractViolationError


def _types():
    try:
        from purra.operations import (
            AgentOperationController,
            OperationDisplay,
            OperationKind,
            OperationScope,
            OperationStatus,
        )
    except ImportError as error:
        pytest.fail(f"authoritative operation controller is missing: {error}")
    return (
        AgentOperationController,
        OperationDisplay,
        OperationKind,
        OperationScope,
        OperationStatus,
    )


class _WallClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 12, 8, 0, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.value


class _MonotonicClock:
    def __init__(self) -> None:
        self.value = 100.0

    def now(self) -> float:
        return self.value


class _Output:
    def __init__(self) -> None:
        self.events = []
        self.fail_next = False

    async def accept_operation_event(self, event):
        if self.fail_next:
            self.fail_next = False
            raise OSError("journal unavailable")
        self.events.append(event)
        return event


def _controller():
    AgentOperationController, *_rest = _types()
    wall = _WallClock()
    monotonic = _MonotonicClock()
    output = _Output()
    controller = AgentOperationController(
        output,
        wall_clock=wall.now,
        monotonic_clock=monotonic.now,
    )
    return controller, wall, monotonic, output


@pytest.mark.asyncio
async def test_duration_uses_monotonic_clock_and_persists_wall_timestamps():
    (
        _Controller,
        OperationDisplay,
        OperationKind,
        OperationScope,
        OperationStatus,
    ) = _types()
    controller, wall, monotonic, output = _controller()

    receipt = await controller.start(
        OperationKind.TOOL,
        OperationScope(
            run_id="run-1",
            invocation_id="invocation-1",
            display=OperationDisplay(
                label_key="agent.operation.tool",
                label_params={"toolName": "readDocument"},
            ),
        ),
    )
    wall.value -= timedelta(seconds=60)
    monotonic.value += 0.037
    finished = await controller.succeed(receipt.operation_id)

    assert finished.status is OperationStatus.SUCCEEDED
    assert finished.duration_ms == 37
    assert finished.finished_at < receipt.started_at
    assert output.events == [receipt.started_event, finished]
    assert output.events[0].display == {
        "labelKey": "agent.operation.tool",
        "labelParams": {"toolName": "readDocument"},
    }


@pytest.mark.asyncio
async def test_operation_has_exactly_one_terminal_event():
    (
        _Controller,
        _Display,
        OperationKind,
        OperationScope,
        _Status,
    ) = _types()
    controller, _wall, _monotonic, output = _controller()
    receipt = await controller.start(
        OperationKind.MODEL,
        OperationScope(run_id="run-1"),
    )

    await controller.succeed(receipt.operation_id)
    with pytest.raises(ContractViolationError, match="already terminal"):
        await controller.fail(receipt.operation_id, "late_failure")

    assert len(output.events) == 2


@pytest.mark.asyncio
async def test_persistence_failure_does_not_create_untracked_lifecycle_state():
    (
        _Controller,
        _Display,
        OperationKind,
        OperationScope,
        _Status,
    ) = _types()
    controller, _wall, _monotonic, output = _controller()
    output.fail_next = True

    with pytest.raises(OSError, match="journal unavailable"):
        await controller.start(
            OperationKind.CONTEXT_COMPACTION,
            OperationScope(run_id="run-1"),
        )

    assert controller.running_operation_ids == ()


@pytest.mark.asyncio
async def test_failed_terminal_requires_one_stable_error_code():
    (
        _Controller,
        _Display,
        OperationKind,
        OperationScope,
        OperationStatus,
    ) = _types()
    controller, _wall, monotonic, _output = _controller()
    receipt = await controller.start(
        OperationKind.VALIDATION,
        OperationScope(run_id="run-1"),
    )
    monotonic.value += 0.005

    finished = await controller.fail(
        receipt.operation_id,
        "response_constraint_violation",
    )

    assert finished.status is OperationStatus.FAILED
    assert finished.error_code == "response_constraint_violation"
    assert finished.duration_ms == 5


def test_display_metadata_cannot_claim_lifecycle_fields():
    _Controller, OperationDisplay, *_rest = _types()

    with pytest.raises(ValueError, match="lifecycle field"):
        OperationDisplay(
            label_key="agent.operation.tool",
            label_params={"durationMs": 48},
        )
