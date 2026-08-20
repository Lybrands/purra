from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest


def _contracts():
    try:
        return importlib.import_module("purra.operations.contracts")
    except ModuleNotFoundError as error:
        pytest.fail(f"operation contracts are missing: {error}")


def test_finished_operation_rejects_negative_duration():
    contracts = _contracts()

    with pytest.raises(ValueError, match="duration must be non-negative"):
        contracts.OperationFinished(
            operation_id="operation-1",
            run_id="run-1",
            invocation_id=None,
            status=contracts.OperationStatus.SUCCEEDED,
            finished_at=datetime.now(timezone.utc),
            duration_ms=-1,
        )


def test_finished_operation_rejects_running_status():
    contracts = _contracts()

    with pytest.raises(ValueError, match="finished operation requires terminal status"):
        contracts.OperationFinished(
            operation_id="operation-1",
            run_id="run-1",
            invocation_id=None,
            status=contracts.OperationStatus.RUNNING,
            finished_at=datetime.now(timezone.utc),
            duration_ms=1,
        )


def test_failed_operation_requires_error_code():
    contracts = _contracts()

    with pytest.raises(ValueError, match="failed operation requires error code"):
        contracts.OperationFinished(
            operation_id="operation-1",
            run_id="run-1",
            invocation_id=None,
            status=contracts.OperationStatus.FAILED,
            finished_at=datetime.now(timezone.utc),
            duration_ms=1,
        )


def test_started_operation_freezes_display_metadata():
    contracts = _contracts()
    display = {"labelKey": "tool.read", "details": {"name": "读取正文"}}

    started = contracts.OperationStarted(
        operation_id="operation-1",
        run_id="run-1",
        invocation_id="invocation-1",
        kind=contracts.OperationKind.TOOL,
        started_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
        display=display,
    )
    display["details"]["name"] = "已篡改"

    assert started.display["details"]["name"] == "读取正文"
    with pytest.raises(TypeError):
        started.display["labelKey"] = "changed"


def test_operation_timestamps_must_include_timezone():
    contracts = _contracts()

    with pytest.raises(ValueError, match="started_at must be timezone-aware"):
        contracts.OperationStarted(
            operation_id="operation-1",
            run_id="run-1",
            invocation_id=None,
            kind=contracts.OperationKind.MODEL,
            started_at=datetime(2026, 8, 11),
        )
