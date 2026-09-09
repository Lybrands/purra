"""An approval decision cannot preserve permissions revoked while waiting."""
import asyncio
import json
from pathlib import Path

import pytest

from purra.contracts import (
    ApprovalResult, ApprovalStatus, ExecutionState, ToolBatchRequest,
    ToolCall, ToolHandlerResult, ToolPolicy, ToolSchema,
)
from purra.ports import ToolRegistration
from purra.errors import ContractViolationError
from purra.tools import CoreToolExecutor, InMemoryToolCatalog


CASES = json.loads((Path(__file__).parents[1] / "conformance/fixtures/approval_dispatch.json").read_text())["cases"]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
async def test_approval_dispatch_revalidates_before_claim(case):
    signal = asyncio.Event()
    approvals = dispatches = claims = scope_checks = 0

    async def scope(state, arguments, cancellation):
        nonlocal scope_checks
        scope_checks += 1
        assert arguments["target"] == "fixture"
        if approvals:
            if case["name"] == "revoked":
                return "Access revoked"
            if case["name"] == "scope_error":
                raise RuntimeError("PRIVATE_AUTHORITY_DETAILS")
            if case["name"] == "canceled_after_scope":
                signal.set()
            if case["name"] == "control_error":
                raise ContractViolationError("Execution ownership changed", code="run_lease_conflict")

    async def handler(state, arguments, cancellation):
        nonlocal dispatches
        dispatches += 1
        return ToolHandlerResult("done", effect_state="committed")

    class Approval:
        async def request(self, run_id, approval, event_sink, cancellation):
            nonlocal approvals
            approvals += 1
            if case["name"] == "canceled":
                signal.set()
            status = ApprovalStatus.REJECTED if case["name"] == "rejected" else ApprovalStatus.APPROVED
            return ApprovalResult("approval", status)

    class Idempotency:
        async def execute_once(self, run_id, call, operation):
            nonlocal claims
            claims += 1
            return await operation()

    class Sink:
        async def emit(self, event):
            assert "PRIVATE_AUTHORITY_DETAILS" not in str(event.payload)

    tool = ToolRegistration(
        ToolSchema("write", "Write fixture", {"type": "object", "properties": {"target": {"type": "string"}}, "required": ["target"]}),
        handler, ToolPolicy(mode="confirm", title="Write fixture", risk_level="write"), scope_validator=scope,
    )
    executor = CoreToolExecutor(InMemoryToolCatalog((tool,)), approval_gateway=Approval(), idempotency_gateway=Idempotency())
    request = ToolBatchRequest("run", (ToolCall("call", "write", '{"target":"fixture"}'),), frozenset({"write"}), ExecutionState())
    if case["name"] == "control_error":
        with pytest.raises(ContractViolationError) as caught:
            await executor.execute_batch(request, Sink(), signal)
        assert caught.value.code == case["error"]
        assert approvals == 1 and dispatches == claims == 0
        return
    result = await executor.execute_batch(request, Sink(), signal)
    assert approvals == 1
    assert dispatches == case["dispatches"]
    assert claims == case["claims"]
    if "error" in case:
        assert result.results[0].error == case["error"]
        assert result.effect_state.value == "not_started"
    if case["name"] == "unchanged":
        assert scope_checks >= 2
        assert result.outcome.value == "completed"
    if case["name"] in {"canceled", "canceled_after_scope"}:
        assert result.outcome.value == "canceled"
