"""Business repair cannot weaken authorization, approvals or effect fencing."""
from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest

from purra.contracts import (
    ApprovalResult, ApprovalStatus, ExecutionState, ToolBatchOutcome,
    ToolBatchRequest, ToolBatchResult, ToolCall, ToolEffectState,
    ToolExecutionLimits, ToolHandlerResult, ToolPolicy, ToolPreconditionFailure,
    ToolSchema,
)
from purra.ports import ToolRegistration
from purra.recovery import RecoveryLedger
from purra.runtime.tool_recovery import ToolRecoveryDisposition, resolve_tool_recovery
from purra.tools import CoreToolExecutor, InMemoryToolCatalog, inspect_tool_contract


class Sink:
    def __init__(self):
        self.events = []

    async def emit(self, event):
        self.events.append(event)


def missing_read():
    return ToolPreconditionFailure(
        "current_read_required", "Read the current resource before editing.",
        "Read all current pages, then generate a fresh approved proposal.",
        retryable=True,
    )


def registration(handler, **kwargs):
    return ToolRegistration(
        schema=ToolSchema("edit", "Edit resource", {"type": "object", "properties": {}}),
        handler=handler, policy=ToolPolicy("confirm", "Edit resource"), **kwargs,
    )


def request(identifier="edit-1", *, allowed=True):
    return ToolBatchRequest(
        "run", (ToolCall(identifier, "edit", "{}"),),
        frozenset({"edit"}) if allowed else frozenset(), ExecutionState(),
    )


class Approvals:
    def __init__(self, change=None, status=ApprovalStatus.APPROVED):
        self.calls = []
        self.change = change
        self.status = status

    async def request(self, run_id, approval, sink, signal=None):
        self.calls.append(approval.tool_call.id)
        if self.change:
            self.change()
        return ApprovalResult("approval-" + approval.tool_call.id, self.status)


@pytest.mark.asyncio
async def test_missing_precondition_starts_neither_approval_nor_handler():
    starts = []

    async def handler(*args):
        starts.append(True)
        return ToolHandlerResult("saved", effect_state=ToolEffectState.COMMITTED)

    async def check(*args):
        return missing_read()

    approvals, sink = Approvals(), Sink()
    executor = CoreToolExecutor(InMemoryToolCatalog((registration(handler, precondition_validator=check),)), approvals)
    result = await executor.execute_batch(request(), sink)
    assert result.outcome is ToolBatchOutcome.FAILED
    assert result.error == "tool_precondition_failed"
    assert result.effect_state is ToolEffectState.NOT_STARTED
    data = json.loads(result.results[0].content)
    assert data["diagnostics"]["sourceErrorCode"] == "current_read_required"
    assert "all current pages" in data["diagnostics"]["recoveryHint"]
    assert not starts and not approvals.calls
    assert sink.events[-1].payload["sourceErrorCode"] == "current_read_required"


@pytest.mark.asyncio
@pytest.mark.parametrize("outside_allowlist", [False, True])
async def test_authorization_is_terminal_and_never_invokes_repair(outside_allowlist):
    checks = []

    async def deny(*args):
        return "outside book scope"

    async def check(*args):
        checks.append(True)
        return missing_read()

    async def handler(*args):
        raise AssertionError("unauthorized handler")

    approvals = Approvals()
    executor = CoreToolExecutor(InMemoryToolCatalog((registration(handler, scope_validator=deny, precondition_validator=check),)), approvals)
    result = await executor.execute_batch(request(allowed=not outside_allowlist), Sink())
    assert result.outcome is ToolBatchOutcome.REJECTED
    assert result.error == ("tool_not_authorized" if outside_allowlist else "tool_scope_violation")
    assert not checks and not approvals.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("revoke", [False, True])
async def test_approval_rechecks_both_gates_and_fresh_proposal_requires_new_approval(revoke):
    ready, authorized, writes = True, True, []

    async def scope(*args):
        return None if authorized else "scope revoked"

    async def check(*args):
        return None if ready else missing_read()

    async def handler(*args):
        writes.append(True)
        return ToolHandlerResult("saved", effect_state=ToolEffectState.COMMITTED)

    def change():
        nonlocal ready, authorized
        ready = False
        if revoke:
            authorized = False

    approvals = Approvals(change)
    executor = CoreToolExecutor(InMemoryToolCatalog((registration(handler, scope_validator=scope, precondition_validator=check),)), approvals)
    first = await executor.execute_batch(request(), Sink())
    assert first.error == ("tool_scope_violation" if revoke else "tool_precondition_failed")
    assert not writes
    ready = authorized = True
    approvals.change = None
    second = await executor.execute_batch(request("edit-2"), Sink())
    assert second.outcome is ToolBatchOutcome.COMPLETED
    assert writes == [True]
    assert approvals.calls == ["edit-1", "edit-2"]


@pytest.mark.asyncio
async def test_nonrecoverable_and_invalid_preconditions_fail_closed():
    async def handler(*args):
        raise AssertionError("handler must not execute")

    async def permanent(*args):
        return ToolPreconditionFailure("confirmation_required", "Explicit confirmation is required.")

    async def broken(*args):
        raise ValueError("private-sentinel")

    async def invalid(*args):
        return "not a precondition contract"

    for check, error in [(permanent, "tool_precondition_rejected"), (broken, "tool_precondition_validation_failed"), (invalid, "tool_precondition_validation_failed")]:
        approvals = Approvals()
        executor = CoreToolExecutor(InMemoryToolCatalog((registration(handler, precondition_validator=check),)), approvals)
        result = await executor.execute_batch(request(), Sink())
        assert result.outcome is ToolBatchOutcome.REJECTED
        assert result.error == error
        assert "private-sentinel" not in result.results[0].content
        assert not approvals.calls


def failure(effect=ToolEffectState.NOT_STARTED):
    return ToolBatchResult(results=(), outcome=ToolBatchOutcome.FAILED,
        error="tool_precondition_failed", effect_state=effect)


def resolve(result, ledger, *, planning=False, epoch=0, canceled=False):
    return resolve_tool_recovery(result, requested_names={"edit"},
        planning_available=planning, recovery_ledger=ledger,
        input_recovery_epoch=epoch, remaining_model_rounds=20,
        round_number=epoch + 1, cancellation_requested=canceled)


@pytest.mark.parametrize("planning", [False, True])
def test_repair_budget_cannot_reset_after_reads_or_escape_through_replanning(planning):
    ledger = RecoveryLedger()
    decisions = [resolve(failure(), ledger, planning=planning, epoch=i * 4) for i in range(4)]
    allowed = ToolRecoveryDisposition.REPLAN if planning else ToolRecoveryDisposition.RETRY_MODEL
    assert [d.disposition for d in decisions] == [
        allowed, allowed,
        ToolRecoveryDisposition.REJECT, ToolRecoveryDisposition.REJECT,
    ]
    assert "repair prerequisites" in decisions[0].messages[0].content
    assert decisions[2].traces[-1].details["reasonCode"] == "attempt_budget_exhausted"


@pytest.mark.parametrize("effect", [ToolEffectState.COMMITTED, ToolEffectState.UNKNOWN])
@pytest.mark.parametrize("planning", [False, True])
def test_repair_never_repeats_committed_or_unknown_effects(effect, planning):
    decision = resolve(failure(effect), RecoveryLedger(), planning=planning)
    assert decision.disposition is ToolRecoveryDisposition.REJECT
    assert not decision.messages


def test_canceled_run_does_not_schedule_repair():
    assert resolve(failure(), RecoveryLedger(), canceled=True).disposition is ToolRecoveryDisposition.REJECT


@pytest.mark.asyncio
async def test_cancellation_during_validation_never_approves_or_writes():
    signal, entered = asyncio.Event(), asyncio.Event()

    async def check(*args):
        entered.set()
        await asyncio.Event().wait()

    async def handler(*args):
        raise AssertionError("handler must not execute")

    approvals = Approvals()
    executor = CoreToolExecutor(InMemoryToolCatalog((registration(handler, precondition_validator=check),)), approvals)
    pending = asyncio.create_task(executor.execute_batch(request(), Sink(), signal))
    await asyncio.wait_for(entered.wait(), 1)
    signal.set()
    result = await asyncio.wait_for(pending, 1)
    assert result.outcome is ToolBatchOutcome.CANCELED
    assert not approvals.calls


@pytest.mark.asyncio
async def test_parallel_preconditions_reject_before_any_read_handler_starts():
    async def handler(*args):
        raise AssertionError("handler must not execute")

    async def check(state, args, signal=None):
        return missing_read()

    item = replace(registration(handler, precondition_validator=check),
        policy=ToolPolicy("read", "Read resource"), concurrency_safe=True)
    executor = CoreToolExecutor(InMemoryToolCatalog((item,)), limits=ToolExecutionLimits(max_concurrency=2))
    req = replace(request(), calls=(ToolCall("a", "edit", "{}"), ToolCall("b", "edit", "{}")))
    result = await executor.execute_batch(req, Sink())
    assert result.error == "tool_precondition_failed"
    assert result.effect_state is ToolEffectState.NOT_STARTED
    assert len(result.results) == 2


def test_recoverability_is_explicit_and_async_contract_is_checked():
    with pytest.raises(ValueError, match="guidance"):
        ToolPreconditionFailure("code", "message", retryable=True)
    with pytest.raises(TypeError, match="boolean"):
        ToolPreconditionFailure("code", "message", "repair", retryable="true")

    async def handler(*args):
        return ToolHandlerResult("ok")

    report = inspect_tool_contract((registration(handler, precondition_validator=lambda *args: None),))
    assert not report.is_valid
    assert "precondition validator must be async" in report.describe_violations()


def test_repair_budget_survives_checkpoint_restore_and_mode_changes():
    ledger = RecoveryLedger()
    assert resolve(failure(), ledger).disposition is ToolRecoveryDisposition.RETRY_MODEL
    restored = RecoveryLedger()
    restored.restore(ledger.snapshot())
    assert resolve(failure(), restored, planning=True, epoch=10).disposition is ToolRecoveryDisposition.REPLAN
    again = RecoveryLedger()
    again.restore(restored.snapshot())
    assert resolve(failure(), again, epoch=20).disposition is ToolRecoveryDisposition.REJECT


@pytest.mark.asyncio
async def test_planned_write_repairs_via_canonical_replanning_without_widening_the_write_step():
    from purra.api import ContextStrategy, ExecutionProfile, InMemoryAgentAdapters
    from purra.contracts import (
        PlanningKind, PlanningMode, PlanningResult, RunStatus, StepExecutor,
        StepType, TaskSpec, WorkPlan, WorkStep,
    )
    from test_standalone_agent_conformance import _AlwaysPlan, _Context, _ScriptedToolGateway, _core, _request

    ready, reads, writes = False, [], []

    async def check(*args):
        return None if ready else missing_read()

    async def read_handler(*args):
        nonlocal ready
        reads.append(True)
        ready = True
        return ToolHandlerResult("current synthetic resource", effect_state="not_started")

    async def write_handler(*args):
        writes.append(True)
        return ToolHandlerResult("saved synthetic resource", effect_state="committed")

    def plan(repaired):
        steps = []
        if repaired:
            steps.append(WorkStep(id="read", title="Read current resource", type=StepType.READ,
                executor=StepExecutor.TOOL, capability_names=("readCurrent",)))
        edit_id = "edit-repaired" if repaired else "edit"
        steps += [
            WorkStep(id=edit_id, title="Edit current resource", type=StepType.WRITE,
                executor=StepExecutor.TOOL, capability_names=("edit",),
                depends_on=("read",) if repaired else ()),
            WorkStep(id="answer", title="Summarize saved result", type=StepType.WRITE,
                executor=StepExecutor.MODEL, depends_on=(edit_id,)),
        ]
        return PlanningResult(kind=PlanningKind.PLANNED, work_plan=WorkPlan(
            title="Repair the unexecuted write", task_spec=TaskSpec(goal="Save the synthetic resource"),
            steps=tuple(steps)))

    class Planner:
        revisions = 0

        async def create_plan(self, request, capabilities, signal=None, **kwargs):
            return plan(False)

        async def revise_plan(self, request, capabilities, turn, signal=None, **kwargs):
            self.revisions += 1
            return plan(True)

    planner = Planner()
    catalog = InMemoryToolCatalog((
        replace(registration(write_handler, precondition_validator=check), policy=ToolPolicy("propose", "Edit")),
        ToolRegistration(ToolSchema("readCurrent", "Read", {"type": "object", "properties": {}}),
            read_handler, ToolPolicy("read", "Read")),
    ))
    gateway = _ScriptedToolGateway(["edit", "readCurrent", "edit", None])
    adapters = InMemoryAgentAdapters()
    core = _core(gateway=gateway, context=_Context(), tool_catalog=catalog, adapters=adapters,
        profile=ExecutionProfile(planner=planner, planning_policy=_AlwaysPlan(), context_strategy=ContextStrategy.STAGED))
    try:
        result = await (await core.submit(replace(_request(), tools_enabled=True,
            context_window=32768, planning_mode=PlanningMode.PLANNED))).wait()
    finally:
        await core.close()
    assert result.status is RunStatus.DONE, json.dumps({"error": result.error, "revisions": planner.revisions,
        "calls": len(gateway.invocations), "traces": [(t.stage, t.outcome, dict(t.details))
        for t in adapters.runs._state.runs[result.run_id].traces
        if t.stage in {"planning", "recovery_decision", "tool_recovery", "tool_round"}]}, default=str)
    assert planner.revisions == 1
    assert reads == [True] and writes == [True]
    assert len(gateway.invocations) == 4


@pytest.mark.asyncio
async def test_parallel_authorization_denial_wins_over_an_earlier_repairable_failure():
    checks, handled = [], []

    async def scope(state, arguments, signal=None):
        return "other tenant" if arguments.get("id") == "denied" else None

    async def prerequisite(*args):
        checks.append(True)
        return missing_read()

    async def handler(*args):
        handled.append(True)
        raise AssertionError("No handler may start during batch admission")

    read = replace(registration(handler, scope_validator=scope, precondition_validator=prerequisite),
        schema=ToolSchema("edit", "Read", {"type": "object", "properties": {"id": {"type": "string"}}}),
        policy=ToolPolicy("read", "Read"), concurrency_safe=True)
    calls = (ToolCall("first", "edit", '{"id":"allowed"}'), ToolCall("second", "edit", '{"id":"denied"}'))
    executor = CoreToolExecutor(InMemoryToolCatalog((read,)), limits=ToolExecutionLimits(max_concurrency=2))
    result = await executor.execute_batch(replace(request(), calls=calls), Sink())
    assert result.outcome is ToolBatchOutcome.REJECTED
    assert {item.error for item in result.results} == {"tool_scope_violation"}
    assert not checks and not handled


@pytest.mark.asyncio
@pytest.mark.parametrize("prior", [ToolEffectState.COMMITTED, ToolEffectState.UNKNOWN, "unavailable"])
async def test_prior_effects_cannot_be_reclassified_as_an_unstarted_repair(prior):
    class Probe:
        async def execute_once(self, *args):
            raise AssertionError("A blocked call cannot enter idempotent execution")

        async def inspect_effect_state(self, *args):
            return prior

    class LegacyGateway:
        async def execute_once(self, *args):
            raise AssertionError("Missing evidence must fail closed")

    async def handler(*args):
        raise AssertionError("No replay or new effect is allowed")

    async def check(*args):
        return missing_read()

    executor = CoreToolExecutor(InMemoryToolCatalog((registration(handler, precondition_validator=check),)),
        idempotency_gateway=LegacyGateway() if prior == "unavailable" else Probe())
    result = await executor.execute_batch(request(), Sink())
    assert result.outcome is ToolBatchOutcome.REJECTED
    assert result.effect_state is (ToolEffectState.UNKNOWN if prior == "unavailable" else prior)
    assert result.results[0].error == "tool_precondition_effect_unresolved"
    data = json.loads(result.results[0].content)["diagnostics"]
    assert data["retryable"] is False and "recoveryHint" not in data
