"""Public resume behavior against a reopened, durable journal."""
import json
from dataclasses import replace
from pathlib import Path

import pytest

from purra.api import (
    AgentCore, AgentCoreRunOptions, AgentPreset, UserInputRequired, AgentComponentBinding, ExecutionProfile,
    AgentTreePolicy, AgentCapabilityGrant, BeginRootAgentCommand,
    ChildAgentSpec, SpawnAgentsCommand,
)
from purra.contracts import (
    AgentMessage, AgentRunRequest, DomainContext, ModelRequest, ModelStream,
    ModelStreamChunk, RunCreateParams, RuntimeLimits, ToolCallDelta,
    ToolHandlerResult, ToolPolicy, ToolSchema, PlanningResult, WorkPlan, WorkStep,
)
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.model_protocol import generic_capability_snapshot
from purra.ports import ToolRegistration
from purra.tools import InMemoryToolCatalog
from purra_sqlite import SqliteAgentAdapters

from purra.approvals import ApprovalIntent, ApprovalDecisionCommand, ApprovalRequired
from purra.agent_execution_checkpoint import AgentToolExecutionCheckpoint
from purra.structured import json_identity_digest
import time
import asyncio
class ApprovalHost:
    def __init__(self, path, *, revision="1", leased=True, tree=False, first_snapshot=None, planner=None):
        self.storage = SqliteAgentAdapters(path, scope="resume")
        self.approvals = self.storage.approval_store(authorize=lambda *_: True)
        self.model_calls = 0
        self.tool_calls = 0
        self.request = AgentRunRequest(
            messages=(AgentMessage("user", "Look up the value"),),
            model=ModelRequest("fixture", "fixture", replace(generic_capability_snapshot(), max_generation_tokens=128)),
            domain_context=DomainContext("resume"), context_window=65536,
            tools_enabled=True, planning_mode="reactive",
        )
        self.core = AgentCore(
            approval_gateway=self.approvals.gateway(), tool_idempotency_gateway=self.storage.idempotency,
            model_gateway=self, run_repository=self.storage.runs,
            run_tree_repository=self.storage.run_tree if tree else None,
            output_repository=self.storage.outputs, output_publisher=self.storage.publisher,
            execution_lease_store=self.storage.leases if leased else None,
            preset=AgentPreset(id="resume", revision=revision,
                execution_profile=ExecutionProfile(planner=planner),
                component_bindings={"planner": AgentComponentBinding("approval.planner", "1")} if planner is not None else {},
                agent_tree_policy=AgentTreePolicy() if tree else None,
                runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
                tool_catalog=InMemoryToolCatalog((ToolRegistration(
                    schema=ToolSchema("lookup", "Read the value", {"type": "object", "properties": {}}),
                    handler=self.lookup, policy=ToolPolicy(mode="confirm", title="Lookup", risk_level="write"), scope_validator=self.scope,
                ),))),
        )

    async def scope(self, state, arguments, signal=None):
        if getattr(self, "deny_scope", False):
            raise PermissionError("fixture scope revoked")
        self.scope_calls = getattr(self, "scope_calls", 0) + 1
        if getattr(self, "cancel_after_approval", False) and self.scope_calls == 2:
            await self.storage.leases.request_cancellation(self.active_run_id)

    async def lookup(self, state, arguments, signal=None):
        self.tool_calls += 1
        if getattr(self, "block_tool", False):
            self.entered.set()
            await self.release.wait()
        if getattr(self, "unknown_effect", False):
            return ToolHandlerResult(content="uncertain", effect_state="unknown")
        if getattr(self, "fail_receipt", False):
            self.storage._db.execute("CREATE TRIGGER fail_receipt BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture receipt failure'); END")
        if getattr(self, "fail_before_effect", False):
            raise RuntimeError("synthetic handler stopped before effect")
        self.effects = getattr(self, "effects", 0) + 1
        return ToolHandlerResult(content="42", effect_state="committed")

    async def complete(self, *args, **kwargs):
        raise AssertionError("stream expected")

    async def stream(self, messages, invocation, signal=None):
        self.model_calls += 1
        if getattr(self, "require_tool_history", False):
            assert any(m.role.value == "tool" for m in messages) or not invocation.tools
        async def chunks():
            if getattr(self, "script", None) is not None:
                output = self.script(messages, invocation)
                if isinstance(output, tuple):
                    name, arguments = output
                    yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=0, id=f"call-{sum(len(m.tool_calls) for m in messages)+1}", name=name, arguments_fragment=json.dumps(arguments)),), finish_reason="tool_calls")
                else:
                    yield ModelStreamChunk(content_delta=output, finish_reason="stop")
            elif getattr(self, "promote", False) and self.model_calls == 1:
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=0, id="plan", name="request_plan", arguments_fragment="{}"),), finish_reason="tool_calls")
            elif any(m.role.value == "tool" for m in messages) or not invocation.tools:
                yield ModelStreamChunk(content_delta="42", finish_reason="stop")
            else:
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(
                    index=0, id="lookup-1", name="lookup", arguments_fragment="{}",
                ),), finish_reason="tool_calls")
        return ModelStream(chunks=chunks(), model="fixture",
            applied_generation_limit=invocation.output_budget.max_generation_tokens)

    async def boundary(self, checkpoint):
        snapshot = await self.storage.runs.get(checkpoint.run_id)
        call = checkpoint.assistant.tool_calls[0]
        intent = ApprovalIntent(run_id=checkpoint.run_id, root_run_id=checkpoint.run_id,
            tool_call_id=call.id, tool_name=call.name, arguments=json.loads(call.arguments_json),
            preset_fingerprint=json_identity_digest(snapshot.agent_preset_snapshot),
            binding_id="fixture", binding_revision="1", scope_id="fixture", scope_revision="1", effect="write")
        await self.approvals.prepare(checkpoint, intent, expires_at_ms=self.expiry)

    async def close(self):
        await self.core.close()
        self.storage.close()


@pytest.mark.asyncio
async def test_approval_wait_reopen_and_resume_does_not_replay_model_or_write(tmp_path):
    path=tmp_path/'db';host=ApprovalHost(path)
    expiry=int(time.time()*1000)+60000;host.expiry=expiry
    try:
        await host.storage.enable_approvals()
        options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary)
        handle=await host.core.submit(host.request,options=options)
        with pytest.raises(ApprovalRequired):await handle.wait()
        run_id=handle.run_id
        assert (host.model_calls,host.tool_calls)==(1,0)
        snapshot=await host.storage.runs.get(run_id)
        with pytest.raises(ContractViolationError) as missing:await host.core.resume(run_id,host.request)
        assert missing.value.code=="approval_gate_unavailable"
        assert isinstance(snapshot.execution_checkpoint,AgentToolExecutionCheckpoint)
        record=(await host.approvals.list_pending())[0]
        events=await host.storage.outputs.list_events(run_id,after_sequence=0)
        waiting_events=[event for event in events if event.payload.get('type')=='approval.required']
        assert len(waiting_events)==1 and waiting_events[0].visibility.value=='private'
        assert set(waiting_events[0].payload)=={'type','status','revision'}
        assert (await host.storage.leases.get(run_id)).owner_id is None
        await host.close();host=ApprovalHost(path);host.expiry=expiry
        options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary)
        waiting=await host.core.resume(run_id,host.request,options=options)
        with pytest.raises(ApprovalRequired):await waiting.wait()
        assert (host.model_calls,host.tool_calls)==(0,0)
        await host.approvals.decide(ApprovalDecisionCommand(record.approval_id,record.revision,record.intent.digest,'decision','approve'),principal_id='host')
        resumed=await host.core.resume(run_id,host.request,options=options)
        result=await resumed.wait()
        assert result.status.value=='done'
        assert host.tool_calls==1
        assert host.model_calls>=1
        assert (await host.storage.runs.get(run_id)).execution_checkpoint.phase=='model_ready'
        async with host.storage.transaction() as session:
            assert session.extra['approvalExecutions'][record.approval_id]['state']=='complete'
            assert not session.claims
    finally:await host.close()


async def approved_host(path):
    host=ApprovalHost(path);host.expiry=int(time.time()*1000)+60000
    await host.storage.enable_approvals()
    options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary)
    handle=await host.core.submit(host.request,options=options)
    with pytest.raises(ApprovalRequired):await handle.wait()
    record=(await host.approvals.list_pending())[0]
    await host.approvals.decide(ApprovalDecisionCommand(record.approval_id,record.revision,record.intent.digest,'decision','approve'),principal_id='host')
    host.active_run_id=handle.run_id
    return host,options,record


@pytest.mark.asyncio
async def test_concurrent_resume_cannot_dispatch_twice(tmp_path):
    host,options,record=await approved_host(tmp_path/'db')
    host.block_tool=True;host.entered=asyncio.Event();host.release=asyncio.Event()
    other=ApprovalHost(tmp_path/'db');other.expiry=host.expiry
    try:
        first=await host.core.resume(record.intent.run_id,host.request,options=options)
        await asyncio.wait_for(host.entered.wait(),2)
        with pytest.raises(ContractViolationError) as conflict:
            handle=await other.core.resume(record.intent.run_id,other.request,options=AgentCoreRunOptions(tool_checkpoint_handler=other.boundary))
            await handle.wait()
        assert conflict.value.code in {'run_lease_conflict','tool_effect_unknown'}
        assert other.tool_calls==other.model_calls==0
        host.release.set();await first.wait()
        assert host.tool_calls==1
    finally:
        host.release.set();await other.close();await host.close()


@pytest.mark.asyncio
async def test_cancellation_after_gateway_approval_prevents_tool_claim(tmp_path):
    host,options,record=await approved_host(tmp_path/'db');host.cancel_after_approval=True
    try:
        handle=await host.core.resume(record.intent.run_id,host.request,options=options)
        await handle.wait()
        assert host.tool_calls==0
        async with host.storage.transaction() as session:
            assert not session.claims
            assert not session.extra.get('approvalExecutions')
    finally:await host.close()


@pytest.mark.asyncio
async def test_unknown_result_keeps_existing_claim_and_blocks_another_dispatch(tmp_path):
    host,options,record=await approved_host(tmp_path/'db');host.unknown_effect=True
    try:
        handle=await host.core.resume(record.intent.run_id,host.request,options=options)
        result=await handle.wait()
        assert result.status.value=='failed'
        assert host.tool_calls==1
        report=await host.storage.inspect_recovery(record.intent.run_id)
        assert report['observations']['approvalUnknownReceipts']==1
        assert 'tool_effect_unknown' in report['blockers']
        async with host.storage.transaction() as session:
            assert (record.intent.run_id,record.intent.tool_call_id) in session.claims
            assert session.extra['approvalExecutions'][record.approval_id]['effectState']=='unknown'
            assert session.get_tool_receipt((record.intent.run_id,record.intent.tool_call_id)) is None
    finally:await host.close()


@pytest.mark.asyncio
async def test_receipt_failure_after_effect_preserves_unknown_claim_across_reopen(tmp_path):
    path=tmp_path/'db';host,options,record=await approved_host(path);host.fail_receipt=True
    try:
        handle=await host.core.resume(record.intent.run_id,host.request,options=options)
        with pytest.raises(Exception):await handle.wait()
        assert host.tool_calls==1
        host.storage._db.execute('DROP TRIGGER fail_receipt')
        await host.close();host=ApprovalHost(path);host.expiry=record.expires_at_ms
        async with host.storage.transaction() as session:
            assert (record.intent.run_id,record.intent.tool_call_id) in session.claims
        with pytest.raises(ContractViolationError) as blocked:
            resumed=await host.core.resume(record.intent.run_id,host.request,options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
            await resumed.wait()
        assert blocked.value.code=='tool_effect_unknown'
        assert host.tool_calls==host.model_calls==0
    finally:await host.close()


def test_durable_gateway_rejects_missing_or_different_idempotency_adapter(tmp_path):
    from purra.tools.executor import CoreToolExecutor
    storage=SqliteAgentAdapters(tmp_path/'first',scope='fixture')
    other=SqliteAgentAdapters(tmp_path/'second',scope='fixture')
    try:
        gateway=storage.approval_store(authorize=lambda *_:True).gateway()
        for idempotency in [None,other.idempotency]:
            with pytest.raises(ContractViolationError) as rejected:
                CoreToolExecutor(InMemoryToolCatalog(()),gateway,idempotency_gateway=idempotency)
            assert rejected.value.code=='approval_idempotency_unavailable'
        CoreToolExecutor(InMemoryToolCatalog(()),gateway,idempotency_gateway=storage.idempotency)
    finally:other.close();storage.close()


@pytest.mark.asyncio
async def test_changed_current_binding_cannot_use_existing_approval(tmp_path):
    host,options,record=await approved_host(tmp_path/'db')
    original=host.approvals.prepare
    async def changed(checkpoint,intent,**kwargs):
        return await original(checkpoint,replace(intent,binding_revision='changed'),**kwargs)
    host.approvals.prepare=changed
    try:
        handle=await host.core.resume(record.intent.run_id,host.request,options=options)
        result=await handle.wait()
        assert result.status.value=='failed'
        assert host.tool_calls==0
        async with host.storage.transaction() as session:assert not session.claims
    finally:await host.close()


@pytest.mark.asyncio
async def test_committed_receipt_replay_after_expiry_does_not_repeat_effect(tmp_path):
    path=tmp_path/'db';host,options,record=await approved_host(path)
    original=host.storage.runs.commit
    async def stop_after_receipt(run_id,commit):
        if commit.execution_checkpoint is not None and commit.execution_checkpoint.phase=='model_ready':
            raise ApprovalRequired(run_id,'fixture-stop-after-receipt')
        return await original(run_id,commit)
    host.storage.runs.commit=stop_after_receipt
    try:
        handle=await host.core.resume(record.intent.run_id,host.request,options=options)
        with pytest.raises(ApprovalRequired):await handle.wait()
        assert host.tool_calls==1
        await host.close();host=ApprovalHost(path);host.expiry=record.expires_at_ms
        host.approvals._clock=lambda:record.expires_at_ms+1
        assert (await host.approvals.refresh(record.approval_id)).status=='expired'
        report=await host.storage.inspect_recovery(record.intent.run_id)
        assert report['observations']['approvalReceipt']=='complete'
        assert 'approval_expired' not in report['blockers']
        assert 'approval_terminal_receipt_present' in report['cautions']
        resumed=await host.core.resume(record.intent.run_id,host.request,options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
        assert (await resumed.wait()).status.value=='done'
        assert host.tool_calls==0
    finally:await host.close()


class ApprovalPlanner:
    def __init__(self): self.calls = 0; self.revisions = []
    async def create_plan(self, *args, **kwargs):
        self.calls += 1
        return PlanningResult(kind="planned", work_plan=WorkPlan(title="Write and finish", steps=(
            WorkStep(id="prepare", title="Prepare", type="review", executor="model"),
            WorkStep(id="write", title="Write", type="write", executor="tool", capability_names=("lookup",), depends_on=("prepare",)),
            WorkStep(id="finish", title="Finish", type="review", executor="model", depends_on=("write",)),
        )))
    async def revise_plan(self, request, capabilities, turn, *args, **kwargs):
        self.revisions.append(turn.revision)
        raise AssertionError("Pending approval must not trigger replanning")


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", json.loads((Path(__file__).resolve().parents[4] / "conformance/fixtures/approval_runtime_modes.json").read_text())["cases"], ids=lambda item: item["id"])
async def test_planning_approval_restart_preserves_tool_transition_without_model_work(tmp_path, scenario):
    mode = scenario["id"]
    path = tmp_path / 'db'; planner = ApprovalPlanner(); host = ApprovalHost(path, planner=planner)
    host.request = replace(host.request, planning_mode=scenario["requestPlanningMode"])
    original = host.request; expiry = int(time.time()*1000)+60000; host.expiry = expiry
    host.promote = mode == "promoted"
    try:
        await host.storage.enable_approvals()
        handle = await host.core.submit(original, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary, tool_checkpoint_names=frozenset({"lookup"})))
        with pytest.raises(ApprovalRequired): await handle.wait()
        snapshot = await host.storage.runs.get(handle.run_id); checkpoint = snapshot.execution_checkpoint
        assert checkpoint.execution_profile == scenario["checkpointProfile"]
        assert planner.calls == scenario["plannerCreationsBeforeWait"]
        if mode != "auto":
            assert snapshot.steps[0].status.value == "done"
            assert snapshot.steps[1].status.value == "running"
        await host.close(); planner = ApprovalPlanner(); host = ApprovalHost(path, planner=planner); host.expiry = expiry
        host.request = original
        options = AgentCoreRunOptions(tool_checkpoint_handler=host.boundary, tool_checkpoint_names=frozenset({"lookup"}))
        with pytest.raises(ApprovalRequired): await (await host.core.resume(handle.run_id, original, options=options)).wait()
        assert host.model_calls == host.tool_calls == planner.calls == 0
        assert (await host.storage.runs.get(handle.run_id)).execution_checkpoint == checkpoint
        (record,) = await host.approvals.list_pending(run_id=handle.run_id)
        await host.approvals.decide(ApprovalDecisionCommand(approval_id=record.approval_id, expected_revision=record.revision,
            intent_digest=record.intent.digest, command_key="approve", decision="approve"), principal_id="host")
        result = await (await host.core.resume(handle.run_id, original, options=options)).wait()
        assert result.status.value == "done"
        assert host.tool_calls == 1 and planner.calls == 0 and planner.revisions == []
    finally: await host.close()


class TreeApprovalPlanner(ApprovalPlanner):
    async def create_plan(self, *args, **kwargs):
        self.calls += 1
        return PlanningResult(kind="planned", work_plan=WorkPlan(title="Delegate, write, finish", steps=(
            WorkStep(id="delegate", title="Delegate", type="read", executor="tool", capability_names=("delegateToAgents",)),
            WorkStep(id="write", title="Write", type="write", executor="tool", capability_names=("lookup",), depends_on=("delegate",)),
            WorkStep(id="finish", title="Finish", type="review", executor="model", depends_on=("write",)),
        )))

    async def revise_plan(self, request, capabilities, turn, *args, **kwargs):
        self.revisions.append(turn.revision)
        return PlanningResult(kind="planned", work_plan=WorkPlan(title="Write after delegation", steps=(
            WorkStep(id=f"write-{turn.revision}", title="Write", type="write", executor="tool", capability_names=("lookup",)),
            WorkStep(id=f"finish-{turn.revision}", title="Finish", type="review", executor="model", depends_on=(f"write-{turn.revision}",)),
        )))


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["reactive", "planned"])
async def test_tree_root_approval_after_read_only_child_keeps_child_result(tmp_path, mode):
    def script(messages, invocation):
        if any(message.attributes.get("agentId") for message in messages):
            assert all(tool.name != "lookup" for tool in invocation.tools)
            return "Child result"
        names = [call.name for message in messages for call in message.tool_calls]
        if "lookup" in names or not invocation.tools: return "42"
        if "delegateToAgents" in names: return "lookup", {}
        return "delegateToAgents", {"children": [{"name": "reader", "title": "Read", "instruction": "Read only", "objective": "Report"}]}
    path = tmp_path / "db"
    host = ApprovalHost(path, tree=True, planner=TreeApprovalPlanner() if mode == "planned" else None)
    host.script = script; host.expiry = int(time.time()*1000)+60000; expiry = host.expiry
    original = replace(host.request, planning_mode=mode)
    try:
        await host.storage.enable_approvals()
        options = AgentCoreRunOptions(tool_checkpoint_handler=host.boundary, tool_checkpoint_names=frozenset({"lookup"}))
        handle = await host.core.submit(original, options=options)
        with pytest.raises(ApprovalRequired):
            result = await handle.wait()
            pytest.fail(f"Expected approval wait, received {result!r}")
        if mode == "planned":
            assert (await host.storage.runs.get(handle.run_id)).execution_checkpoint.planning_state["revision"] == 1
        children = await host.storage.run_tree.list_descendants(handle.run_id)
        assert len(children) == 1 and children[0].status.value == "done"
        await host.close()
        host = ApprovalHost(path, tree=True, planner=TreeApprovalPlanner() if mode == "planned" else None)
        host.script = script; host.expiry = expiry
        options = AgentCoreRunOptions(tool_checkpoint_handler=host.boundary, tool_checkpoint_names=frozenset({"lookup"}))
        with pytest.raises(ApprovalRequired): await (await host.core.resume(handle.run_id, original, options=options)).wait()
        assert host.model_calls == host.tool_calls == 0
        (record,) = await host.approvals.list_pending(run_id=handle.run_id)
        await host.approvals.decide(ApprovalDecisionCommand(approval_id=record.approval_id, expected_revision=record.revision,
            intent_digest=record.intent.digest, command_key="approve", decision="approve"), principal_id="host")
        result = await (await host.core.resume(handle.run_id, original, options=options)).wait()
        assert result.status.value == "done" and host.tool_calls == 1
        assert await host.storage.run_tree.list_descendants(handle.run_id) == children
    finally: await host.close()


@pytest.mark.asyncio
async def test_auto_remaining_plan_after_approved_write_preserves_second_wait(tmp_path):
    path = tmp_path / 'db'; host = ApprovalHost(path, planner=ApprovalPlanner())
    original = replace(host.request, planning_mode="auto"); expiry = int(time.time()*1000)+60000; host.expiry = expiry
    async def approve_pending(current, run_id):
        record = next(record for record in await current.approvals.list_pending(run_id=run_id) if record.status == "pending")
        await current.approvals.decide(ApprovalDecisionCommand(approval_id=record.approval_id, expected_revision=record.revision,
            intent_digest=record.intent.digest, command_key="approve", decision="approve"), principal_id="host")
    try:
        await host.storage.enable_approvals()
        options = AgentCoreRunOptions(tool_checkpoint_handler=host.boundary, tool_checkpoint_names=frozenset({"lookup"}))
        handle = await host.core.submit(original, options=options)
        with pytest.raises(ApprovalRequired): await handle.wait()
        assert (await host.storage.runs.get(handle.run_id)).execution_checkpoint.initial_planning_open is True
        await approve_pending(host, handle.run_id)
        await host.close(); planner = ApprovalPlanner(); host = ApprovalHost(path, planner=planner); host.expiry = expiry
        host.script = lambda messages, invocation: ("request_remaining_plan", {}) if host.model_calls == 1 else ("lookup", {})
        options = AgentCoreRunOptions(tool_checkpoint_handler=host.boundary, tool_checkpoint_names=frozenset({"lookup"}))
        with pytest.raises(ApprovalRequired): await (await host.core.resume(handle.run_id, original, options=options)).wait()
        checkpoint = (await host.storage.runs.get(handle.run_id)).execution_checkpoint
        assert checkpoint.execution_profile == "planned" and not checkpoint.initial_planning_open
        assert host.tool_calls == 1 and planner.calls == 1
        await host.close(); planner = ApprovalPlanner(); host = ApprovalHost(path, planner=planner); host.expiry = expiry
        await approve_pending(host, handle.run_id)
        options = AgentCoreRunOptions(tool_checkpoint_handler=host.boundary, tool_checkpoint_names=frozenset({"lookup"}))
        result = await (await host.core.resume(handle.run_id, original, options=options)).wait()
        assert result.status.value == "done" and host.tool_calls == 1 and planner.calls == 0
    finally: await host.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['pending','approved','expired'])
async def test_approval_inspection_never_refreshes_or_dispatches(tmp_path, monkeypatch, status):
    host=ApprovalHost(tmp_path/'db');host.expiry=int(time.time()*1000)+60000
    try:
        await host.storage.enable_approvals()
        options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary)
        handle=await host.core.submit(host.request,options=options)
        with pytest.raises(ApprovalRequired): await handle.wait()
        record=(await host.approvals.list_pending())[0]
        if status == 'approved':
            await host.approvals.decide(ApprovalDecisionCommand(record.approval_id,record.revision,record.intent.digest,'approve','approve'),principal_id='private-principal')
        if status == 'expired': monkeypatch.setattr(time,'time',lambda:host.expiry/1000)
        before=list(host.storage._db.iterdump())
        report=await host.storage.inspect_recovery(handle.run_id)
        assert list(host.storage._db.iterdump()) == before
        assert report['observations']['approvalState']==status
        assert report['observations']['approvalCheckpointIntent']=='matched'
        assert report['observations']['approvalRecords']==1
        assert report['observations']['approvalReceipt']=='absent'
        assert report['observations']['attemptsAfterCheckpoint']==0
        assert (host.model_calls,host.tool_calls)==(1,0)
        assert record.approval_id not in json.dumps(report)
        assert record.intent.digest not in json.dumps(report)
        assert 'private-principal' not in json.dumps(report)
    finally: await host.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['expired','replaced-owner','advanced-epoch'])
async def test_late_approved_result_cannot_cross_lease_fence(tmp_path, fault):
    path=tmp_path/'db';host,options,record=await approved_host(path)
    host.block_tool=True;host.entered=asyncio.Event();host.release=asyncio.Event()
    other=ApprovalHost(path);other.expiry=host.expiry
    run_id=record.intent.run_id
    try:
        handle=await host.core.resume(run_id,host.request,options=options)
        await asyncio.wait_for(host.entered.wait(),2)
        async with other.storage.transaction() as session:
            old=session.leases[run_id]
            replacement=replace(old,expires_at_ms=0) if fault=='expired' else replace(old,
                owner_id='replacement-owner' if fault=='replaced-owner' else old.owner_id,
                attempt=old.attempt+1,expires_at_ms=int(time.time()*1000)+60000)
            session.leases[run_id]=replacement
            claim=dict(session.extra['approvalExecutions'][record.approval_id])
        before=await other.storage.runs.get(run_id)
        events=await other.storage.outputs.list_events(run_id,after_sequence=0)
        if fault=='advanced-epoch':
            from purra.execution.ownership import execution_claim
            token=execution_claim.set((run_id,old.owner_id,old.attempt))
            try:
                assert not await other.storage.leases.renew(run_id,old.owner_id,lease_duration_ms=60000)
                assert not await other.storage.leases.release(run_id,old.owner_id)
            finally: execution_claim.reset(token)
        host.release.set()
        try: await asyncio.wait_for(handle.wait(),3)
        except ContractViolationError: pass
        async with other.storage.transaction() as session:
            assert session.get_tool_receipt((run_id,record.intent.tool_call_id)) is None
            assert (run_id,record.intent.tool_call_id) in session.claims
            assert session.extra['approvalExecutions'][record.approval_id]==claim
            if fault!='expired': assert session.leases[run_id]==replacement
        assert host.tool_calls==1
        assert await other.storage.runs.get(run_id)==before
        assert await other.storage.outputs.list_events(run_id,after_sequence=0)==events
        await other.close();other=ApprovalHost(path);other.expiry=host.expiry
        with pytest.raises(ContractViolationError):
            resumed=await other.core.resume(run_id,other.request,options=AgentCoreRunOptions(tool_checkpoint_handler=other.boundary))
            await resumed.wait()
        assert other.tool_calls==other.model_calls==0
        assert 'tool_effect_unknown' in (await other.storage.inspect_recovery(run_id))['blockers']
    finally:
        host.release.set();await other.close();await host.close()

@pytest.mark.asyncio
@pytest.mark.parametrize('completed', [True, False])
async def test_host_reconciliation_preserves_terminal_run_after_reopen(tmp_path, completed):
    path = tmp_path/'db'
    host, options, record = await approved_host(path)
    host.unknown_effect = True
    try:
        result = await (await host.core.resume(record.intent.run_id, host.request, options=options)).wait()
        assert result.status.value == 'failed'
        key = (record.intent.run_id, record.intent.tool_call_id)
        async with host.storage.transaction() as session:
            call = session.claims[key]
        proof = {'result': ToolHandlerResult('verified', effect_state='committed')} if completed else {'not_executed': True}
        schedule = host.storage.recovery_schedule(clock_ms=lambda: 100)
        await schedule.wake(record.intent.run_id)
        token = await schedule.ready(record.intent.run_id)
        await schedule.settle(record.intent.run_id, token, True)
        host.storage._db.execute("CREATE TRIGGER fail_reconcile BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END")
        with pytest.raises(Exception):
            await host.storage.reconcile_tool(record.intent.run_id, call, **proof)
        host.storage._db.execute('DROP TRIGGER fail_reconcile')
        assert await schedule.ready(record.intent.run_id) is None
        async with host.storage.transaction() as session:
            assert key in session.claims
        await host.storage.reconcile_tool(record.intent.run_id, call, **proof)
        assert await schedule.ready(record.intent.run_id) is not None
        await host.close()
        host = ApprovalHost(path)
        async with host.storage.transaction() as session:
            assert key not in session.claims
            receipt = session.get_tool_receipt(key)
            assert (receipt is not None) == completed
        with pytest.raises(ContractViolationError) as error:
            await (await host.core.resume(record.intent.run_id, host.request, options=options)).wait()
        assert error.value.code == 'run_terminal'
        assert host.model_calls == host.tool_calls == 0
    finally:
        await host.close()

@pytest.mark.asyncio
@pytest.mark.parametrize('completed', [True, False])
@pytest.mark.parametrize('gate', ['allowed', 'revoked'])
async def test_running_tool_checkpoint_resumes_after_host_reconciliation(tmp_path, completed, gate):
    path = tmp_path/'db'
    host, options, record = await approved_host(path)
    host.fail_receipt = True
    host.fail_before_effect = not completed
    key = (record.intent.run_id, record.intent.tool_call_id)
    try:
        with pytest.raises(Exception):
            await (await host.core.resume(record.intent.run_id, host.request, options=options)).wait()
        assert getattr(host, 'effects', 0) == int(completed)
        host.storage._db.execute('DROP TRIGGER fail_receipt')
        await host.close()
        host = ApprovalHost(path)
        host.expiry = record.expires_at_ms
        saved = await host.storage.runs.get(record.intent.run_id)
        assert saved.status.value == 'running'
        async with host.storage.transaction() as session:
            call = session.claims[key]
        proof = {'result': ToolHandlerResult('42', effect_state='committed')} if completed else {'not_executed': True}
        schedule = host.storage.recovery_schedule(clock_ms=lambda: 100)
        await schedule.wake(record.intent.run_id)
        token = await schedule.ready(record.intent.run_id)
        await schedule.settle(record.intent.run_id, token, True)
        host.storage._db.execute("CREATE TRIGGER fail_reconcile BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END")
        with pytest.raises(Exception):
            await host.storage.reconcile_tool(record.intent.run_id, call, **proof)
        host.storage._db.execute('DROP TRIGGER fail_reconcile')
        assert await schedule.ready(record.intent.run_id) is None
        async with host.storage.transaction() as session:
            assert key in session.claims
        await host.storage.reconcile_tool(record.intent.run_id, call, **proof)
        assert await schedule.ready(record.intent.run_id) is not None
        await host.close()
        host = ApprovalHost(path)
        host.expiry = record.expires_at_ms
        host.require_tool_history = True
        host.deny_scope = gate == 'revoked'
        resumed = await host.core.resume(record.intent.run_id, host.request, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
        result = await resumed.wait()
        if gate == 'revoked':
            assert result.status.value == 'failed'
            assert host.tool_calls == host.model_calls == 0
            return
        assert result.status.value == 'done'
        assert host.tool_calls == (0 if completed else 1)
        assert getattr(host, 'effects', 0) == (0 if completed else 1)
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_recovery_worker_rediscovers_approval_after_reopen(tmp_path):
    from purra.api import RecoveryWorker
    path = tmp_path / 'worker.db'
    host = ApprovalHost(path)
    expiry = int(time.time() * 1000) + 60000
    host.expiry = expiry
    try:
        await host.storage.enable_approvals()
        handle = await host.core.submit(host.request, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
        with pytest.raises(ApprovalRequired):
            await handle.wait()
        run_id = handle.run_id
        await host.close()
        host = ApprovalHost(path)
        host.expiry = expiry
        async def resume(run_id):
            resumed = await host.core.resume(run_id, host.request, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
            return await resumed.wait()
        worker = RecoveryWorker(discover=host.storage.list_running, inspect=host.storage.inspect_recovery, resume=resume)
        for _ in range(2):
            report = await worker.run_once()
            assert report[0].action == 'blocked'
            assert 'approval_required' in report[0].reasons
        assert (host.model_calls, host.tool_calls) == (0, 0)
        record = (await host.approvals.list_pending())[0]
        await host.approvals.decide(ApprovalDecisionCommand(record.approval_id, record.revision, record.intent.digest, 'worker-approve', 'approve'), principal_id='host')
        report = await worker.run_once()
        assert [(item.run_id, item.action) for item in report] == [(run_id, 'settled')]
        assert host.tool_calls == 1
        assert await worker.run_once() == ()
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_worker_clean_diagnosis_cannot_bypass_changed_scope(tmp_path):
    from purra.api import RecoveryWorker
    host, options, record = await approved_host(tmp_path / 'race.db')
    outcomes = []
    try:
        async def inspect(run_id):
            report = await host.storage.inspect_recovery(run_id)
            assert not report['blockers']
            host.deny_scope = True
            return report
        async def resume(run_id):
            handle = await host.core.resume(run_id, host.request, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
            outcomes.append(await handle.wait())
        before = host.model_calls
        report = await RecoveryWorker(discover=host.storage.list_running, inspect=inspect, resume=resume).run_once()
        assert report[0].action == 'settled'
        assert outcomes[0].status.value == 'failed'
        assert host.tool_calls == 0 and host.model_calls == before
    finally:
        await host.close()


@pytest.mark.asyncio
async def test_worker_service_wakes_after_committed_approval(tmp_path):
    import asyncio
    from purra.api import RecoveryWorker
    host = ApprovalHost(tmp_path / 'service.db')
    host.expiry = int(time.time() * 1000) + 60000
    stop = asyncio.Event()
    reports = []
    schedule = host.storage.recovery_schedule()
    try:
        await host.storage.enable_approvals()
        options = AgentCoreRunOptions(tool_checkpoint_handler=host.boundary)
        handle = await host.core.submit(host.request, options=options)
        with pytest.raises(ApprovalRequired): await handle.wait()
        async def resume(run_id):
            return await (await host.core.resume(run_id, host.request, options=options)).wait()
        async def observed(report):
            reports.append(report)
            if len(reports) == 1:
                assert report[0].action == 'blocked'
                assert (host.model_calls, host.tool_calls) == (1, 0)
                record = (await host.approvals.list_pending())[0]
                await host.approvals.decide(ApprovalDecisionCommand(record.approval_id, record.revision, record.intent.digest, 'wake', 'approve'), principal_id='host')
                worker.wake()
            else:
                stop.set()
        worker = RecoveryWorker(discover=host.storage.list_running, inspect=host.storage.inspect_recovery, resume=resume, schedule=schedule)
        await asyncio.wait_for(worker.run(stop=stop, poll_interval_ms=60000, max_backoff_ms=60000, on_scan=observed), 5)
        assert len(reports) == 2 and reports[1][0].action == 'settled'
        assert host.tool_calls == 1
        assert await host.storage.list_running() == ()
    finally:
        stop.set()
        await host.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('decision', ['approve', 'reject'])
async def test_decision_and_registered_wake_commit_atomically(tmp_path, decision):
    host = ApprovalHost(tmp_path / 'atomic.db')
    host.expiry = int(time.time() * 1000) + 60000
    try:
        await host.storage.enable_approvals()
        handle = await host.core.submit(host.request, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
        with pytest.raises(ApprovalRequired): await handle.wait()
        record = (await host.approvals.list_pending())[0]
        schedule = host.storage.recovery_schedule(clock_ms=lambda: 100)
        await schedule.wake(handle.run_id)
        token = await schedule.ready(handle.run_id)
        await schedule.settle(handle.run_id, token, True)
        command = ApprovalDecisionCommand(record.approval_id, record.revision, record.intent.digest, 'atomic', decision)
        host.storage._db.execute("CREATE TRIGGER fail_decision BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END")
        with pytest.raises(Exception): await host.approvals.decide(command, principal_id='host')
        host.storage._db.execute('DROP TRIGGER fail_decision')
        assert (await host.approvals.get(record.approval_id)).status == 'pending'
        assert await schedule.ready(handle.run_id) is None
        receipt = await host.approvals.decide(command, principal_id='host')
        token = await schedule.ready(handle.run_id)
        assert token is not None
        await schedule.settle(handle.run_id, token, True)
        assert await host.approvals.decide(command, principal_id='host') == receipt
        assert await schedule.ready(handle.run_id) is None
    finally: await host.close()


@pytest.mark.asyncio
async def test_terminal_schedule_prune_fences_old_scans(tmp_path):
    host, options, record = await approved_host(tmp_path / 'prune.db')
    run_id = record.intent.run_id
    try:
        schedule = host.storage.recovery_schedule(clock_ms=lambda: 100)
        await schedule.wake(run_id)
        stale = await schedule.ready(run_id)
        assert await host.storage.prune_recovery_schedule([run_id]) == ()
        assert (await (await host.core.resume(run_id, host.request, options=options)).wait()).status.value == 'done'
        assert await host.storage.prune_recovery_schedule([run_id, run_id]) == (run_id,)
        assert not await schedule.settle(run_id, stale, True)
        await host.close()
        host = ApprovalHost(tmp_path / 'prune.db')
        schedule = host.storage.recovery_schedule(clock_ms=lambda: 100)
        await schedule.wake(run_id)
        assert not await schedule.settle(run_id, stale, True)
        assert (await host.storage.runs.get(run_id)).status.value != 'running'
    finally: await host.close()


@pytest.mark.asyncio
async def test_candidate_page_can_feed_worker_without_resuming_terminal_run(tmp_path):
    from purra.api import RecoveryWorker
    host, options, record = await approved_host(tmp_path / 'candidate.db')
    try:
        assert (await (await host.core.resume(record.intent.run_id, host.request, options=options)).wait()).status.value == 'done'
        assert (await host.storage.inspect_recovery_many([record.intent.run_id]))[record.intent.run_id] == await host.storage.inspect_recovery(record.intent.run_id)
        page = await host.storage.list_run_candidates(limit=1)
        assert page['runIds'] == (record.intent.run_id,)
        async def discover(): return (await host.storage.list_run_candidates(limit=1))['runIds']
        calls = []
        async def resume(run_id): calls.append(run_id)
        result = await RecoveryWorker(discover=discover, inspect=host.storage.inspect_recovery, resume=resume).run_once()
        assert result[0].action == 'blocked' and 'run_terminal' in result[0].reasons
        assert calls == []
    finally: await host.close()
