import json
from pathlib import Path

import pytest

from purra.planning_stream import PlanningStreamParser, PlanningStreamError


FIXTURE = json.loads((Path(__file__).parents[1] / "conformance/fixtures/planning_stream.json").read_text())


@pytest.mark.parametrize("case", FIXTURE["valid"], ids=lambda case: case["name"])
def test_protocol_at_every_character_boundary(case):
    for boundary in range(len(case["wire"]) + 1):
        parser = PlanningStreamParser()
        records = [*parser.feed(case["wire"][:boundary]), *parser.feed(case["wire"][boundary:])]
        assert [record.text for record in records] == case["texts"]
        assert parser.finish() == case["plan"]
        assert parser.rejected_progress_records == case.get(
            "rejectedProgressRecords",
            0,
        )
        for record in records:
            raw = case["wire"].encode()[record.source_start:record.source_end].decode()
            assert json.loads(raw)["text"] == record.text


@pytest.mark.parametrize("case", FIXTURE["invalid"], ids=lambda case: case["name"])
def test_protocol_fails_closed(case):
    parser = PlanningStreamParser()
    with pytest.raises(PlanningStreamError):
        for char in case["wire"]:
            parser.feed(char)
        parser.finish()


def test_partial_and_unknown_records_are_never_public_and_buffers_are_bounded():
    parser = PlanningStreamParser()
    assert parser.feed('{"v":1,"type":"progress","text":"SECRET') == ()
    with pytest.raises(PlanningStreamError):
        parser.feed("x" * 262144)
    with pytest.raises(PlanningStreamError):
        parser.feed('"}\n')


import asyncio
from dataclasses import replace

from purra.api import AgentPlanner, ExecutionProfile, InMemoryAgentAdapters
from purra.contracts import ModelStream, ModelStreamChunk, ModelFinishReason, ModelTokenUsage, PlannerLimits, PlanningCapabilities, PlanningConstraints, PlanningMode, RuntimeLimits
from purra.contracts import ModelStreamActivity, ModelTransportDiagnostics
from purra.output import OutputEventKind, OutputVisibility
from purra.output import AgentOutputEventDraft
from purra.errors import ContractViolationError, InvalidPlannerOutputError
from test_standalone_agent_conformance import _core, _request as _reactive_request, _Context


def _request():
    return replace(_reactive_request(), planning_mode=PlanningMode.PLANNED)


def wire(plan=None, text=None):
    return json.dumps({"v": 1, "type": "progress", "text": text} if text else
                      {"v": 1, "type": "plan", "plan": plan or {"needsTodos": False, "reason": "PRIVATE_PLAN_MARKER"}}, ensure_ascii=False) + "\n"


class Policy:
    def planning_constraints(self, request, capabilities): return PlanningConstraints(allow_model_only_fallback=False)


class StreamGateway:
    def __init__(self, scripts):
        self.scripts = list(scripts)
        self.opened = 0
        self.closed = 0
        self.planning_calls = 0
        self.executions = 0
        self.message_rounds = []

    async def complete(self, *args, **kwargs):
        raise AssertionError("planning and repair must stream")

    async def stream(self, messages, invocation, signal=None):
        planning = any("planning component" in m.content for m in messages if m.role.value == "system")
        if planning:
            self.planning_calls += 1
            self.message_rounds.append(tuple(messages))
            script = self.scripts.pop(0)
        else:
            self.executions += 1
            script = ["done"]
        self.opened += 1

        async def chunks():
            try:
                for item in script:
                    if isinstance(item, asyncio.Event):
                        await item.wait()
                    elif isinstance(item, Exception):
                        raise item
                    elif isinstance(item, (ModelStreamChunk, ModelStreamActivity)):
                        yield item
                    else:
                        yield ModelStreamChunk(content_delta=item)
                yield ModelStreamChunk(finish_reason=ModelFinishReason.STOP,
                                       usage=ModelTokenUsage(input_tokens=10, generation_tokens=8))
            finally:
                pass
        source = chunks()
        gateway = self
        closed = False
        class OwnedChunks:
            def __aiter__(self): return self
            async def __anext__(self): return await anext(source)
            async def aclose(self):
                nonlocal closed
                if not closed:
                    closed = True
                    gateway.closed += 1
                    await source.aclose()
        return ModelStream(chunks=OwnedChunks(), model="portable-model", applied_generation_limit=invocation.output_budget.max_generation_tokens,
                           activity_support=getattr(self, "activity_support", "semantic_only"))


def managed(scripts, *, planner_limits=PlannerLimits(), result_validator=None, **kwargs):
    gateway = StreamGateway(scripts)
    adapters = InMemoryAgentAdapters()
    core = _core(gateway=gateway, context=_Context(), adapters=adapters,
                 profile=ExecutionProfile(
                     planner=AgentPlanner(gateway, limits=planner_limits, result_validator=result_validator),
                     planning_policy=Policy(),
                 ), **kwargs)
    return core, adapters, gateway


@pytest.mark.asyncio
async def test_public_progress_is_persisted_before_final_and_replayed_without_private_data():
    release = asyncio.Event()
    core, adapters, gateway = managed([[ModelStreamChunk(reasoning_delta="PRIVATE_REASONING"), wire(text="准备核对证据。"), release, wire().rstrip("\n")]])
    try:
        handle = await core.submit(_request())
        seen = []
        async for event in handle.subscribe():
            seen.append(event)
            if event.kind is OutputEventKind.PLANNING_PROGRESS:
                stored = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
                assert event in stored
                assert gateway.executions == 0
                assert event.source.value == "provider"
                release.set()
        assert (await handle.wait()).status.value == "done"
        replay = [event async for event in handle.subscribe()]
        assert replay == seen
        text = str([(e.kind, dict(e.payload)) for e in replay])
        assert "PRIVATE_REASONING" not in text and "PRIVATE_PLAN_MARKER" not in text
        all_events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
        phase = next(e for e in replay if e.kind is OutputEventKind.OPERATION_STARTED and e.payload["kind"] == "planning")
        terminal = [e for e in replay if e.kind is OutputEventKind.OPERATION_FINISHED and e.payload["operationId"] == phase.payload["operationId"]]
        assert len(terminal) == 1 and terminal[0].payload["status"] == "succeeded"
        assert terminal[0].payload["display"]["labelParams"]["modelAttempts"] == 1
        diagnostics = next(e for e in all_events if e.kind is OutputEventKind.MODEL_DIAGNOSTICS)
        assert diagnostics.payload["firstSemanticChunkMs"] is not None
        assert diagnostics.payload["firstPublicProgressMs"] <= diagnostics.payload["planReceivedMs"]
        assert diagnostics.payload["httpFirstByteAtMs"] is None
        assert gateway.opened == gateway.closed == 2
    finally:
        release.set()
        await core.close()


@pytest.mark.asyncio
async def test_invalid_optional_progress_is_omitted_without_repairing_valid_plan():
    invalid_progress = json.dumps({
        "v": 1,
        "type": "progress",
        "text": "第一行\n第二行",
    }, ensure_ascii=False) + "\n"
    core, adapters, gateway = managed([[invalid_progress, wire()]])
    try:
        handle = await core.submit(_request())
        assert (await handle.wait()).status.value == "done"
        public = [event async for event in handle.subscribe()]
        assert not any(
            event.kind is OutputEventKind.PLANNING_PROGRESS
            for event in public
        )
        events = await adapters.outputs.list_events(
            handle.run_id,
            after_sequence=0,
        )
        diagnostics = next(
            event for event in events
            if event.kind is OutputEventKind.MODEL_DIAGNOSTICS
            and "rejectedPublicProgressRecords" in event.payload
        )
        assert diagnostics.payload["rejectedPublicProgressRecords"] == 1
        assert diagnostics.payload["firstPublicProgressMs"] is None
        assert gateway.planning_calls == 1
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_planner_attempt_deadline_is_independent_from_run_timeout():
    gate = asyncio.Event()
    core, _adapters, gateway = managed(
        [[gate]],
        planner_limits=PlannerLimits(
            max_repair_attempts=0,
            attempt_timeout_ms=20,
        ),
        runtime_limits=RuntimeLimits(
            max_run_generation_tokens=None,
            provider_invocation_timeout_ms=1_000,
            root_run_timeout_ms=2_000,
        ),
    )
    try:
        handle = await core.submit(_request())
        assert (await handle.wait()).status.value == "failed"
        events = [event async for event in handle.subscribe()]
        assert any(
            event.payload.get("errorCode") == "planning_deadline_exceeded"
            for event in events
        )
        assert gateway.planning_calls == 1
    finally:
        gate.set()
        await core.close()


@pytest.mark.asyncio
async def test_repair_preserves_progress_history_and_charges_only_provider_attempts():
    core, adapters, gateway = managed([[wire(text="准备检查范围。")], [wire(text="准备调整检查范围。"), wire()]])
    try:
        handle = await core.submit(_request())
        assert (await handle.wait()).status.value == "done"
        public = [e async for e in handle.subscribe()]
        progress = [e for e in public if e.kind is OutputEventKind.PLANNING_PROGRESS]
        assert [e.payload["attempt"] for e in progress] == [0, 1]
        assert len({e.payload["operationId"] for e in progress}) == 1
        assert len({e.invocation_id for e in progress}) == 2
        events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
        assert any(e.kind is OutputEventKind.STREAM_ABORTED and e.invocation_id == progress[0].invocation_id for e in events)
        budget = adapters.runs._state.runs[handle.run_id]
        assert len(budget.model_attempt_ids) == gateway.opened == 3
        assert gateway.closed == gateway.opened
    finally:
        await core.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("repair_attempts", [1, 2])
async def test_format_and_host_repairs_receive_only_the_previous_output(repair_attempts):
    target = {"collection": {"version": 1, "scope": {"count": 3}}}
    plan = {
        "needsTodos": True,
        "title": "Draft selected items",
        "taskSpec": {"goal": "Draft three items", "operation": "write", "target": target},
        "todos": [
            {"id": "analyze", "title": "Analyze selected items", "type": "analyze",
             "executor": "model", "riskLevel": "read"},
            {"id": "draft", "title": "Draft selected items", "type": "write",
             "executor": "model", "riskLevel": "read", "dependsOn": ["analyze"]},
            {"id": "review", "title": "Review drafted items", "type": "review",
             "executor": "model", "riskLevel": "read", "dependsOn": ["draft"]},
        ],
        "repairEvidence": "PRIVATE_REPAIR_EVIDENCE",
    }
    invalid = {**plan, "taskSpec": {**plan["taskSpec"], "target": target["collection"]}}
    first = [wire(text="准备创作。"), json.dumps(plan) + "\n"]
    second = [wire(text="准备确认范围。"), wire(invalid)]
    core, adapters, gateway = managed(
        [[ModelStreamChunk(reasoning_delta="PRIVATE_REASONING"), *first], second, [wire(plan)]],
        planner_limits=PlannerLimits(max_repair_attempts=repair_attempts),
        result_validator=lambda _request, result: (
            "taskSpec.target.collection is required"
            if "collection" not in result.work_plan.task_spec.target else None
        ),
    )
    try:
        handle = await core.submit(_request())
        result = await handle.wait()
        assert result.status.value == ("done" if repair_attempts == 2 else "failed")
        assert gateway.planning_calls == repair_attempts + 1
        assert gateway.executions == (1 if repair_attempts == 2 else 0)
        assert gateway.opened == gateway.closed
        assert len(adapters.runs._state.runs[handle.run_id].model_attempt_ids) == gateway.opened
        repair = gateway.message_rounds[1]
        assert repair[:-2] == gateway.message_rounds[0]
        assert repair[-2].role.value == "assistant"
        assert repair[-2].content == "".join(first)
        assert "unsupported version or envelope" in repair[-1].content
        if repair_attempts == 2:
            repair = gateway.message_rounds[2]
            assert repair[:-2] == gateway.message_rounds[0]
            assert repair[-2].role.value == "assistant"
            assert repair[-2].content == "".join(second)
            assert "taskSpec.target.collection is required" in repair[-1].content
        else:
            assert result.error == "invalid_plan"
        assert "PRIVATE_REASONING" not in str(gateway.message_rounds)
        public = [event async for event in handle.subscribe()]
        events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
        diagnostics = [event for event in events if event.kind is OutputEventKind.MODEL_DIAGNOSTICS]
        for serialized in (str(public), str(diagnostics)):
            assert "PRIVATE_REPAIR_EVIDENCE" not in serialized
            assert "PRIVATE_REASONING" not in serialized
    finally:
        await core.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("repair_attempts", [0, 1])
async def test_rejected_planner_output_is_bounded_and_excluded_from_error_serialization(repair_attempts):
    rejected = json.dumps({"needsTodos": False, "reason": "😀" * 66_000}, ensure_ascii=False) + "\n"
    gateway = StreamGateway([
        [ModelStreamChunk(reasoning_delta="PRIVATE_REASONING"), rejected],
        [wire()],
    ])
    planner = AgentPlanner(gateway, limits=PlannerLimits(max_repair_attempts=repair_attempts))
    if repair_attempts:
        await planner.create_plan(_request(), PlanningCapabilities())
        evidence = gateway.message_rounds[1][-2].content
        assert "PRIVATE_REASONING" not in str(gateway.message_rounds)
    else:
        with pytest.raises(InvalidPlannerOutputError) as caught:
            await planner.create_plan(_request(), PlanningCapabilities())
        error = caught.value
        evidence = error.rejected_output
        assert "😀" not in str(error)
        assert "😀" not in repr(error)
        assert "rejected_output" not in vars(error)
        assert "😀" not in json.dumps(vars(error), ensure_ascii=False)
        assert "PRIVATE_REASONING" not in str(error.args)
    expected_limit = 2_048 if repair_attempts else 65_536
    assert evidence == rejected[:expected_limit]
    assert len(evidence) == expected_limit
    assert gateway.opened == gateway.closed == repair_attempts + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("position", ["before", "during", "repair"])
async def test_planning_cancellation_closes_streams_and_no_late_progress(position):
    gate = asyncio.Event()
    scripts = [[gate, wire(text="迟到说明"), wire()]] if position == "before" else [[wire(text="准备核对。"), gate, wire()]]
    if position == "repair": scripts = [[wire(text="准备核对。")], [gate, wire(text="迟到说明"), wire()]]
    core, adapters, gateway = managed(scripts)
    try:
        handle = await core.submit(_request())
        for _ in range(200):
            if gateway.planning_calls == (2 if position == "repair" else 1): break
            await asyncio.sleep(.001)
        await handle.cancel("test")
        assert (await handle.wait()).status.value == "canceled"
        before = [e async for e in handle.subscribe()]
        gate.set()
        await asyncio.sleep(.01)
        assert before == [e async for e in handle.subscribe()]
        assert "迟到说明" not in str(before)
        assert gateway.closed == gateway.opened
        assert not core._operations.running_operation_ids
    finally:
        gate.set()
        await core.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("script", [[ModelStreamChunk(reasoning_delta="SECRET")], [wire(text="只有意图。")], [wire() + wire()], ["unknown\n"]])
async def test_no_complete_plan_never_executes_and_repair_is_bounded(script):
    core, adapters, gateway = managed([script, script])
    try:
        handle = await core.submit(_request())
        assert (await handle.wait()).status.value == "failed"
        assert gateway.planning_calls == 2 and gateway.executions == 0
        assert gateway.closed == gateway.opened
        assert "SECRET" not in str([e async for e in handle.subscribe()])
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_projection_dedup_forgery_run_isolation_and_terminal_fences():
    gate = asyncio.Event()
    core, adapters, gateway = managed([[wire(text="准备核对。"), gate, wire()], [wire()]])
    try:
        first = await core.submit(_request())
        async for event in first.subscribe():
            if event.kind is OutputEventKind.PLANNING_PROGRESS: break
        draft = AgentOutputEventDraft(**{name: getattr(event, name) for name in
            ("run_id", "turn_id", "output_stream_id", "invocation_id", "source_event_key", "source", "kind", "channel", "visibility", "payload", "occurred_at")})
        assert (await adapters.outputs.append_event(draft)).event_id == event.event_id
        with pytest.raises(ContractViolationError):
            await adapters.outputs.append_event(replace(draft, payload={**draft.payload, "text": "forged"}))
        with pytest.raises(ContractViolationError):
            await adapters.outputs.append_event(replace(draft, source_event_key=f"planning:{event.invocation_id}:2", payload={**draft.payload, "recordIndex": 2, "sourceStart": 1}))
        with pytest.raises(ContractViolationError):
            await adapters.outputs.append_event(replace(draft, kind=OutputEventKind.PROVIDER_CONTENT_DELTA,
                source_event_key="raw-leak", payload={"delta": "PRIVATE"}))
        second = await core.submit(_request())
        assert (await second.wait()).status.value == "done"
        with pytest.raises(ContractViolationError):
            await adapters.outputs.append_event(replace(draft, run_id=second.run_id, source_event_key="other-run"))
        gate.set()
        await first.wait()
        with pytest.raises(ContractViolationError):
            await adapters.outputs.publish_stream_content_as_commentary(event.output_stream_id)
        with pytest.raises(ContractViolationError):
            await adapters.outputs.append_event(replace(draft, source_event_key="late-new"))
        with pytest.raises(ContractViolationError):
            await core._operations.succeed(event.payload["operationId"])
        assert len([e async for e in first.subscribe() if e.kind is OutputEventKind.PLANNING_PROGRESS]) == 1
    finally:
        gate.set()
        await core.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("limits,script", [
    ({"max_model_invocation_attempts": 1}, [wire(text="准备检查。")]),
    ({"max_provider_output_events": 1}, [wire(text="准备检查。"), wire()]),
    ({"max_provider_output_bytes": 20}, [wire(text="准备检查。"), wire()]),
    ({"max_stream_content_chars": 20}, [wire()]),
    ({"max_stream_chunks": 1}, [wire(text="准备检查。"), wire()]),
    ({"max_run_generation_tokens": 7}, [wire()]),
])
async def test_planning_and_repair_share_run_and_stream_budgets(limits, script):
    core, adapters, gateway = managed([script, script], runtime_limits=RuntimeLimits(**{"max_run_generation_tokens": None, **limits}))
    try:
        handle = await core.submit(_request())
        assert (await handle.wait()).status.value == "failed"
        assert gateway.executions == 0
        assert gateway.planning_calls == 1
        assert gateway.closed == gateway.opened
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_silent_planning_has_an_absolute_timeout_without_invented_progress():
    gate = asyncio.Event()
    core, adapters, gateway = managed([[gate]], runtime_limits=RuntimeLimits(max_run_generation_tokens=None, provider_invocation_timeout_ms=15))
    try:
        handle = await core.submit(_request())
        assert (await handle.wait()).status.value == "failed"
        events = [e async for e in handle.subscribe()]
        assert not any(e.kind is OutputEventKind.PLANNING_PROGRESS for e in events)
        assert any(e.payload.get("errorCode") == "model_invocation_deadline_exceeded" for e in events)
        assert gateway.opened == gateway.closed
    finally:
        gate.set()
        await core.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["persistence", "publisher", "gateway"])
async def test_planning_exceptions_never_execute_or_commit_a_successful_phase(boundary):
    core, adapters, gateway = managed([[wire(text="准备检查。"), wire()]])
    if boundary == "persistence":
        original = adapters.outputs.append_event
        async def fail(draft):
            if draft.kind is OutputEventKind.PLANNING_PROGRESS: raise OSError("PRIVATE disk detail")
            return await original(draft)
        adapters.outputs.append_event = fail
    elif boundary == "publisher":
        original = adapters.publisher.publish_committed
        async def fail(event):
            if event.kind is OutputEventKind.PLANNING_PROGRESS: raise OSError("PRIVATE socket detail")
            return await original(event)
        adapters.publisher.publish_committed = fail
    else:
        gateway.scripts = [[OSError("PRIVATE gateway detail")]]
    try:
        handle = await core.submit(_request())
        assert (await handle.wait()).status.value == "failed"
        public = [e async for e in handle.subscribe()]
        phase = next(e for e in public if e.kind is OutputEventKind.OPERATION_STARTED and e.payload["kind"] == "planning")
        ends = [e for e in public if e.kind is OutputEventKind.OPERATION_FINISHED and e.payload["operationId"] == phase.payload["operationId"]]
        assert len(ends) == 1 and ends[0].payload["status"] == "failed"
        assert gateway.executions == 0 and gateway.opened == gateway.closed
        assert "PRIVATE" not in str(public)
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_output_policy_can_suppress_progress_without_fabricating_a_first_public_time():
    core, adapters, gateway = managed([[wire(text="准备检查。"), wire()]])
    class PolicyFilter:
        async def authorize_provider_chunk(self, spec, chunk): return chunk
        async def authorize_planning_progress(self, spec, progress): return None
    core._output_processor._policy = PolicyFilter()
    try:
        handle = await core.submit(_request())
        assert (await handle.wait()).status.value == "done"
        assert not [e async for e in handle.subscribe() if e.kind is OutputEventKind.PLANNING_PROGRESS]
        events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
        assert next(e for e in events if e.kind is OutputEventKind.MODEL_DIAGNOSTICS).payload["firstPublicProgressMs"] is None
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_adapter_transport_evidence_is_optional_typed_and_distinct_from_repair():
    evidence = ModelTransportDiagnostics(request_sent_at_ms=1000, first_byte_at_ms=1012, http_attempts=2)
    core, adapters, gateway = managed([[ModelStreamActivity("transport", evidence), wire()]])
    gateway.activity_support = "transport"
    try:
        handle = await core.submit(_request())
        await handle.wait()
        events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
        diagnostics = next(e.payload for e in events if e.kind is OutputEventKind.MODEL_DIAGNOSTICS)
        assert diagnostics["sdkHttpAttempts"] == 2 and diagnostics["attempt"] == 0
        assert diagnostics["httpRequestSentAtMs"] == 1000 and diagnostics["httpFirstByteAtMs"] == 1012
        assert diagnostics["firstActivityMs"] is not None
        assert diagnostics["firstPublicProgressMs"] is None
    finally:
        await core.close()
    with pytest.raises(TypeError):
        ModelTransportDiagnostics(headers="secret")


@pytest.mark.asyncio
async def test_dynamic_revision_uses_the_managed_stream_and_keeps_completed_work():
    from purra.api import AgentCore
    from purra.contracts import ToolCallDelta, ToolHandlerResult, ToolPolicy, ToolSchema
    from purra.ports import ToolRegistration
    from purra.tools import InMemoryToolCatalog
    initial = {"needsTodos": True, "title": "Inspect", "todos": [
        {"id": "inspect", "title": "Inspect", "type": "read", "executor": "tool", "expectedTools": ["lookup"]},
        {"id": "analyze", "title": "Analyze", "type": "analyze", "executor": "model", "dependsOn": ["inspect"]},
        {"id": "review", "title": "Review", "type": "review", "executor": "model", "dependsOn": ["analyze"]},
        {"id": "respond", "title": "Respond", "type": "review", "executor": "model", "dependsOn": ["review"]},
    ]}

    class Gateway(StreamGateway):
        async def stream(self, messages, invocation, signal=None):
            if invocation.tools:
                async def chunks():
                    yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=0, id="lookup-call", name="lookup", arguments_fragment="{}"),), finish_reason=ModelFinishReason.TOOL_CALLS)
                return ModelStream(chunks=chunks(), model="portable-model", applied_generation_limit=invocation.output_budget.max_generation_tokens)
            return await super().stream(messages, invocation, signal)

    gateway = Gateway([[wire(text="准备检查证据。"), wire(plan=initial)], [wire(text="准备依据结果组织回答。"), wire()]])
    calls = []
    async def lookup(state, arguments, signal=None):
        calls.append(arguments)
        return ToolHandlerResult(content="evidence", planning_disposition="replan", effect_state="not_started")
    adapters = InMemoryAgentAdapters()
    core = AgentCore(model_gateway=gateway, run_repository=adapters.runs, output_repository=adapters.outputs,
        output_publisher=adapters.publisher, context_provider=_Context(), planner=AgentPlanner(gateway),
        planning_policy=Policy(), runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        tool_catalog=InMemoryToolCatalog((ToolRegistration(
            schema=ToolSchema(name="lookup", description="Inspect", parameters={"type": "object", "properties": {}}),
            handler=lookup, policy=ToolPolicy(mode="read", title="Inspect")),)))
    try:
        handle = await core.submit(replace(_request(), tools_enabled=True, context_window=32_768))
        result = await handle.wait()
        assert result.status.value == "done", result
        events = [e async for e in handle.subscribe()]
        progress = [e for e in events if e.kind is OutputEventKind.PLANNING_PROGRESS]
        assert [e.payload["revision"] for e in progress] == [0, 1]
        assert len({e.payload["operationId"] for e in progress}) == 2
        assert len(calls) == 1 and gateway.planning_calls == 2
    finally:
        await core.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('planning_mode', [PlanningMode.REACTIVE, PlanningMode.PLANNED])
async def test_custom_planner_lifecycle_does_not_fabricate_model_work(planning_mode):
    from purra.api import current_planning_context
    from purra.planner import normalize_work_plan
    class Custom:
        calls = 0
        async def create_plan(self, request, capabilities, signal=None, **kwargs):
            self.calls += 1
            assert current_planning_context().scope.run_id
            return normalize_work_plan({'needsTodos': False}, capabilities)
    planner = Custom()
    gateway = StreamGateway([])
    adapters = InMemoryAgentAdapters()
    core = _core(gateway=gateway, context=_Context(), adapters=adapters,
                 profile=ExecutionProfile(planner=planner, planning_policy=Policy()))
    try:
        handle = await core.submit(replace(_request(), planning_mode=planning_mode))
        assert (await handle.wait()).status.value == 'done'
        events = [e async for e in handle.subscribe()]
        phases = [e for e in events if e.kind is OutputEventKind.OPERATION_STARTED and e.payload['kind'] == 'planning']
        planned = int(planning_mode is PlanningMode.PLANNED)
        assert len(phases) == planner.calls == planned
        assert gateway.planning_calls == 0
        assert not [e for e in events if e.kind is OutputEventKind.PLANNING_PROGRESS]
        assert current_planning_context() is None
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_non_streaming_planner_and_host_validator_fail_before_execution():
    from purra.model_protocol import FeatureSupport
    request = _request()
    request = replace(request, model=replace(request.model, capability_snapshot=replace(
        request.model.capability_snapshot, protocol=replace(request.model.capability_snapshot.protocol, streaming=FeatureSupport.UNAVAILABLE))))
    core, _, gateway = managed([[wire()]])
    try:
        result = await (await core.submit(request)).wait()
        assert result.error == 'model_stream_unavailable'
        assert gateway.opened == 0
    finally:
        await core.close()
    core, adapters, gateway = managed([[wire(text='准备验证。'), wire()], [wire()]])
    core._planner._result_validator = lambda request, result: 'Host semantic constraint'
    try:
        result = await (await core.submit(_request())).wait()
        assert result.status.value == 'failed'
        assert gateway.planning_calls == 2 and gateway.executions == 0
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_active_planning_cannot_extend_its_absolute_deadline():
    core, _, gateway = managed([[wire()]], runtime_limits=RuntimeLimits(
        max_run_generation_tokens=None, provider_invocation_timeout_ms=20))
    original = gateway.stream
    async def stream(messages, invocation, signal=None):
        source = await original(messages, invocation, signal)
        async def active():
            try:
                while True:
                    await asyncio.sleep(.001)
                    yield ModelStreamChunk(reasoning_delta='PRIVATE_ACTIVE')
            finally:
                await source.chunks.aclose()
        return replace(source, chunks=active())
    gateway.stream = stream
    try:
        handle = await core.submit(_request())
        result = await asyncio.wait_for(handle.wait(), timeout=1)
        assert result.status.value == 'failed'
        assert 'deadline' in result.error
        assert gateway.opened == gateway.closed == 1
        assert not [e async for e in handle.subscribe() if e.kind is OutputEventKind.PLANNING_PROGRESS]
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_failed_phase_finish_is_fenced_by_terminal_commit_and_releases_local_scope():
    core, adapters, gateway = managed([[wire(text='准备检查。'), wire()]])
    original = adapters.outputs.append_event
    failed = False
    async def append(draft):
        nonlocal failed
        if draft.kind is OutputEventKind.OPERATION_FINISHED and draft.payload.get('parentOperationId') is None and not failed:
            failed = True
            raise OSError('phase finish storage unavailable')
        return await original(draft)
    adapters.outputs.append_event = append
    try:
        handle = await core.submit(_request())
        assert (await handle.wait()).status.value == 'failed'
        events = [e async for e in handle.subscribe()]
        phase = next(e for e in events if e.kind is OutputEventKind.OPERATION_STARTED and e.payload['kind'] == 'planning')
        ends = [e for e in events if e.kind is OutputEventKind.OPERATION_FINISHED and e.payload['operationId'] == phase.payload['operationId']]
        assert len(ends) == 1 and ends[0].payload['status'] == 'failed'
        assert not core._operations.running_operation_ids
        assert gateway.executions == 0
    finally:
        await core.close()


def test_json_byte_ceiling_applies_across_multiple_records():
    parser = PlanningStreamParser()
    record = wire(text='Intent').rstrip('\n') + (' ' * 250_000) + '\n'
    for _ in range(4):
        assert len(parser.feed(record)) == 1
    with pytest.raises(PlanningStreamError):
        parser.feed(record)


@pytest.mark.asyncio
@pytest.mark.parametrize('case', FIXTURE['managedRuns'], ids=lambda case: case['name'])
async def test_shared_managed_planning_lifecycle(case):
    scripts = [[wire(text=f'Intent {attempt}') if record == 'progress' else wire()
                for record in records] for attempt, records in enumerate(case['attempts'])]
    core, adapters, gateway = managed(scripts)
    try:
        handle = await core.submit(_request())
        result = await handle.wait()
        assert result.status.value == case['status']
        assert gateway.planning_calls == len(case['attempts'])
        events = [e async for e in handle.subscribe()]
        progress = [e for e in events if e.kind is OutputEventKind.PLANNING_PROGRESS]
        assert [e.payload['attempt'] for e in progress] == list(range(len(case['attempts'])))
        assert len({e.payload['operationId'] for e in progress}) == 1
        assert events == [e async for e in handle.subscribe()]
        if case['status'] == 'failed': assert result.error == 'invalid_planning_stream'
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_core_keeps_host_operation_observer_without_losing_canonical_planning():
    from purra.api import AgentCore
    from purra.operations import AgentOperationController
    seen = []
    class Observer:
        async def accept_operation_event(self, event): seen.append(event)
    adapters = InMemoryAgentAdapters()
    gateway = StreamGateway([[wire(text='准备核对。'), wire()]])
    core = AgentCore(model_gateway=gateway, planner=AgentPlanner(gateway), planning_policy=Policy(),
        run_repository=adapters.runs, output_repository=adapters.outputs, output_publisher=adapters.publisher,
        operation_controller=AgentOperationController(Observer()), runtime_limits=RuntimeLimits(max_run_generation_tokens=None))
    try:
        handle = await core.submit(replace(_request(), context_window=32768))
        assert (await handle.wait()).status.value == 'done'
        events = [e async for e in handle.subscribe()]
        persisted_ids = [e.payload['operationId'] for e in events if e.kind in {OutputEventKind.OPERATION_STARTED, OutputEventKind.OPERATION_FINISHED}]
        assert persisted_ids == [e.operation_id for e in seen]
        assert len([e for e in events if e.kind is OutputEventKind.PLANNING_PROGRESS]) == 1
    finally:
        await core.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['reject', 'clarify'])
async def test_host_admission_denial_does_not_succeed_or_install_plan(mode):
    from purra.task_admission import TaskAdmissionDecision
    from test_standalone_agent_conformance import _Planner
    class Admission:
        async def evaluate(self, request, plan, signal=None):
            return TaskAdmissionDecision(mode=mode, reason_code='fixture-denied', message='Needs host input')
    gateway = StreamGateway([])
    adapters = InMemoryAgentAdapters()
    core = _core(gateway=gateway, context=_Context(), adapters=adapters,
        profile=ExecutionProfile(planner=_Planner(), planning_policy=Policy(), task_admission_evaluator=Admission()))
    try:
        handle = await core.submit(_request())
        result = await handle.wait()
        assert result.final_response == 'Needs host input'
        events = [e async for e in handle.subscribe()]
        ends = [e for e in events if e.kind is OutputEventKind.OPERATION_FINISHED]
        assert ends[0].payload['status'] == 'failed'
        assert ends[0].payload['errorCode'] == 'planning_not_admitted'
        assert not (await adapters.runs.get(handle.run_id)).steps
        assert gateway.opened == 0
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_cancel_settles_already_reported_planning_usage_once():
    gate = asyncio.Event()
    core, adapters, _ = managed([[ModelStreamChunk(content_delta=wire(text='准备检查。'),
        usage=ModelTokenUsage(input_tokens=10, generation_tokens=3)), gate, wire()]])
    try:
        handle = await core.submit(_request())
        async for event in handle.subscribe():
            if event.kind is OutputEventKind.PLANNING_PROGRESS: break
        await handle.cancel('test cancellation')
        await handle.wait()
        usages = adapters.runs._state.runs[handle.run_id].model_usage_by_invocation
        assert len(usages) == 1
        assert next(iter(usages.values())).generation_tokens == 3
        await handle.cancel('repeat cancellation')
        assert len(usages) == 1
    finally:
        gate.set()
        await core.close()
