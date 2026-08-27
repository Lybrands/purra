from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import json

import pytest

from purra.contracts import (
    DomainEffect,
    ModelFinishReason,
    ModelRequest,
    ModelStreamChunk,
    ModelTokenUsage,
    RunCreateParams,
    RuntimeLimits,
)
from purra.api import InMemoryAgentAdapters
from purra.events import AgentEvent
from purra.model_invocation import ModelInvocationReceipt
from purra.model_protocol import generic_capability_snapshot
from purra.json_values import thaw_json_mapping
from purra.operations import OperationKind, OperationStarted
from purra.output import (
    AgentOutputEvent,
    AgentOutputIntent,
    DomainEffectOutput,
    OutputChannel,
    OutputCommitMode,
    OutputEventKind,
    OutputSource,
    OutputStreamSpec,
    OutputVisibility,
    RuntimeOutputEvent,
    ToolOutputEvent,
)


def _processor_type():
    try:
        from purra.output.processor import AgentOutputProcessor
    except ModuleNotFoundError as error:
        pytest.fail(f"canonical output processor is missing: {error}")
    return AgentOutputProcessor


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _limit():
    request = ModelRequest(
        provider="test",
        model="model",
        capability_snapshot=replace(
            generic_capability_snapshot(),
            profile_id="test:model",
            max_call_output_tokens=200,
        ),
    )
    from purra.model_invocation import AgentModelCall

    return AgentModelCall(
        request=request,
        output_intent=AgentOutputIntent.FINAL_PUBLIC,
        commit_mode=OutputCommitMode.LIVE,
    ).output_limit


def _receipt(spec: OutputStreamSpec) -> ModelInvocationReceipt:
    return ModelInvocationReceipt(
        invocation_id=spec.invocation_id,
        output_stream_id=spec.output_stream_id,
        run_id=spec.run_id,
        turn_id=spec.turn_id,
        model="model",
        output_intent=spec.intent,
        commit_mode=spec.commit_mode,
        output_limit=_limit(),
        input_fingerprint="input-fingerprint",
        tool_schema_fingerprint="tool-schema-fingerprint",
    )


def _spec(
    *,
    intent: AgentOutputIntent = AgentOutputIntent.FINAL_PUBLIC,
    commit_mode: OutputCommitMode = OutputCommitMode.LIVE,
) -> OutputStreamSpec:
    return OutputStreamSpec(
        output_stream_id="output-1",
        run_id="run-1",
        turn_id="turn-1",
        invocation_id="invocation-1",
        intent=intent,
        commit_mode=commit_mode,
    )


class _Repository:
    def __init__(self):
        self.specs = {}
        self.events = []
        self.promoted = ()
        self.fail_next = False

    async def open_stream(self, spec):
        self.specs[spec.output_stream_id] = spec
        return spec

    async def append_event(self, draft):
        if self.fail_next:
            self.fail_next = False
            raise OSError("disk full")
        event = AgentOutputEvent(
            event_id=f"event-{len(self.events) + 1}",
            output_stream_id=draft.output_stream_id,
            run_id=draft.run_id,
            turn_id=draft.turn_id,
            invocation_id=draft.invocation_id,
            sequence=len(self.events) + 1,
            source=draft.source,
            kind=draft.kind,
            channel=draft.channel,
            visibility=draft.visibility,
            payload=draft.payload,
            occurred_at=draft.occurred_at,
            emitted_at=_now(),
        )
        self.events.append(event)
        return event

    async def append_batch(self, drafts):
        events = []
        for draft in drafts:
            events.append(await self.append_event(draft))
        return tuple(events)

    async def commit_stream(self, output_stream_id, finish_reason):
        del output_stream_id, finish_reason
        return self.events[-1]

    async def abort_stream(self, output_stream_id, error_code):
        del output_stream_id, error_code
        return self.events[-1]

    async def publish_stream_content_as_commentary(self, output_stream_id):
        del output_stream_id
        return self.promoted


class _Publisher:
    def __init__(self):
        self.published = []

    async def publish_committed(self, event):
        self.published.append(event)


class _Recovery:
    def __init__(self):
        self.codes = []

    async def notify_output_failure(self, run_id, code):
        self.codes.append((run_id, code))


class _ControlledClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.waiters = []

    async def sleep(self, delay: float) -> None:
        future = asyncio.get_running_loop().create_future()
        waiter = (self.now + delay, future)
        self.waiters.append(waiter)
        try:
            await future
        finally:
            if waiter in self.waiters:
                self.waiters.remove(waiter)

    async def advance(self, delay: float) -> None:
        await asyncio.sleep(0)
        self.now += delay
        for target, future in tuple(self.waiters):
            if target <= self.now and not future.done():
                future.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)


async def _opened_processor(spec: OutputStreamSpec):
    from purra.output.processor import OutputBatchLimits

    repository = _Repository()
    publisher = _Publisher()
    recovery = _Recovery()
    processor = _processor_type()(
        repository,
        publisher,
        recovery_observer=recovery,
        batch_limits=OutputBatchLimits(max_fragments=1),
    )
    await processor.open_model_stream(_receipt(spec), spec)
    return processor, repository, publisher, recovery


@pytest.mark.asyncio
async def test_live_chunk_is_persisted_then_published_as_versioned_batch():
    processor, repository, publisher, _recovery = await _opened_processor(_spec())

    events = await processor.accept_provider_chunk(
        "output-1",
        ModelStreamChunk(content_delta="甲乙"),
    )

    assert [
        event.payload["entries"][0]["payload"]["delta"] for event in events
    ] == ["甲乙"]
    opened, content = repository.events
    assert opened.kind is OutputEventKind.STREAM_OPENED
    assert opened.visibility is OutputVisibility.PRIVATE
    assert opened.payload["inputFingerprint"] == "input-fingerprint"
    assert publisher.published == [content]
    assert content.source is OutputSource.PROVIDER
    assert content.channel is OutputChannel.FINAL


@pytest.mark.asyncio
async def test_execution_public_provider_chunk_uses_commentary_channel():
    processor, _repository, publisher, _recovery = await _opened_processor(
        _spec(intent=AgentOutputIntent.EXECUTION_PUBLIC)
    )

    await processor.accept_provider_chunk(
        "output-1",
        ModelStreamChunk(content_delta="我会先核对当前正文"),
    )

    assert publisher.published[-1].channel is OutputChannel.COMMENTARY
    assert (
        publisher.published[-1].payload["entries"][0]["payload"]["delta"]
        == "我会先核对当前正文"
    )


@pytest.mark.asyncio
async def test_promoted_commentary_is_published_after_repository_commit():
    processor, repository, publisher, _recovery = await _opened_processor(
        _spec(
            intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
        )
    )
    await processor.accept_provider_chunk(
        "output-1",
        ModelStreamChunk(content_delta="我先核对当前正文"),
    )
    promoted = replace(
        repository.events[-1],
        channel=OutputChannel.COMMENTARY,
        visibility=OutputVisibility.PUBLIC,
    )
    repository.promoted = (promoted,)

    result = await processor.publish_model_stream_commentary("output-1")

    assert result == (promoted,)
    assert publisher.published == [promoted]


@pytest.mark.asyncio
async def test_gated_and_private_chunks_never_publish():
    processor, repository, publisher, _recovery = await _opened_processor(
        _spec(
            intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.GATED,
        )
    )

    await processor.accept_provider_chunk(
        "output-1",
        ModelStreamChunk(content_delta="候选"),
    )

    assert repository.events[-1].visibility is OutputVisibility.PRIVATE
    assert publisher.published == []


@pytest.mark.asyncio
async def test_non_provider_events_cannot_create_public_text():
    repository = _Repository()
    publisher = _Publisher()
    processor = _processor_type()(repository, publisher)
    await processor.accept_operation_event(
        OperationStarted(
            operation_id="operation-1",
            run_id="run-1",
            invocation_id=None,
            kind=OperationKind.TOOL,
            started_at=_now(),
        )
    )
    await processor.accept_tool_event(
        ToolOutputEvent(
            operation_id="operation-1",
            run_id="run-1",
            invocation_id=None,
            tool_call_id="call-1",
            tool_name="read",
            status="succeeded",
            occurred_at=_now(),
        )
    )
    await processor.accept_domain_effect_event(
        DomainEffectOutput(
            effect_id="effect-1",
            run_id="run-1",
            effect=DomainEffect(type="artifact.finalized"),
            occurred_at=_now(),
        )
    )

    public_text = [
        event
        for event in repository.events
        if event.visibility is OutputVisibility.PUBLIC
        and event.channel in {OutputChannel.COMMENTARY, OutputChannel.FINAL}
    ]
    assert public_text == []


@pytest.mark.asyncio
async def test_persistence_failure_never_creates_ghost_public_event():
    processor, repository, publisher, recovery = await _opened_processor(_spec())
    repository.fail_next = True

    with pytest.raises(Exception) as captured:
        await processor.accept_provider_chunk(
            "output-1",
            ModelStreamChunk(content_delta="不可见"),
        )

    assert type(captured.value).__name__ == "OutputPersistenceError"
    assert captured.value.code == "output_persistence_failed"
    assert publisher.published == []
    assert recovery.codes == [("run-1", "output_persistence_failed")]


@pytest.mark.asyncio
async def test_reasoning_and_usage_are_diagnostic_not_public_text():
    processor, repository, publisher, _recovery = await _opened_processor(_spec())

    await processor.accept_provider_chunk(
        "output-1",
        ModelStreamChunk(
            reasoning_delta="private reasoning",
            finish_reason=ModelFinishReason.STOP,
        ),
    )

    assert repository.events[-1].kind is OutputEventKind.PROVIDER_DELTA_BATCH
    assert (
        repository.events[-1].payload["entries"][0]["kind"]
        == OutputEventKind.PROVIDER_REASONING_DELTA.value
    )
    assert repository.events[-1].visibility is OutputVisibility.DIAGNOSTIC
    assert publisher.published == []


@pytest.mark.asyncio
async def test_ten_thousand_one_character_chunks_coalesce_deterministically():
    from purra.output.processor import AgentOutputProcessor, OutputBatchLimits

    repository = _Repository()
    publisher = _Publisher()
    processor = AgentOutputProcessor(
        repository,
        publisher,
        batch_limits=OutputBatchLimits(
            max_payload_bytes=1_000_000,
            max_fragments=64,
            max_latency_ms=60_000,
        ),
    )
    spec = _spec()
    await processor.open_model_stream(_receipt(spec), spec)
    for index in range(10_000):
        await processor.accept_provider_chunk(
            spec.output_stream_id,
            ModelStreamChunk(
                content_delta="x",
                usage=(
                    ModelTokenUsage(input_tokens=0, output_tokens=0)
                    if index == 9_999
                    else None
                ),
            ),
        )
    batches = [
        event for event in repository.events
        if event.kind is OutputEventKind.PROVIDER_DELTA_BATCH
    ]
    assert len(batches) == 157
    assert "".join(
        entry["payload"]["delta"]
        for event in batches
        for entry in event.payload["entries"]
    ) == "x" * 10_000


@pytest.mark.asyncio
async def test_latency_flush_uses_the_injected_clock_and_leaves_no_pending_timer():
    from purra.output.processor import AgentOutputProcessor, OutputBatchLimits

    gate = asyncio.Event()
    flushed = asyncio.Event()
    observed_delays = []

    async def controlled_sleep(delay: float) -> None:
        observed_delays.append(delay)
        await gate.wait()

    class TimedRepository(_Repository):
        async def append_batch(self, drafts):
            result = await super().append_batch(drafts)
            flushed.set()
            return result

    repository = TimedRepository()
    processor = AgentOutputProcessor(
        repository,
        _Publisher(),
        batch_limits=OutputBatchLimits(
            max_payload_bytes=1_000_000,
            max_fragments=100,
            max_latency_ms=7,
        ),
        sleep=controlled_sleep,
    )
    spec = _spec()
    await processor.open_model_stream(_receipt(spec), spec)
    assert await processor.accept_provider_chunk(
        spec.output_stream_id,
        ModelStreamChunk(content_delta="delayed"),
    ) == ()
    await asyncio.sleep(0)
    assert observed_delays == [0.007]
    assert repository.events[-1].kind is OutputEventKind.STREAM_OPENED

    gate.set()
    await asyncio.wait_for(flushed.wait(), timeout=1)
    assert repository.events[-1].kind is OutputEventKind.PROVIDER_DELTA_BATCH


@pytest.mark.asyncio
async def test_private_stream_uses_the_background_latency_ceiling():
    from purra.output.processor import AgentOutputProcessor, OutputBatchLimits

    gate = asyncio.Event()
    observed_delays = []

    async def controlled_sleep(delay: float) -> None:
        observed_delays.append(delay)
        await gate.wait()

    repository = _Repository()
    processor = AgentOutputProcessor(
        repository,
        _Publisher(),
        batch_limits=OutputBatchLimits(
            max_payload_bytes=1_000_000,
            max_fragments=100,
            max_latency_ms=7,
            max_background_latency_ms=70,
        ),
        sleep=controlled_sleep,
    )
    spec = _spec(
        intent=AgentOutputIntent.STRUCTURED_PRIVATE,
        commit_mode=OutputCommitMode.PRIVATE,
    )
    await processor.open_model_stream(_receipt(spec), spec)
    await processor.accept_provider_chunk(
        spec.output_stream_id,
        ModelStreamChunk(reasoning_delta="delayed"),
    )
    await asyncio.sleep(0)

    assert observed_delays == [0.07]
    gate.set()
    await processor.finish_model_stream(
        spec.output_stream_id,
        ModelFinishReason.STOP,
    )


@pytest.mark.asyncio
async def test_size_threshold_preempts_public_and_background_timers():
    from purra.output.processor import AgentOutputProcessor, OutputBatchLimits

    for spec in (
        _spec(),
        _spec(
            intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
        ),
    ):
        observed_delays = []

        async def controlled_sleep(delay: float) -> None:
            observed_delays.append(delay)

        processor = AgentOutputProcessor(
            _Repository(),
            _Publisher(),
            batch_limits=OutputBatchLimits(
                max_payload_bytes=1_000_000,
                max_fragments=1,
                max_latency_ms=7,
                max_background_latency_ms=70,
            ),
            sleep=controlled_sleep,
        )
        await processor.open_model_stream(_receipt(spec), spec)
        events = await processor.accept_provider_chunk(
            spec.output_stream_id,
            ModelStreamChunk(content_delta="immediate"),
        )
        await asyncio.sleep(0)

        assert len(events) == 1
        assert observed_delays == []


@pytest.mark.asyncio
async def test_timer_and_terminal_flush_append_each_source_range_once():
    from purra.output.processor import AgentOutputProcessor, OutputBatchLimits

    gate = asyncio.Event()

    async def controlled_sleep(_delay: float) -> None:
        await gate.wait()

    repository = _Repository()
    processor = AgentOutputProcessor(
        repository,
        _Publisher(),
        batch_limits=OutputBatchLimits(
            max_payload_bytes=1_000_000,
            max_fragments=100,
            max_latency_ms=7,
            max_background_latency_ms=70,
        ),
        sleep=controlled_sleep,
    )
    spec = _spec(
        intent=AgentOutputIntent.STRUCTURED_PRIVATE,
        commit_mode=OutputCommitMode.PRIVATE,
    )
    await processor.open_model_stream(_receipt(spec), spec)
    await processor.accept_provider_chunk(
        spec.output_stream_id,
        ModelStreamChunk(reasoning_delta="race"),
    )
    await asyncio.sleep(0)

    gate.set()
    await processor.finish_model_stream(
        spec.output_stream_id,
        ModelFinishReason.STOP,
    )
    await asyncio.sleep(0)

    batches = [
        event for event in repository.events
        if event.kind is OutputEventKind.PROVIDER_DELTA_BATCH
    ]
    assert len(batches) == 1
    assert batches[0].payload["sourceChunkStart"] == 1
    assert batches[0].payload["sourceChunkEnd"] == 1


@pytest.mark.asyncio
async def test_abort_flush_cancels_the_background_timer():
    from purra.output.processor import AgentOutputProcessor, OutputBatchLimits

    started = asyncio.Event()
    canceled = asyncio.Event()

    async def controlled_sleep(_delay: float) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            canceled.set()

    repository = _Repository()
    processor = AgentOutputProcessor(
        repository,
        _Publisher(),
        batch_limits=OutputBatchLimits(
            max_payload_bytes=1_000_000,
            max_fragments=100,
            max_latency_ms=7,
            max_background_latency_ms=70,
        ),
        sleep=controlled_sleep,
    )
    spec = _spec(
        intent=AgentOutputIntent.REASONING_PRIVATE,
        commit_mode=OutputCommitMode.PRIVATE,
    )
    await processor.open_model_stream(_receipt(spec), spec)
    await processor.accept_provider_chunk(
        spec.output_stream_id,
        ModelStreamChunk(reasoning_delta="pending"),
    )
    await asyncio.wait_for(started.wait(), timeout=1)

    await processor.abort_model_stream(spec.output_stream_id, "request_canceled")
    await asyncio.wait_for(canceled.wait(), timeout=1)

    assert sum(
        event.kind is OutputEventKind.PROVIDER_DELTA_BATCH
        for event in repository.events
    ) == 1


async def _append_fragmented_reasoning_corpus(processor, run_id, clock):
    fragments = tuple(
        "rrr" if index < 3_395 else "rr"
        for index in range(6_921)
    )
    offset = 0
    for attempt in range(5):
        count = 1_385 if attempt == 0 else 1_384
        spec = replace(
            _spec(
                intent=AgentOutputIntent.REASONING_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
            ),
            output_stream_id=f"output-{attempt}",
            invocation_id=f"invocation-{attempt}",
            run_id=run_id,
            turn_id=None,
        )
        await processor.open_model_stream(_receipt(spec), spec)
        for fragment in fragments[offset:offset + count]:
            await processor.accept_provider_chunk(
                spec.output_stream_id,
                ModelStreamChunk(reasoning_delta=fragment),
            )
            await clock.advance(0.005)
        offset += count
        await processor.finish_model_stream(
            spec.output_stream_id,
            ModelFinishReason.STOP,
        )


def _batch_payload_bytes(batches) -> int:
    return sum(
        len(json.dumps(
            thaw_json_mapping(event.payload),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"))
        for event in batches
    )


async def _fragmented_reasoning_corpus(background_latency_ms: int):
    from purra.output.processor import AgentOutputProcessor, OutputBatchLimits

    repository = _Repository()
    clock = _ControlledClock()
    processor = AgentOutputProcessor(
        repository,
        _Publisher(),
        batch_limits=OutputBatchLimits(
            max_payload_bytes=16_384,
            max_fragments=64,
            max_latency_ms=25,
            max_background_latency_ms=background_latency_ms,
        ),
        sleep=clock.sleep,
    )
    await _append_fragmented_reasoning_corpus(processor, "run-1", clock)

    batches = tuple(
        event for event in repository.events
        if event.kind is OutputEventKind.PROVIDER_DELTA_BATCH
    )
    reconstructed = "".join(
        entry["payload"]["delta"]
        for event in batches
        for entry in event.payload["entries"]
    )
    payload_bytes = _batch_payload_bytes(batches)
    return len(batches), payload_bytes, reconstructed


@pytest.mark.asyncio
async def test_incident_corpus_background_batching_reduces_canonical_overhead():
    baseline = await _fragmented_reasoning_corpus(25)
    repaired = await _fragmented_reasoning_corpus(250)
    print(
        "incident corpus: "
        f"25ms={baseline[0]} batches/{baseline[1]} bytes; "
        f"250ms={repaired[0]} batches/{repaired[1]} bytes"
    )

    assert len(repaired[2]) == 17_237
    assert repaired[2] == baseline[2]
    assert repaired[0] * 4 < baseline[0]
    assert repaired[1] < 1_000_000


@pytest.mark.asyncio
async def test_eight_mib_candidate_clears_the_incident_corpus_with_two_x_headroom():
    from purra.output.processor import AgentOutputProcessor

    candidate = 8 * 1024 * 1024
    adapters = InMemoryAgentAdapters()
    begun = await adapters.runs.begin(
        RunCreateParams(
            session_id=None,
            prompt="calibrate Provider output",
            mode=None,
            runtime_limits=RuntimeLimits(max_run_output_tokens=None, max_provider_output_bytes=candidate),
        ),
        AgentEvent(type="run.started"),
    )
    clock = _ControlledClock()
    processor = AgentOutputProcessor(
        adapters.outputs,
        adapters.publisher,
        sleep=clock.sleep,
    )

    await _append_fragmented_reasoning_corpus(processor, begun.run_id, clock)
    events = await adapters.outputs.list_events(
        begun.run_id,
        after_sequence=0,
        limit=500,
    )
    batches = tuple(
        event for event in events
        if event.kind is OutputEventKind.PROVIDER_DELTA_BATCH
    )
    payload_bytes = _batch_payload_bytes(batches)
    reconstructed = "".join(
        entry["payload"]["delta"]
        for event in batches
        for entry in event.payload["entries"]
    )
    print(
        f"8 MiB candidate: {len(batches)} batches/{payload_bytes} bytes/"
        f"{candidate / payload_bytes:.2f}x headroom"
    )

    assert len(batches) == 140
    assert len(reconstructed) == 17_237
    assert candidate >= 2 * max(payload_bytes, 990_883)


@pytest.mark.asyncio
async def test_eight_mib_candidate_rejects_oversize_before_provider_append():
    from purra.output.processor import AgentOutputProcessor

    candidate = 8 * 1024 * 1024
    adapters = InMemoryAgentAdapters()
    begun = await adapters.runs.begin(
        RunCreateParams(
            session_id=None,
            prompt="reject oversized Provider output",
            mode=None,
            runtime_limits=RuntimeLimits(max_run_output_tokens=None, max_provider_output_bytes=candidate),
        ),
        AgentEvent(type="run.started"),
    )
    processor = AgentOutputProcessor(adapters.outputs, adapters.publisher)
    spec = replace(
        _spec(),
        output_stream_id="oversized-output",
        invocation_id="oversized-invocation",
        run_id=begun.run_id,
        turn_id=None,
    )
    await processor.open_model_stream(_receipt(spec), spec)

    with pytest.raises(Exception) as exceeded:
        await processor.accept_provider_chunk(
            spec.output_stream_id,
            ModelStreamChunk(content_delta="x" * candidate),
        )

    events = await adapters.outputs.list_events(
        begun.run_id,
        after_sequence=0,
        limit=20,
    )
    assert getattr(exceeded.value, "code", None) == "runtime_budget_exceeded"
    assert not any(
        event.kind is OutputEventKind.PROVIDER_DELTA_BATCH
        for event in events
    )


@pytest.mark.asyncio
async def test_private_protocol_plan_steps_never_enter_public_journal():
    repository = _Repository()
    publisher = _Publisher()
    processor = _processor_type()(repository, publisher)

    await processor.accept_runtime_event(RuntimeOutputEvent(
        event_id="runtime-plan",
        run_id="run-1",
        event_type="run.todos_updated",
        payload={
            "title": "生成场景表",
            "steps": [
                {
                    "id": "private-protocol",
                    "title": "追加内部批次",
                    "protocol_private": True,
                },
                {
                    "id": "public-capability",
                    "title": "生成完整场景表",
                    "protocol_private": False,
                },
            ],
        },
        occurred_at=_now(),
    ))

    assert repository.events[-1].payload["data"]["steps"] == (
        {
            "id": "public-capability",
            "title": "生成完整场景表",
            "protocol_private": False,
        },
    )

    hidden = await processor.accept_runtime_event(RuntimeOutputEvent(
        event_id="runtime-private-step",
        run_id="run-1",
        event_type="run.todo_updated",
        payload={
            "step_id": "private-protocol",
            "step": {"id": "private-protocol", "protocol_private": True},
        },
        occurred_at=_now(),
    ))

    assert hidden is None
    assert len(repository.events) == 1


@pytest.mark.asyncio
async def test_durable_progress_is_persisted_privately_with_turn_association():
    repository = _Repository()
    publisher = _Publisher()
    processor = _processor_type()(repository, publisher)

    event = await processor.accept_runtime_event(RuntimeOutputEvent(
        event_id="runtime-long-task-progress",
        run_id="run-parent",
        turn_id="turn-product",
        event_type="long_task.progress",
        payload={
            "taskId": "task-1",
            "completedUnits": 1,
            "units": [{
                "id": "recipe:validate",
                "title": "校验候选稿",
                "plannerStepId": "deliver",
            }],
        },
        occurred_at=_now(),
    ))

    assert event is not None
    assert event.turn_id == "turn-product"
    assert event.visibility is OutputVisibility.PRIVATE
    assert event.channel is OutputChannel.DIAGNOSTIC
    assert event.payload["data"]["units"][0]["id"] == "recipe:validate"
    assert publisher.published == []


@pytest.mark.asyncio
async def test_private_runtime_protocol_events_never_enter_canonical_journal():
    repository = _Repository()
    publisher = _Publisher()
    processor = _processor_type()(repository, publisher)

    for index, event_type in enumerate((
        "model.call_recorded",
        "tool.calls_started",
        "tool.results",
        "delegation.claimed",
    )):
        hidden = await processor.accept_runtime_event(RuntimeOutputEvent(
            event_id=f"private-{index}",
            run_id="run-1",
            event_type=event_type,
            payload={"content": "private protocol payload"},
            occurred_at=_now(),
        ))
        assert hidden is None

    assert repository.events == []
    assert publisher.published == []
