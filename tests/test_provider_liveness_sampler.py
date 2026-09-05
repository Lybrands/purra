from __future__ import annotations

from dataclasses import replace
import json

import pytest

from purra.contracts import (
    AgentMessage,
    ModelCompletion,
    ModelFinishReason,
    ModelInvocation,
    ModelRequest,
    ModelStream,
    ModelStreamActivity,
    ModelStreamActivitySupport,
    ModelStreamChunk,
    ModelTokenUsage,
    ToolCallDelta,
)
from purra.errors import ModelGatewayError
from purra.model_protocol import generic_capability_snapshot
from scripts.provider_liveness_sampler import (
    JsonlSampleSink,
    SamplingModelGateway,
)


class _StepClock:
    def __init__(self, step_ms: int = 10) -> None:
        self.value = 0
        self.step_ns = step_ms * 1_000_000

    def __call__(self) -> int:
        self.value += self.step_ns
        return self.value


def _invocation() -> ModelInvocation:
    return ModelInvocation(request=ModelRequest(
        provider="sample",
        model="sample-model",
        capability_snapshot=replace(
            generic_capability_snapshot(),
            profile_id="sample:model",
            max_generation_tokens=256,
        ),
    ))


class _StreamingGateway:
    def __init__(self) -> None:
        self.closed = False

    async def stream(self, messages, invocation, signal=None):
        del messages, invocation, signal

        async def chunks():
            try:
                yield ModelStreamChunk(
                    reasoning_delta="SECRET_REASONING",
                    tool_call_deltas=(ToolCallDelta(
                        index=0,
                        name="lookup",
                        arguments_fragment='{"secret":"SECRET_ARGUMENT"}',
                    ),),
                )
                yield ModelStreamChunk(
                    content_delta="SECRET_OUTPUT",
                    usage=ModelTokenUsage(input_tokens=3, generation_tokens=2),
                )
                yield ModelStreamChunk(finish_reason=ModelFinishReason.STOP)
            finally:
                self.closed = True

        return ModelStream(chunks=chunks(), model="SECRET_MODEL")

    async def complete(self, messages, invocation, signal=None):
        del messages, invocation, signal
        return ModelCompletion(
            message=AgentMessage(role="assistant", content="SECRET_COMPLETION"),
            model="SECRET_MODEL",
            finish_reason=ModelFinishReason.STOP,
        )


class _FailingGateway(_StreamingGateway):
    async def stream(self, messages, invocation, signal=None):
        del messages, invocation, signal
        raise ModelGatewayError(
            "SECRET_PROVIDER_ERROR",
            code="upstream_stream_interrupted",
            retryable=True,
        )


@pytest.mark.asyncio
async def test_sampler_preserves_transport_activity_without_counting_it_as_progress():
    activity = ModelStreamActivity("transport")
    content = ModelStreamChunk(content_delta="ok", finish_reason=ModelFinishReason.STOP)
    class Gateway(_StreamingGateway):
        async def stream(self, messages, invocation, signal=None):
            async def chunks():
                yield activity
                yield content
            return ModelStream(chunks(), model="local", activity_support=ModelStreamActivitySupport.TRANSPORT)
    samples = []
    sampled = SamplingModelGateway(Gateway(), samples.append, clock_ns=_StepClock())
    stream = await sampled.stream((), _invocation())
    received = [item async for item in stream.chunks]
    assert received[0] is activity and received[1] is content
    assert stream.activity_support is ModelStreamActivitySupport.TRANSPORT
    sample = samples[0]
    assert sample.outcome == "completed"
    assert sample.activity_evidence == "transport_and_semantic_chunks"
    assert sample.chunk_count == 1 and sample.content_chars == 2
    assert len(sample.activity_offsets_ms) == 2 and len(sample.progress_offsets_ms) == 1
    assert sample.first_activity_ms < sample.first_progress_ms


@pytest.mark.asyncio
async def test_stream_sampler_preserves_values_and_records_no_content():
    samples = []
    gateway = _StreamingGateway()
    sampled = SamplingModelGateway(
        gateway,
        samples.append,
        clock_ns=_StepClock(),
        id_factory=lambda: "sample-1",
    )

    stream = await sampled.stream(
        (AgentMessage(role="user", content="SECRET_PROMPT"),),
        _invocation(),
    )
    chunks = [chunk async for chunk in stream.chunks]

    assert [chunk.reasoning_delta for chunk in chunks] == [
        "SECRET_REASONING", "", "",
    ]
    assert gateway.closed is True
    assert sampled.recording_errors == ()
    assert len(samples) == 1
    value = samples[0].to_mapping()
    assert value["activityEvidence"] == "semantic_chunks_only"
    assert value["chunkCount"] == 3
    assert value["contentChars"] == len("SECRET_OUTPUT")
    assert value["reasoningChars"] == len("SECRET_REASONING")
    assert value["toolDeltaCount"] == 1
    assert value["toolArgumentChars"] == len('{"secret":"SECRET_ARGUMENT"}')
    assert value["usageObserved"] is True
    assert value["finishReason"] == "stop"
    assert value["outcome"] == "completed"
    assert value["iteratorCloseReturned"] is True
    encoded = json.dumps(value, ensure_ascii=False)
    for secret in (
        "SECRET_PROMPT",
        "SECRET_REASONING",
        "SECRET_ARGUMENT",
        "SECRET_OUTPUT",
        "SECRET_MODEL",
    ):
        assert secret not in encoded


@pytest.mark.asyncio
async def test_sampler_records_only_stable_error_code():
    samples = []
    sampled = SamplingModelGateway(
        _FailingGateway(),
        samples.append,
        clock_ns=_StepClock(),
        id_factory=lambda: "sample-error",
    )

    with pytest.raises(ModelGatewayError, match="SECRET_PROVIDER_ERROR"):
        await sampled.stream((), _invocation())

    encoded = json.dumps(samples[0].to_mapping())
    assert samples[0].error_code == "upstream_stream_interrupted"
    assert samples[0].outcome == "failed"
    assert "SECRET_PROVIDER_ERROR" not in encoded


@pytest.mark.asyncio
async def test_consumer_close_closes_upstream_and_records_once():
    samples = []
    gateway = _StreamingGateway()
    sampled = SamplingModelGateway(
        gateway,
        samples.append,
        clock_ns=_StepClock(),
        id_factory=lambda: "sample-close",
    )
    stream = await sampled.stream((), _invocation())

    await anext(stream.chunks)
    await stream.chunks.aclose()
    await stream.chunks.aclose()

    assert gateway.closed is True
    assert len(samples) == 1
    assert samples[0].outcome == "consumer_closed"
    assert samples[0].iterator_close_returned is True


@pytest.mark.asyncio
async def test_completion_and_jsonl_sink_remain_content_free(tmp_path):
    output = tmp_path / "samples" / "provider-liveness.jsonl"
    sink = JsonlSampleSink(output)
    sampled = SamplingModelGateway(
        _StreamingGateway(),
        sink,
        clock_ns=_StepClock(),
        id_factory=lambda: "sample-complete",
    )

    completion = await sampled.complete(
        (AgentMessage(role="user", content="SECRET_PROMPT"),),
        _invocation(),
    )

    assert completion.message.content == "SECRET_COMPLETION"
    value = json.loads(output.read_text())
    assert value["sampleId"] == "sample-complete"
    assert value["mode"] == "complete"
    assert value["outcome"] == "completed"
    assert value["finishReason"] == "stop"
    for secret in ("SECRET_PROMPT", "SECRET_COMPLETION", "SECRET_MODEL"):
        assert secret not in output.read_text()


@pytest.mark.asyncio
async def test_sink_failure_does_not_change_provider_result():
    def fail_sink(sample):
        del sample
        raise OSError("sample disk unavailable")

    sampled = SamplingModelGateway(
        _StreamingGateway(),
        fail_sink,
        clock_ns=_StepClock(),
    )
    stream = await sampled.stream((), _invocation())

    chunks = [chunk async for chunk in stream.chunks]

    assert chunks[-1].finish_reason is ModelFinishReason.STOP
    assert len(sampled.recording_errors) == 1
    assert str(sampled.recording_errors[0]) == "sample disk unavailable"
