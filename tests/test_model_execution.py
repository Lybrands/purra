from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from purra.contracts import (
    AgentMessage,
    MessageRole,
    ModelCompletion,
    ModelFinishReason,
    ModelRequest,
    ModelStream,
    ModelStreamChunk,
    ReasoningMode,
)
from purra.errors import ModelGatewayError, UnsupportedModelFeatureError
from purra.model_execution import (
    AgentModelTask,
    AgentModelTaskRunner,
)
from purra.model_invocation import AgentModelInvocationManager, ModelInvocationContext
from purra.model_protocol import generic_capability_snapshot


def _call() -> AgentModelTask:
    return AgentModelTask(
        request=ModelRequest(
            provider="test",
            model="model",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="test:model",
                max_output_tokens=200,
            ),
        ),
        reasoning_mode=ReasoningMode.DISABLED,
    )


def _model_tasks(gateway) -> AgentModelTaskRunner:
    return AgentModelTaskRunner(
        AgentModelInvocationManager(gateway),
        ModelInvocationContext(run_id="model-task-test-run"),
    )


class _Gateway:
    def __init__(self, *, finish_reason=ModelFinishReason.STOP):
        self.finish_reason = finish_reason
        self.invocations = []

    async def complete(self, messages, invocation, signal=None):
        del messages, signal
        self.invocations.append(invocation)
        return ModelCompletion(
            message=AgentMessage(role="assistant", content="done"),
            model="model",
            finish_reason=self.finish_reason,
        )

    async def stream(self, messages, invocation, signal=None):
        del messages, signal
        self.invocations.append(invocation)

        async def chunks():
            yield ModelStreamChunk(content_delta="done")
            yield ModelStreamChunk(finish_reason=self.finish_reason)

        return ModelStream(chunks=chunks(), model="model")


class _ScriptedStreamGateway(_Gateway):
    def __init__(self, rounds):
        super().__init__()
        self.rounds = list(rounds)
        self.message_rounds = []

    async def stream(self, messages, invocation, signal=None):
        del signal
        self.message_rounds.append(tuple(messages))
        self.invocations.append(invocation)
        round_chunks = self.rounds.pop(0)

        async def chunks():
            for chunk in round_chunks:
                yield chunk

        return ModelStream(chunks=chunks(), model="model")


def test_complete_resolves_the_exact_provider_output_limit():
    async def run():
        gateway = _Gateway()
        result = await _model_tasks(gateway).complete((), _call())
        assert result.completion.message.content == "done"
        assert result.output_limit.max_tokens == 200
        assert gateway.invocations[0].output_limit == result.output_limit
        assert gateway.invocations[0].max_output_tokens == 200

    asyncio.run(run())


def test_reasoning_incompatibility_is_not_replayed_with_another_mode():
    class Gateway(_Gateway):
        async def complete(self, messages, invocation, signal=None):
            if not self.invocations:
                self.invocations.append(invocation)
                raise UnsupportedModelFeatureError()
            return await super().complete(messages, invocation, signal)

    async def run():
        gateway = Gateway()
        with pytest.raises(UnsupportedModelFeatureError):
            await _model_tasks(gateway).complete((), _call())
        assert [item.reasoning_mode for item in gateway.invocations] == [
            ReasoningMode.DISABLED,
        ]

    asyncio.run(run())


@pytest.mark.parametrize(
    ("reason", "code"),
    [
        (ModelFinishReason.LENGTH, "model_output_truncated"),
        (None, "upstream_stream_interrupted"),
    ],
)
def test_complete_rejects_non_terminal_or_incomplete_output_without_retry(
    reason,
    code,
):
    async def run():
        gateway = _Gateway(finish_reason=reason)
        with pytest.raises(ModelGatewayError) as captured:
            await _model_tasks(gateway).complete((), _call())
        assert captured.value.code == code
        assert len(gateway.invocations) == 1

    asyncio.run(run())


def test_stream_rejects_truncation_after_preserving_diagnostic_chunks():
    async def run():
        gateway = _Gateway(finish_reason=ModelFinishReason.LENGTH)
        stream = await _model_tasks(gateway).stream((), _call())
        observed = []
        with pytest.raises(ModelGatewayError) as captured:
            async for chunk in stream.chunks:
                observed.append(chunk)
        assert captured.value.code == "model_output_truncated"
        assert "".join(chunk.content_delta for chunk in observed) == "done"
        assert len(gateway.invocations) == 1

    asyncio.run(run())


def test_stream_text_retries_reasoning_only_without_changing_mode():
    async def run():
        gateway = _ScriptedStreamGateway([
            [
                ModelStreamChunk(reasoning_delta='{"answer":"hidden"}'),
                ModelStreamChunk(finish_reason=ModelFinishReason.STOP),
            ],
            [
                ModelStreamChunk(content_delta='{"answer":"visible"}'),
                ModelStreamChunk(finish_reason=ModelFinishReason.STOP),
            ],
        ])
        observed = []

        async def observe(chunk):
            observed.append(chunk)

        result = await _model_tasks(gateway).stream_text(
            (AgentMessage(role=MessageRole.USER, content="return JSON"),),
            _call(),
            on_chunk=observe,
        )

        assert result.content == '{"answer":"visible"}'
        assert result.reasoning == ""
        assert result.attempts == 2
        assert len(observed) == 4
        assert [item.reasoning_mode for item in gateway.invocations] == [
            ReasoningMode.DISABLED,
            ReasoningMode.DISABLED,
        ]
        replay = gateway.message_rounds[1]
        assert replay[-2].role is MessageRole.ASSISTANT
        assert replay[-2].reasoning == '{"answer":"hidden"}'
        assert replay[-1].role is MessageRole.DEVELOPER
        assert "reasoning alone" in str(replay[-1].content)

    asyncio.run(run())


@pytest.mark.parametrize("reasoning", ["private analysis", ""])
def test_stream_text_rejects_repeated_empty_official_responses(reasoning):
    async def run():
        empty_round = [
            *(
                [ModelStreamChunk(reasoning_delta=reasoning)]
                if reasoning
                else []
            ),
            ModelStreamChunk(finish_reason=ModelFinishReason.STOP),
        ]
        gateway = _ScriptedStreamGateway([
            empty_round,
            empty_round,
            empty_round,
        ])

        with pytest.raises(ModelGatewayError) as captured:
            await _model_tasks(gateway).stream_text((), _call())

        assert captured.value.code == "empty_model_response"
        assert len(gateway.invocations) == 3

    asyncio.run(run())


def test_stream_text_never_retries_length_termination():
    async def run():
        gateway = _ScriptedStreamGateway([[
            ModelStreamChunk(reasoning_delta="unfinished"),
            ModelStreamChunk(finish_reason=ModelFinishReason.LENGTH),
        ]])

        with pytest.raises(ModelGatewayError) as captured:
            await _model_tasks(gateway).stream_text((), _call())

        assert captured.value.code == "model_output_truncated"
        assert len(gateway.invocations) == 1

    asyncio.run(run())
