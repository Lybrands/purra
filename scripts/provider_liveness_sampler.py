"""Content-free, opt-in sampling around the public ModelGateway port.

This module is a test/calibration aid. It is not included in the PurrA wheel and
must not influence Provider calls when its sample sink fails.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import json
from pathlib import Path
from threading import Lock
from time import monotonic_ns
from typing import Any
from uuid import uuid4

from purra.contracts import (
    AgentMessage,
    ModelCompletion,
    ModelInvocation,
    ModelStream,
    ModelStreamActivity,
    ModelStreamChunk,
)
from purra.ports import CancellationSignal, ModelGateway


@dataclass(frozen=True, slots=True)
class ProviderLivenessSample:
    """One content-free Provider invocation timing sample."""

    sample_id: str
    mode: str
    activity_evidence: str
    stream_open_ms: int | None
    first_activity_ms: int | None
    first_progress_ms: int | None
    activity_offsets_ms: tuple[int, ...]
    progress_offsets_ms: tuple[int, ...]
    max_activity_gap_ms: int | None
    max_progress_gap_ms: int | None
    chunk_count: int
    content_chars: int
    reasoning_chars: int
    tool_delta_count: int
    tool_argument_chars: int
    usage_observed: bool
    finish_reason: str | None
    total_duration_ms: int
    outcome: str
    error_code: str | None
    cancellation_requested: bool
    iterator_close_returned: bool | None
    schema_version: int = 1
    sampler_version: str = "1"

    def to_mapping(self) -> dict[str, object]:
        return {
            "schemaVersion": self.schema_version,
            "samplerVersion": self.sampler_version,
            "sampleId": self.sample_id,
            "mode": self.mode,
            "activityEvidence": self.activity_evidence,
            "streamOpenMs": self.stream_open_ms,
            "firstActivityMs": self.first_activity_ms,
            "firstProgressMs": self.first_progress_ms,
            "activityOffsetsMs": list(self.activity_offsets_ms),
            "progressOffsetsMs": list(self.progress_offsets_ms),
            "maxActivityGapMs": self.max_activity_gap_ms,
            "maxProgressGapMs": self.max_progress_gap_ms,
            "chunkCount": self.chunk_count,
            "contentChars": self.content_chars,
            "reasoningChars": self.reasoning_chars,
            "toolDeltaCount": self.tool_delta_count,
            "toolArgumentChars": self.tool_argument_chars,
            "usageObserved": self.usage_observed,
            "finishReason": self.finish_reason,
            "totalDurationMs": self.total_duration_ms,
            "outcome": self.outcome,
            "errorCode": self.error_code,
            "cancellationRequested": self.cancellation_requested,
            "iteratorCloseReturned": self.iterator_close_returned,
        }


class JsonlSampleSink:
    """Append validated sample mappings to one explicitly selected file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self._lock = Lock()

    def __call__(self, sample: ProviderLivenessSample) -> None:
        encoded = json.dumps(
            sample.to_mapping(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self.path.open("a", encoding="utf-8") as target:
            target.write(encoded)
            target.write("\n")


class SamplingModelGateway:
    """Observe timing/counts while preserving the wrapped gateway's values."""

    def __init__(
        self,
        gateway: ModelGateway,
        sink: Callable[[ProviderLivenessSample], None],
        *,
        clock_ns: Callable[[], int] = monotonic_ns,
        id_factory: Callable[[], str] = lambda: str(uuid4()),
    ) -> None:
        if not isinstance(gateway, ModelGateway):
            raise TypeError("liveness sampler requires a ModelGateway")
        if not callable(sink):
            raise TypeError("liveness sampler requires a callable sink")
        self._gateway = gateway
        self._sink = sink
        self._clock_ns = clock_ns
        self._id_factory = id_factory
        self._recording_errors: list[Exception] = []

    @property
    def recording_errors(self) -> tuple[Exception, ...]:
        return tuple(self._recording_errors)

    async def stream(
        self,
        messages: Sequence[AgentMessage],
        invocation: ModelInvocation,
        signal: CancellationSignal | None = None,
    ) -> ModelStream:
        state = _SampleState(
            sample_id=self._id_factory(),
            mode="stream",
            started_ns=self._clock_ns(),
            clock_ns=self._clock_ns,
        )
        try:
            stream = await self._gateway.stream(messages, invocation, signal)
        except BaseException as error:
            self._record(state.finish(
                error=error,
                signal=signal,
                iterator_close_returned=None,
            ))
            raise
        state.stream_open_ms = state.elapsed_ms()
        iterator = stream.chunks.__aiter__()

        async def observed_chunks():
            observed_error: BaseException | None = None
            exhausted = False
            close_returned = False
            try:
                while True:
                    try:
                        chunk = await anext(iterator)
                    except StopAsyncIteration:
                        exhausted = True
                        break
                    state.accept(chunk)
                    yield chunk
            except GeneratorExit:
                raise
            except BaseException as error:
                observed_error = error
                raise
            finally:
                close_error: BaseException | None = None
                close = getattr(iterator, "aclose", None)
                try:
                    if callable(close):
                        await close()
                    close_returned = True
                except BaseException as error:
                    close_error = error
                    if observed_error is None:
                        raise
                finally:
                    self._record(state.finish(
                        error=observed_error or close_error,
                        signal=signal,
                        iterator_close_returned=close_returned,
                        exhausted=exhausted,
                    ))

        return ModelStream(
            chunks=observed_chunks(),
            model=stream.model,
            applied_generation_limit=stream.applied_generation_limit,
            metadata=stream.metadata,
            activity_support=stream.activity_support,
        )

    async def complete(
        self,
        messages: Sequence[AgentMessage],
        invocation: ModelInvocation,
        signal: CancellationSignal | None = None,
    ) -> ModelCompletion:
        state = _SampleState(
            sample_id=self._id_factory(),
            mode="complete",
            started_ns=self._clock_ns(),
            clock_ns=self._clock_ns,
        )
        try:
            completion = await self._gateway.complete(messages, invocation, signal)
        except BaseException as error:
            self._record(state.finish(
                error=error,
                signal=signal,
                iterator_close_returned=None,
            ))
            raise
        state.usage_observed = completion.usage is not None
        state.finish_reason = _finish_reason(completion.finish_reason)
        self._record(state.finish(
            error=None,
            signal=signal,
            iterator_close_returned=None,
            exhausted=True,
        ))
        return completion

    def _record(self, sample: ProviderLivenessSample | None) -> None:
        if sample is None:
            return
        try:
            self._sink(sample)
        except Exception as error:  # sampling must not alter Provider behavior
            self._recording_errors.append(error)


class _SampleState:
    def __init__(
        self,
        *,
        sample_id: str,
        mode: str,
        started_ns: int,
        clock_ns: Callable[[], int],
    ) -> None:
        self.sample_id = str(sample_id)
        self.mode = mode
        self.started_ns = int(started_ns)
        self.clock_ns = clock_ns
        self.stream_open_ms: int | None = None
        self.activity_offsets: list[int] = []
        self.progress_offsets: list[int] = []
        self.chunk_count = 0
        self.content_chars = 0
        self.reasoning_chars = 0
        self.tool_delta_count = 0
        self.tool_argument_chars = 0
        self.usage_observed = False
        self.finish_reason: str | None = None
        self._recorded = False
        self.transport_activity_observed = False

    def elapsed_ms(self) -> int:
        return max(0, (int(self.clock_ns()) - self.started_ns) // 1_000_000)

    def accept(self, chunk: ModelStreamChunk | ModelStreamActivity) -> None:
        offset = self.elapsed_ms()
        self.activity_offsets.append(offset)
        if isinstance(chunk, ModelStreamActivity):
            self.transport_activity_observed = True
            return
        self.chunk_count += 1
        self.content_chars += len(chunk.content_delta)
        self.reasoning_chars += len(chunk.reasoning_delta)
        self.tool_delta_count += len(chunk.tool_call_deltas)
        self.tool_argument_chars += sum(
            len(delta.arguments_fragment) for delta in chunk.tool_call_deltas
        )
        self.usage_observed = self.usage_observed or chunk.usage is not None
        if chunk.finish_reason is not None:
            self.finish_reason = _finish_reason(chunk.finish_reason)
        if _has_progress(chunk):
            self.progress_offsets.append(offset)

    def finish(
        self,
        *,
        error: BaseException | None,
        signal: CancellationSignal | None,
        iterator_close_returned: bool | None,
        exhausted: bool = False,
    ) -> ProviderLivenessSample | None:
        if self._recorded:
            return None
        self._recorded = True
        cancellation_requested = bool(signal is not None and signal.is_set())
        error_code = _error_code(error, signal)
        outcome = (
            "failed"
            if error is not None
            else "completed"
            if self.finish_reason is not None or self.mode == "complete"
            else "stream_ended"
            if exhausted
            else "consumer_closed"
        )
        return ProviderLivenessSample(
            sample_id=self.sample_id,
            mode=self.mode,
            activity_evidence=("transport_and_semantic_chunks"
                if self.transport_activity_observed else "semantic_chunks_only"),
            stream_open_ms=self.stream_open_ms,
            first_activity_ms=_first(self.activity_offsets),
            first_progress_ms=_first(self.progress_offsets),
            activity_offsets_ms=tuple(self.activity_offsets),
            progress_offsets_ms=tuple(self.progress_offsets),
            max_activity_gap_ms=_max_gap(self.activity_offsets),
            max_progress_gap_ms=_max_gap(self.progress_offsets),
            chunk_count=self.chunk_count,
            content_chars=self.content_chars,
            reasoning_chars=self.reasoning_chars,
            tool_delta_count=self.tool_delta_count,
            tool_argument_chars=self.tool_argument_chars,
            usage_observed=self.usage_observed,
            finish_reason=self.finish_reason,
            total_duration_ms=self.elapsed_ms(),
            outcome=outcome,
            error_code=error_code,
            cancellation_requested=cancellation_requested,
            iterator_close_returned=iterator_close_returned,
        )


def _has_progress(chunk: ModelStreamChunk) -> bool:
    return bool(
        chunk.content_delta
        or chunk.reasoning_delta
        or chunk.usage is not None
        or chunk.finish_reason is not None
        or any(
            delta.id
            or delta.type
            or delta.name
            or delta.arguments_fragment
            for delta in chunk.tool_call_deltas
        )
    )


def _finish_reason(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value))


def _error_code(
    error: BaseException | None,
    signal: CancellationSignal | None,
) -> str | None:
    if error is None:
        return None
    code = str(getattr(error, "code", "") or "").strip()
    if code:
        return code
    signal_code = str(getattr(signal, "reason_code", "") or "").strip()
    return signal_code or "unclassified_error"


def _first(values: Sequence[int]) -> int | None:
    return values[0] if values else None


def _max_gap(values: Sequence[int]) -> int | None:
    if len(values) < 2:
        return None
    return max(current - previous for previous, current in zip(values, values[1:]))


__all__ = [
    "JsonlSampleSink",
    "ProviderLivenessSample",
    "SamplingModelGateway",
]
